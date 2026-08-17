// Package minecraft is the worked example: a relay consumer built against a
// real protocol rather than a toy one.
//
// It implements three of the four seams — Framer, Codec, and Prober — over a
// protocol that was not designed with this framework in mind, which is the
// only way to find out whether the seams are real.
//
// It deliberately does not stand between an encrypted login. Doing that as a
// third party means running two key exchanges and holding the client's session
// credentials, which is a project in itself and teaches nothing about the
// framework. Decoding stops at that point and the session continues as opaque
// passthrough; see Codec.
//
// Passthrough is the whole of what happens after the key exchange, and it takes
// both seams to do it. The codec stops decoding, and the framer stops framing:
// an enciphered stream carries no length prefix a third party can read, so the
// boundaries a proxy would frame on are not there to be found. See
// Framer.ReadMessage for what it does instead, and encryption.go for how the
// two seams agree on when.
package minecraft

import (
	"fmt"
	"io"
	"sync/atomic"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
)

// opaqueChunkBytes bounds one opaque read. It is a socket read's worth rather
// than a frame's worth, because that is what an opaque message is: whatever
// arrived, with no structure claiming otherwise. It is a ceiling and not a
// size — see Framer.opaqueFrom, which allocates what has actually arrived.
const opaqueChunkBytes = 32 << 10

// Framer adapts the Java edition frame envelope to relay.Framer.
//
// A relay message is one frame payload: the length prefix is framing and
// belongs here, and everything inside it is the codec's problem. The
// interesting content is in the protocol library's wire package, not in this
// file — this is an adapter and nothing more.
//
// It is per session and per direction rather than shared, because two of the
// three fields below are one connection's state. A framer that gives up on
// framing has given up on it for one session, and the scratch reader it uses to
// decide that is single-threaded only as long as one pump owns it.
type Framer struct {
	inner  protocol.Framer
	limits protocol.Limits
	link   *link

	// framing is what the most recent ReadMessage returned: a frame payload, or
	// an opaque chunk. WriteMessage follows it rather than the link, because the
	// message that completes the key exchange is read framed and written after
	// the latch is already set — see WriteMessage.
	//
	// It is atomic rather than plain because Session.Inject writes from whatever
	// goroutine called it, while the pump reads.
	framing atomic.Bool

	// pending replays the byte ReadMessage consumes before it can know whether
	// this message is still a frame. One pump owns a framer, and ReadMessage is
	// not re-entrant, so one scratch reader per framer is enough.
	pending prefixReader
}

// NewFramer builds a framer bound to limits, which is what bounds the length a
// frame prefix may claim.
//
// The relay session may be nil, for a framer used outside a relay — a stub
// endpoint, or a tool reading frames out of a file. A framer with no session
// frames every message, because nothing can tell it not to.
func NewFramer(session *relay.Session, limits protocol.Limits) (*Framer, error) {
	inner, err := java.NewFramer(limits)
	if err != nil {
		return nil, fmt.Errorf("minecraft: build the framer: %w", err)
	}

	f := &Framer{inner: inner, limits: limits, link: linkFor(session)}
	f.framing.Store(true)

	return f, nil
}

// ReadMessage implements relay.Framer.
//
// The payload is copied because Frame.Payload returns a borrowed view into the
// frame's own buffer, and relay.Framer promises the caller a slice the framer
// will not reuse.
//
// Removing the copy does not currently fail any test, and that is worth saying
// rather than leaving for someone to rediscover: the underlying ReadFrame
// allocates a fresh buffer per frame, so two live payloads never alias and no
// black-box test can tell a copy from a view into new memory. The copy is here
// for the documented contract — Payload is borrowed and must not be retained —
// not for an observed bug, and it stops being free the day that allocation
// becomes a pool.
//
// The error is returned unwrapped so io.EOF survives: the relay reads a clean
// peer close as io.EOF and anything else as a fault, and the underlying framer
// already draws that line in the right place — an EOF before the first byte of
// a length prefix is a clean close, and an EOF anywhere after it is a truncated
// frame reported as io.ErrUnexpectedEOF.
//
// The first byte is read here rather than inside the inner framer, and the
// latch is checked after it rather than only before, because of where a pump
// stands when the key exchange completes. The clientbound pump is parked on the
// first byte of a frame that has not arrived; the exchange completes on the
// other pump; the next byte to arrive is ciphertext. Checking only on entry
// would let that pump read a length out of it — which is the failure this
// costs one byte of bookkeeping to avoid, and it is not a small one: a length
// read out of ciphertext is a number, so the pump waits for a megabyte that
// was never sent and the session stops without an error to point at.
func (f *Framer) ReadMessage(r relay.Reader) ([]byte, error) {
	if f.link.encrypted.Load() {
		return f.opaque(r)
	}

	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if f.link.encrypted.Load() {
		return f.opaqueFrom(first, r)
	}

	f.pending.reset(first, r)

	frame, err := f.inner.ReadFrame(&f.pending)
	if err != nil {
		return nil, err
	}

	f.framing.Store(true)

	payload := frame.Payload()

	return append([]byte(nil), payload...), nil
}

// opaque hands up bytes that are no longer messages.
//
// What it returns is a socket read, not a frame, and nothing downstream should
// read it as one: Encrypted is how a sink or a hook asks. The relay does not
// need to be told, because it forwards whatever a framer returns and its codec
// has already stopped decoding.
func (f *Framer) opaque(r relay.Reader) ([]byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	return f.opaqueFrom(first, r)
}

// opaqueFrom builds one opaque message from a byte already in hand and whatever
// else has already arrived behind it.
//
// It blocks for the first byte and never for the rest, which is the shape this
// whole path exists for: waiting for bytes that were never sent is the failure
// being fixed, and a pump holding one byte while it waits for more is that
// failure in miniature. It is also what sizes the allocation to the traffic —
// one read's worth, whatever that turned out to be, rather than a buffer picked
// in advance and mostly wasted.
//
// A reader that cannot say how much it is holding gets one byte per message.
// That is correct and slow, and it does not arise in a relay: the reader is a
// Conduit, and the stub endpoints in the tests read through bufio.
func (f *Framer) opaqueFrom(first byte, r relay.Reader) ([]byte, error) {
	f.framing.Store(false)

	head := []byte{first}

	waiting, ok := r.(interface{ Buffered() int })
	if !ok {
		return head, nil
	}

	pending := min(waiting.Buffered(), opaqueChunkBytes)
	if pending <= 0 {
		return head, nil
	}

	chunk := make([]byte, 1+pending)
	chunk[0] = first

	n, err := io.ReadFull(r, chunk[1:])
	if err != nil && n == 0 {
		// The byte already in hand is still a byte that crossed. Handing it up
		// and letting the next read report the fault keeps the stream whole;
		// returning the error here would drop it.
		return head, nil
	}

	return chunk[:1+n], nil
}

// WriteMessage implements relay.Framer.
//
// It builds the frame rather than writing the payload directly, because the
// length prefix is not this file's to guess. Once this framer has stopped
// framing there is no prefix to build: what it was handed is what crossed the
// wire, and adding a length to it would corrupt the stream it is relaying.
//
// The decision follows the last read rather than the latch, and the difference
// is one message wide. The frame that completes the key exchange is read as a
// frame — it travels in the clear, it is the last one that does — and it is
// written after the codec has already latched. Keying the write off the latch
// would drop that frame's length prefix on the way out, so the upstream would
// never see the exchange complete and would never switch its own cipher.
func (f *Framer) WriteMessage(w io.Writer, raw []byte) error {
	if !f.framing.Load() {
		if _, err := w.Write(raw); err != nil {
			return fmt.Errorf("minecraft: relay opaque bytes: %w", err)
		}

		return nil
	}

	frame, err := f.inner.BuildFrame(raw)
	if err != nil {
		return fmt.Errorf("minecraft: build frame: %w", err)
	}

	if err := f.inner.WriteFrame(w, frame); err != nil {
		return fmt.Errorf("minecraft: write frame: %w", err)
	}

	return nil
}

// prefixReader replays one byte in front of a reader.
//
// It exists so ReadMessage can look at a byte and then hand it back unread. A
// bufio.Reader would do it with UnreadByte, but the reader here is the relay's
// and is only promised to read.
type prefixReader struct {
	rest   io.Reader
	first  byte
	unread bool
}

func (p *prefixReader) reset(first byte, rest io.Reader) {
	p.first, p.rest, p.unread = first, rest, true
}

func (p *prefixReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	if p.unread {
		p.unread = false
		b[0] = p.first

		return 1, nil
	}

	return p.rest.Read(b)
}
