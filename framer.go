package relay

import (
	"context"
	"io"
	"net"
	"time"
)

// Reader is the read side a Framer sees.
//
// It is an interface rather than *bufio.Reader because in a running session the
// read side is a *Conduit, which has to be the outermost buffer so that a
// mid-stream transform swap can tell whether any bytes are still unread. A
// *bufio.Reader satisfies it too, which is what lets relaytest exercise a
// Framer over a plain buffer with no session in sight.
//
// Peek is deliberately not part of it: peeking past a transform means
// transforming bytes without consuming them, and a stream cipher cannot be
// asked to do that. A PreFrame hook, which genuinely needs Peek, runs before
// any swap can have happened and is handed the raw buffered reader instead.
type Reader interface {
	io.Reader
	io.ByteReader
}

// Framer turns a byte stream into messages and back. It is the one interface a
// consumer must implement, and the only thing the core needs in order to proxy
// a protocol it knows nothing else about.
//
// ReadMessage must return exactly one complete message, or an error. io.EOF
// means the peer closed cleanly. The returned slice is handed to hooks and may
// be retained by the relay until the message is written, so an implementation
// must not reuse the buffer it returns.
//
// WriteMessage must write every byte or report an error. A short write that
// reports success desynchronises the stream for good.
type Framer interface {
	ReadMessage(Reader) ([]byte, error)
	WriteMessage(io.Writer, []byte) error
}

// Codec is the optional second half: it makes decoded packets visible to hooks
// and sinks.
//
// Decode returns the descriptor alongside the value because the decoder already
// knows the identity, and recovering it through a second dispatch would be
// waste on the hot path.
//
// A Decode error does not end the session. The message is forwarded as opaque
// bytes with a zero Descriptor, because a proxy that refuses to relay what it
// cannot parse is less useful than the connection it replaced.
type Codec interface {
	Decode(Direction, []byte) (any, Descriptor, error)
	Encode(any) ([]byte, error)
}

// Prober reports whether an upstream is usable. A nil error means healthy.
//
// The default speaks no protocol, so it can only tell that something holds the
// port open. A consumer that implements this properly gets health that means
// the server answered.
type Prober interface {
	Probe(ctx context.Context, addr string) error
}

// DialProber is the default Prober: a TCP dial that is closed immediately.
type DialProber struct {
	Timeout time.Duration
}

// Probe implements Prober.
func (p DialProber) Probe(ctx context.Context, addr string) error {
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	return conn.Close()
}
