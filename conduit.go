package relay

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
)

// Transform is a change to how one direction's bytes are encoded, from some
// message boundary onwards. A nil half leaves that side of the stream alone.
//
// It exists because framing is not constant for the life of a connection: real
// protocols negotiate a cipher partway through, and every byte after the agreed
// one is encoded differently against a keystream that does not restart.
//
// Compression is deliberately not this. A negotiated compression threshold
// compresses each message independently inside the frame envelope, so nothing
// carries from one frame to the next and it belongs to the Framer, which
// already owns the envelope. Reaching for a Transform to do it produces
// something that works until the first frame boundary lands mid-buffer.
//
// Read transforms in place, because the conduit owns the buffer it is handed
// and an allocation per read on a proxy holding thousands of sessions is not
// free. Write returns a new slice, because the caller owns the message it
// passed and a hook or a sink may still be holding a view of it.
type Transform struct {
	Read  func([]byte)
	Write func([]byte) []byte
}

// Conduit is one direction's byte layer: the socket, its raw buffer, and the
// transform currently applied to bytes crossing it.
//
// It buffers raw bytes and transforms them as it hands them out, not as it
// buffers them. That ordering is the whole design. Because the buffer holds
// untransformed bytes, a swap never has to reach into it or rebuild anything
// above it; because the lock is never held around a socket read, a swap can
// land while the read pump is parked, which is exactly when a cipher negotiated
// in the other direction needs to be installed here.
//
// A Conduit is safe for concurrent use by one reader, one writer, and any
// number of goroutines calling Swap.
type Conduit struct {
	conn     net.Conn
	buffered *bufio.Reader

	mu    sync.Mutex
	read  func([]byte)
	write func([]byte) []byte
	// pending is how many raw bytes the buffer still holds, recorded by the
	// reader under the mutex.
	//
	// Swap cannot ask the bufio.Reader directly: the pump is normally parked
	// inside a socket read when a swap arrives, and reading bufio state
	// concurrently with that is a data race. The reader publishes the count
	// instead, which is exact for the same reason the swap is safe at all — a
	// parked read has buffered nothing, so the last recorded count still holds.
	pending int
}

// NewConduit wraps a connection. bufSize is the raw read buffer; at ten
// thousand sessions the default is worth roughly 80 MiB, which is what makes it
// a knob rather than a constant.
func NewConduit(conn net.Conn, bufSize int) *Conduit {
	if bufSize <= 0 {
		bufSize = 4096
	}

	return &Conduit{conn: conn, buffered: bufio.NewReaderSize(conn, bufSize)}
}

// PreFrameReader returns the raw buffered reader a PreFrame hook inspects.
//
// The hook runs before any message and therefore before any swap, so reading
// the buffer directly is identical to reading through the conduit. This is the
// one place Peek is available, and the reason Reader does not offer it.
func (c *Conduit) PreFrameReader() *bufio.Reader { return c.buffered }

// Conn returns the underlying connection.
func (c *Conduit) Conn() net.Conn { return c.conn }

// Read implements io.Reader, applying the active read transform to the bytes as
// they are handed out.
func (c *Conduit) Read(p []byte) (int, error) {
	n, err := c.buffered.Read(p)

	// The lock is taken after the read, never around it, so a socket read that
	// blocks forever cannot stop another goroutine from swapping.
	c.mu.Lock()
	if n > 0 && c.read != nil {
		c.read(p[:n])
	}
	c.pending = c.buffered.Buffered()
	c.mu.Unlock()

	return n, err
}

// ReadByte implements io.ByteReader, which together with Read satisfies Reader.
func (c *Conduit) ReadByte() (byte, error) {
	var one [1]byte
	if _, err := io.ReadFull(c, one[:]); err != nil {
		return 0, err
	}

	return one[0], nil
}

// Write implements io.Writer. It never retains p and never mutates it.
//
// The count returned is always in the caller's bytes, never the transformed
// ones, and the write is all-or-nothing. A transform may change length, so
// there is no honest way to express a partial write in terms of p: half a
// transformed block is not half a message, and no caller could resume from it.
// Reporting the transformed count instead would be worse than useless, because
// io.Copy and bufio both believe it.
func (c *Conduit) Write(p []byte) (int, error) {
	c.mu.Lock()
	active := c.write
	c.mu.Unlock()

	out := p
	if active != nil {
		out = active(p)
	}

	if _, err := c.conn.Write(out); err != nil {
		return 0, err
	}

	return len(p), nil
}

// Buffered reports how many raw bytes are waiting to be handed out.
func (c *Conduit) Buffered() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.pending
}

// Swap installs a transform over whatever is already active, so a session that
// layers a second encoding over a first ends up with both, in that order.
//
// It refuses when the read buffer still holds unread bytes *and* the swap
// changes the read side: those bytes arrived before the switch and belong to
// the old encoding, so transforming them on the way out would corrupt the very
// next message with nothing to point at afterwards. Failing here names the
// cause at the cause.
//
// A write-only swap is not refused, because buffered read bytes have nothing to
// do with it. That distinction is what lets a caller arm the two halves at
// different moments, which a proxy standing between two peers needs: the read
// side has to be armed before the peer is told to switch, since the peer may
// answer in the new encoding immediately, while the write side must stay in the
// old encoding until the message that tells it has actually gone out.
//
// In practice the buffer is empty exactly when it should be. Every protocol
// that renegotiates mid-stream requires the peer to stop sending across the
// boundary, because both endpoints have the same problem and solve it the same
// way. A non-empty buffer means the peer broke that rule or the caller swapped
// at the wrong message, and both deserve an error.
//
// A swap takes effect at a message boundary in the stream, not at a byte offset
// agreed with the peer. A proxy that swaps at the same message an endpoint
// would is exactly as correct as that endpoint, and no more.
func (c *Conduit) Swap(t Transform) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if t.Read != nil && c.pending > 0 {
		return fmt.Errorf("%w: %d unread bytes", ErrSwapPending, c.pending)
	}

	// Read transforms compose oldest-first: bytes come off the wire encoded by
	// the outermost layer last applied, so they are undone in the order the
	// layers were added. Write transforms compose in the mirror order, newest
	// first, so a message leaves through the layers in the order the peer will
	// undo them. Getting this backwards produces a stream that works for one
	// layer and breaks the moment a second is added.
	if t.Read != nil {
		prior, next := c.read, t.Read
		if prior == nil {
			c.read = next
		} else {
			c.read = func(p []byte) { prior(p); next(p) }
		}
	}

	if t.Write != nil {
		prior, next := c.write, t.Write
		if prior == nil {
			c.write = next
		} else {
			c.write = func(p []byte) []byte { return next(prior(p)) }
		}
	}

	return nil
}
