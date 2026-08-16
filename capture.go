package relay

import (
	"context"
	"net"
	"sync"
)

// capturePendingLimit bounds what a capture holds before its session has a sink
// identifier.
//
// The window is small — accept, an optional pre-frame hook, an upstream dial —
// and the only bytes crossing it are whatever the hook read. A limit is here
// anyway because "small in practice" is not a bound, and an unbounded buffer on
// the accept path is how a proxy holding thousands of connections runs out of
// memory.
const capturePendingLimit = 1 << 20

// captureChunk is one recorded run of bytes, kept with its direction so a
// replay sees them in the order they crossed.
type captureChunk struct {
	dir  Direction
	data []byte
}

// captureConn records every byte crossing a connection, below any framing and
// below any mid-stream transform.
//
// It wraps the client connection rather than both, because what a capture is
// for is replaying the session a client had: the upstream link carries the same
// messages, possibly under a different encoding, and recording both would
// double the storage to say the same thing twice.
//
// Bytes are recorded after the socket call rather than before, so what is
// stored is what actually moved — a short write records what was written, not
// what was offered.
type captureConn struct {
	net.Conn

	sink Sink
	ctx  context.Context

	// mu orders the two directions against each other. Sink.RawChunk is called
	// under it, which is safe because the Sink contract forbids blocking, and it
	// is what makes the recorded interleaving match the wire.
	mu      sync.Mutex
	id      int64
	live    bool
	pending []captureChunk
	held    int
	dropped bool
}

func newCaptureConn(ctx context.Context, conn net.Conn, sink Sink) *captureConn {
	return &captureConn{Conn: conn, sink: sink, ctx: ctx}
}

// Read records what arrived from the client, which is traffic bound for the
// upstream.
func (c *captureConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.record(ToServer, p[:n])
	}

	return n, err
}

// Write records what was sent to the client.
func (c *captureConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.record(ToClient, p[:n])
	}

	return n, err
}

// record hands one run of bytes to the sink, or holds it until there is a
// session to attach it to.
func (c *captureConn) record(dir Direction, b []byte) {
	// The caller owns p and reuses it, so the copy happens before anything else.
	chunk := append([]byte(nil), b...)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.live {
		c.sink.RawChunk(c.ctx, c.id, dir, chunk)

		return
	}

	if c.held+len(chunk) > capturePendingLimit {
		// Dropping is the right failure for a recorder, and the drop is reported
		// once the session exists rather than swallowed.
		c.dropped = true

		return
	}

	c.pending = append(c.pending, captureChunk{dir: dir, data: chunk})
	c.held += len(chunk)
}

// activate attaches the capture to a sink session and flushes whatever was held
// while there was nothing to attach it to. It reports whether anything was
// dropped before it ran.
func (c *captureConn) activate(id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.id = id
	c.live = true

	for _, chunk := range c.pending {
		c.sink.RawChunk(c.ctx, id, chunk.dir, chunk.data)
	}

	c.pending = nil
	c.held = 0

	return c.dropped
}
