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

	ctx context.Context

	// mu orders the two directions against each other. Sink.RawChunk is called
	// under it, which is what makes the recorded interleaving match the wire —
	// and which is why a sink that blocks there stalls both directions at once
	// rather than only the one it was called from. That is the one stall a
	// per-direction pump cannot confine, and it is why the sink installed here
	// is the session's, queue and all, rather than the configured one.
	mu      sync.Mutex
	sink    Sink
	id      int64
	live    bool
	pending []captureChunk
	held    int
	dropped bool
}

// newCaptureConn wraps a connection before there is a session to record it
// against. The sink arrives with activate, which is also when the identifier
// every record is keyed by does.
func newCaptureConn(ctx context.Context, conn net.Conn) *captureConn {
	return &captureConn{Conn: conn, ctx: ctx}
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
func (c *captureConn) activate(sink Sink, id int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sink = sink
	c.id = id
	c.live = true

	for _, chunk := range c.pending {
		c.sink.RawChunk(c.ctx, id, chunk.dir, chunk.data)
	}

	c.pending = nil
	c.held = 0

	return c.dropped
}
