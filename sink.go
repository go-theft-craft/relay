package relay

import (
	"context"
	"time"
)

// SessionInfo describes a session at the moment it opened.
type SessionInfo struct {
	ClientAddr   string
	UpstreamAddr string
	Port         int
	OpenedAt     time.Time
}

// MessageRecord is one message as a sink sees it.
//
// Raw is borrowed for the duration of the call, like everywhere else a message
// crosses an interface. A sink that stores it must copy.
type MessageRecord struct {
	Dir     Direction
	Desc    Descriptor
	Raw     []byte
	Decoded any
	At      time.Time
}

// Sink records what crossed the wire.
//
// Only OpenSession returns an error, and no method may block: batching and
// asynchrony belong to the implementation, which can size its own queue for its
// own storage. A core that owned that goroutine could not tune it for anyone. A
// sink that blocks stalls a session's read pump and, through backpressure, its
// peer.
type Sink interface {
	OpenSession(context.Context, SessionInfo) (int64, error)
	Message(context.Context, int64, MessageRecord)
	// RawChunk receives every byte crossing the client connection, below any
	// framing and below any mid-stream transform, when Config.CaptureRaw is set.
	// It is not called at all when it is not.
	RawChunk(context.Context, int64, Direction, []byte)
	CloseSession(context.Context, int64)
}

// nopSink is what a proxy configured without a sink uses, so the session path
// never branches on nil.
type nopSink struct{}

func (nopSink) OpenSession(context.Context, SessionInfo) (int64, error) { return 0, nil }
func (nopSink) Message(context.Context, int64, MessageRecord)           {}
func (nopSink) RawChunk(context.Context, int64, Direction, []byte)      {}
func (nopSink) CloseSession(context.Context, int64)                     {}
