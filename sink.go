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
//
// Under a queueing Config.SinkOverflow the core has already copied, so the
// bytes a sink is handed are its own and copying again buys nothing. That is
// the one obligation that differs between policies, and it differs in the safe
// direction: a sink written for the borrowed case is correct under both, which
// is why the sentence above is still the rule.
type MessageRecord struct {
	Dir     Direction
	Desc    Descriptor
	Raw     []byte
	Decoded any
	At      time.Time
}

// Sink records what crossed the wire.
//
// Only OpenSession returns an error, and no method may block. What happens when
// one does is Config.SinkOverflow's to decide, and the choice is the consumer's
// because both answers are right for somebody:
//
//   - SinkOverflowBlock, the default, calls the sink inline on the read pump. A
//     sink that blocks stalls the session and, through backpressure, its peer.
//     The rule is documented and unenforced, which is what it has always been.
//   - SinkOverflowDrop and SinkOverflowEndSession put a bounded queue and a
//     goroutine in front of the sink, per session. A sink that blocks then costs
//     records or costs the session, and never costs throughput.
//
// The default is Block because enforcement is not free: a queued call must
// outlive its arguments, so every message is copied for every sink, including
// sinks that never read Raw. The measurement is in
// docs/2026-08-17-enforce-the-sink-contract.md.
//
// What a sink still owns under every policy is batching. It knows its own
// storage and can size a batch for it; a core that batched would be guessing on
// its behalf, and that is why the queue here is a queue and not a buffer.
//
// OpenSession is called inline under every policy. It returns an error, and it
// assigns the identifier every later call is keyed by, so there is nothing
// useful to do with it asynchronously.
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
