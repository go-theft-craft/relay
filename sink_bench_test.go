package relay

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

// This file answers one question, asked by
// docs/2026-08-17-enforce-the-sink-contract.md before anything is built: what
// does one copy per message per sink cost on the relay path?
//
// It matters because MessageRecord.Raw is borrowed for the duration of the
// call. Any design that queues sink calls has to copy every message for every
// sink before it can return, including sinks that never look at Raw — so that
// copy is the floor, and whether it is worth paying by default is a number
// rather than an opinion.

// benchFramer is a length-prefixed framer sized for measurement.
//
// lineFramer, which the tests use, reads a byte at a time and grows a slice as
// it goes. That is fine for a test and wrong here: its allocations would sit in
// the baseline and make anything measured against them look cheap. One
// right-sized allocation per message is the floor a real framer works at, since
// Framer.ReadMessage may not reuse the buffer it returns, so that is what this
// does.
//
// It holds no state because Config.Framer is one instance shared by every
// session and both directions, and a scratch buffer on it would be a race
// rather than an optimisation.
type benchFramer struct{}

func (benchFramer) ReadMessage(r Reader) ([]byte, error) {
	high, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	low, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	raw := make([]byte, int(high)<<8|int(low))
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, err
	}

	return raw, nil
}

func (benchFramer) WriteMessage(w io.Writer, raw []byte) error {
	header := [2]byte{byte(len(raw) >> 8), byte(len(raw))}
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	_, err := w.Write(raw)

	return err
}

// discardSink is the baseline: a sink that looks at nothing, which is what the
// core pays for today.
type discardSink struct{}

func (discardSink) OpenSession(context.Context, SessionInfo) (int64, error) { return 1, nil }
func (discardSink) Message(context.Context, int64, MessageRecord)           {}
func (discardSink) RawChunk(context.Context, int64, Direction, []byte)      {}
func (discardSink) CloseSession(context.Context, int64)                     {}

// copyingSink is the same sink with the one copy a queued design cannot avoid.
//
// The result is kept on the struct so the compiler cannot decide the append was
// pointless and delete the thing being measured.
type copyingSink struct {
	last []byte
}

func (copyingSink) OpenSession(context.Context, SessionInfo) (int64, error) { return 1, nil }

func (s *copyingSink) Message(_ context.Context, _ int64, record MessageRecord) {
	s.last = append([]byte(nil), record.Raw...)
}

func (copyingSink) RawChunk(context.Context, int64, Direction, []byte) {}
func (copyingSink) CloseSession(context.Context, int64)                {}

// nullConn discards writes and delivers no reads.
//
// It stands in for the socket because net.Pipe is synchronous: every message
// would carry two goroutine handoffs the relay path does not own, and a
// difference of a few hundred nanoseconds would be buried under them. A
// discarded write is not what a real socket costs either, but it errs the right
// way — a real write costs more, so the copy's share of a real message is
// smaller than what this measures, not larger.
type nullConn struct{}

func (nullConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (nullConn) Write(p []byte) (int, error)     { return len(p), nil }
func (nullConn) Close() error                    { return nil }
func (nullConn) LocalAddr() net.Addr             { return nullAddr{} }
func (nullConn) RemoteAddr() net.Addr            { return nullAddr{} }
func (nullConn) SetDeadline(time.Time) error     { return nil }
func (nullConn) SetReadDeadline(time.Time) error { return nil }

func (nullConn) SetWriteDeadline(time.Time) error { return nil }

type nullAddr struct{}

func (nullAddr) Network() string { return "null" }
func (nullAddr) String() string  { return "null" }

// benchSession builds a session that is never run. relay is called directly
// instead, because the read pump is not what is being measured and driving one
// would put a pipe back in the middle of it.
func benchSession(b *testing.B, sink Sink) *Session {
	b.Helper()

	cfg := Config{
		Ports:  []PortConfig{{Port: 25565, Upstreams: []Upstream{{Addr: "127.0.0.1:1"}}}},
		Framer: benchFramer{},
		Sink:   sink,
	}
	if err := cfg.validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}

	return newSession(
		context.Background(), &cfg,
		nullConn{}, nullConn{},
		SessionInfo{ClientAddr: "bench", UpstreamAddr: "bench", OpenedAt: time.Now()},
		1,
	)
}

// BenchmarkRelayedMessage measures one message through decode, hooks, the sink
// call, and the write, with and without the copy.
//
// 100 bytes is the size the plan asks about; 1500 is a full ethernet payload,
// which is what a chunk-carrying protocol actually pushes.
func BenchmarkRelayedMessage(b *testing.B) {
	for _, size := range []int{100, 1500} {
		payload := make([]byte, size)

		b.Run(fmt.Sprintf("%dB/borrowed", size), func(b *testing.B) {
			session := benchSession(b, discardSink{})

			b.ReportAllocs()

			for b.Loop() {
				if err := session.relay(ToServer, payload); err != nil {
					b.Fatalf("relay: %v", err)
				}
			}
		})

		b.Run(fmt.Sprintf("%dB/copied", size), func(b *testing.B) {
			sink := &copyingSink{}
			session := benchSession(b, sink)

			b.ReportAllocs()

			for b.Loop() {
				if err := session.relay(ToServer, payload); err != nil {
					b.Fatalf("relay: %v", err)
				}
			}

			runtime.KeepAlive(sink.last)
		})
	}
}

// BenchmarkSinkCopy is the copy on its own, so the relay-path numbers above can
// be read as a share of something rather than as a bare difference.
func BenchmarkSinkCopy(b *testing.B) {
	for _, size := range []int{100, 1500} {
		payload := make([]byte, size)

		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			var sunk []byte

			b.ReportAllocs()

			for b.Loop() {
				sunk = append([]byte(nil), payload...)
			}

			runtime.KeepAlive(sunk)
		})
	}
}
