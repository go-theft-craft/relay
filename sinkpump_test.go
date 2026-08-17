package relay

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// parkingSink is a sink that breaks the no-blocking rule on purpose: it enters
// one method and stays there until the test lets it out.
//
// It is the whole point of these tests. Sink has always said no method may
// block, and nothing checked — so what a consumer actually gets from that rule
// is whatever the core does when a sink ignores it, and that is a behaviour
// worth pinning rather than a promise worth repeating.
type parkingSink struct {
	// where names the method that parks. The others return immediately, and
	// CloseSession in particular must, or every teardown would wait out the
	// drain grace for no reason.
	where sinkCallKind

	park    chan struct{}
	entered atomic.Int64
	closed  atomic.Int64
}

func newParkingSink(where sinkCallKind) *parkingSink {
	return &parkingSink{where: where, park: make(chan struct{})}
}

// release lets everything parked in the sink out. Tests register it as a
// cleanup after the harness they use, so it runs before the harness tears down:
// a session whose sink is still parked cannot finish, and the failure would be a
// timeout naming the wrong thing.
func (s *parkingSink) release() {
	select {
	case <-s.park:
	default:
		close(s.park)
	}
}

func (s *parkingSink) OpenSession(context.Context, SessionInfo) (int64, error) { return 7, nil }

func (s *parkingSink) Message(context.Context, int64, MessageRecord) {
	s.maybePark(sinkCallMessage)
}

func (s *parkingSink) RawChunk(context.Context, int64, Direction, []byte) {
	s.maybePark(sinkCallRawChunk)
}

func (s *parkingSink) CloseSession(context.Context, int64) { s.closed.Add(1) }

func (s *parkingSink) maybePark(where sinkCallKind) {
	if s.where != where {
		return
	}

	s.entered.Add(1)
	<-s.park
}

// shortGrace keeps a teardown that has to wait out a wedged sink from spending
// the default five seconds doing it.
const shortGrace = 50 * time.Millisecond

// TestSinkOverflowBlockStallsTheReadPump documents the default as a choice
// rather than an accident.
//
// Under SinkOverflowBlock the sink is called on the read pump, so a sink that
// parks holds the session: the message never reaches the upstream. That is the
// behaviour the contract's comment describes and the behaviour this
// repository's own capture sink produced for three releases. It is the default
// because enforcing it costs a copy and an allocation per message per sink,
// measured in docs/2026-08-17-enforce-the-sink-contract.md.
func TestSinkOverflowBlockStallsTheReadPump(t *testing.T) {
	sink := newParkingSink(sinkCallMessage)

	h := newHarness(t, Config{Sink: sink})
	t.Cleanup(sink.release)

	writeLine(t, h.client, "one")

	waitFor(t, func() bool { return sink.entered.Load() == 1 })

	// relay records before it forwards, so a sink parked in Message is holding
	// the message short of the upstream.
	if err := h.server.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	upstream := bufio.NewReader(h.server)
	if _, err := upstream.ReadString('\n'); err == nil {
		t.Fatal("the upstream got the message while the sink was parked; Block no longer blocks")
	}

	// And it is the sink holding it, not something else: releasing delivers it.
	sink.release()

	if err := h.server.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	if got := readLine(t, upstream); got != "one" {
		t.Fatalf("the upstream received %q after the sink was released, want one", got)
	}
}

// TestSinkOverflowDropDoesNotStallTheReadPump is the same session with the
// policy changed, and it is what the policy is for: the record waits, the
// session does not.
func TestSinkOverflowDropDoesNotStallTheReadPump(t *testing.T) {
	sink := newParkingSink(sinkCallMessage)

	h := newHarness(t, Config{
		Sink:           sink,
		SinkOverflow:   SinkOverflowDrop,
		SinkQueueDepth: 4,
		DrainGrace:     shortGrace,
	})
	t.Cleanup(sink.release)

	writeLine(t, h.client, "one")

	if got := readLine(t, bufio.NewReader(h.server)); got != "one" {
		t.Fatalf("the upstream received %q, want one", got)
	}

	// The message arrived because the read pump handed it to a queue, not
	// because the sink was quick: it is still parked in the call it was given.
	waitFor(t, func() bool { return sink.entered.Load() == 1 })
}

// TestSinkOverflowEndSessionReportsErrSinkOverflow covers the policy that
// motivated the whole exercise. A recorder that cannot keep up would rather stop
// than write a file with a hole in the middle of it, and it can only make that
// choice if the reason reaches the consumer.
func TestSinkOverflowEndSessionReportsErrSinkOverflow(t *testing.T) {
	sink := newParkingSink(sinkCallMessage)

	h := newHarness(t, Config{
		Sink:           sink,
		SinkOverflow:   SinkOverflowEndSession,
		SinkQueueDepth: 1,
		DrainGrace:     shortGrace,
	})
	t.Cleanup(sink.release)

	// The upstream has to be drained or the session stalls on the pipe rather
	// than on the queue, and the test would be measuring the wrong stall.
	go func() { _, _ = io.Copy(io.Discard, h.server) }()

	// One call is parked in the sink, one record fits the queue, and the next
	// has nowhere to go.
	for range 16 {
		if _, err := h.client.Write([]byte("flood\n")); err != nil {
			break
		}
	}

	if err := h.nextError(t); !errors.Is(err, ErrSinkOverflow) {
		t.Fatalf("session error = %v, want ErrSinkOverflow", err)
	}

	waitFor(t, func() bool { return h.session.Context().Err() != nil })
}

// TestSinkOverflowDropCountsWhatItLost is the other half of that policy. A drop
// nobody can count is not a policy, it is a silence.
func TestSinkOverflowDropCountsWhatItLost(t *testing.T) {
	sink := newParkingSink(sinkCallMessage)

	h := newHarness(t, Config{
		Sink:           sink,
		SinkOverflow:   SinkOverflowDrop,
		SinkQueueDepth: 1,
		DrainGrace:     shortGrace,
	})
	t.Cleanup(sink.release)

	go func() { _, _ = io.Copy(io.Discard, h.server) }()

	for range 16 {
		if _, err := h.client.Write([]byte("flood\n")); err != nil {
			break
		}
	}

	waitFor(t, func() bool { return h.session.SinkDropped() > 0 })

	// Dropping is not ending: the session is still relaying, which is the
	// difference between this policy and the one above.
	if err := h.session.Context().Err(); err != nil {
		t.Fatalf("the session ended with %v; Drop drops records, not sessions", err)
	}
}

// TestSinkDroppedIsZeroWithoutAQueue pins the reading of the counter under the
// default, where there is nothing to drop and no queue to drop it from.
func TestSinkDroppedIsZeroWithoutAQueue(t *testing.T) {
	h := newHarness(t, Config{Sink: &recordingSink{}})

	if got := h.session.SinkDropped(); got != 0 {
		t.Fatalf("SinkDropped() = %d under SinkOverflowBlock, want 0", got)
	}
}

// TestSinkOverflowDropUncouplesTheDirectionsUnderRawCapture is the case a
// per-direction queue would not have fixed.
//
// captureConn holds one mutex across Sink.RawChunk so the recorded interleaving
// matches the wire, which means a sink parked there is holding both directions,
// not the one it was called from. It is the one stall that is not confined to a
// single read pump, and it is why the sink the capture is handed is the
// session's — queue and all — rather than the configured one.
func TestSinkOverflowDropUncouplesTheDirectionsUnderRawCapture(t *testing.T) {
	up := newEchoUpstream(t)
	sink := newParkingSink(sinkCallRawChunk)

	p, _, _ := runProxy(t, Config{
		Ports:          []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:           sink,
		CaptureRaw:     true,
		SinkOverflow:   SinkOverflowDrop,
		SinkQueueDepth: 8,
		DrainGrace:     shortGrace,
	})
	t.Cleanup(sink.release)

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")

	// Parked on the client's own bytes, which are travelling the other way from
	// the echo this then waits for.
	waitFor(t, func() bool { return sink.entered.Load() == 1 })

	if got := readLine(t, bufio.NewReader(conn)); got != "hello" {
		t.Fatalf("read %q back, want the echo delivered while the sink was parked on a raw chunk", got)
	}
}
