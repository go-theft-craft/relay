package relay

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingSink keeps everything it was handed so a test can assert on the
// order and the direction of what crossed the wire.
type recordingSink struct {
	mu       sync.Mutex
	opened   []SessionInfo
	messages []MessageRecord
	raw      []rawRecord
	chunks   int
	closed   int
	nextID   int64
}

// rawRecord is one Sink.RawChunk call, kept with the session it belonged to so
// a test can prove chunks are attributable.
type rawRecord struct {
	id   int64
	dir  Direction
	data []byte
}

func (s *recordingSink) OpenSession(_ context.Context, info SessionInfo) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.opened = append(s.opened, info)
	s.nextID++

	return s.nextID, nil
}

func (s *recordingSink) Message(_ context.Context, _ int64, rec MessageRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// The record borrows its bytes, so a sink that stores them has to copy.
	rec.Raw = append([]byte(nil), rec.Raw...)
	s.messages = append(s.messages, rec)
}

func (s *recordingSink) RawChunk(_ context.Context, id int64, dir Direction, chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.chunks++
	// Borrowed like every other buffer crossing this interface.
	s.raw = append(s.raw, rawRecord{id: id, dir: dir, data: append([]byte(nil), chunk...)})
}

func (s *recordingSink) CloseSession(context.Context, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed++
}

func (s *recordingSink) recorded() []MessageRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]MessageRecord(nil), s.messages...)
}

func (s *recordingSink) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// countingCodec treats a message as its own decoded value, which is enough to
// exercise the decode and re-encode paths without a protocol.
type countingCodec struct {
	decodes  atomic.Int64
	encodes  atomic.Int64
	decodeer error
}

func (c *countingCodec) Decode(_ Direction, raw []byte) (any, Descriptor, error) {
	c.decodes.Add(1)
	if c.decodeer != nil {
		return nil, Descriptor{}, c.decodeer
	}

	return string(raw), Descriptor{ID: 42, Name: "line"}, nil
}

func (c *countingCodec) Encode(value any) ([]byte, error) {
	c.encodes.Add(1)

	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("countingCodec: cannot encode %T", value)
	}

	return []byte(s), nil
}

// harness is a running session over two pipes, with the peer ends in hand.
type harness struct {
	session *Session
	client  net.Conn // the test's end of the client connection
	server  net.Conn // the test's end of the upstream connection
	errs    chan error
	done    chan struct{}
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()

	if cfg.Framer == nil {
		cfg.Framer = lineFramer{}
	}
	cfg.Ports = []PortConfig{{Port: 25565, Upstreams: []Upstream{{Addr: "127.0.0.1:1"}}}}

	errs := make(chan error, 16)
	prior := cfg.OnSessionError
	cfg.OnSessionError = func(s *Session, err error) {
		if prior != nil {
			prior(s, err)
		}
		select {
		case errs <- err:
		default:
		}
	}

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	clientPeer, clientConn := net.Pipe()
	upstreamConn, serverPeer := net.Pipe()

	info := SessionInfo{
		ClientAddr:   "client",
		UpstreamAddr: "upstream",
		Port:         25565,
		OpenedAt:     time.Now(),
	}

	sinkID, err := cfg.Sink.OpenSession(context.Background(), info)
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	s := newSession(context.Background(), &cfg, clientConn, upstreamConn, info, sinkID)

	h := &harness{session: s, client: clientPeer, server: serverPeer, errs: errs, done: make(chan struct{})}

	go func() {
		defer close(h.done)
		s.run()
	}()

	t.Cleanup(func() {
		s.Close()
		_ = clientPeer.Close()
		_ = serverPeer.Close()

		select {
		case <-h.done:
		case <-time.After(10 * time.Second):
			t.Error("the session never finished")
		}
	})

	return h
}

// nextError waits for a session fault, so a test never races the reporting
// goroutine.
func (h *harness) nextError(t *testing.T) error {
	t.Helper()

	select {
	case err := <-h.errs:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("no session error was reported")

		return nil
	}
}

func writeLine(t *testing.T, w io.Writer, line string) {
	t.Helper()

	if _, err := w.Write([]byte(line + "\n")); err != nil {
		t.Fatalf("write %q: %v", line, err)
	}
}

func readLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()

	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	return strings.TrimSuffix(line, "\n")
}

func TestSessionRelaysBothDirections(t *testing.T) {
	h := newHarness(t, Config{})

	upstream := bufio.NewReader(h.server)
	client := bufio.NewReader(h.client)

	writeLine(t, h.client, "up")
	if got := readLine(t, upstream); got != "up" {
		t.Fatalf("upstream received %q, want up", got)
	}

	writeLine(t, h.server, "down")
	if got := readLine(t, client); got != "down" {
		t.Fatalf("client received %q, want down", got)
	}
}

func TestSessionDropStopsTheChain(t *testing.T) {
	var second atomic.Bool

	h := newHarness(t, Config{Hooks: []Hook{
		HookFunc(func(context.Context, *Session, *Message) (Action, error) { return Drop, nil }),
		HookFunc(func(context.Context, *Session, *Message) (Action, error) {
			second.Store(true)

			return Forward, nil
		}),
	}})

	writeLine(t, h.client, "dropped")
	writeLine(t, h.client, "also-dropped")

	// Nothing should arrive; give the pump time to have relayed it if it were
	// going to, then prove the connection is still live by relaying downward.
	_ = h.server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := bufio.NewReader(h.server).ReadString('\n'); err == nil {
		t.Fatal("a dropped message reached the upstream")
	}
	_ = h.server.SetReadDeadline(time.Time{})

	if second.Load() {
		t.Fatal("a hook after the one that returned Drop still ran")
	}
}

func TestSessionReplaceContinuesTheChain(t *testing.T) {
	var seen atomic.Value

	h := newHarness(t, Config{Hooks: []Hook{
		HookFunc(func(_ context.Context, _ *Session, m *Message) (Action, error) {
			m.SetRaw([]byte("edited"))

			return Replace, nil
		}),
		HookFunc(func(_ context.Context, _ *Session, m *Message) (Action, error) {
			seen.Store(string(m.Raw))

			return Forward, nil
		}),
	}})

	writeLine(t, h.client, "original")

	if got := readLine(t, bufio.NewReader(h.server)); got != "edited" {
		t.Fatalf("upstream received %q, want edited", got)
	}
	if got, _ := seen.Load().(string); got != "edited" {
		t.Fatalf("the second hook saw %q, want edited", got)
	}
}

func TestSessionReEncodesOnce(t *testing.T) {
	codec := &countingCodec{}

	h := newHarness(t, Config{
		Codec: codec,
		Hooks: []Hook{
			HookFunc(func(_ context.Context, _ *Session, m *Message) (Action, error) {
				m.SetDecoded("rewritten")

				return Replace, nil
			}),
			HookFunc(func(context.Context, *Session, *Message) (Action, error) { return Forward, nil }),
			HookFunc(func(context.Context, *Session, *Message) (Action, error) { return Forward, nil }),
		},
	})

	writeLine(t, h.client, "original")

	if got := readLine(t, bufio.NewReader(h.server)); got != "rewritten" {
		t.Fatalf("upstream received %q, want rewritten", got)
	}
	if n := codec.encodes.Load(); n != 1 {
		t.Fatalf("Encode ran %d times, want exactly 1 — re-encoding belongs after the whole chain", n)
	}
}

func TestSessionSurvivesADecodeError(t *testing.T) {
	codec := &countingCodec{decodeer: errors.New("undecodable")}

	var desc Descriptor
	h := newHarness(t, Config{
		Codec: codec,
		Hooks: []Hook{HookFunc(func(_ context.Context, _ *Session, m *Message) (Action, error) {
			desc = m.Desc

			return Forward, nil
		})},
	})

	upstream := bufio.NewReader(h.server)

	writeLine(t, h.client, "one")
	if got := readLine(t, upstream); got != "one" {
		t.Fatalf("upstream received %q, want the opaque bytes back", got)
	}
	if desc != (Descriptor{}) {
		t.Fatalf("Desc = %+v, want the zero descriptor after a decode error", desc)
	}

	// The session is still open, which is the whole point.
	writeLine(t, h.client, "two")
	if got := readLine(t, upstream); got != "two" {
		t.Fatalf("upstream received %q after a decode error, want two", got)
	}
}

func TestSessionEndsOnAHookError(t *testing.T) {
	h := newHarness(t, Config{Hooks: []Hook{
		HookFunc(func(context.Context, *Session, *Message) (Action, error) {
			return Forward, errors.New("hook said no")
		}),
	}})

	writeLine(t, h.client, "boom")

	if err := h.nextError(t); !errors.Is(err, ErrHook) {
		t.Fatalf("reported error %v, want one wrapping ErrHook", err)
	}

	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the session stayed open after a hook error")
	}
}

func TestSessionPanicEndsOnlyThatSession(t *testing.T) {
	panicking := newHarness(t, Config{Hooks: []Hook{
		HookFunc(func(context.Context, *Session, *Message) (Action, error) {
			panic("hook exploded")
		}),
	}})
	healthy := newHarness(t, Config{})

	writeLine(t, panicking.client, "boom")

	err := panicking.nextError(t)
	if !errors.Is(err, ErrHook) {
		t.Fatalf("reported error %v, want one wrapping ErrHook", err)
	}
	if !strings.Contains(err.Error(), "hook exploded") {
		t.Fatalf("the report lost the panic value: %v", err)
	}
	if !strings.Contains(err.Error(), "relay.(*Session).callHook") {
		t.Fatalf("the report lost the stack: %v", err)
	}

	// The unrelated session keeps relaying, which is the reason the recovery
	// exists at all.
	writeLine(t, healthy.client, "still here")
	if got := readLine(t, bufio.NewReader(healthy.server)); got != "still here" {
		t.Fatalf("the healthy session relayed %q, want still here", got)
	}
}

// oversizeFramer hands back more bytes than it was given, which is how a test
// reaches the limit without writing megabytes into a pipe.
type oversizeFramer struct {
	lineFramer
	size int
}

func (f oversizeFramer) ReadMessage(r Reader) ([]byte, error) {
	if _, err := f.lineFramer.ReadMessage(r); err != nil {
		return nil, err
	}

	return make([]byte, f.size), nil
}

func TestSessionEnforcesMaxMessageSize(t *testing.T) {
	h := newHarness(t, Config{
		Framer:         oversizeFramer{size: 64},
		MaxMessageSize: 8,
	})

	writeLine(t, h.client, "trigger")

	if err := h.nextError(t); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("reported error %v, want ErrMessageTooLarge", err)
	}
}

func TestSessionTreatsEOFAsACleanClose(t *testing.T) {
	h := newHarness(t, Config{})

	_ = h.client.Close()

	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the session stayed open after the client closed")
	}

	select {
	case err := <-h.errs:
		t.Fatalf("a clean close was reported as a fault: %v", err)
	default:
	}
}

func TestSessionRecordsToTheSink(t *testing.T) {
	sink := &recordingSink{}
	h := newHarness(t, Config{Sink: sink})

	writeLine(t, h.client, "up")
	_ = readLine(t, bufio.NewReader(h.server))
	writeLine(t, h.server, "down")
	_ = readLine(t, bufio.NewReader(h.client))

	if len(sink.opened) != 1 {
		t.Fatalf("OpenSession ran %d times, want 1", len(sink.opened))
	}

	records := sink.recorded()
	if len(records) != 2 {
		t.Fatalf("recorded %d messages, want 2", len(records))
	}
	if records[0].Dir != ToServer || string(records[0].Raw) != "up" {
		t.Fatalf("first record = %v %q, want to_server up", records[0].Dir, records[0].Raw)
	}
	if records[1].Dir != ToClient || string(records[1].Raw) != "down" {
		t.Fatalf("second record = %v %q, want to_client down", records[1].Dir, records[1].Raw)
	}

	h.session.Close()
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the session never finished")
	}

	if got := sink.closeCount(); got != 1 {
		t.Fatalf("CloseSession ran %d times, want exactly 1", got)
	}
}

func TestSessionMetadataSnapshotIsACopy(t *testing.T) {
	ready := make(chan struct{})

	h := newHarness(t, Config{Hooks: []Hook{
		HookFunc(func(_ context.Context, s *Session, _ *Message) (Action, error) {
			s.Set("player", "alice")
			close(ready)

			return Forward, nil
		}),
	}})

	writeLine(t, h.client, "hello")
	<-ready

	snap := h.session.Snapshot()
	if snap.Meta["player"] != "alice" {
		t.Fatalf("snapshot metadata = %v, want alice", snap.Meta["player"])
	}
	if snap.ID != h.session.ID || snap.Info.ClientAddr != "client" {
		t.Fatalf("snapshot identity = %+v, want the session's own", snap)
	}

	snap.Meta["player"] = "mallory"

	if value, _ := h.session.Get("player"); value != "alice" {
		t.Fatalf("mutating a snapshot changed the session: %v", value)
	}
}

// TestSessionCrossDirectionSwap is the arrangement every real consumer will
// use: a hook running on one pump swaps the connection the *other* pump is
// parked on, mid-read.
//
// Only the read half is swapped, which models an upstream that starts speaking
// the new encoding while the proxy still writes to it in the clear. Swapping
// both halves here would encipher the trigger on its way out, and the peer,
// still reading plaintext, could not have read it.
func TestSessionCrossDirectionSwap(t *testing.T) {
	swapped := make(chan error, 1)

	h := newHarness(t, Config{Hooks: []Hook{
		HookFunc(func(_ context.Context, s *Session, m *Message) (Action, error) {
			if string(m.Raw) != "START" {
				return Forward, nil
			}

			swapped <- s.Swap(ToServer, Transform{Read: flip})

			return Forward, nil
		}),
	}})

	upstream := bufio.NewReader(h.server)

	writeLine(t, h.client, "START")
	if got := readLine(t, upstream); got != "START" {
		t.Fatalf("the trigger arrived as %q, want START", got)
	}
	if err := <-swapped; err != nil {
		t.Fatalf("cross-direction Swap: %v", err)
	}

	// The upstream now speaks the transformed encoding on its own stream.
	if _, err := h.server.Write(flipped([]byte("secret\n"))); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := readLine(t, bufio.NewReader(h.client)); got != "secret" {
		t.Fatalf("client received %q, want secret", got)
	}
}
