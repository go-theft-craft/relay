package relay

import (
	"bufio"
	"bytes"
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

// echoUpstream is a stub server that reflects every line it is sent. It is the
// far end of every end-to-end case here.
type echoUpstream struct {
	ln net.Listener

	mu       sync.Mutex
	accepted int
}

func newEchoUpstream(t *testing.T) *echoUpstream {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	u := &echoUpstream{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			u.mu.Lock()
			u.accepted++
			u.mu.Unlock()

			go func() {
				defer func() { _ = conn.Close() }()

				br := bufio.NewReader(conn)
				for {
					line, err := br.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := conn.Write([]byte(line)); err != nil {
						return
					}
				}
			}()
		}
	}()

	return u
}

func (u *echoUpstream) addr() string { return u.ln.Addr().String() }

func (u *echoUpstream) connections() int {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.accepted
}

// scriptedUpstream reads one newline-terminated request and answers with a
// fixed reply, which lets a test give the two directions different framing.
type scriptedUpstream struct {
	ln net.Listener
}

func newScriptedUpstream(t *testing.T, _, reply string) *scriptedUpstream {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go func() {
				defer func() { _ = conn.Close() }()

				if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
					return
				}

				_, _ = conn.Write([]byte(reply))
			}()
		}
	}()

	return &scriptedUpstream{ln: ln}
}

func (u *scriptedUpstream) addr() string { return u.ln.Addr().String() }

// delimFramer frames on an arbitrary byte, so a test can hand the two
// directions framers that disagree about where a message ends.
type delimFramer struct {
	delim byte
}

func (f delimFramer) ReadMessage(r Reader) ([]byte, error) {
	var msg []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == f.delim {
			return msg, nil
		}

		msg = append(msg, b)
	}
}

func (f delimFramer) WriteMessage(w io.Writer, raw []byte) error {
	_, err := w.Write(append(append([]byte(nil), raw...), f.delim))

	return err
}

// deadAddr returns an address nothing is listening on, by binding one and
// closing it again.
func deadAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := ln.Addr().String()
	_ = ln.Close()

	return addr
}

// runProxy starts a proxy on ephemeral ports.
//
// The returned wait function reports what Run returned and is safe to call more
// than once, because the cleanup calls it too.
func runProxy(t *testing.T, cfg Config) (proxy *Proxy, wait func(*testing.T) error, sessionErrors chan error) {
	t.Helper()

	if cfg.Framer == nil && cfg.NewFramer == nil {
		cfg.Framer = lineFramer{}
	}

	sessionErrs := make(chan error, 16)
	prior := cfg.OnSessionError
	cfg.OnSessionError = func(s *Session, err error) {
		if prior != nil {
			prior(s, err)
		}
		select {
		case sessionErrs <- err:
		default:
		}
	}

	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var runErr error
	stopped := make(chan struct{})

	go func() {
		runErr = p.Run(ctx)
		close(stopped)
	}()

	// Run binds before it serves, so wait for the addresses to appear rather
	// than sleeping a guess.
	waitFor(t, func() bool { return len(p.Addrs()) == len(cfg.Ports) })

	wait = func(t *testing.T) error {
		t.Helper()

		select {
		case <-stopped:
			return runErr
		case <-time.After(10 * time.Second):
			t.Error("Run never returned")

			return nil
		}
	}

	t.Cleanup(func() {
		cancel()
		_ = wait(t)
	})

	return p, wait, sessionErrs
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(2 * time.Millisecond)
	}

	t.Fatal("condition never became true")
}

// onlyAddr returns the single address a one-port proxy bound.
func onlyAddr(t *testing.T, p *Proxy) string {
	t.Helper()

	addrs := p.Addrs()
	if len(addrs) != 1 {
		t.Fatalf("Addrs() has %d entries, want 1", len(addrs))
	}

	for _, addr := range addrs {
		return addr.String()
	}

	return ""
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func nextSessionError(t *testing.T, errs chan error) error {
	t.Helper()

	select {
	case err := <-errs:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("no session error was reported")

		return nil
	}
}

func TestProxyRelaysEndToEnd(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")

	if got := readLine(t, bufio.NewReader(conn)); got != "hello" {
		t.Fatalf("got %q back, want hello", got)
	}
}

// TestProxyBindsEphemeralPorts is what lets every other test here avoid
// hard-coding a port.
func TestProxyBindsEphemeralPorts(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{
			{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}},
			{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}},
		},
	})

	addrs := p.Addrs()
	if len(addrs) != 2 {
		t.Fatalf("Addrs() has %d entries, want 2", len(addrs))
	}

	for port, addr := range addrs {
		if port == 0 {
			t.Fatalf("Addrs() reported port 0 for %v, want the port actually bound", addr)
		}

		conn := dial(t, addr.String())
		writeLine(t, conn, "ping")
		if got := readLine(t, bufio.NewReader(conn)); got != "ping" {
			t.Fatalf("port %d relayed %q, want ping", port, got)
		}
	}
}

// TestProxyListensOnADeadPort is the accepted consequence of lazy health: the
// listener opens either way, and a client sees connect-then-drop rather than
// connection refused. The earlier behaviour — never opening a listener whose
// upstreams failed a startup probe — meant a stale probe routed nothing.
func TestProxyListensOnADeadPort(t *testing.T) {
	p, _, sessionErrs := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: deadAddr(t)}}}},
	})

	conn := dial(t, onlyAddr(t, p))

	// Connecting succeeded, which is the first half of the claim.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("the connection stayed open with no healthy upstream")
	}

	if err := nextSessionError(t, sessionErrs); !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("reported %v, want ErrNoHealthyUpstream", err)
	}
}

func TestProxyFailsOverOnADialError(t *testing.T) {
	dead := deadAddr(t)
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: dead}, {Addr: up.addr()}}}},
		// A prober that trusts everything forces the failover to happen at the
		// dial, which is the path under test.
		Prober: probeFunc(func(context.Context, string) error { return nil }),
	})

	addr := onlyAddr(t, p)

	conn := dial(t, addr)
	writeLine(t, conn, "first")
	if got := readLine(t, bufio.NewReader(conn)); got != "first" {
		t.Fatalf("got %q, want first", got)
	}
	if n := up.connections(); n != 1 {
		t.Fatalf("the live upstream saw %d connections, want 1", n)
	}

	// The dead upstream is now marked down in the same cache the probes use, so
	// the next client does not retry it inside the TTL.
	second := dial(t, addr)
	writeLine(t, second, "again")
	if got := readLine(t, bufio.NewReader(second)); got != "again" {
		t.Fatalf("got %q, want again", got)
	}

	waitFor(t, func() bool { return up.connections() == 2 })
}

func TestProxyReportsNoHealthyUpstream(t *testing.T) {
	p, _, sessionErrs := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: deadAddr(t)}, {Addr: deadAddr(t)}}}},
	})

	conn := dial(t, onlyAddr(t, p))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1)
	_, _ = conn.Read(buf)

	if err := nextSessionError(t, sessionErrs); !errors.Is(err, ErrNoHealthyUpstream) {
		t.Fatalf("reported %v, want ErrNoHealthyUpstream", err)
	}
}

func TestProxyMaxSessionsClosesOverflow(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports:       []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		MaxSessions: 1,
		Overflow:    OverflowClose,
	})

	addr := onlyAddr(t, p)

	first := dial(t, addr)
	writeLine(t, first, "held")
	if got := readLine(t, bufio.NewReader(first)); got != "held" {
		t.Fatalf("the first session relayed %q, want held", got)
	}
	waitFor(t, func() bool { return p.SessionCount() == 1 })

	second := dial(t, addr)
	_ = second.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1)
	if _, err := second.Read(buf); err == nil {
		t.Fatal("a session over the limit was served")
	}

	if got := p.SessionCount(); got != 1 {
		t.Fatalf("SessionCount() = %d, want it never to exceed MaxSessions", got)
	}
}

func TestProxyMaxSessionsWaitsForASlot(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports:       []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		MaxSessions: 1,
		Overflow:    OverflowWait,
	})

	addr := onlyAddr(t, p)

	first := dial(t, addr)
	writeLine(t, first, "one")
	if got := readLine(t, bufio.NewReader(first)); got != "one" {
		t.Fatalf("the first session relayed %q, want one", got)
	}

	second, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = second.Close() }()

	// Free the slot; the waiting connection must then be served rather than
	// dropped.
	_ = first.Close()

	writeLine(t, second, "two")
	_ = second.SetReadDeadline(time.Now().Add(10 * time.Second))

	if got := readLine(t, bufio.NewReader(second)); got != "two" {
		t.Fatalf("the waiting session relayed %q, want two", got)
	}
}

func TestProxyPreFrameHandledSkipsTheUpstream(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		PreFrame: PreFrameFunc(func(_ context.Context, s *Session, br *bufio.Reader) (PreFrameResult, error) {
			line, err := br.ReadString('\n')
			if err != nil {
				return Continue, err
			}
			if !strings.HasPrefix(line, "PING") {
				return Continue, nil
			}

			if _, err := s.Client.Write([]byte("PONG\n")); err != nil {
				return Handled, err
			}

			return Handled, nil
		}),
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "PING")

	if got := readLine(t, bufio.NewReader(conn)); got != "PONG" {
		t.Fatalf("got %q, want PONG", got)
	}
	if n := up.connections(); n != 0 {
		t.Fatalf("the upstream saw %d connections, want none — the hook handled it", n)
	}
}

// TestProxyPreFrameContinueKeepsPeekedBytes proves a hook that only looks does
// not consume: the framer must still see the whole first message.
func TestProxyPreFrameContinueKeepsPeekedBytes(t *testing.T) {
	up := newEchoUpstream(t)

	peeked := make(chan string, 1)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		PreFrame: PreFrameFunc(func(_ context.Context, _ *Session, br *bufio.Reader) (PreFrameResult, error) {
			head, err := br.Peek(5)
			if err != nil {
				return Continue, err
			}
			peeked <- string(head)

			return Continue, nil
		}),
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "intact")

	if got := <-peeked; got != "intac" {
		t.Fatalf("the hook peeked %q, want intac", got)
	}
	if got := readLine(t, bufio.NewReader(conn)); got != "intact" {
		t.Fatalf("the relayed message was %q, want intact — peeking must not consume", got)
	}
}

func TestProxyShutdownIsGraceful(t *testing.T) {
	up := newEchoUpstream(t)

	p, waitRun, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
	})

	addr := onlyAddr(t, p)

	conn := dial(t, addr)
	writeLine(t, conn, "live")
	if got := readLine(t, bufio.NewReader(conn)); got != "live" {
		t.Fatalf("got %q, want live", got)
	}
	waitFor(t, func() bool { return p.SessionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// The listener is gone, so nothing new is accepted.
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("the proxy still accepted a connection after Shutdown")
	}

	if err := waitRun(t); err != nil {
		t.Fatalf("Run returned %v after a graceful shutdown, want nil", err)
	}

	if got := p.SessionCount(); got != 0 {
		t.Fatalf("SessionCount() = %d after Shutdown, want 0", got)
	}
}

func TestProxyRunReportsAFatalBindError(t *testing.T) {
	up := newEchoUpstream(t)

	// Take a real port first, so the second proxy has something to collide with.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = held.Close() }()

	tcp, ok := held.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address is %T, want *net.TCPAddr", held.Addr())
	}

	p, err := New(Config{
		Framer: lineFramer{},
		Ports:  []PortConfig{{Port: tcp.Port, Upstreams: []Upstream{{Addr: up.addr()}}}},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runErr := p.Run(context.Background())
	if runErr == nil {
		t.Fatal("Run returned nil for a port it could not bind")
	}
	if !strings.Contains(runErr.Error(), fmt.Sprint(tcp.Port)) {
		t.Fatalf("the bind error does not name the port: %v", runErr)
	}
}

func TestProxyNewReportsConfigFaults(t *testing.T) {
	_, err := New(Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New with an empty config = %v, want ErrInvalidConfig", err)
	}
}

func TestProxyListsLiveSessions(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Hooks: []Hook{HookFunc(func(_ context.Context, s *Session, _ *Message) (Action, error) {
			s.Set("saw", true)

			return Forward, nil
		})},
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")
	_ = readLine(t, bufio.NewReader(conn))

	waitFor(t, func() bool { return p.SessionCount() == 1 })

	snaps := p.Sessions()
	if len(snaps) != 1 {
		t.Fatalf("Sessions() returned %d entries, want 1", len(snaps))
	}
	if snaps[0].Meta["saw"] != true {
		t.Fatalf("the snapshot lost the hook's metadata: %+v", snaps[0].Meta)
	}
	if snaps[0].Info.UpstreamAddr != up.addr() {
		t.Fatalf("snapshot upstream = %q, want %q", snaps[0].Info.UpstreamAddr, up.addr())
	}
	if got := p.UpstreamCount(up.addr()); got != 1 {
		t.Fatalf("UpstreamCount = %d, want 1", got)
	}
}

// TestProxyBuildsACodecPerSession covers the config seam a stateful codec
// needs. A codec that carries connection state — a handshake, a per-direction
// decoder — must not be shared, or every client advances everyone else's.
func TestProxyBuildsACodecPerSession(t *testing.T) {
	up := newEchoUpstream(t)

	var built atomic.Int64

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewCodec: func(*Session) (Codec, error) {
			built.Add(1)

			return &countingCodec{}, nil
		},
	})

	addr := onlyAddr(t, p)

	for range 3 {
		conn := dial(t, addr)
		writeLine(t, conn, "hello")

		if got := readLine(t, bufio.NewReader(conn)); got != "hello" {
			t.Fatalf("got %q, want hello", got)
		}
	}

	if n := built.Load(); n != 3 {
		t.Fatalf("NewCodec ran %d times for 3 sessions, want 3", n)
	}
}

// TestProxyReportsACodecThatCannotBeBuilt keeps a failure at session setup from
// becoming a session that silently relays undecoded.
func TestProxyReportsACodecThatCannotBeBuilt(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, sessionErrs := runProxy(t, Config{
		Ports:    []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewCodec: func(*Session) (Codec, error) { return nil, errors.New("no codec today") },
	})

	conn := dial(t, onlyAddr(t, p))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1)
	_, _ = conn.Read(buf)

	if err := nextSessionError(t, sessionErrs); !strings.Contains(err.Error(), "no codec today") {
		t.Fatalf("reported %v, want the codec's own error", err)
	}
	if n := up.connections(); n != 0 {
		t.Fatalf("the upstream saw %d connections, want none — setup failed before the dial", n)
	}
}

// TestProxyBuildsAFramerPerSessionAndDirection covers the case NewFramer exists
// for: a protocol whose boundaries are found by decoding, so the framer carries
// per-connection state and reads the two directions differently. The upstream
// framer here uppercases nothing and the client one does, which is only
// observable if the two are genuinely separate instances.
func TestProxyBuildsAFramerPerSessionAndDirection(t *testing.T) {
	up := newEchoUpstream(t)

	var mu sync.Mutex
	built := make([]Direction, 0, 6)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewFramer: func(_ *Session, dir Direction) (Framer, error) {
			mu.Lock()
			built = append(built, dir)
			mu.Unlock()

			return lineFramer{}, nil
		},
	})

	addr := onlyAddr(t, p)

	for range 3 {
		conn := dial(t, addr)
		writeLine(t, conn, "hello")

		if got := readLine(t, bufio.NewReader(conn)); got != "hello" {
			t.Fatalf("got %q, want hello", got)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(built) != 6 {
		t.Fatalf("NewFramer ran %d times for 3 sessions, want 6 — one per direction", len(built))
	}

	var toServer, toClient int
	for _, dir := range built {
		if dir == ToServer {
			toServer++
		} else {
			toClient++
		}
	}

	if toServer != 3 || toClient != 3 {
		t.Fatalf("built %d ToServer and %d ToClient framers, want 3 each", toServer, toClient)
	}
}

// TestProxyFramerPerDirectionFramesEachWayItsOwnWay is the property NewFramer
// buys over Framer: the two directions can disagree about where a message ends.
func TestProxyFramerPerDirectionFramesEachWayItsOwnWay(t *testing.T) {
	// The upstream terminates its reply with a semicolon rather than a newline,
	// so a single shared framer could not read both sides of this session.
	up := newScriptedUpstream(t, "read-until-newline", "pong;")

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewFramer: func(_ *Session, dir Direction) (Framer, error) {
			if dir == ToClient {
				return delimFramer{delim: ';'}, nil
			}

			return lineFramer{}, nil
		},
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "ping")

	got, err := bufio.NewReader(conn).ReadString(';')
	if err != nil {
		t.Fatalf("read the reply: %v", err)
	}
	if got != "pong;" {
		t.Fatalf("got %q, want %q", got, "pong;")
	}
}

// TestProxyReportsAFramerThatCannotBeBuilt keeps a framer failure from becoming
// a session with no framing at all.
func TestProxyReportsAFramerThatCannotBeBuilt(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, sessionErrs := runProxy(t, Config{
		Ports:     []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewFramer: func(*Session, Direction) (Framer, error) { return nil, errors.New("no framer today") },
	})

	conn := dial(t, onlyAddr(t, p))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1)
	_, _ = conn.Read(buf)

	if err := nextSessionError(t, sessionErrs); !strings.Contains(err.Error(), "no framer today") {
		t.Fatalf("reported %v, want the framer's own error", err)
	}
	if n := up.connections(); n != 0 {
		t.Fatalf("the upstream saw %d connections, want none — setup failed before the dial", n)
	}
}

// TestProxyReportsANilFramerFromNewFramer catches the mistake that would
// otherwise surface as a nil dereference on the first message.
func TestProxyReportsANilFramerFromNewFramer(t *testing.T) {
	up := newEchoUpstream(t)

	p, _, sessionErrs := runProxy(t, Config{
		Ports:     []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewFramer: func(*Session, Direction) (Framer, error) { return nil, nil },
	})

	conn := dial(t, onlyAddr(t, p))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1)
	_, _ = conn.Read(buf)

	if err := nextSessionError(t, sessionErrs); !strings.Contains(err.Error(), "returned no") {
		t.Fatalf("reported %v, want a complaint about the nil framer", err)
	}
}

// TestProxySinkSeesTheUpstream pins when a session is opened on the sink. The
// record is written once the two connections are joined, so the upstream
// address in it is the one actually dialled rather than an empty string.
func TestProxySinkSeesTheUpstream(t *testing.T) {
	up := newEchoUpstream(t)
	sink := &recordingSink{}

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:  sink,
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")
	_ = readLine(t, bufio.NewReader(conn))

	waitFor(t, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()

		return len(sink.opened) == 1
	})

	sink.mu.Lock()
	opened := sink.opened[0]
	sink.mu.Unlock()

	if opened.UpstreamAddr != up.addr() {
		t.Fatalf("OpenSession saw upstream %q, want %q", opened.UpstreamAddr, up.addr())
	}
	if opened.ClientAddr == "" {
		t.Fatal("OpenSession saw no client address")
	}
}

// TestProxyPreFrameHandledOpensNoRecord is the other half of that pairing. A
// connection answered before an upstream was dialled never became a session, so
// opening a record nothing would close would leave a permanently half-open row.
func TestProxyPreFrameHandledOpensNoRecord(t *testing.T) {
	up := newEchoUpstream(t)
	sink := &recordingSink{}

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:  sink,
		PreFrame: PreFrameFunc(func(_ context.Context, s *Session, br *bufio.Reader) (PreFrameResult, error) {
			if _, err := br.ReadString('\n'); err != nil {
				return Continue, err
			}
			if _, err := s.Client.Write([]byte("PONG\n")); err != nil {
				return Handled, err
			}

			return Handled, nil
		}),
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "PING")

	if got := readLine(t, bufio.NewReader(conn)); got != "PONG" {
		t.Fatalf("got %q, want PONG", got)
	}

	sink.mu.Lock()
	opened, closed := len(sink.opened), sink.closed
	sink.mu.Unlock()

	if opened != 0 || closed != 0 {
		t.Fatalf("a handled connection produced %d opens and %d closes, want none of either", opened, closed)
	}
}

// rawChunks returns the recorded raw bytes joined per direction, which is how a
// capture is actually read back: as two streams, not as a list of syscalls.
func (s *recordingSink) rawStreams() (toServer, toClient []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range s.raw {
		if c.dir == ToServer {
			toServer = append(toServer, c.data...)

			continue
		}

		toClient = append(toClient, c.data...)
	}

	return toServer, toClient
}

func TestProxyCapturesRawBytes(t *testing.T) {
	up := newEchoUpstream(t)
	sink := &recordingSink{}

	p, _, _ := runProxy(t, Config{
		Ports:      []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:       sink,
		CaptureRaw: true,
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")

	if got := readLine(t, bufio.NewReader(conn)); got != "hello" {
		t.Fatalf("got %q, want hello", got)
	}

	waitFor(t, func() bool {
		toServer, toClient := sink.rawStreams()

		return len(toServer) > 0 && len(toClient) > 0
	})

	toServer, toClient := sink.rawStreams()
	if string(toServer) != "hello\n" {
		t.Fatalf("captured to_server %q, want the framed bytes the client sent", toServer)
	}
	if string(toClient) != "hello\n" {
		t.Fatalf("captured to_client %q, want the framed bytes the client received", toClient)
	}

	// Every chunk must belong to the session that produced it.
	sink.mu.Lock()
	defer sink.mu.Unlock()

	for _, c := range sink.raw {
		if c.id == 0 {
			t.Fatal("a raw chunk was recorded against session 0, so it cannot be joined to a session row")
		}
	}
}

// TestProxyCapturesBytesReadBeforeTheSessionOpened is the case the buffering
// exists for. A pre-frame hook reads from the socket before there is a sink
// session to attach anything to, and a capture that started later would be
// missing exactly the bytes that opened the conversation.
func TestProxyCapturesBytesReadBeforeTheSessionOpened(t *testing.T) {
	up := newEchoUpstream(t)
	sink := &recordingSink{}

	p, _, _ := runProxy(t, Config{
		Ports:      []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:       sink,
		CaptureRaw: true,
		PreFrame: PreFrameFunc(func(_ context.Context, _ *Session, br *bufio.Reader) (PreFrameResult, error) {
			// Peeking still pulls the bytes off the socket, which is where the
			// capture sits.
			if _, err := br.Peek(6); err != nil {
				return Continue, err
			}

			return Continue, nil
		}),
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "opener")
	_ = readLine(t, bufio.NewReader(conn))

	waitFor(t, func() bool {
		toServer, _ := sink.rawStreams()

		return len(toServer) >= len("opener\n")
	})

	toServer, _ := sink.rawStreams()
	if string(toServer) != "opener\n" {
		t.Fatalf("captured %q, want the opening bytes the pre-frame hook had already read", toServer)
	}
}

// TestProxyCapturesNothingWhenOff keeps the cost off the default path.
func TestProxyCapturesNothingWhenOff(t *testing.T) {
	up := newEchoUpstream(t)
	sink := &recordingSink{}

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:  sink,
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")
	_ = readLine(t, bufio.NewReader(conn))

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if len(sink.raw) != 0 {
		t.Fatalf("%d raw chunks were recorded with CaptureRaw off", len(sink.raw))
	}
}

// TestProxyCaptureRecordsPostTransformBytes proves the capture sits below a
// mid-stream transform: what it stores is what was on the wire, not the
// plaintext the framer handled.
func TestProxyCaptureRecordsPostTransformBytes(t *testing.T) {
	up := newEchoUpstream(t)
	sink := &recordingSink{}

	swapped := make(chan error, 1)

	p, _, _ := runProxy(t, Config{
		Ports:      []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		Sink:       sink,
		CaptureRaw: true,
		Hooks: []Hook{HookFunc(func(_ context.Context, s *Session, m *Message) (Action, error) {
			// Only the client's own trigger, and only once. Matching both
			// directions would fire again on the echo, after the swap had already
			// changed what an echo looks like.
			if m.Dir != ToServer || string(m.Raw) != "START" {
				return Forward, nil
			}

			// Encipher only what the proxy writes to the client from here on.
			swapped <- s.Swap(ToClient, Transform{Write: flipped})

			// Dropped rather than forwarded, so no echo of it crosses the
			// boundary this hook just moved.
			return Drop, nil
		})},
	})

	conn := dial(t, onlyAddr(t, p))

	writeLine(t, conn, "START")
	if err := <-swapped; err != nil {
		t.Fatalf("Swap: %v", err)
	}

	writeLine(t, conn, "after")

	// The client now reads transformed bytes, so it reads them raw.
	got := make([]byte, len("after\n"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, flipped([]byte("after\n"))) {
		t.Fatalf("the client received %q, want the transformed form", got)
	}

	waitFor(t, func() bool {
		toServer, toClient := sink.rawStreams()

		return len(toServer) >= len("START\nafter\n") && len(toClient) > 0
	})

	toServer, toClient := sink.rawStreams()

	if string(toServer) != "START\nafter\n" {
		t.Fatalf("captured to_server %q, want both lines as the client sent them", toServer)
	}
	if !bytes.Equal(toClient, flipped([]byte("after\n"))) {
		t.Fatalf("captured to_client %q, want the transformed bytes — the capture must sit below the transform", toClient)
	}
}

// TestProxySessionScopedConstructorsShareState is the property the session
// argument buys: the two framers of a session, and the codec built beside them,
// can find each other.
//
// Nothing else in the API joins them. A framer that learns something on one
// direction — a negotiated encoding, a decision to stop framing at all — has
// nowhere else to put it, and a consumer that must react to it has no other way
// to hear about it.
func TestProxySessionScopedConstructorsShareState(t *testing.T) {
	up := newEchoUpstream(t)

	const key = "shared"

	seen := make(chan int, 4)

	p, _, _ := runProxy(t, Config{
		Ports: []PortConfig{{Port: 0, Upstreams: []Upstream{{Addr: up.addr()}}}},
		NewCodec: func(s *Session) (Codec, error) {
			// The codec is built first, so it is where the shared thing starts.
			s.Set(key, new(atomic.Int64))

			return &countingCodec{}, nil
		},
		NewFramer: func(s *Session, _ Direction) (Framer, error) {
			shared, ok := s.Get(key)
			if !ok {
				return nil, errors.New("framer could not see the codec's state")
			}

			counter, ok := shared.(*atomic.Int64)
			if !ok {
				return nil, errors.New("shared state was not what the codec left")
			}

			seen <- int(counter.Add(1))

			return lineFramer{}, nil
		},
	})

	conn := dial(t, onlyAddr(t, p))
	writeLine(t, conn, "hello")

	if got := readLine(t, bufio.NewReader(conn)); got != "hello" {
		t.Fatalf("got %q, want hello", got)
	}

	// Both framers incremented the same counter, so they saw 1 and 2 rather
	// than 1 and 1.
	first, second := <-seen, <-seen
	if first+second != 3 {
		t.Errorf("framers saw %d and %d, want 1 and 2 — they did not share one counter", first, second)
	}
}
