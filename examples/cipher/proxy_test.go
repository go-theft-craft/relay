package cipher_test

import (
	"bufio"
	"bytes"
	"context"
	stdcipher "crypto/cipher"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/cipher"
)

// endpoint is one end of one link, speaking the line protocol and switching
// onto the keystreams when it is told to. Both the test client and the stub
// upstream are one of these, which is what keeps the test honest: each is an
// ordinary endpoint, not something that knows it is talking to a proxy.
type endpoint struct {
	conn net.Conn
	br   *bufio.Reader
	role cipher.Role
	// readTimeout bounds a read on the test's own end. A keystream that has
	// desynchronised does not produce a wrong line, it produces a line that
	// never ends, so without this a broken proxy hangs the suite instead of
	// failing it.
	readTimeout time.Duration

	mu   sync.Mutex
	send stdcipher.Stream
	recv stdcipher.Stream
}

func newEndpoint(conn net.Conn, role cipher.Role) *endpoint {
	return &endpoint{conn: conn, br: bufio.NewReader(conn), role: role}
}

// enable switches this endpoint onto the keystreams, from the next byte on.
func (e *endpoint) enable(t *testing.T) {
	t.Helper()

	send, recv, err := cipher.Streams(e.role)
	if err != nil {
		t.Fatalf("Streams: %v", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.send, e.recv = send, recv
}

func (e *endpoint) writeLine(t *testing.T, line string) {
	t.Helper()

	if err := e.writeLineErr(line); err != nil {
		t.Fatalf("write %q: %v", line, err)
	}
}

func (e *endpoint) writeLineErr(line string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := []byte(line + "\n")
	if e.send != nil {
		e.send.XORKeyStream(out, out)
	}

	_, err := e.conn.Write(out)

	return err
}

func (e *endpoint) readLine(t *testing.T) string {
	t.Helper()

	line, err := e.readLineErr()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	return line
}

func (e *endpoint) readLineErr() (string, error) {
	if e.readTimeout > 0 {
		if err := e.conn.SetReadDeadline(time.Now().Add(e.readTimeout)); err != nil {
			return "", err
		}
	}

	var out []byte
	for {
		b, err := e.br.ReadByte()
		if err != nil {
			return "", err
		}

		e.mu.Lock()
		if e.recv != nil {
			one := [1]byte{b}
			e.recv.XORKeyStream(one[:], one[:])
			b = one[0]
		}
		e.mu.Unlock()

		if b == '\n' {
			return string(out), nil
		}

		out = append(out, b)
	}
}

// ready is the upstream's acknowledgement of the boundary, and the first
// enciphered message of any session.
const ready = "READY"

// startCipher crosses the boundary the way a real endpoint has to: send the
// trigger, switch, and then send nothing until the acknowledgement comes back.
//
// Sending past the boundary without waiting is the one thing the protocol
// forbids, and the proxy refuses it rather than corrupting the stream —
// TestSwapWithBytesBufferedIsRefused is that case on purpose.
func startCipher(t *testing.T, client *endpoint) {
	t.Helper()

	client.writeLine(t, cipher.Trigger)
	client.enable(t)

	if got := client.readLine(t); got != ready {
		t.Fatalf("the acknowledgement was %q, want %s", got, ready)
	}
}

// stubUpstream echoes every line, switching onto the keystreams when the
// trigger reaches it — exactly what a real server does at the boundary.
type stubUpstream struct {
	ln net.Listener
}

func newStubUpstream(t *testing.T) *stubUpstream {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	u := &stubUpstream{ln: ln}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go u.serve(t, conn)
		}
	}()

	return u
}

func (u *stubUpstream) serve(t *testing.T, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	e := newEndpoint(conn, cipher.RoleServer)

	for {
		line, err := e.readLineErr()
		if err != nil {
			return
		}

		if line == cipher.Trigger {
			// The trigger crosses in the clear and everything after it does not.
			e.enable(t)

			// The acknowledgement is what lets the other end know the boundary
			// has been crossed. Every protocol that renegotiates mid-stream needs
			// one, because both endpoints have to stop sending across the switch
			// — and here the proxy is a third endpoint with the same problem.
			if err := e.writeLineErr(ready); err != nil {
				return
			}

			continue
		}

		if err := e.writeLineErr("echo:" + line); err != nil {
			return
		}
	}
}

func (u *stubUpstream) addr() string { return u.ln.Addr().String() }

// runProxy starts the example proxy on an ephemeral port and returns its
// address and a channel of whatever it reported per session.
func runProxy(t *testing.T, upstream string) (addr string, sessionErrs chan error) {
	t.Helper()

	sessionErrs = make(chan error, 8)

	p, err := relay.New(relay.Config{
		Framer: cipher.LineFramer{},
		Ports:  []relay.PortConfig{{Port: 0, Upstreams: []relay.Upstream{{Addr: upstream}}}},
		Hooks:  []relay.Hook{cipher.Hook()},
		OnSessionError: func(_ *relay.Session, err error) {
			select {
			case sessionErrs <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})

	go func() {
		_ = p.Run(ctx)
		close(stopped)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(p.Addrs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	for _, a := range p.Addrs() {
		addr = a.String()
	}
	if addr == "" {
		t.Fatal("the proxy never bound a port")
	}

	t.Cleanup(func() {
		cancel()

		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error("the proxy never stopped")
		}
	})

	return addr, sessionErrs
}

func dialClient(t *testing.T, addr string) *endpoint {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	e := newEndpoint(conn, cipher.RoleClient)
	e.readTimeout = 10 * time.Second

	return e
}

// TestPlaintextBeforeTheBoundary is the baseline: until the trigger, this is an
// ordinary proxy and the bytes on the wire are readable.
func TestPlaintextBeforeTheBoundary(t *testing.T) {
	up := newStubUpstream(t)
	addr, _ := runProxy(t, up.addr())

	client := dialClient(t, addr)
	client.writeLine(t, "hello")

	if got := client.readLine(t); got != "echo:hello" {
		t.Fatalf("got %q, want echo:hello", got)
	}
}

// TestTriggerCrossesInTheClear pins the ordering the whole design turns on: the
// message that names the boundary is itself unenciphered, because both
// endpoints have to be able to read it.
func TestTriggerCrossesInTheClear(t *testing.T) {
	up := newStubUpstream(t)
	addr, _ := runProxy(t, up.addr())

	client := dialClient(t, addr)

	// If the trigger had been enciphered on its way out, the upstream would have
	// read something else, never switched, and never acknowledged — so
	// startCipher itself would fail.
	startCipher(t, client)

	client.writeLine(t, "after")

	if got := client.readLine(t); got != "echo:after" {
		t.Fatalf("got %q, want echo:after — the trigger did not cross in the clear", got)
	}
}

// TestCiphertextOnTheWire asserts both halves. Only asserting that the upstream
// got the plaintext would pass for a proxy that never enciphered anything.
func TestCiphertextOnTheWire(t *testing.T) {
	up := newStubUpstream(t)
	addr, _ := runProxy(t, up.addr())

	client := dialClient(t, addr)
	startCipher(t, client)

	const plaintext = "a-recognisable-secret"

	// What this endpoint is about to put on the wire, computed the same way it
	// computes it, so the comparison is against the real bytes.
	send, _, err := cipher.Streams(cipher.RoleClient)
	if err != nil {
		t.Fatalf("Streams: %v", err)
	}

	onWire := []byte(plaintext + "\n")
	send.XORKeyStream(onWire, onWire)

	if bytes.Contains(onWire, []byte(plaintext)) {
		t.Fatal("the bytes on the wire still contain the plaintext")
	}

	client.writeLine(t, plaintext)

	if got := client.readLine(t); got != "echo:"+plaintext {
		t.Fatalf("got %q, want echo:%s", got, plaintext)
	}
}

// TestBothDirections is the cross-direction swap, and the part the design
// exists for: the upstream's reply is enciphered on its own link and reaches
// the client as plaintext.
func TestBothDirections(t *testing.T) {
	up := newStubUpstream(t)
	addr, _ := runProxy(t, up.addr())

	client := dialClient(t, addr)
	startCipher(t, client)

	for i := range 5 {
		line := fmt.Sprintf("round-%d", i)
		client.writeLine(t, line)

		if got := client.readLine(t); got != "echo:"+line {
			t.Fatalf("got %q, want echo:%s", got, line)
		}
	}
}

// TestLongSessionStaysSynchronised is what a single-message assertion would
// miss. A keystream that restarts, or a swap applied one message early,
// corrupts everything downstream of it rather than one message.
func TestLongSessionStaysSynchronised(t *testing.T) {
	up := newStubUpstream(t)
	addr, _ := runProxy(t, up.addr())

	client := dialClient(t, addr)
	startCipher(t, client)

	for i := range 400 {
		line := fmt.Sprintf("message-%04d-%s", i, strings.Repeat("x", i%37))
		client.writeLine(t, line)

		if got := client.readLine(t); got != "echo:"+line {
			t.Fatalf("message %d came back as %q, want echo:%s", i, got, line)
		}
	}
}

// TestSwapWithBytesBufferedIsRefused covers the one refusal. The client sends
// the trigger and a following message in a single write, so bytes that belong
// to the old encoding are already buffered when the swap is attempted.
func TestSwapWithBytesBufferedIsRefused(t *testing.T) {
	up := newStubUpstream(t)
	addr, sessionErrs := runProxy(t, up.addr())

	client := dialClient(t, addr)

	if _, err := client.conn.Write([]byte(cipher.Trigger + "\ntoo-soon\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case err := <-sessionErrs:
		if !errors.Is(err, relay.ErrSwapPending) {
			t.Fatalf("reported %v, want it wrapping ErrSwapPending", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("swapping with bytes still buffered was accepted silently")
	}
}
