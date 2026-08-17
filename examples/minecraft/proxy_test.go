package minecraft_test

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	capturepkg "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
	capturesink "github.com/go-theft-craft/relay/examples/minecraft/capture"
	"github.com/go-theft-craft/relay/examples/minecraft/store"
)

// The documents the stub serves and the hook rewrites to. They are deliberately
// distinguishable at a glance in a failure message.
const (
	stubDocument     = `{"description":"the stub upstream"}`
	rewrittenDocment = `{"description":"rewritten by a hook"}`
)

// stubServer speaks just enough of the protocol to answer a handshake and a
// status request. It is a real endpoint rather than an echo, which is what
// makes this an end-to-end test of the seams rather than of the plumbing.
type stubServer struct {
	ln net.Listener

	mu       sync.Mutex
	sessions int
}

func newStubServer(t *testing.T) *stubServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	s := &stubServer{ln: ln}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			s.mu.Lock()
			s.sessions++
			s.mu.Unlock()

			go s.serve(t, conn)
		}
	}()

	return s
}

func (s *stubServer) addr() string { return s.ln.Addr().String() }

func (s *stubServer) connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sessions
}

func (s *stubServer) serve(t *testing.T, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	descriptor := protocols.Default()

	limits, err := protocol.NewLimits()
	if err != nil {
		return
	}

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		return
	}

	session, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		return
	}

	reader := bufio.NewReader(conn)

	for {
		raw, err := framer.ReadMessage(reader)
		if err != nil {
			return
		}

		packet, err := session.DecodeFrame(raw)
		if err != nil {
			return
		}

		if fields, ok := protocols.ReadHandshake(packet); ok {
			if fields.NextState == 1 {
				session.SetState(protocol.State("status"))
			}

			continue
		}

		// The only other packet this stub understands is the status request.
		if packet.State != protocol.State("status") || packet.ID != 0 {
			continue
		}

		response, err := protocols.StatusResponse(descriptor, stubDocument)
		if err != nil {
			return
		}

		encoded, err := session.EncodeFrame(response)
		if err != nil {
			return
		}
		if err := framer.WriteMessage(conn, encoded); err != nil {
			return
		}
	}
}

// statusClient runs the client half of a status exchange through whatever
// address it is given, and returns the document it was answered with.
func statusClient(t *testing.T, addr string) string {
	t.Helper()

	descriptor := protocols.Default()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	session, err := descriptor.NewSession(protocol.RoleClient, limits)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}

	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatalf("port: %v", err)
	}

	handshake, err := protocols.Handshake(descriptor, host, uint16(port), 1)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	writePacket(t, framer, session, conn, handshake)
	session.SetState(protocol.State("status"))

	writePacket(t, framer, session, conn, statusRequestPacket(t, descriptor))

	raw, err := framer.ReadMessage(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("read the status response: %v", err)
	}

	packet, err := session.DecodeFrame(raw)
	if err != nil {
		t.Fatalf("decode the status response: %v", err)
	}

	return responseDocument(t, packet)
}

func writePacket(t *testing.T, framer *minecraft.Framer, session protocol.Session, w io.Writer, packet protocol.Packet) {
	t.Helper()

	encoded, err := session.EncodeFrame(packet)
	if err != nil {
		t.Fatalf("EncodeFrame(%s): %v", packet.Name, err)
	}
	if err := framer.WriteMessage(w, encoded); err != nil {
		t.Fatalf("WriteMessage(%s): %v", packet.Name, err)
	}
}

func statusRequestPacket(t *testing.T, descriptor protocol.Protocol) protocol.Packet {
	t.Helper()

	factory, ok := descriptor.(protocol.PacketFactory)
	if !ok {
		t.Skip("this protocol cannot build packet values")
	}

	value, known := factory.NewPacketValue(protocol.State("status"), protocol.DirectionServerbound, 0)
	if !known {
		t.Skip("this protocol has no status request")
	}

	return protocol.Packet{
		State:     protocol.State("status"),
		Direction: protocol.DirectionServerbound,
		ID:        0,
		Value:     value,
	}
}

// responseDocument digs the JSON document out of a status response without the
// test needing to know the generated type's name.
func responseDocument(t *testing.T, packet protocol.Packet) string {
	t.Helper()

	type responder interface{ GetResponse() string }

	if r, ok := packet.Value.(responder); ok {
		return r.GetResponse()
	}

	// Fall back to the rendered form, which still lets the assertions below
	// distinguish the stub's document from the rewritten one.
	return renderValue(packet.Value)
}

func renderValue(value any) string {
	if value == nil {
		return ""
	}

	return strings.TrimSpace(fmt.Sprintf("%+v", value))
}

// runExampleProxy wires every seam the example implements and returns the
// address to connect to along with the sink recording it.
func runExampleProxy(t *testing.T, upstream string, hooks ...relay.Hook) (addr string, sink *store.SQLite, dbPath string, idle func()) {
	t.Helper()

	return runExampleProxyWith(t, upstream, false, hooks...)
}

// runExampleProxyWith is runExampleProxy with raw capture switchable, because
// capture is off by default and one test needs it on.
func runExampleProxyWith(t *testing.T, upstream string, capture bool, hooks ...relay.Hook) (addr string, sink *store.SQLite, dbPath string, idle func()) {
	t.Helper()

	return runExampleProxyRecording(t, upstream, capture, nil, hooks...)
}

// runExampleProxyRecording adds a second sink alongside the store, which is how
// the command runs when it is asked to record: one connection, two things
// watching it.
func runExampleProxyRecording(t *testing.T, upstream string, capture bool, extra relay.Sink, hooks ...relay.Hook) (addr string, sink *store.SQLite, dbPath string, idle func()) {
	t.Helper()

	descriptor := protocols.Default()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	dbPath = filepath.Join(t.TempDir(), "relay.db")

	sink, err = store.Open(dbPath, store.WithFlushInterval(20*time.Millisecond))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}

	sinks := relay.Sink(sink)
	if extra != nil {
		sinks = minecraft.NewMultiSink(sink, extra)
	}

	p, err := relay.New(relay.Config{
		Ports:  []relay.PortConfig{{Port: 0, Upstreams: []relay.Upstream{{Addr: upstream}}}},
		Framer: framer,
		NewCodec: func() (relay.Codec, error) {
			return minecraft.NewCodec(descriptor, limits)
		},
		Prober:     minecraft.Prober{Descriptor: descriptor, Timeout: 5 * time.Second},
		Sink:       sinks,
		Hooks:      hooks,
		CaptureRaw: capture,
	})
	if err != nil {
		t.Fatalf("relay.New: %v", err)
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

		_ = sink.Close()
	})

	// idle waits for every session to finish, so a test can close the sink
	// knowing CloseSession has already been called rather than racing it.
	idle = func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if p.SessionCount() == 0 {
				return
			}

			time.Sleep(2 * time.Millisecond)
		}

		t.Fatal("sessions were still live after the exchange finished")
	}

	return addr, sink, dbPath, idle
}

// TestEndToEndStatusExchange is the assertion the whole example exists to make:
// a real client completes a real exchange through the proxy, and the recording
// proves the codec ran rather than that bytes moved.
func TestEndToEndStatusExchange(t *testing.T) {
	up := newStubServer(t)
	addr, sink, dbPath, idle := runExampleProxy(t, up.addr())

	if got := statusClient(t, addr); !strings.Contains(got, "the stub upstream") {
		t.Fatalf("the client was answered with %q, want the stub's document", got)
	}

	// The prober speaks the protocol too, so the stub saw a probe as well as the
	// relayed session. That is the seam working, not noise.
	if up.connections() < 2 {
		t.Fatalf("the stub saw %d connections, want the probe and the session", up.connections())
	}

	idle()

	if err := sink.Close(); err != nil {
		t.Fatalf("close the sink: %v", err)
	}

	db := openDB(t, dbPath)

	var (
		client   string
		upstream string
		closedAt sql.NullString
	)
	row := db.QueryRow(`SELECT client_addr, upstream_addr, closed_at FROM sessions ORDER BY id LIMIT 1`)
	if err := row.Scan(&client, &upstream, &closedAt); err != nil {
		t.Fatalf("scan the session row: %v", err)
	}
	if upstream != up.addr() {
		t.Fatalf("recorded upstream %q, want %q", upstream, up.addr())
	}
	if client == "" {
		t.Fatal("the client address was not recorded")
	}
	if !closedAt.Valid {
		t.Fatal("CloseSession never reached the row")
	}

	// A decoded packet is what proves the codec ran. Bytes alone would have
	// moved with no codec configured at all.
	var named int
	if err := db.QueryRow(`SELECT count(*) FROM messages WHERE packet_name != ''`).Scan(&named); err != nil {
		t.Fatalf("count named messages: %v", err)
	}
	if named < 2 {
		t.Fatalf("%d messages carried a packet name, want at least the handshake and the response", named)
	}

	if dropped := sink.Dropped(); dropped != 0 {
		t.Fatalf("%d records were dropped", dropped)
	}
}

// TestEndToEndHookRewrite proves a hook's SetDecoded reaches the client and the
// recording, which is the whole point of decoding in a proxy.
func TestEndToEndHookRewrite(t *testing.T) {
	up := newStubServer(t)

	rewrite := relay.HookFunc(func(_ context.Context, _ *relay.Session, m *relay.Message) (relay.Action, error) {
		if m.Dir != relay.ToClient || m.Desc.Name != "server_info" {
			return relay.Forward, nil
		}

		replacement, err := protocols.StatusResponse(protocols.Default(), rewrittenDocment)
		if err != nil {
			return relay.Drop, err
		}

		m.SetDecoded(replacement)

		return relay.Replace, nil
	})

	addr, sink, dbPath, idle := runExampleProxy(t, up.addr(), rewrite)

	got := statusClient(t, addr)
	if !strings.Contains(got, "rewritten by a hook") {
		t.Fatalf("the client was answered with %q, want the rewritten document", got)
	}
	if strings.Contains(got, "the stub upstream") {
		t.Fatalf("the client saw the original document as well: %q", got)
	}

	idle()

	if err := sink.Close(); err != nil {
		t.Fatalf("close the sink: %v", err)
	}

	// The sink is handed the final bytes, so it records what the client actually
	// received rather than what the upstream sent.
	var recorded int
	err := openDB(t, dbPath).QueryRow(
		`SELECT count(*) FROM messages WHERE packet_name = 'server_info' AND decoded LIKE '%rewritten by a hook%'`,
	).Scan(&recorded)
	if err != nil {
		t.Fatalf("count rewritten rows: %v", err)
	}
	if recorded == 0 {
		t.Fatal("the sink recorded the original response rather than the rewritten one")
	}
}

// TestProberRejectsAServerThatDoesNotSpeak is the seam the default TCP dial
// cannot reach: something holds the port open and never answers.
func TestProberRejectsAServerThatDoesNotSpeak(t *testing.T) {
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = silent.Close() })

	// Accept and then say nothing at all, which is exactly what a wedged server
	// looks like from outside.
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}

			_ = conn
		}
	}()

	prober := minecraft.Prober{Descriptor: protocols.Default(), Timeout: 500 * time.Millisecond}

	err = prober.Probe(context.Background(), silent.Addr().String())
	if err == nil {
		t.Fatal("a server that never answers was reported healthy")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("the probe reported cancellation rather than the upstream's silence: %v", err)
	}
}

func openDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

// TestEndToEndRawCapture proves the bytes reach the sink's raw_chunks table,
// which is what makes Sink.RawChunk part of the interface rather than a hole in
// it.
func TestEndToEndRawCapture(t *testing.T) {
	up := newStubServer(t)
	addr, sink, dbPath, idle := runExampleProxyWith(t, up.addr(), true)

	if got := statusClient(t, addr); !strings.Contains(got, "the stub upstream") {
		t.Fatalf("the client was answered with %q, want the stub's document", got)
	}

	idle()

	if err := sink.Close(); err != nil {
		t.Fatalf("close the sink: %v", err)
	}

	db := openDB(t, dbPath)

	var chunks, bytesRecorded int
	err := db.QueryRow(`SELECT count(*), coalesce(sum(length(bytes)), 0) FROM raw_chunks`).Scan(&chunks, &bytesRecorded)
	if err != nil {
		t.Fatalf("count raw chunks: %v", err)
	}
	if chunks == 0 {
		t.Fatal("no raw chunks were recorded with CaptureRaw on")
	}
	if bytesRecorded == 0 {
		t.Fatal("raw chunks were recorded but carried no bytes")
	}

	// Both directions of the conversation are there, not just the one that
	// happened to be flushed first.
	for _, dir := range []string{"to_server", "to_client"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM raw_chunks WHERE direction = ?`, dir).Scan(&n); err != nil {
			t.Fatalf("count %s chunks: %v", dir, err)
		}
		if n == 0 {
			t.Fatalf("no %s chunks were recorded", dir)
		}
	}

	// Every chunk belongs to a session row, which is what makes a capture
	// replayable rather than a pile of bytes.
	var orphans int
	err = db.QueryRow(
		`SELECT count(*) FROM raw_chunks WHERE session_id NOT IN (SELECT id FROM sessions)`,
	).Scan(&orphans)
	if err != nil {
		t.Fatalf("count orphan chunks: %v", err)
	}
	if orphans != 0 {
		t.Fatalf("%d raw chunks are not attached to any session", orphans)
	}
}

// TestEndToEndRecording is the evidence that the capture sink works where it
// will actually be used: through a real exchange, on a real connection, rather
// than against hand-built records.
//
// It asserts on packet identities, not on byte counts. A recording that holds
// the right number of unidentifiable frames is exactly the failure the codec
// work before it was meant to close.
func TestEndToEndRecording(t *testing.T) {
	up := newStubServer(t)
	dir := t.TempDir()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	inner, err := java.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	recorder, err := capturesink.NewRecorder(capturesink.Options{
		Dir:        dir,
		Descriptor: protocols.Default(),
		Limits:     limits,
		Framer:     inner,
		OnError:    func(err error) { t.Errorf("recorder reported: %v", err) },
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	addr, _, _, idle := runExampleProxyRecording(t, up.addr(), false, recorder)

	if got := statusClient(t, addr); !strings.Contains(got, "the stub upstream") {
		t.Fatalf("the client was answered with %q, want the stub's document", got)
	}

	idle()

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("the exchange produced %d recordings, want 1", len(matches))
	}

	file, err := os.Open(matches[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = file.Close() }()

	reader, err := capturepkg.NewReader(file)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var names []string
	for {
		record, err := reader.Next()
		if err != nil {
			break
		}
		if record.Kind == capturepkg.KindPacket {
			names = append(names, record.Name)
		}
	}

	if !reader.Complete() {
		t.Error("the recording has no trailer; the session did not close it")
	}
	if len(names) < 3 {
		t.Fatalf("recorded packets %v, want at least the handshake, the request, and the response", names)
	}

	for _, name := range names {
		if name == "" {
			t.Errorf("recorded packets %v include an unidentified one", names)
		}
	}
}
