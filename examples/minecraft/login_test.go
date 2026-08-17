package minecraft_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	capturepkg "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
	capturesink "github.com/go-theft-craft/relay/examples/minecraft/capture"
	"github.com/go-theft-craft/relay/examples/minecraft/replaycheck"
	"github.com/go-theft-craft/relay/examples/minecraft/store"
	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

// The end-to-end tests beside this one speak a status ping, which is the whole
// protocol a server-list query needs and nothing like a session. Nothing in this
// repository had ever negotiated compression or reached play until a real 1.8.9
// server was put behind the proxy by hand, and the first capture taken that way
// did not replay: the frame that turns compression on was being withheld as
// though it carried key material, and every frame after it was then stored
// wearing an envelope no replay knew about.
//
// A live procedure found that, and a live procedure costs a person with a
// Minecraft client. What follows is that run reduced to a stub: a login, a
// negotiated threshold, and two frames of play in each direction, asserted with
// the same gate the live procedure uses. The class of defect it covers is
// everything that only happens once a connection stops being a ping.
//
// The threshold is a table rather than a constant because the two settings
// exercise different bytes. At vanilla's default of 256 a small packet travels
// uncompressed behind a zero-length prefix — which is the exact byte that used to
// be read as a packet ID — and at 1 every packet is genuinely deflated.
func TestALoginThatNegotiatesCompressionProducesAReplayableCapture(t *testing.T) {
	t.Parallel()

	for _, threshold := range []int32{256, 1} {
		t.Run(thresholdName(threshold), func(t *testing.T) {
			t.Parallel()

			descriptor, known := protocols.Resolve("java/1.8.9")
			if !known {
				t.Skip("protocol java/1.8.9 is not registered")
			}

			up := newLoginStub(t, descriptor, threshold)

			dir := t.TempDir()
			limits := loginLimits(t)

			inner, err := java.NewFramer(limits)
			if err != nil {
				t.Fatalf("NewFramer: %v", err)
			}

			recorder, err := capturesink.NewRecorder(capturesink.Options{
				Dir:        dir,
				Descriptor: descriptor,
				Limits:     limits,
				Framer:     inner,
				OnError:    func(err error) { t.Errorf("recorder reported: %v", err) },
			})
			if err != nil {
				t.Fatalf("NewRecorder: %v", err)
			}

			addr, idle := runLoginProxy(t, descriptor, up.addr(), recorder)

			loginClient(t, descriptor, addr)
			idle()

			matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
			if err != nil {
				t.Fatalf("Glob: %v", err)
			}
			if len(matches) != 1 {
				t.Fatalf("the login produced %d recordings, want 1", len(matches))
			}

			assertCompressionWasRecorded(t, matches[0], threshold)

			// The gate the live procedure runs, run here instead. A capture that
			// does not reproduce itself is not evidence of anything, and this is
			// the assertion that was missing while the stub only spoke status.
			result, err := replaycheck.Check(t.Context(), matches[0])
			if err != nil {
				t.Fatalf("replaycheck.Check: %v", err)
			}
			if !result.OK() {
				t.Errorf("the capture does not replay: %s", result.Explain())
			}
			if result.Records == 0 {
				t.Error("the capture holds no replayable records")
			}

			assertTheTrajectoryCameBack(t, matches[0])
			assertTheArrowCameBack(t, matches[0])
		})
	}
}

func thresholdName(threshold int32) string {
	if threshold == 1 {
		return "every_packet_compressed"
	}

	return "vanilla_default_threshold"
}

// assertCompressionWasRecorded is the regression assertion for the defect that
// motivated this test: the frame that enables compression must keep its body,
// because the threshold it carries is the one field a replay cannot infer.
func assertCompressionWasRecorded(t *testing.T, path string, threshold int32) {
	t.Helper()

	var found, reachedPlay bool

	var names []string

	for _, record := range loginRecords(t, path) {
		if record.Kind != capturepkg.KindPacket {
			continue
		}

		names = append(names, string(record.BeforeState)+"/"+record.Name)

		if record.State == protocol.State("play") {
			reachedPlay = true
		}

		// Matched on state and ID rather than name, because the name is the
		// protocol's to choose and an assertion on it fails for the wrong
		// reason when it changes.
		if record.BeforeState != protocol.State("login") || record.PacketID != compressPacketID {
			continue
		}

		found = true

		if record.Redacted {
			t.Error("the set compression frame was withheld; the capture has lost the threshold")
		}
		if len(record.Payload) == 0 {
			t.Error("the set compression record carries no body")
		}
	}

	if !found {
		t.Errorf("no set compression record at threshold %d; the capture holds %v", threshold, names)
	}
	if !reachedPlay {
		t.Error("the capture never reached play; this test is not covering what it claims to")
	}
}

// assertTheTrajectoryCameBack proves the play frames are readable after the
// fact, not merely present. Decoding them is what the recorder is for, and under
// compression it is what stopped working.
func assertTheTrajectoryCameBack(t *testing.T, path string) {
	t.Helper()

	traces, _, err := trace.ExtractFile(path)
	if err != nil {
		t.Fatalf("trace.ExtractFile: %v", err)
	}

	for _, candidate := range traces {
		if candidate.Family != trace.FamilyPlayer {
			continue
		}

		if len(candidate.Samples) < 2 {
			t.Errorf("the player trace holds %d samples, want the teleport and the step", len(candidate.Samples))
		}

		// The stub teleports to a known place and the client steps one block
		// east of it. Anything else is a scale or an axis fault, which is the
		// other thing the live procedure watches for.
		last := candidate.Samples[len(candidate.Samples)-1]
		if last.Position.X != stubWalkX || last.Position.Y != stubSpawnY || last.Position.Z != stubSpawnZ {
			t.Errorf("the player ended at (%v, %v, %v), want (%v, %v, %v)",
				last.Position.X, last.Position.Y, last.Position.Z, stubWalkX, stubSpawnY, stubSpawnZ)
		}

		return
	}

	t.Error("the capture produced no player trace")
}

// assertTheArrowCameBack covers the other half of the extractor: motion the
// server narrated about an entity the client never sent a packet for, including
// a relative move, which is the form most motion arrives in.
func assertTheArrowCameBack(t *testing.T, path string) {
	t.Helper()

	traces, _, err := trace.ExtractFile(path)
	if err != nil {
		t.Fatalf("trace.ExtractFile: %v", err)
	}

	for _, candidate := range traces {
		if candidate.Family != trace.FamilyArrow {
			continue
		}

		if len(candidate.Samples) != 2 {
			t.Fatalf("the arrow trace holds %d samples, want the spawn and the move", len(candidate.Samples))
		}

		spawn := candidate.Samples[0].Position
		if spawn.X != stubSpawnX || spawn.Y != stubSpawnY || spawn.Z != stubSpawnZ {
			t.Errorf("the arrow spawned at (%v, %v, %v), want (%v, %v, %v) — a fixed-point scale or an axis fault",
				spawn.X, spawn.Y, spawn.Z, stubSpawnX, stubSpawnY, stubSpawnZ)
		}

		// One block east of the spawn: the relative move accumulated onto it
		// rather than replacing it or being read at the wrong scale.
		moved := candidate.Samples[1].Position
		if moved.X != stubSpawnX+1 || moved.Y != stubSpawnY || moved.Z != stubSpawnZ {
			t.Errorf("the arrow moved to (%v, %v, %v), want (%v, %v, %v)",
				moved.X, moved.Y, moved.Z, stubSpawnX+1, stubSpawnY, stubSpawnZ)
		}

		return
	}

	t.Error("the capture produced no arrow trace")
}

func loginRecords(t *testing.T, path string) []capturepkg.Record {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = file.Close() }()

	reader, err := capturepkg.NewReader(file)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var records []capturepkg.Record
	for {
		record, err := reader.Next()
		if err != nil {
			break
		}

		records = append(records, record)
	}

	if !reader.Complete() {
		t.Error("the recording has no trailer; the session did not close it")
	}

	return records
}

// Where the stub puts the player and where the client walks to. They are
// constants so the assertion can name the number it expects rather than
// recomputing it.
// compressPacketID is set compression in protocol 47's login state.
const compressPacketID = 0x03

// The arrow the stub spawns. Type 60 is protocol 47's arrow, and the ID is
// arbitrary beyond being one the join never claimed.
const (
	stubArrowID   int32 = 4242
	stubArrowType int8  = 60
)

const (
	stubSpawnX = 8.5
	stubSpawnY = 65.0
	stubSpawnZ = -4.5
	stubWalkX  = 9.5
)

// loginStub speaks a login rather than a ping: handshake, login start, set
// compression, login success, and then one clientbound position so the client
// has somewhere to walk from.
//
// It is deliberately a separate stub from the status one. Teaching that stub to
// log in as well would have left one function answering two protocols, and the
// point of this one is that it reaches play — which the status stub must never
// do, because tests depend on it stopping.
type loginStub struct {
	ln        net.Listener
	threshold int32
}

func newLoginStub(t *testing.T, descriptor protocol.Protocol, threshold int32) *loginStub {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	s := &loginStub{ln: ln, threshold: threshold}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go s.serve(t, descriptor, conn)
		}
	}()

	return s
}

func (s *loginStub) addr() string { return s.ln.Addr().String() }

func (s *loginStub) serve(t *testing.T, descriptor protocol.Protocol, conn net.Conn) {
	defer func() { _ = conn.Close() }()

	limits := loginLimits(t)

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		return
	}

	session, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		return
	}

	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	reader := bufio.NewReader(conn)

	// The handshake first, because it says which of two conversations this is.
	// The proxy probes an upstream with a status ping before it will forward
	// anything to it, so a stub that only spoke login would never be reached:
	// the probe would fail and the session would be refused as unhealthy.
	raw, err := framer.ReadMessage(reader)
	if err != nil {
		return
	}

	handshake, err := session.DecodeFrame(raw)
	if err != nil {
		return
	}

	advanceBoth(session, handshake)

	fields, ok := protocols.ReadHandshake(handshake)
	if !ok {
		return
	}

	if fields.NextState == 1 {
		s.serveStatus(descriptor, session, framer, conn, reader)

		return
	}

	// Login start, which names the player and is the last frame before the
	// threshold applies.
	raw, err = framer.ReadMessage(reader)
	if err != nil {
		return
	}

	start, err := session.DecodeFrame(raw)
	if err != nil {
		return
	}

	advanceBoth(session, start)

	// Set compression, then login success. The order is the protocol's: the
	// threshold applies to every frame after the packet that announces it,
	// including the success that follows immediately.
	if !writeLoginPacket(session, framer, conn, 0x03, protocol.State("login"), &v1_8.LoginClientboundCompress{Threshold: s.threshold}) {
		return
	}

	if !writeLoginPacket(session, framer, conn, 0x02, protocol.State("login"), &v1_8.LoginClientboundSuccess{
		UUID:     "d8f0a3c2-0000-4000-8000-000000000001",
		Username: "Stubbed",
	}) {
		return
	}

	// One clientbound teleport, which is what anchors a player's trace: a client
	// is not spawned to itself, so nothing else says where it starts.
	if !writeLoginPacket(session, framer, conn, 0x08, protocol.State("play"), &v1_8.PlayClientboundPosition{
		X: stubSpawnX, Y: stubSpawnY, Z: stubSpawnZ, Yaw: 0, Pitch: 0, Flags: 0,
	}) {
		return
	}

	// An arrow, spawned and then moved. The player's own trajectory comes back
	// through a different path than everything else's — the client reports it,
	// the server narrates the rest — so a capture that only proved the player
	// would leave the half that carries every other entity untested.
	if !writeLoginPacket(session, framer, conn, 0x0e, protocol.State("play"), &v1_8.PlayClientboundSpawnEntity{
		EntityID: stubArrowID,
		Type:     stubArrowType,
		X:        int32(stubSpawnX * 32),
		Y:        int32(stubSpawnY * 32),
		Z:        int32(stubSpawnZ * 32),
		ObjectData: v1_8.PlayClientboundSpawnEntityObjectDataSwitch{
			Default: v1_8.PlayClientboundSpawnEntityObjectDataSwitchDefault{VelocityX: 0, VelocityY: 0, VelocityZ: 0},
		},
	}) {
		return
	}

	// A relative move, which is how the server reports most motion. It is sent
	// in thirty-seconds of a block, so this is one block east and nothing else —
	// the same axis the player walks, which would catch a transposition that
	// happened to be symmetric.
	if !writeLoginPacket(session, framer, conn, 0x15, protocol.State("play"), &v1_8.PlayClientboundRelEntityMove{
		EntityID: stubArrowID,
		DX:       32,
		DY:       0,
		DZ:       0,
		OnGround: false,
	}) {
		return
	}

	// Then read whatever the client reports until it goes away. The frames
	// matter more than the answers: they are the serverbound half of a play
	// session, and they are what a capture of one has to hold.
	for {
		raw, err := framer.ReadMessage(reader)
		if err != nil {
			return
		}

		packet, err := session.DecodeFrame(raw)
		if err != nil {
			return
		}

		advanceBoth(session, packet)
	}
}

// serveStatus answers the proxy's probe. It is the same exchange the status stub
// beside this one exists for, kept here because a login stub that cannot be
// probed is a login stub that is never dialled.
func (s *loginStub) serveStatus(descriptor protocol.Protocol, session protocol.Session, framer *minecraft.Framer, conn net.Conn, reader *bufio.Reader) {
	for {
		raw, err := framer.ReadMessage(reader)
		if err != nil {
			return
		}

		packet, err := session.DecodeFrame(raw)
		if err != nil {
			return
		}

		advanceBoth(session, packet)

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

// loginClient is the client half: it logs in, follows the threshold it is given,
// and reports one step east of wherever it was put.
func loginClient(t *testing.T, descriptor protocol.Protocol, addr string) {
	t.Helper()

	limits := loginLimits(t)

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

	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}

	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		t.Fatalf("port: %v", err)
	}

	// Next state 2 is login, where 1 would be status. That single byte is the
	// difference between this test and the ones beside it.
	handshake, err := protocols.Handshake(descriptor, host, uint16(port), 2)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	writePacket(t, framer, session, conn, handshake)
	session.SetState(protocol.State("login"))

	writePacket(t, framer, session, conn, protocol.Packet{
		State:     protocol.State("login"),
		Direction: protocol.DirectionServerbound,
		ID:        0x00,
		Name:      "login/login_start",
		Value:     &v1_8.LoginServerboundLoginStart{Username: "Stubbed"},
	})

	reader := bufio.NewReader(conn)

	for {
		raw, err := framer.ReadMessage(reader)
		if err != nil {
			t.Fatalf("read a login frame: %v", err)
		}

		packet, err := session.DecodeFrame(raw)
		if err != nil {
			t.Fatalf("decode a login frame: %v", err)
		}

		advanceBoth(session, packet)

		// The teleport is the cue: the client now knows where it is, so it can
		// report having moved, which is the only way a player's own trajectory
		// ever reaches a capture.
		if position, ok := packet.Value.(*v1_8.PlayClientboundPosition); ok {
			writePacket(t, framer, session, conn, protocol.Packet{
				State:     protocol.State("play"),
				Direction: protocol.DirectionServerbound,
				ID:        0x06,
				Name:      "play/position_look",
				Value: &v1_8.PlayServerboundPositionLook{
					X: position.X + 1, Y: position.Y, Z: position.Z,
					Yaw: 0, Pitch: 0, OnGround: true,
				},
			})

			// Give the proxy a moment to record the frame before the socket
			// closes under it. A capture missing its last frame would fail the
			// gate for a reason that has nothing to do with what is being tested.
			time.Sleep(200 * time.Millisecond)

			return
		}
	}
}

// advanceBoth applies whatever transition a packet implies, which is what moves
// a session out of login and turns compression on. It mirrors what the codec
// does, because a stub that did not follow the protocol would be testing a
// conversation neither peer could have had.
func advanceBoth(session protocol.Session, packet protocol.Packet) {
	transition, proposed, err := session.ProposeTransition(packet)
	if err != nil || !proposed {
		return
	}

	if session.ValidateTransition(transition) == nil {
		session.ApplyTransition(transition)
	}
}

func writeLoginPacket(session protocol.Session, framer *minecraft.Framer, w io.Writer, id int32, state protocol.State, value any) bool {
	packet := protocol.Packet{
		State:     state,
		Direction: protocol.DirectionClientbound,
		ID:        id,
		Value:     value,
	}

	encoded, err := session.EncodeFrame(packet)
	if err != nil {
		return false
	}
	if err := framer.WriteMessage(w, encoded); err != nil {
		return false
	}

	advanceBoth(session, packet)

	return true
}

func loginLimits(t *testing.T) protocol.Limits {
	t.Helper()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	return limits
}

// runLoginProxy is the example proxy for a chosen protocol. The runner beside it
// hardcodes the default, which is right for every test that speaks status and
// wrong here: a capture recorded under one protocol's header and replayed under
// another proves nothing.
func runLoginProxy(t *testing.T, descriptor protocol.Protocol, upstream string, extra relay.Sink) (addr string, idle func()) {
	t.Helper()

	limits := loginLimits(t)

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	sink, err := store.Open(filepath.Join(t.TempDir(), "relay.db"), store.WithFlushInterval(20*time.Millisecond))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}

	p, err := relay.New(relay.Config{
		Ports:  []relay.PortConfig{{Port: 0, Upstreams: []relay.Upstream{{Addr: upstream}}}},
		Framer: framer,
		NewCodec: func() (relay.Codec, error) {
			return minecraft.NewCodec(descriptor, limits)
		},
		Prober: minecraft.Prober{Descriptor: descriptor, Timeout: 5 * time.Second},
		Sink:   minecraft.NewMultiSink(sink, extra),
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

	idle = func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if p.SessionCount() == 0 {
				return
			}

			time.Sleep(2 * time.Millisecond)
		}

		t.Fatal("sessions were still live after the login finished")
	}

	return addr, idle
}
