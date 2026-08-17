package minecraft_test

import (
	"bufio"
	"bytes"
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
// The threshold is a script rather than a constant because the settings
// exercise different bytes and because the setting is reversible. At vanilla's
// default of 256 a small packet travels uncompressed behind a zero-length
// prefix — which is the exact byte that used to be read as a packet ID — and at
// 1 every packet is genuinely deflated.
//
// The scripts with a second entry are the case nothing here had run. Protocol
// 47 has a play-state set compression as well as the login-state one, so a
// threshold can change after the join, and a negative one turns compression
// off. That frame travels compressed and disables compression behind itself,
// the mirror of the login frame that travels in the clear and enables it.
//
// What those two cases hold to account is the codec, the replay gate, and the
// extractor, across an envelope that changes twice: take the play-state
// transition out of the codec and only these two fail.
//
// They do not reach the recorder's withhold-before-advance split, which is worth
// writing down rather than assuming. SensitiveFrame answers false outside the
// login state without reading the frame at all, so a recorder carrying a stale
// threshold through play has nothing to be wrong about — remove the transition
// from capture.advance and every case here still passes. That split is
// load-bearing during the login and only there, which is where the single-entry
// scripts already pin it.
func TestALoginThatNegotiatesCompressionProducesAReplayableCapture(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		script []int32
	}{
		{"vanilla_default_threshold", []int32{256}},
		{"every_packet_compressed", []int32{1}},
		{"re_enabled_higher_in_play", []int32{1, 256}},
		{"disabled_in_play", []int32{256, -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path, _ := recordALogin(t, tc.script)

			assertCompressionWasRecorded(t, path, tc.script)

			// The gate the live procedure runs, run here instead. A capture that
			// does not reproduce itself is not evidence of anything, and this is
			// the assertion that was missing while the stub only spoke status.
			result, err := replaycheck.Check(t.Context(), path)
			if err != nil {
				t.Fatalf("replaycheck.Check: %v", err)
			}
			if !result.OK() {
				t.Errorf("the capture does not replay: %s", result.Explain())
			}
			if result.Records == 0 {
				t.Error("the capture holds no replayable records")
			}

			assertTheTrajectoryCameBack(t, path)
			assertTheArrowCameBack(t, path)
		})
	}
}

// recordALogin runs one login through the proxy against a stub following the
// given threshold script, and returns the recording it produced.
//
// It is a function rather than the body of the test above because the gate's
// own negative case needs a real recording to corrupt, and building one a
// second way would prove something about the second way.
func recordALogin(t *testing.T, script []int32) (path string, descriptor protocol.Protocol) {
	t.Helper()

	descriptor, known := protocols.Resolve("java/1.8.9")
	if !known {
		t.Skip("protocol java/1.8.9 is not registered")
	}

	up := newLoginStub(t, descriptor, script)

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

	return matches[0], descriptor
}

// assertCompressionWasRecorded is the regression assertion for the defect that
// motivated this test: a frame that carries a threshold must keep its body,
// because the threshold is the one field a replay cannot infer.
//
// It asks about every such frame rather than the first, because the last one is
// what decides the envelope every later frame wears. A capture that kept the
// login threshold and lost a change made in play replays into exactly the same
// wall as the original defect, several hundred frames further in.
func assertCompressionWasRecorded(t *testing.T, path string, script []int32) {
	t.Helper()

	records := loginRecords(t, path)

	var (
		reachedPlay bool
		names       []string
		frames      []uint64
	)

	for _, record := range records {
		if record.Kind != capturepkg.KindPacket {
			continue
		}

		names = append(names, string(record.BeforeState)+"/"+record.Name)

		if record.State == protocol.State("play") {
			reachedPlay = true
		}

		if !carriesAThreshold(record) {
			continue
		}

		frames = append(frames, record.Frame)

		if record.Redacted {
			t.Errorf("the %s set compression packet record was withheld; the capture has lost a threshold", record.BeforeState)
		}
		if len(record.Payload) == 0 {
			t.Errorf("the %s set compression packet record carries no body", record.BeforeState)
		}
	}

	if len(frames) != len(script) {
		t.Errorf("the capture holds %d threshold-carrying records, want %d for script %v; it holds %v",
			len(frames), len(script), script, names)
	}
	if !reachedPlay {
		t.Error("the capture never reached play; this test is not covering what it claims to")
	}

	assertTheirRawFramesSurvived(t, records, frames)
}

// carriesAThreshold reports whether a packet record is a set compression.
//
// Matched on state, direction, and ID rather than name, because the name is the
// protocol's to choose and an assertion on it fails for the wrong reason when it
// changes. The state is the one the frame arrived in, which is what separates
// the login packet from the play one: they are different identities.
func carriesAThreshold(record capturepkg.Record) bool {
	if record.Direction != protocol.DirectionClientbound {
		return false
	}

	switch record.BeforeState {
	case protocol.State("login"):
		return record.PacketID == compressPacketID
	case protocol.State("play"):
		return record.PacketID == playCompressPacketID
	default:
		return false
	}
}

// assertTheirRawFramesSurvived follows each threshold-carrying packet record to
// the raw record beside it.
//
// The packet record proves the threshold was decoded; the raw record is the one
// a replay actually reads, and the recorder withholds the pair together. Frame
// numbers correlate them, which is what they are in the format for.
func assertTheirRawFramesSurvived(t *testing.T, records []capturepkg.Record, frames []uint64) {
	t.Helper()

	wanted := make(map[uint64]bool, len(frames))
	for _, frame := range frames {
		wanted[frame] = true
	}

	found := 0

	for _, record := range records {
		if record.Kind != capturepkg.KindRawFrame || !wanted[record.Frame] {
			continue
		}

		found++

		if record.Redacted {
			t.Errorf("raw frame %d was withheld; a replay cannot recover the threshold it carried", record.Frame)
		}
		if len(record.Payload) == 0 {
			t.Errorf("raw frame %d carries no body", record.Frame)
		}
	}

	if found != len(frames) {
		t.Errorf("%d of %d threshold-carrying frames have a raw record", found, len(frames))
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

// TestTheReplayGateRefusesACorruptedThreshold is what makes every verdict above
// worth reading. Each case asks the same gate for the same answer, and a gate
// that cannot say no is a decode log.
//
// The corruption is semantic rather than a flipped bit. The recording is
// rewritten record for record with the threshold that disables compression
// replaced by one that leaves it on, and the writer recomputes every checksum
// and the trailer digest along the way — so the result is a well-formed file
// that is wrong about exactly one thing. Every frame after that record was
// written in the clear and will now be read as deflated.
//
// The faithful copy is the control, and it is not optional: without it a rewrite
// that mangled something else would fail the gate for its own reasons and this
// test would pass without ever having touched the threshold.
func TestTheReplayGateRefusesACorruptedThreshold(t *testing.T) {
	t.Parallel()

	const enabled, disabled int32 = 256, -1

	source, descriptor := recordALogin(t, []int32{enabled, disabled})

	dir := t.TempDir()

	faithful := filepath.Join(dir, "faithful.mccap")
	rewriteCapture(t, source, faithful, func(record capturepkg.Record) capturepkg.Record { return record })

	control, err := replaycheck.Check(t.Context(), faithful)
	if err != nil {
		t.Fatalf("replaycheck.Check on the faithful copy: %v — the rewrite is not faithful", err)
	}
	if !control.OK() {
		t.Fatalf("the faithful copy does not replay: %s — the rewrite is not faithful", control.Explain())
	}

	// Zero rather than the original 256, so the frames after it are judged
	// against a setting that compresses everything. A different positive
	// threshold would change nothing: the read side branches on whether
	// compression is on, not on where the line is.
	corrupt := filepath.Join(dir, "corrupt.mccap")
	frame := theLastThresholdFrame(t, source)
	wrong := thresholdFrameBytes(t, descriptor, enabled, 0)

	rewriteCapture(t, source, corrupt, func(record capturepkg.Record) capturepkg.Record {
		// Only the raw record is rewritten. That is the one a replay decodes;
		// the packet record beside it holds a body the envelope has already
		// been stripped from, which the player counts and does not re-read.
		if record.Kind == capturepkg.KindRawFrame && record.Frame == frame {
			record.Payload = wrong
			record.OriginalLen = len(wrong)
		}

		return record
	})

	result, err := replaycheck.Check(t.Context(), corrupt)
	if err == nil && result.OK() {
		t.Errorf("the gate accepted a capture whose last threshold was corrupted: %d records, %s",
			result.Records, result.Explain())
	}
}

// theLastThresholdFrame reports the frame number of the final set compression in
// a recording, which is the one that decides the envelope every later frame
// wears.
func theLastThresholdFrame(t *testing.T, path string) uint64 {
	t.Helper()

	var frame uint64

	found := false

	for _, record := range loginRecords(t, path) {
		if record.Kind == capturepkg.KindPacket && carriesAThreshold(record) {
			frame, found = record.Frame, true
		}
	}

	if !found {
		t.Fatal("the recording carries no threshold at all")
	}

	return frame
}

// thresholdFrameBytes builds a play-state set compression frame the way the stub
// would have, announcing one threshold while wearing the envelope another one
// established during the login.
//
// The envelope matters as much as the announcement: a frame encoded outside the
// setting the replaying session is in when it arrives would fail to decode on
// its own account, and the gate would then be refusing the wrong thing.
func thresholdFrameBytes(t *testing.T, descriptor protocol.Protocol, inForce, announce int32) []byte {
	t.Helper()

	limits := loginLimits(t)

	framer, err := minecraft.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	session, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	session.SetState(protocol.State("login"))
	advanceBoth(session, protocol.Packet{
		State:     protocol.State("login"),
		Direction: protocol.DirectionClientbound,
		ID:        compressPacketID,
		Value:     &v1_8.LoginClientboundCompress{Threshold: inForce},
	})
	session.SetState(protocol.State("play"))

	encoded, err := session.EncodeFrame(protocol.Packet{
		State:     protocol.State("play"),
		Direction: protocol.DirectionClientbound,
		ID:        playCompressPacketID,
		Value:     &v1_8.PlayClientboundSetCompression{Threshold: announce},
	})
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	var frame bytes.Buffer
	if err := framer.WriteMessage(&frame, encoded); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	return frame.Bytes()
}

// rewriteCapture copies a recording record for record, passing each one through
// mutate on the way out.
//
// It exists so a test can produce a file that is wrong about one thing and right
// about everything else. Byte surgery cannot: each record carries a CRC over its
// own body and the trailer carries a digest over them all, so a hand-edited file
// is refused as damaged before anything reads what it says. Going back through
// the writer recomputes both, which leaves the recording's claims as the only
// thing left to be wrong.
func rewriteCapture(t *testing.T, source, destination string, mutate func(capturepkg.Record) capturepkg.Record) {
	t.Helper()

	file, err := os.Open(source)
	if err != nil {
		t.Fatalf("Open %s: %v", source, err)
	}
	defer func() { _ = file.Close() }()

	reader, err := capturepkg.NewReader(file)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	out, err := os.Create(destination)
	if err != nil {
		t.Fatalf("Create %s: %v", destination, err)
	}
	defer func() { _ = out.Close() }()

	writer, err := capturepkg.NewWriter(out, reader.Header())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for {
		record, err := reader.Next()
		if err != nil {
			break
		}

		stage, replayable := replayableStage(record.Kind)
		if !replayable {
			continue
		}

		record = mutate(record)

		observation := protocol.Observation{
			Sequence:    record.Sequence,
			Frame:       record.Frame,
			Direction:   record.Direction,
			Stage:       stage,
			Elapsed:     record.Elapsed,
			Before:      protocol.NewSnapshot(record.BeforeState, nil),
			After:       protocol.NewSnapshot(record.State, nil),
			OriginalLen: record.OriginalLen,
			Redacted:    record.Redacted,
			Bytes:       record.Payload,
		}
		if record.Kind == capturepkg.KindPacket {
			observation.Packet = &protocol.PacketMetadata{
				State:     record.State,
				Direction: record.Direction,
				ID:        record.PacketID,
				Name:      record.Name,
			}
		}

		if err := writer.Observe(t.Context(), observation); err != nil {
			t.Fatalf("Observe: %v", err)
		}
	}

	if !reader.Complete() {
		t.Fatal("the source recording has no trailer; there is nothing to copy faithfully")
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close the rewritten capture: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("Close %s: %v", destination, err)
	}
}

// replayableStage maps the record kinds back to the observations they were
// written from. A recording taken by this example holds only these two: the
// recorder emits a raw record per frame and a packet record for every frame that
// decoded, and nothing else.
func replayableStage(kind capturepkg.Kind) (protocol.ObservationStage, bool) {
	switch kind {
	case capturepkg.KindRawFrame:
		return protocol.ObservationRawFrame, true
	case capturepkg.KindPacket:
		return protocol.ObservationPacket, true
	case capturepkg.KindTrailer, capturepkg.KindSecret, capturepkg.KindRejected:
	}

	return "", false
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
// compressPacketID is set compression in protocol 47's login state, and
// playCompressPacketID is the one that arrives after the join. They are
// different packets with different transition rules, not one packet seen twice.
const (
	compressPacketID     int32 = 0x03
	playCompressPacketID int32 = 0x46
)

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
//
// The script is the thresholds it will announce, in order. The first is the one
// every real server sends during the login; any after it arrive in play, which
// is where a server that changes its mind puts them.
type loginStub struct {
	ln     net.Listener
	script []int32
}

func newLoginStub(t *testing.T, descriptor protocol.Protocol, script []int32) *loginStub {
	t.Helper()

	if len(script) == 0 {
		t.Fatal("a login stub with no threshold script would never negotiate compression")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	s := &loginStub{ln: ln, script: script}

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
	if !writeLoginPacket(session, framer, conn, compressPacketID, protocol.State("login"), &v1_8.LoginClientboundCompress{Threshold: s.script[0]}) {
		return
	}

	if !writeLoginPacket(session, framer, conn, 0x02, protocol.State("login"), &v1_8.LoginClientboundSuccess{
		UUID:     "d8f0a3c2-0000-4000-8000-000000000001",
		Username: "Stubbed",
	}) {
		return
	}

	// The rest of the script, if there is any, arrives in play. Each of these
	// frames crosses under the setting it is about to replace — the one that
	// disables compression is itself compressed — and everything below it,
	// in both directions, then crosses under the new one. Those later frames
	// are what the assertions read back.
	for _, threshold := range s.script[1:] {
		if !writeLoginPacket(session, framer, conn, playCompressPacketID, protocol.State("play"), &v1_8.PlayClientboundSetCompression{Threshold: threshold}) {
			return
		}
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
		NewCodec: func(*relay.Session) (relay.Codec, error) {
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
