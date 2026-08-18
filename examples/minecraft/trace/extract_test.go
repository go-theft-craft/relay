package trace_test

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/protocols"

	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

// tolerance47 is what protocol 47's own encoding allows, read from the package
// rather than written here: the derivation lives beside the number in
// tolerance.go, and a test with its own copy is a second place for it to drift.
func tolerance47(t *testing.T) float64 {
	t.Helper()

	allowance, err := trace.ToleranceFor(v1_8.Protocol().ID())
	if err != nil {
		t.Fatalf("ToleranceFor 47: %v", err)
	}

	return allowance.Relative
}

func testLimits(t *testing.T) protocol.Limits {
	t.Helper()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	return limits
}

// recorder builds capture records the way the sink does, by encoding through a
// real server-role session. A test that hand-assembled payloads would prove the
// extractor agrees with the test's idea of the wire format.
type recorder struct {
	t       *testing.T
	session protocol.Session
	client  protocol.Session
	next    uint64
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()

	return newRecorderFor(t, v1_8.Protocol())
}

// newRecorderFor builds a recorder for one protocol. The version is a parameter
// because the extractor is one implementation per version and each one has to
// be driven by that version's own encoder.
func newRecorderFor(t *testing.T, descriptor protocol.Protocol) *recorder {
	t.Helper()

	session, err := descriptor.NewSession(protocol.RoleServer, testLimits(t))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session.SetState(protocol.State("play"))

	client, err := descriptor.NewSession(protocol.RoleClient, testLimits(t))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	client.SetState(protocol.State("play"))

	return &recorder{t: t, session: session, client: client}
}

// record builds a clientbound record: something the server said.
func (r *recorder) record(value any) mccapture.Record {
	r.t.Helper()

	return r.encode(r.session, protocol.DirectionClientbound, value)
}

// send builds a serverbound record: something the player said. It is the half
// that holds their own trajectory, so a test about player motion is written
// with this one.
func (r *recorder) send(value any) mccapture.Record {
	r.t.Helper()

	return r.encode(r.client, protocol.DirectionServerbound, value)
}

func (r *recorder) encode(session protocol.Session, dir protocol.Direction, value any) mccapture.Record {
	r.t.Helper()

	identified, ok := value.(interface{ PacketID() int32 })
	if !ok {
		r.t.Fatalf("%T has no PacketID", value)
	}

	packet := protocol.Packet{
		State:     protocol.State("play"),
		Direction: dir,
		ID:        identified.PacketID(),
		Value:     value,
	}

	payload, err := session.EncodeFrame(packet)
	if err != nil {
		r.t.Fatalf("EncodeFrame: %v", err)
	}

	r.next++

	return mccapture.Record{
		Kind:        mccapture.KindPacket,
		Sequence:    r.next,
		Direction:   dir,
		BeforeState: protocol.State("play"),
		State:       protocol.State("play"),
		PacketID:    packet.ID,
		Payload:     payload,
	}
}

func extract(t *testing.T, records []mccapture.Record) []trace.Trace {
	t.Helper()

	traces, err := trace.Extract(v1_8.Protocol(), testLimits(t), records)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	return traces
}

func TestRelativeMovesAccumulateOntoTheLastSpawn(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	records := []mccapture.Record{
		r.record(&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32}),
		// Half a block on X, a quarter on Z, twice.
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16, DZ: 8}),
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16, DZ: 8}),
	}

	traces := extract(t, records)
	if len(traces) != 1 {
		t.Fatalf("extracted %d traces, want 1", len(traces))
	}
	if traces[0].Family != trace.FamilyPlayer {
		t.Errorf("family = %q, want %q", traces[0].Family, trace.FamilyPlayer)
	}

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if math.Abs(last.Position.X-101.0) > tolerance47(t) {
		t.Errorf("X accumulated to %.4f, want 101.0", last.Position.X)
	}
	if math.Abs(last.Position.Z-200.5) > tolerance47(t) {
		t.Errorf("Z accumulated to %.4f, want 200.5", last.Position.Z)
	}
	if math.Abs(last.Position.Y-64.0) > tolerance47(t) {
		t.Errorf("Y drifted to %.4f with no vertical move", last.Position.Y)
	}
}

// TestATeleportResetsRatherThanAccumulates is the other half of the same rule.
// A teleport is absolute, and adding it to the running position would double
// every correction the server sends — which is precisely what the movement
// scenarios in later stages exist to check.
func TestATeleportResetsRatherThanAccumulates(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	traces := extract(t, []mccapture.Record{
		r.record(&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32}),
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 32}),
		r.record(&v1_8.PlayClientboundEntityTeleport{EntityID: 7, X: 10 * 32, Y: 70 * 32, Z: 20 * 32, OnGround: true}),
	})

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if math.Abs(last.Position.X-10.0) > tolerance47(t) {
		t.Errorf("X after a teleport = %.4f, want 10.0", last.Position.X)
	}
	if !last.OnGround {
		t.Error("the teleport reported on-ground and the sample did not")
	}
}

// TestThePlayersOwnMotionIsTraced is the case a clientbound-only extractor
// cannot see at all. A server does not spawn a client to itself and does not
// narrate its walking back to it, so everything the player did between
// corrections exists only in what they reported.
func TestThePlayersOwnMotionIsTraced(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	traces := extract(t, []mccapture.Record{
		r.record(&v1_8.PlayClientboundLogin{EntityID: 42}),
		r.record(&v1_8.PlayClientboundPosition{X: 100, Y: 64, Z: 200}),
		r.send(&v1_8.PlayServerboundPosition{X: 100.5, Y: 64, Z: 200, OnGround: true}),
		r.send(&v1_8.PlayServerboundPositionLook{X: 101, Y: 64, Z: 200.5, OnGround: true}),
	})

	if len(traces) != 1 {
		t.Fatalf("extracted %d traces, want 1", len(traces))
	}
	if traces[0].Family != trace.FamilyPlayer {
		t.Errorf("family = %q, want %q", traces[0].Family, trace.FamilyPlayer)
	}
	if traces[0].EntityID != 42 {
		t.Errorf("EntityID = %d, want 42 from the join packet", traces[0].EntityID)
	}
	if len(traces[0].Samples) != 3 {
		t.Fatalf("%d samples, want 3 — the teleport and both reports", len(traces[0].Samples))
	}

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if math.Abs(last.Position.X-101.0) > tolerance47(t) || math.Abs(last.Position.Z-200.5) > tolerance47(t) {
		t.Errorf("the player ended at %.4f/%.4f, want 101.0/200.5", last.Position.X, last.Position.Z)
	}
	if !last.OnGround {
		t.Error("the player reported on-ground and the sample did not")
	}
}

// TestARelativeTeleportCorrectsRatherThanReplaces covers the flag byte. A
// server nudging a player half a block sends the nudge, not the destination,
// and reading it as absolute would move them to the middle of nowhere.
func TestARelativeTeleportCorrectsRatherThanReplaces(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	traces := extract(t, []mccapture.Record{
		r.record(&v1_8.PlayClientboundLogin{EntityID: 42}),
		r.record(&v1_8.PlayClientboundPosition{X: 100, Y: 64, Z: 200}),
		r.record(&v1_8.PlayClientboundPosition{X: 0.5, Y: 0, Z: 0, Flags: 0x01 | 0x02 | 0x04}),
	})

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if math.Abs(last.Position.X-100.5) > tolerance47(t) {
		t.Errorf("a relative correction put X at %.4f, want 100.5", last.Position.X)
	}
	if math.Abs(last.Position.Y-64.0) > tolerance47(t) {
		t.Errorf("a zero relative Y moved the player to %.4f, want 64.0", last.Position.Y)
	}
}

// TestAFirstPositionThatIsRelativeIsAFinding is the anchor rule applied to the
// player. There is no spawn to correct against, so a correction arriving first
// means the recording started somewhere this extractor cannot reconstruct.
func TestAFirstPositionThatIsRelativeIsAFinding(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	_, err := trace.Extract(v1_8.Protocol(), testLimits(t), []mccapture.Record{
		r.record(&v1_8.PlayClientboundLogin{EntityID: 42}),
		r.record(&v1_8.PlayClientboundPosition{X: 0.5, Flags: 0x01}),
	})

	if !errors.Is(err, trace.ErrUnknownEntity) {
		t.Fatalf("Extract = %v, want ErrUnknownEntity", err)
	}
}

// TestKnockbackOnThePlayerIsNotAnUnknownEntity guards the routing that the
// player's trace needs and no other entity does. Velocity for the player's own
// entity ID finds no spawn, and treating that as a finding would fail every
// extraction of a capture in which anything hit the player.
func TestKnockbackOnThePlayerIsNotAnUnknownEntity(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	traces := extract(t, []mccapture.Record{
		r.record(&v1_8.PlayClientboundLogin{EntityID: 42}),
		r.record(&v1_8.PlayClientboundPosition{X: 100, Y: 64, Z: 200}),
		r.record(&v1_8.PlayClientboundEntityVelocity{EntityID: 42, VelocityX: 8000}),
	})

	if got := traces[0].Samples[len(traces[0].Samples)-1].Velocity.X; math.Abs(got-1.0) > 1e-9 {
		t.Errorf("knockback velocity X = %.6f, want 1.0 blocks per tick", got)
	}
}

func TestAnUnknownEntityIsAFindingNotASilentDrop(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	_, err := trace.Extract(v1_8.Protocol(), testLimits(t), []mccapture.Record{
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 99, DX: 32}),
	})

	if !errors.Is(err, trace.ErrUnknownEntity) {
		t.Fatalf("Extract = %v, want ErrUnknownEntity — a move with no anchor invents a trajectory", err)
	}
}

// TestAReusedEntityIDStartsANewTrace covers the thing that makes runtime
// identifiers dangerous: the server hands the same number out again once the
// entity is gone, and appending to the old trace would splice two trajectories
// into one that neither entity followed.
func TestAReusedEntityIDStartsANewTrace(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	traces := extract(t, []mccapture.Record{
		r.record(&v1_8.PlayClientboundSpawnEntity{EntityID: 12, Type: 2, X: 0, Y: 64 * 32, Z: 0}),
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 12, DX: 32}),
		r.record(&v1_8.PlayClientboundEntityDestroy{EntityIds: []int32{12}}),
		r.record(&v1_8.PlayClientboundSpawnEntity{EntityID: 12, Type: 60, X: 500 * 32, Y: 64 * 32, Z: 0}),
	})

	if len(traces) != 2 {
		t.Fatalf("a reused entity ID produced %d traces, want 2", len(traces))
	}
	if traces[0].Family != trace.FamilyItem {
		t.Errorf("first family = %q, want %q", traces[0].Family, trace.FamilyItem)
	}
	if traces[1].Family != trace.FamilyArrow {
		t.Errorf("second family = %q, want %q", traces[1].Family, trace.FamilyArrow)
	}
	if got := traces[1].Samples[0].Position.X; math.Abs(got-500.0) > tolerance47(t) {
		t.Errorf("the second trace starts at X %.4f, want 500.0 — it inherited the first", got)
	}
}

// TestVelocityDoesNotInventAPosition keeps the extractor honest about what the
// server actually said. Velocity arrives on its own in protocol 47, and adding
// a sample for it would put the entity somewhere no packet reported.
func TestVelocityDoesNotInventAPosition(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	traces := extract(t, []mccapture.Record{
		r.record(&v1_8.PlayClientboundSpawnEntity{EntityID: 3, Type: 60, X: 0, Y: 64 * 32, Z: 0}),
		r.record(&v1_8.PlayClientboundEntityVelocity{EntityID: 3, VelocityX: 8000}),
	})

	if len(traces[0].Samples) != 1 {
		t.Fatalf("velocity added a sample: %d samples, want 1", len(traces[0].Samples))
	}
	if got := traces[0].Samples[0].Velocity.X; math.Abs(got-1.0) > 1e-9 {
		t.Errorf("velocity X = %.6f, want 1.0 blocks per tick", got)
	}
}

// writeRecording writes a real recording and returns its path, so a test about
// damaged files starts from an undamaged one this code actually produced.
func writeRecording(t *testing.T) string {
	t.Helper()

	limits := testLimits(t)
	path := filepath.Join(t.TempDir(), "session.mccap")

	sink, err := mccapture.NewFileSink(path, mccapture.Header{
		Protocol:          v1_8.Protocol().ID(),
		FrameBytes:        limits.FrameBytes(),
		DecompressedBytes: limits.DecompressedBytes(),
		Created:           time.Unix(0, 0).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}

	play := protocol.State("play")
	r := newRecorder(t)

	for i, record := range []mccapture.Record{
		r.record(&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32}),
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 32}),
	} {
		observation := protocol.Observation{
			Sequence:  uint64(i + 1),
			Frame:     uint64(i + 1),
			Direction: protocol.DirectionClientbound,
			Stage:     protocol.ObservationPacket,
			Before:    protocol.NewSnapshot(play, nil),
			After:     protocol.NewSnapshot(play, nil),
			Packet: &protocol.PacketMetadata{
				State:     play,
				Direction: protocol.DirectionClientbound,
				ID:        record.PacketID,
			},
			Bytes:       record.Payload,
			OriginalLen: len(record.Payload),
		}

		if err := sink.Observe(t.Context(), observation); err != nil {
			t.Fatalf("Observe: %v", err)
		}
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	return path
}

// TestATruncatedRecordingIsAnErrorNotAShortTrace is the difference between a
// tool and an oracle. A file that stops early still yields records, and
// returning the trajectories built from them with no error beside them hands a
// caller a trace that ends where the disk did and looks exactly like one that
// ended where the session did.
func TestATruncatedRecordingIsAnErrorNotAShortTrace(t *testing.T) {
	t.Parallel()

	path := writeRecording(t)

	complete, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if _, _, err := trace.ExtractFile(path); err != nil {
		t.Fatalf("the undamaged recording did not extract: %v", err)
	}

	// Four bytes short of the end lands inside the trailer's checksum, which is
	// what a killed process or a full disk leaves behind.
	if err := os.WriteFile(path, complete[:len(complete)-4], 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, _, err := trace.ExtractFile(path); err == nil {
		t.Fatal("ExtractFile on a truncated recording returned no error")
	}
}

// TestARecordingThatDecodesNothingIsAnErrorNotAnEmptyResult reproduces the
// shape of the first live capture taken against a compressing vanilla server.
// Every stored payload still wore its compression envelope while the decoding
// session believed none had been negotiated, so the leading data-length byte
// read as a packet ID and every play packet failed. Skipping them one at a time
// produced a successful run with no trajectories in it — which is indisputable
// evidence that nothing moved, and was nothing of the kind.
func TestARecordingThatDecodesNothingIsAnErrorNotAnEmptyResult(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	records := []mccapture.Record{
		r.record(&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32}),
		r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16}),
	}

	// A zero data-length varint in front of each payload is what an unstripped
	// uncompressed envelope looks like to a session that is not expecting one.
	for i := range records {
		records[i].Payload = append([]byte{0x00}, records[i].Payload...)
	}

	traces, err := trace.Extract(v1_8.Protocol(), testLimits(t), records)
	if !errors.Is(err, trace.ErrNoTrajectories) {
		t.Fatalf("Extract = (%d traces, %v), want ErrNoTrajectories", len(traces), err)
	}
}

// TestSomeFramesParsingDoesNotMakeARecordingReadable is the case that killed
// the first version of this guard, which asked whether anything decoded rather
// than whether anything moved.
//
// Over a real unreadable capture, 431 of 7954 play records decoded and none of
// them carried motion. Protocol 47 has zero-field packets, so a frame whose
// first byte lands on one parses no matter what follows it, and a handful of
// those keeps a decode counter off zero indefinitely while the file is
// gibberish. Here keep-alive plays that part.
func TestSomeFramesParsingDoesNotMakeARecordingReadable(t *testing.T) {
	t.Parallel()

	r := newRecorder(t)
	spawn := r.record(&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32})
	move := r.record(&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16})
	spawn.Payload = append([]byte{0x00}, spawn.Payload...)
	move.Payload = append([]byte{0x00}, move.Payload...)

	traces, err := trace.Extract(v1_8.Protocol(), testLimits(t), []mccapture.Record{
		// Decodes cleanly, and says nothing about where anything is.
		r.record(&v1_8.PlayClientboundKeepAlive{KeepAliveID: 1}),
		spawn,
		move,
		r.record(&v1_8.PlayClientboundKeepAlive{KeepAliveID: 2}),
	})

	if !errors.Is(err, trace.ErrNoTrajectories) {
		t.Fatalf("Extract = (%d traces, %v), want ErrNoTrajectories", len(traces), err)
	}
}

// TestACaptureThatNeverReachedPlayIsNotAFailure is the other side of that rule.
// A recording that ends during login holds no trajectories because there were
// none to hold, and reporting that as a decode failure would make the gate cry
// wolf on every status ping and every rejected login.
func TestACaptureThatNeverReachedPlayIsNotAFailure(t *testing.T) {
	t.Parallel()

	traces, err := trace.Extract(v1_8.Protocol(), testLimits(t), []mccapture.Record{{
		Kind:        mccapture.KindPacket,
		Sequence:    1,
		Direction:   protocol.DirectionClientbound,
		BeforeState: protocol.State("login"),
		State:       protocol.State("login"),
		Payload:     []byte{0x7f, 0x00},
	}})
	if err != nil {
		t.Fatalf("Extract on a login-only recording: %v", err)
	}
	if len(traces) != 0 {
		t.Errorf("extracted %d traces from a login-only recording, want 0", len(traces))
	}
}

// unregistered is a descriptor for a version no rule set reads. It stands for
// the next protocol to exist rather than for one that does: 47 and 775 both
// have rule sets now, so refusing a real version would only test that the
// registry is incomplete.
type unregistered struct{ protocol.Protocol }

func (unregistered) ID() string { return "java/0.0.0" }

// TestAnotherProtocolIsRefused states the boundary rather than guessing across
// it. Decoding one version with another's packet identifiers and scales would
// produce numbers that look like a trajectory and are not one.
func TestAnotherProtocolIsRefused(t *testing.T) {
	t.Parallel()

	descriptor := unregistered{Protocol: protocols.Default()}
	if _, err := trace.Extract(descriptor, testLimits(t), nil); !errors.Is(err, trace.ErrUnsupportedProtocol) {
		t.Fatalf("Extract on %s = %v, want ErrUnsupportedProtocol", descriptor.ID(), err)
	}
}
