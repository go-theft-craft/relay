package trace

import (
	"errors"
	"fmt"
	"os"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
)

// ErrUnsupportedProtocol reports a recording this extractor cannot read.
//
// Only protocol 47 is implemented. The movement packets, their fixed-point
// scales, and the set of spawn packets are all version-specific, so a second
// version is a second implementation rather than a flag — and claiming support
// by decoding 775 with 47's rules would produce plausible numbers that are
// quietly wrong.
var ErrUnsupportedProtocol = errors.New("trace: unsupported protocol")

// ErrUnknownEntity reports a movement for an entity that never spawned.
//
// It is an error rather than a skipped record. A relative move with no anchor
// means the recording is incomplete or this extractor is wrong, and starting
// the trace at the origin instead would invent a trajectory nobody observed —
// which is exactly the kind of plausible artefact an oracle must never produce.
var ErrUnknownEntity = errors.New("trace: movement for an entity that never spawned")

// statePlay is the only connection state a trajectory can come from.
const statePlay = protocol.State("play")

// ErrUndecodable reports a recording whose play packets this build could not
// read at all.
//
// It exists because the alternative is worse than a crash: an empty trace list
// and a zero exit code, which reads as "nothing moved" when it means "nothing
// was read". The first capture taken through a proxy against a compressing
// vanilla server did exactly that — 15916 records skipped one at a time, every
// play packet mis-read, and a successful-looking run with no trajectories in it.
var ErrUndecodable = errors.New("trace: the recording's play packets did not decode")

// Extract accumulates every entity's absolute motion from one recording.
//
// It takes the descriptor and limits rather than deriving them, because the
// caller has already opened the file and read its header; ExtractFile is the
// convenience that does both.
//
// Traces come back in the order their entities first appeared, so a diff
// between two runs of the same scenario lines up.
func Extract(descriptor protocol.Protocol, limits protocol.Limits, records []mccapture.Record) ([]Trace, error) {
	if descriptor == nil {
		return nil, fmt.Errorf("%w: no descriptor", ErrUnsupportedProtocol)
	}
	if descriptor.ID() != v1_8.Protocol().ID() {
		return nil, fmt.Errorf("%w: %q, want %q", ErrUnsupportedProtocol, descriptor.ID(), v1_8.Protocol().ID())
	}

	client, err := descriptor.NewSession(protocol.RoleClient, limits)
	if err != nil {
		return nil, fmt.Errorf("trace: build the clientbound decoding session: %w", err)
	}

	server, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		return nil, fmt.Errorf("trace: build the serverbound decoding session: %w", err)
	}

	e := &extractor{client: client, server: server, live: make(map[int32]*Trace)}

	for _, record := range records {
		if err := e.consume(record); err != nil {
			return nil, err
		}
	}

	// A recording whose play half did not decode at all produced no trajectories
	// because nothing was read, not because nothing moved, and those two look
	// identical in the output. Skipping one unreadable packet is deliberate;
	// skipping every one of them and reporting success is the same failure
	// ExtractFile guards at the other end of the file, arriving through a
	// different door.
	//
	// The rule is all-or-nothing on purpose. A capture legitimately holds frames
	// this build cannot parse, so any threshold below "none of them" would be a
	// number nobody could defend — while none at all can only mean the session
	// was decoding under different rules than the ones that wrote the file.
	if e.playOffered > 0 && e.playDecoded == 0 {
		return nil, fmt.Errorf("%w: %d play records, none decoded", ErrUndecodable, e.playOffered)
	}

	return e.done(), nil
}

// extractor holds the decode sessions and the traces being built.
//
// There are two sessions because a capture holds both halves of the
// conversation and each half decodes under the opposite role. The clientbound
// half carries every other entity's motion; the serverbound half carries the
// connecting player's own, and nothing else does. A client is not spawned to
// itself, and after the server's opening teleport it reports where it walked
// rather than being told — so an extractor reading only clientbound frames
// produces traces for every entity in the world except the one the session was
// about.
type extractor struct {
	client protocol.Session
	server protocol.Session

	// live maps a runtime entity ID to the trace currently accumulating for it.
	// The server reuses an ID once the entity is gone, so a spawn for an ID
	// already present closes the previous trace rather than appending to it.
	live map[int32]*Trace
	// order preserves first-appearance order, which a map cannot.
	order []*Trace

	// player is the connecting client's own trace. It is held apart from live
	// because it is not keyed by a spawn: the join packet names the entity ID
	// before any packet says where it is, and the position arrives later from
	// either direction.
	player *Trace
	// self is the entity ID the join packet named, and named reports whether
	// one did. A capture that starts after the join still traces the player;
	// the trace just carries a zero ID, which is honest about not knowing.
	self  int32
	named bool

	// playOffered and playDecoded count the play-state packet records this
	// extractor was handed and the ones it could read. They are counted for the
	// play state alone because that is the only state trajectories come from: a
	// capture that ends during login is complete and traceless, and must not be
	// confused with one that decoded nothing.
	playOffered int
	playDecoded int
}

// consume applies one record.
//
// Every decodable packet is offered to its session's transition rules first,
// including ones this extractor has no interest in, because the transitions
// that enable compression and move the connection into play come from packets
// that carry no motion at all.
func (e *extractor) consume(record mccapture.Record) error {
	if record.Kind != mccapture.KindPacket || record.Redacted || len(record.Payload) == 0 {
		return nil
	}

	session := e.client
	if record.Direction != protocol.DirectionClientbound {
		session = e.server
	}

	// The recording says which state the frame belonged to. Trusting it lets a
	// capture that starts mid-session decode, which re-deriving the state from
	// the first handshake could not. Both sessions are moved, because the state
	// belongs to the connection rather than to a direction.
	if record.BeforeState != "" {
		if record.BeforeState != e.client.State() {
			e.client.SetState(record.BeforeState)
		}
		if record.BeforeState != e.server.State() {
			e.server.SetState(record.BeforeState)
		}
	}

	play := session.State() == statePlay
	if play {
		e.playOffered++
	}

	packet, err := session.DecodeFrame(record.Payload)
	if err != nil {
		// An undecodable packet is not fatal. A recording is expected to hold
		// frames this build cannot parse — that is why the raw records exist —
		// and an entity trace is not made wrong by a chat message it skipped.
		// Extract counts them, though, because a file in which every one of them
		// failed is not a file with nothing in it.
		return nil
	}

	if play {
		e.playDecoded++
	}

	if transition, ok, err := session.ProposeTransition(packet); err == nil && ok {
		for _, target := range []protocol.Session{e.client, e.server} {
			if target.ValidateTransition(transition) == nil {
				target.ApplyTransition(transition)
			}
		}
	}

	return e.apply(record, packet)
}

// apply is the packet-to-motion mapping, written as one switch so a reader
// tracing a wrong trajectory can see every packet that could have produced it.
func (e *extractor) apply(record mccapture.Record, packet protocol.Packet) error {
	switch value := packet.Value.(type) {
	// The join packet is the only place the connecting player's own entity ID
	// appears. It carries no position, so it opens no trace.
	case *v1_8.PlayClientboundLogin:
		e.self, e.named = value.EntityID, true

	// The server's teleport places the player, and its flag byte says which
	// axes are corrections rather than placements.
	case *v1_8.PlayClientboundPosition:
		// A teleport says nothing about footing, so the sample keeps whatever
		// the player last reported rather than claiming they left the ground.
		return e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, value.Flags, e.playerOnGround())

	// What the player did between corrections is only ever reported by the
	// player. Walking, sprinting, jumping, and falling all arrive here.
	case *v1_8.PlayServerboundPosition:
		return e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, 0, value.OnGround)

	case *v1_8.PlayServerboundPositionLook:
		return e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, 0, value.OnGround)

	case *v1_8.PlayClientboundNamedEntitySpawn:
		e.spawn(record, value.EntityID, FamilyPlayer, fixed(value.X, value.Y, value.Z), Vec3{})

	case *v1_8.PlayClientboundSpawnEntity:
		data := value.ObjectData.Default
		e.spawn(record, value.EntityID, objectFamily(value.Type), fixed(value.X, value.Y, value.Z), velocity(data.VelocityX, data.VelocityY, data.VelocityZ))

	case *v1_8.PlayClientboundSpawnEntityLiving:
		e.spawn(record, value.EntityID, FamilyLiving, fixed(value.X, value.Y, value.Z), velocity(value.VelocityX, value.VelocityY, value.VelocityZ))

	case *v1_8.PlayClientboundSpawnEntityExperienceOrb:
		e.spawn(record, value.EntityID, FamilyExperienceOrb, fixed(value.X, value.Y, value.Z), Vec3{})

	case *v1_8.PlayClientboundEntityTeleport:
		return e.absolute(record, value.EntityID, fixed(value.X, value.Y, value.Z), value.OnGround)

	case *v1_8.PlayClientboundRelEntityMove:
		return e.relative(record, value.EntityID, delta(value.DX, value.DY, value.DZ), value.OnGround)

	case *v1_8.PlayClientboundEntityMoveLook:
		return e.relative(record, value.EntityID, delta(value.DX, value.DY, value.DZ), value.OnGround)

	case *v1_8.PlayClientboundEntityVelocity:
		return e.velocity(value.EntityID, velocity(value.VelocityX, value.VelocityY, value.VelocityZ))

	case *v1_8.PlayClientboundEntityDestroy:
		for _, id := range value.EntityIds {
			e.close(id)
		}
	}

	return nil
}

// The bits protocol 47's teleport sets to mean "add this to where you already
// are" rather than "you are here". The rotation bits are ignored: a trace holds
// no rotation, so reading them would only invite the reader to think it does.
const (
	positionRelativeX int8 = 0x01
	positionRelativeY int8 = 0x02
	positionRelativeZ int8 = 0x04
)

// playerOnGround reports the footing the player last claimed, and false before
// they have claimed any.
func (e *extractor) playerOnGround() bool {
	if e.player == nil {
		return false
	}

	return last(e.player).OnGround
}

// playerAt adds a sample to the connecting player's own trace, opening it on
// the first position either side reports.
//
// Unlike every other entity there is no spawn packet to anchor this trace, so
// the first position opens it. That is safe for an absolute position and not
// for a relative one: a correction with nothing to correct has no anchor, and
// starting it from the origin would draw a trajectory from a place the player
// never was.
func (e *extractor) playerAt(record mccapture.Record, at Vec3, flags int8, onGround bool) error {
	if e.player == nil {
		if flags&(positionRelativeX|positionRelativeY|positionRelativeZ) != 0 {
			return fmt.Errorf("%w: the player, whose first position is relative", ErrUnknownEntity)
		}

		e.player = &Trace{EntityID: e.self, Family: FamilyPlayer}
		e.order = append(e.order, e.player)
		e.player.Samples = append(e.player.Samples, Sample{
			Sequence: record.Sequence,
			Elapsed:  record.Elapsed,
			Position: at,
			OnGround: onGround,
		})

		return nil
	}

	previous := last(e.player)
	position := at

	if flags&positionRelativeX != 0 {
		position.X += previous.Position.X
	}
	if flags&positionRelativeY != 0 {
		position.Y += previous.Position.Y
	}
	if flags&positionRelativeZ != 0 {
		position.Z += previous.Position.Z
	}

	// A serverbound position names no entity, so the ID can only be learned
	// from a join packet — which may arrive after the trace has opened when a
	// recording starts mid-session and then reconnects.
	if e.named && e.player.EntityID == 0 {
		e.player.EntityID = e.self
	}

	e.player.Samples = append(e.player.Samples, Sample{
		Sequence: record.Sequence,
		Elapsed:  record.Elapsed,
		Position: position,
		Velocity: previous.Velocity,
		OnGround: onGround,
	})

	return nil
}

// spawn starts a trace and records the spawn position as its first sample.
func (e *extractor) spawn(record mccapture.Record, id int32, family string, at Vec3, motion Vec3) {
	e.close(id)

	trace := &Trace{EntityID: id, Family: family}
	trace.Samples = append(trace.Samples, Sample{
		Sequence: record.Sequence,
		Elapsed:  record.Elapsed,
		Position: at,
		Velocity: motion,
	})

	e.live[id] = trace
	e.order = append(e.order, trace)
}

func (e *extractor) absolute(record mccapture.Record, id int32, at Vec3, onGround bool) error {
	trace, ok := e.live[id]
	if !ok {
		return fmt.Errorf("%w: entity %d", ErrUnknownEntity, id)
	}

	trace.Samples = append(trace.Samples, Sample{
		Sequence: record.Sequence,
		Elapsed:  record.Elapsed,
		Position: at,
		Velocity: last(trace).Velocity,
		OnGround: onGround,
	})

	return nil
}

// relative accumulates a delta onto the last absolute position, because a
// consumer comparing a simulated trajectory needs positions, not deltas.
func (e *extractor) relative(record mccapture.Record, id int32, by Vec3, onGround bool) error {
	trace, ok := e.live[id]
	if !ok {
		return fmt.Errorf("%w: entity %d", ErrUnknownEntity, id)
	}

	previous := last(trace)

	return e.absolute(record, id, Vec3{
		X: previous.Position.X + by.X,
		Y: previous.Position.Y + by.Y,
		Z: previous.Position.Z + by.Z,
	}, onGround)
}

// velocity updates what the entity was last told to move at without adding a
// sample. Protocol 47 sends velocity on its own, and inventing a position for
// it would put a sample where the server reported no position at all.
func (e *extractor) velocity(id int32, motion Vec3) error {
	trace, ok := e.live[id]

	// Knockback is sent to the player about their own entity, which lives in no
	// spawn table because it was never spawned. Missing this routing would turn
	// every explosion and every hit into a failed extraction.
	if !ok && e.named && id == e.self && e.player != nil {
		trace, ok = e.player, true
	}

	if !ok {
		return fmt.Errorf("%w: entity %d", ErrUnknownEntity, id)
	}

	trace.Samples[len(trace.Samples)-1].Velocity = motion

	return nil
}

// close ends the trace for an ID so a later spawn starts a new one.
func (e *extractor) close(id int32) { delete(e.live, id) }

func (e *extractor) done() []Trace {
	traces := make([]Trace, 0, len(e.order))
	for _, trace := range e.order {
		traces = append(traces, *trace)
	}

	return traces
}

func last(trace *Trace) Sample { return trace.Samples[len(trace.Samples)-1] }

// fixed converts protocol 47's fixed-point position to blocks.
func fixed(x, y, z int32) Vec3 {
	return Vec3{X: float64(x) / positionScale, Y: float64(y) / positionScale, Z: float64(z) / positionScale}
}

// delta converts a relative move, which uses the same scale in one byte.
func delta(dx, dy, dz int8) Vec3 {
	return Vec3{X: float64(dx) / positionScale, Y: float64(dy) / positionScale, Z: float64(dz) / positionScale}
}

func velocity(vx, vy, vz int16) Vec3 {
	return Vec3{X: float64(vx) / velocityScale, Y: float64(vy) / velocityScale, Z: float64(vz) / velocityScale}
}

// objectFamily names the two object types the gameplay work cares about. The
// rest keep the generic family with their type appended, so a recording of
// something unmodelled is still readable rather than silently mislabelled.
func objectFamily(kind int8) string {
	switch kind {
	case objectTypeItem:
		return FamilyItem
	case objectTypeArrow:
		return FamilyArrow
	default:
		return fmt.Sprintf("%s/%d", FamilyObject, kind)
	}
}

// The two object type identifiers this file names. They are protocol 47
// constants.
const (
	objectTypeArrow int8 = 60
	objectTypeItem  int8 = 2
)

// ExtractFile reads a recording and extracts its traces.
//
// The protocol comes from the file's own header rather than from a flag: a
// recording states what it was taken against, and a caller who had to say it
// again could say it wrong.
func ExtractFile(path string) ([]Trace, mccapture.Header, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, mccapture.Header{}, fmt.Errorf("trace: open: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader, err := mccapture.NewReader(file)
	if err != nil {
		return nil, mccapture.Header{}, fmt.Errorf("trace: read header: %w", err)
	}

	header := reader.Header()

	descriptor, known := protocols.Resolve(header.Protocol)
	if !known {
		return nil, header, fmt.Errorf("%w: the recording names %q", ErrUnsupportedProtocol, header.Protocol)
	}

	limits, err := protocol.NewLimits(
		protocol.MaxFrameBytes(header.FrameBytes),
		protocol.MaxDecompressedBytes(header.DecompressedBytes),
	)
	if err != nil {
		return nil, header, fmt.Errorf("trace: limits from the header: %w", err)
	}

	// A read that stops for any reason other than the end of the capture is a
	// damaged file, and it is returned rather than absorbed. Breaking on every
	// error would hand back the traces extracted so far with no error beside
	// them, which is the one thing an oracle must never do: a truncated
	// trajectory looks exactly like a complete one that ended early.
	var records []mccapture.Record
	for {
		record, err := reader.Next()
		if errors.Is(err, mccapture.ErrEndOfCapture) {
			break
		}
		if err != nil {
			return nil, header, fmt.Errorf("trace: read record %d: %w", len(records)+1, err)
		}

		records = append(records, record)
	}

	traces, err := Extract(descriptor, limits, records)

	return traces, header, err
}
