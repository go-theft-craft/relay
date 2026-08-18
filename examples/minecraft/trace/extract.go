package trace

import (
	"errors"
	"fmt"
	"os"
	"slices"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
)

// ErrUnsupportedProtocol reports a recording this extractor cannot read.
//
// The movement packets, their scales, and the set of spawn packets are all
// version-specific, so a version is an implementation rather than a flag — and
// claiming support by decoding one version with another's rules would produce
// plausible numbers that are quietly wrong. SupportedProtocols names the
// versions that have one; anything else fails here rather than being decoded
// by the wrong rules.
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

// ErrNoTrajectories reports a recording that reached play and yielded no
// motion, which means it was not read rather than that nothing happened.
//
// It exists because the alternative is worse than a crash: an empty trace list
// and a zero exit code. The first capture taken through a proxy against a
// compressing vanilla server did exactly that — 15916 records skipped one at a
// time and a successful-looking run with nothing in it.
var ErrNoTrajectories = errors.New("trace: the recording reached play and produced no trajectories")

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
	handler, ok := lookup(descriptor.ID())
	if !ok {
		return nil, fmt.Errorf("%w: %q, want one of %v",
			ErrUnsupportedProtocol, descriptor.ID(), SupportedProtocols())
	}

	client, err := descriptor.NewSession(protocol.RoleClient, limits)
	if err != nil {
		return nil, fmt.Errorf("trace: build the clientbound decoding session: %w", err)
	}

	server, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		return nil, fmt.Errorf("trace: build the serverbound decoding session: %w", err)
	}

	e := &extractor{rules: handler, client: client, server: server, live: make(map[int32]*Trace)}

	for _, record := range records {
		if err := e.consume(record); err != nil {
			return nil, err
		}
	}

	// A play capture that yielded no trajectory at all was not read. Skipping one
	// unreadable packet is deliberate; skipping every packet that carries motion
	// and reporting success says nothing moved when it means nothing was read,
	// and those are indistinguishable in the output. It is the same failure
	// ExtractFile guards at the other end of the file, arriving through a
	// different door.
	//
	// The test is whether anything moved, not whether anything decoded, because
	// the second question has a useless answer on a real session. Measured over
	// a genuinely unreadable capture: 7954 play records offered, 431 decoded,
	// zero trajectories. Protocol 47's arm_animation has an empty body, so any
	// frame whose first byte lands on its ID parses into a valid zero-field
	// packet no matter what the following bytes are — 413 of them here, plus a
	// chat and seventeen map_chunk_bulk. A decode counter can therefore sit
	// comfortably off zero while the file is gibberish.
	//
	// Motion has no such escape. A server teleports a player before they can
	// act, and a client reports its position around twenty times a second, so a
	// capture holding play frames and no position at all in either direction has
	// failed to be read — or is far too short to be evidence of anything, which
	// earns the same answer.
	if e.playOffered > 0 && len(e.order) == 0 {
		return nil, fmt.Errorf(
			"%w: %d play records offered, %d decoded, no trajectories",
			ErrNoTrajectories, e.playOffered, e.playDecoded,
		)
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
	// rules is the one version's packet handling, resolved once per extraction
	// from the descriptor the recording named.
	rules versionRules

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

// apply hands one decoded packet to the version's rules.
//
// The rules report whether the packet carried motion. Nothing counts that
// today — playOffered and playDecoded already separate "not read" from "read"
// — but the driver is where such a counter would go, which is why the answer
// comes back here rather than being swallowed inside a version file.
func (e *extractor) apply(record mccapture.Record, packet protocol.Packet) error {
	_, err := e.rules.Apply(e, record, packet)

	return err
}

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

func velocity(vx, vy, vz int16) Vec3 {
	return Vec3{X: float64(vx) / velocityScale, Y: float64(vy) / velocityScale, Z: float64(vz) / velocityScale}
}

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

// versionRules turns one version's play packets into motion on the shared
// accumulator. The packet sets, the coordinate scales, and the spawn packets
// are all version-specific, so this is an interface rather than a switch: a
// registry keyed by protocol ID means an unregistered version fails at
// ErrUnsupportedProtocol rather than being decoded by the wrong rules.
type versionRules interface {
	// ProtocolID is the descriptor ID these rules read.
	ProtocolID() string
	// Apply folds one decoded play packet into the accumulator. It reports
	// false when the packet carries no motion, so the driver can tell "read
	// and irrelevant" from "not read".
	Apply(e *extractor, record mccapture.Record, packet protocol.Packet) (bool, error)
}

// rules is keyed by protocol ID. Each version file registers itself from init,
// so adding a version is adding a file.
var rules = map[string]versionRules{}

func register(r versionRules) {
	if _, dup := rules[r.ProtocolID()]; dup {
		panic("trace: two rule sets registered for " + r.ProtocolID())
	}

	rules[r.ProtocolID()] = r
}

func lookup(id string) (versionRules, bool) {
	r, ok := rules[id]

	return r, ok
}

// SupportedProtocols is the sorted list of protocol IDs this package reads.
//
// Exported because the conformance harness enumerates it to refuse a scenario
// that checks one version and claims nothing about another, and because an
// error naming what the tool can read is more use than one naming only what it
// cannot.
func SupportedProtocols() []string {
	ids := make([]string, 0, len(rules))
	for id := range rules {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	return ids
}
