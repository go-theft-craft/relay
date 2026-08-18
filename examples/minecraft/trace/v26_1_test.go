package trace_test

import (
	"errors"
	"math"
	"testing"

	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v26_1"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

// The entity type IDs 26.1 gives the two families this milestone cares about.
// They are read from the version's own registry rather than written here: a
// spawn packet on 775 names a type instead of carrying a packet per family, so
// a test that hardcoded the number would pass against a registry that had
// moved.
func entityType(t *testing.T, name string) int32 {
	t.Helper()

	set, err := v26_1.Data()
	if err != nil {
		t.Fatalf("load the 26.1 data set: %v", err)
	}

	entities := set.Entities().ByName(name)
	if len(entities) != 1 {
		t.Fatalf("registry names %d entities %q, want exactly 1", len(entities), name)
	}

	return int32(entities[0].ID)
}

// joinState is the least a 26.1 join packet needs to encode: its gamemode is a
// mapped field, and the zero value maps to nothing.
func joinState() v26_1.SpawnInfo {
	return v26_1.SpawnInfo{Name: "minecraft:overworld", Gamemode: "survival"}
}

func extract775(t *testing.T, records []mccapture.Record) []trace.Trace {
	t.Helper()

	traces, err := trace.Extract(v26_1.Protocol(), testLimits(t), records)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}

	return traces
}

func TestProtocol775IsRegistered(t *testing.T) {
	t.Parallel()

	got := trace.SupportedProtocols()
	found := false
	for _, id := range got {
		if id == v26_1.Protocol().ID() {
			found = true
		}
	}
	if !found {
		t.Fatalf("SupportedProtocols() = %v, want it to contain %q", got, v26_1.Protocol().ID())
	}
}

func TestProtocol775TeleportIsExact(t *testing.T) {
	t.Parallel()

	// 775 sends absolute positions as float64, so the trajectory carries the
	// server's own number. Asserting equality rather than a tolerance is the
	// point: a rounding step borrowed from the 47 path would fail here.
	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundSpawnEntity{
			EntityID: 42, Type: entityType(t, "arrow"), X: 0, Y: 64, Z: 0,
		}),
		r.record(&v26_1.PlayClientboundEntityTeleport{
			EntityID: 42, X: 1.0 / 3.0, Y: 64.5, Z: -2.25,
		}),
	}

	traces := extract775(t, records)
	if len(traces) != 1 {
		t.Fatalf("extracted %d traces, want 1", len(traces))
	}
	if traces[0].Family != trace.FamilyArrow {
		t.Errorf("family = %q, want %q", traces[0].Family, trace.FamilyArrow)
	}

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if last.Position.X != 1.0/3.0 {
		t.Fatalf("X = %v, want %v exactly; the 775 teleport is float64 and must "+
			"not be rounded", last.Position.X, 1.0/3.0)
	}
}

func TestProtocol775RelativeMoveUsesItsOwnScale(t *testing.T) {
	t.Parallel()

	// The delta widened from int8 to int16 between 47 and 775, and its scale
	// from 32 units a block to 4096. Reading it at 47's scale produces a
	// plausible number wrong by the ratio of the two, which is exactly what a
	// shared implementation would have done silently.
	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundSpawnEntity{
			EntityID: 7, Type: entityType(t, "item"), X: 0, Y: 64, Z: 0,
		}),
		r.record(&v26_1.PlayClientboundRelEntityMove{EntityID: 7, DX: 4096}),
	}

	traces := extract775(t, records)
	if traces[0].Family != trace.FamilyItem {
		t.Errorf("family = %q, want %q", traces[0].Family, trace.FamilyItem)
	}

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if math.Abs(last.Position.X-1.0) > 1e-9 {
		t.Fatalf("X = %v after a one-block delta, want 1.0", last.Position.X)
	}
}

func TestProtocol775MovementForAnUnspawnedEntityIsAFinding(t *testing.T) {
	t.Parallel()

	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundRelEntityMove{EntityID: 99, DX: 1}),
	}

	_, err := trace.Extract(v26_1.Protocol(), testLimits(t), records)
	if !errors.Is(err, trace.ErrUnknownEntity) {
		t.Fatalf("err = %v, want ErrUnknownEntity; a relative move with no anchor "+
			"must not start a trace at the origin", err)
	}
}

func TestProtocol775AReusedEntityIDStartsANewTrace(t *testing.T) {
	t.Parallel()

	// The server reuses a runtime ID once the entity is gone. Appending to the
	// dead trace would splice two trajectories into one nobody followed, and on
	// 775 the guard has to survive one consolidated spawn packet standing in
	// for 47's several.
	r := newRecorderFor(t, v26_1.Protocol())
	arrow := entityType(t, "arrow")
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundSpawnEntity{EntityID: 5, Type: arrow, X: 0, Y: 64, Z: 0}),
		r.record(&v26_1.PlayClientboundRelEntityMove{EntityID: 5, DX: 4096}),
		r.record(&v26_1.PlayClientboundEntityDestroy{EntityIds: []int32{5}}),
		r.record(&v26_1.PlayClientboundSpawnEntity{EntityID: 5, Type: arrow, X: 100, Y: 64, Z: 0}),
	}

	traces := extract775(t, records)
	if len(traces) != 2 {
		t.Fatalf("extracted %d traces, want 2", len(traces))
	}
	if got := traces[1].Samples[0].Position.X; got != 100 {
		t.Fatalf("the second trace starts at X = %v, want 100", got)
	}
	if got := len(traces[1].Samples); got != 1 {
		t.Fatalf("the second trace holds %d samples, want 1", got)
	}
}

func TestProtocol775VelocityDoesNotInventAPosition(t *testing.T) {
	t.Parallel()

	// Velocity says how fast, not where. A sample added for one would put the
	// entity somewhere the server never reported it.
	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundSpawnEntity{
			EntityID: 8, Type: entityType(t, "arrow"), X: 0, Y: 64, Z: 0,
		}),
		r.record(&v26_1.PlayClientboundEntityVelocity{
			EntityID: 8, Velocity: java.LPVec3{X: 0.5, Y: -0.25, Z: 0.125},
		}),
	}

	traces := extract775(t, records)
	if got := len(traces[0].Samples); got != 1 {
		t.Fatalf("the trace holds %d samples, want 1: a velocity is not a position", got)
	}

	// 775 carries velocity as a quantised vector in blocks per tick rather than
	// 47's thousandths, so the conversion is the identity and the only error is
	// the encoding's own.
	velocity := traces[0].Samples[0].Velocity
	if math.Abs(velocity.X-0.5) > 1e-3 || math.Abs(velocity.Y+0.25) > 1e-3 || math.Abs(velocity.Z-0.125) > 1e-3 {
		t.Fatalf("velocity = %+v, want about {0.5 -0.25 0.125}", velocity)
	}
}

func TestProtocol775ThePlayersOwnPositionIsTraced(t *testing.T) {
	t.Parallel()

	// The player is not spawned to themselves: the join packet names the entity
	// ID, the server's teleport places them, and everything after that arrives
	// from the client. A version that traced only clientbound frames would
	// produce a trace for every entity except the one the session was about.
	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundLogin{EntityID: 11, WorldState: joinState()}),
		r.record(&v26_1.PlayClientboundPosition{X: 8.5, Y: 64, Z: -3.5}),
		r.send(&v26_1.PlayServerboundPosition{
			X: 9.5, Y: 64, Z: -3.5, Flags: v26_1.MovementFlags{OnGround: true},
		}),
	}

	traces := extract775(t, records)
	if len(traces) != 1 {
		t.Fatalf("extracted %d traces, want 1", len(traces))
	}
	if traces[0].EntityID != 11 {
		t.Errorf("entity ID = %d, want the one the join packet named, 11", traces[0].EntityID)
	}

	last := traces[0].Samples[len(traces[0].Samples)-1]
	if last.Position.X != 9.5 || !last.OnGround {
		t.Fatalf("last sample = %+v, want the client's own (9.5, on ground)", last)
	}
}

func TestProtocol775ARelativeTeleportAccumulates(t *testing.T) {
	t.Parallel()

	// 775 carries relativity in a flags struct rather than 47's bitmask. A
	// correction that says "add this" and is read as "you are here" moves the
	// player to a place the server never put them.
	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundLogin{EntityID: 3, WorldState: joinState()}),
		r.record(&v26_1.PlayClientboundPosition{X: 8.5, Y: 64, Z: -3.5}),
		r.record(&v26_1.PlayClientboundPosition{
			X: 1, Y: 0, Z: 2,
			Flags: v26_1.PlayClientboundPositionFlagsFlags{X: true, Y: true, Z: true},
		}),
	}

	traces := extract775(t, records)
	last := traces[0].Samples[len(traces[0].Samples)-1]
	if last.Position.X != 9.5 || last.Position.Y != 64 || last.Position.Z != -1.5 {
		t.Fatalf("position = %+v, want the relative correction added to (8.5, 64, -3.5)",
			last.Position)
	}
}

func TestProtocol775SyncEntityPositionIsAbsolute(t *testing.T) {
	t.Parallel()

	// 775 only: 47 has no counterpart. It carries the entity's velocity beside
	// its position, so it is an absolute and a velocity rather than only an
	// absolute.
	r := newRecorderFor(t, v26_1.Protocol())
	records := []mccapture.Record{
		r.record(&v26_1.PlayClientboundSpawnEntity{
			EntityID: 4, Type: entityType(t, "item"), X: 0, Y: 64, Z: 0,
		}),
		r.record(&v26_1.PlayClientboundSyncEntityPosition{
			EntityID: 4, X: 1.5, Y: 63.25, Z: -1.5, Dx: 0.1, Dy: -0.2, Dz: 0.3,
		}),
	}

	traces := extract775(t, records)
	last := traces[0].Samples[len(traces[0].Samples)-1]
	if last.Position.X != 1.5 || last.Position.Y != 63.25 || last.Position.Z != -1.5 {
		t.Fatalf("position = %+v, want the packet's own absolute", last.Position)
	}
	if last.Velocity.X != 0.1 || last.Velocity.Y != -0.2 || last.Velocity.Z != 0.3 {
		t.Fatalf("velocity = %+v, want the packet's own delta", last.Velocity)
	}
}
