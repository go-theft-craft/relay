// Package trace turns a recording into per-entity trajectories.
//
// It is the half of the oracle that makes a capture comparable: a recording
// holds packets, and the thing being verified produces positions, so something
// has to accumulate one into the other. That accumulation is the whole of this
// package, and it is deliberately dumb — it reads what the server said and
// nothing else. Any inference about what an entity *should* have done belongs
// to the simulation being judged, not to the file judging it.
package trace

import "time"

// Vec3 is a position or a velocity in blocks.
//
// It is declared here rather than imported from the simulation: a capture is an
// oracle and must not depend on the thing it verifies, or a wrong constant in
// the kernel would be reproduced identically on both sides of the comparison
// and cancel out.
type Vec3 struct {
	X, Y, Z float64
}

// Sample is one observed position.
//
// It carries the recording's own coordinates — a sequence number and the time
// since the session opened — rather than a tick number. A capture has no ticks
// in it: the server does not send them, and a tick index reconstructed by
// dividing elapsed time by fifty milliseconds would be a guess dressed as a
// measurement. A consumer comparing against a simulation aligns on time.
type Sample struct {
	Sequence uint64
	Elapsed  time.Duration
	// Position is absolute, with relative moves already accumulated onto the
	// last absolute position the server sent.
	Position Vec3
	// Velocity is what the server last reported, and zero when it reported
	// none. Protocol 47 sends velocity separately from movement, so a sample
	// carries the most recent value rather than one measured for this instant.
	Velocity Vec3
	OnGround bool
}

// Trace is one entity's observed motion over one recording.
//
// EntityID is a runtime identifier and the server reuses it after the entity is
// removed, so two traces in one recording may share one. Family is what the
// spawn packet said it was.
//
// The connecting player gets a trace like anything else, built from what they
// told the server rather than from a spawn — so its EntityID is whatever the
// join packet named, and zero in a recording that started after the join.
type Trace struct {
	EntityID int32
	Family   string
	Samples  []Sample
}

// The families a spawn packet resolves to. They are strings rather than an
// enumeration because a consumer filters by them and a capture written today is
// read by a tool built later, which must be able to see a family it does not
// know rather than fail to parse the file.
const (
	FamilyPlayer        = "player"
	FamilyItem          = "item"
	FamilyArrow         = "arrow"
	FamilyLiving        = "living"
	FamilyExperienceOrb = "experience_orb"
	FamilyObject        = "object"
)

// Fixed-point scales protocol 47 sends coordinates in. Positions are
// thirty-seconds of a block and velocities are eight-thousandths of a block per
// tick. Getting either wrong produces a trajectory that is the right shape and
// the wrong size, which is the failure mode hardest to see in a plot.
const (
	positionScale = 32.0
	velocityScale = 8000.0
)
