package trace

import (
	"fmt"

	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v26_1"
)

// Tolerance is how far a compared trajectory may differ from a captured one
// before the difference is a finding rather than the wire's resolution.
//
// It has two numbers because 775 has two regimes. Absolute is the allowance at
// a sample the server stated outright; Relative is the allowance accumulated
// across a run of relative moves, per move.
type Tolerance struct {
	// Absolute is in blocks.
	Absolute float64
	// Relative is in blocks per relative move.
	Relative float64
	// Why records the derivation, so a report can print why a comparison
	// passed at the number it used.
	Why string
}

// ToleranceFor returns the tolerance for one protocol ID.
//
// It returns ErrUnsupportedProtocol for a version with no registered
// extractor, so a comparison cannot silently default to 1.8's number — which
// on 775 would be a hundred and twenty-eight times looser than the wire.
func ToleranceFor(protocolID string) (Tolerance, error) {
	tolerance, known := tolerances[protocolID]
	if !known {
		return Tolerance{}, fmt.Errorf("%w: %q, want one of %v",
			ErrUnsupportedProtocol, protocolID, SupportedProtocols())
	}

	return tolerance, nil
}

// tolerances is keyed by protocol ID. Each entry states its derivation,
// because the number is worthless without it: a reader who does not know why
// the allowance is what it is cannot tell a passing comparison from a
// comparison that was never tight enough to fail.
var tolerances = map[string]Tolerance{
	v1_8.Protocol().ID(): {
		Absolute: 1.0 / positionScale,
		Relative: 1.0 / positionScale,
		Why: "Protocol 47 transmits absolute positions as int32 fixed point in " +
			"units of 1/32 of a block and relative moves as int8 at the same " +
			"scale, so every sample is quantised to 1/32. This catches wrong " +
			"constants and wrong axis order, not last-place drift.",
	},
	v26_1.Protocol().ID(): {
		Absolute: 0,
		Relative: 1.0 / relMoveScale,
		Why: "Protocol 775 transmits absolute positions as float64, so a sample " +
			"taken at a teleport, a position, or a sync is the server's own " +
			"number and needs no allowance. Relative moves are int16 at 1/4096 " +
			"of a block — measured against a pinned 26.1.2 server, see " +
			"relMoveScale — so only a run of relative moves accumulates error, " +
			"at that resolution per move.",
	},
}
