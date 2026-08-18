package trace

import (
	"fmt"
	"sync"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/data"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v26_1"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"
)

// relMoveScale is the number of delta units in one block on protocol 775.
//
// Measured against a pinned vanilla 26.1.2 server rather than taken from prose,
// because this project has already paid once for a wire detail read out of a
// document. Recording
// 65fa45c61d4667ff9c23de29a508a246e5224fe538d6779c4ea5c3f2709bbdde holds two
// arrows summoned onto the same flat surface three blocks apart in height, at
// y = -55 and y = -52. Each reported its whole fall as one relative move: -20275
// units and -32563 units. Landing on one surface makes the difference in units
// equal to the difference in blocks, so the scale is (32563 - 20275) / 3 =
// 4096 exactly, with no assumption about where the surface was.
//
// Protocol 47's equivalent is 32 and its delta is int8. Reading a 775 delta at
// 47's scale misreads the move by the ratio of the two.
const relMoveScale = 4096.0

// v26_1Rules reads protocol 775.
//
// Absolute positions are float64 here, so a sample taken at a teleport, a
// position, or a sync carries the server's own number exactly. Quantisation
// enters only across runs of relative moves. See tolerance.go.
type v26_1Rules struct{}

func init() { register(v26_1Rules{}) }

func (v26_1Rules) ProtocolID() string { return v26_1.Protocol().ID() }

func (r v26_1Rules) Apply(e *extractor, record mccapture.Record, packet protocol.Packet) (bool, error) {
	switch value := packet.Value.(type) {
	// The join packet is the only place the connecting player's own entity ID
	// appears. It carries no position, so it opens no trace.
	case *v26_1.PlayClientboundLogin:
		e.self, e.named = value.EntityID, true

	// The server's teleport places the player. 775 carries relativity per axis
	// in a flags struct, and carries the player's velocity in the same packet.
	case *v26_1.PlayClientboundPosition:
		// A teleport says nothing about footing, so the sample keeps whatever
		// the player last reported rather than claiming they left the ground.
		if err := e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z},
			relativeMask(value.Flags), e.playerOnGround()); err != nil {
			return true, err
		}

		return true, e.playerVelocity(Vec3{X: value.Dx, Y: value.Dy, Z: value.Dz}, value.Flags)

	// What the player did between corrections is only ever reported by the
	// player.
	case *v26_1.PlayServerboundPosition:
		return true, e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, 0, value.Flags.OnGround)

	case *v26_1.PlayServerboundPositionLook:
		return true, e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, 0, value.Flags.OnGround)

	// One spawn packet for every family. 47 sent a packet per kind and 775 sends
	// a type instead, so the family comes from the version's own entity
	// registry rather than from packet identity.
	case *v26_1.PlayClientboundSpawnEntity:
		e.spawn(record, value.EntityID, family775(value.Type),
			Vec3{X: value.X, Y: value.Y, Z: value.Z}, velocityBlocks(value.Velocity))

	case *v26_1.PlayClientboundEntityTeleport:
		return true, e.absolute(record, value.EntityID,
			Vec3{X: value.X, Y: value.Y, Z: value.Z}, value.OnGround)

	// 775 only; protocol 47 has no counterpart. It carries the entity's
	// velocity beside its position, so it is an absolute and a velocity.
	case *v26_1.PlayClientboundSyncEntityPosition:
		if err := e.absolute(record, value.EntityID,
			Vec3{X: value.X, Y: value.Y, Z: value.Z}, value.OnGround); err != nil {
			return true, err
		}

		return true, e.velocity(value.EntityID, Vec3{X: value.Dx, Y: value.Dy, Z: value.Dz})

	case *v26_1.PlayClientboundRelEntityMove:
		return true, e.relative(record, value.EntityID,
			r.delta(value.DX, value.DY, value.DZ), value.OnGround)

	case *v26_1.PlayClientboundEntityMoveLook:
		return true, e.relative(record, value.EntityID,
			r.delta(value.DX, value.DY, value.DZ), value.OnGround)

	case *v26_1.PlayClientboundEntityVelocity:
		return true, e.velocity(value.EntityID, velocityBlocks(value.Velocity))

	case *v26_1.PlayClientboundEntityDestroy:
		for _, id := range value.EntityIds {
			e.close(id)
		}

	default:
		return false, nil
	}

	return true, nil
}

// delta converts 775's int16 relative move to blocks.
func (v26_1Rules) delta(dx, dy, dz int16) Vec3 {
	return Vec3{
		X: float64(dx) / relMoveScale,
		Y: float64(dy) / relMoveScale,
		Z: float64(dz) / relMoveScale,
	}
}

// velocityBlocks reads 775's velocity, which is already in blocks per tick.
//
// This is the identity rather than 47's division by eight thousand: 775 sends a
// quantised vector that decodes to the value itself, so the only error is the
// encoding's own resolution.
//
// A live capture against a pinned 26.1.2 server disagrees with that decoding,
// and the disagreement is recorded rather than worked around here. See
// relay/docs/verification/2026-08-17-capture-oracle.md: velocities read out of
// a real session cluster on repeated values near one and do not match the
// motion the same entity's relative moves report, which points at the LPVec3
// codec in minecraft-protocol rather than at this file. Positions are
// unaffected — they are float64 and int16 deltas, neither of which goes through
// that codec.
func velocityBlocks(v java.LPVec3) Vec3 { return Vec3{X: v.X, Y: v.Y, Z: v.Z} }

// relativeMask projects 775's per-axis flags onto the three bits playerAt
// already reads.
//
// Only the position axes are projected. The rotation flags carry no position,
// and the velocity flags are handled by playerVelocity rather than dropped:
// they say whether the packet's delta adds to the player's current velocity,
// which is a different question from where the player is.
func relativeMask(flags v26_1.PlayClientboundPositionFlagsFlags) int8 {
	var mask int8
	if flags.X {
		mask |= positionRelativeX
	}
	if flags.Y {
		mask |= positionRelativeY
	}
	if flags.Z {
		mask |= positionRelativeZ
	}

	return mask
}

// playerVelocity records the velocity 775's position packet carries beside the
// position, on the sample that packet just added.
//
// It is a method on the accumulator rather than a call to velocity because the
// position packet names no entity: it is about the connecting player, whose
// trace is held apart from the spawn table. Routing it through velocity would
// need an ID the packet does not carry.
func (e *extractor) playerVelocity(motion Vec3, flags v26_1.PlayClientboundPositionFlagsFlags) error {
	if e.player == nil || len(e.player.Samples) == 0 {
		return fmt.Errorf("%w: the player, whose velocity arrived before any position", ErrUnknownEntity)
	}

	sample := &e.player.Samples[len(e.player.Samples)-1]
	previous := sample.Velocity

	if flags.Dx {
		motion.X += previous.X
	}
	if flags.Dy {
		motion.Y += previous.Y
	}
	if flags.Dz {
		motion.Z += previous.Z
	}

	sample.Velocity = motion

	return nil
}

// family775 names an entity from 775's own registry.
//
// 47 had a spawn packet per family and 775 has one packet carrying a type, so
// the mapping that used to be packet identity is a registry lookup. The four
// families 47 named keep their names; everything else keeps the generic family
// with the registry's own name appended, so a recording of something this
// milestone does not model is still readable rather than silently mislabelled.
func family775(kind int32) string {
	entity, known := entities775()[kind]
	if !known {
		return fmt.Sprintf("%s/%d", FamilyObject, kind)
	}

	switch entity.Name {
	case "item":
		return FamilyItem
	case "arrow":
		return FamilyArrow
	case "experience_orb":
		return FamilyExperienceOrb
	case "player":
		return FamilyPlayer
	}

	switch entity.Type {
	case data.EntityTypeMob, data.EntityTypeLiving, data.EntityTypeAnimal,
		data.EntityTypeHostile, data.EntityTypePassive, data.EntityTypeAmbient,
		data.EntityTypeWaterCreature, data.EntityTypePlayer:
		return FamilyLiving
	}

	return FamilyObject + "/" + entity.Name
}

// entities775 is 775's entity registry keyed by the type ID a spawn packet
// carries.
//
// It is built once and lazily: the whole data set costs about thirteen
// milliseconds to construct, which is nothing per process and would be
// something per spawn packet.
var entities775 = sync.OnceValue(func() map[int32]data.Entity {
	set, err := v26_1.Data()
	if err != nil {
		// A generated data set that fails to build is a build problem, not a
		// recording problem. Returning an empty map keeps extraction running
		// and labels every entity by its number, which is honest about what is
		// known.
		return map[int32]data.Entity{}
	}

	all := set.Entities().All()
	byID := make(map[int32]data.Entity, len(all))
	for _, entity := range all {
		byID[int32(entity.ID)] = entity
	}

	return byID
})
