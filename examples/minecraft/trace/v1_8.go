package trace

import (
	"fmt"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
)

// v1_8Rules reads protocol 47.
//
// Its positions are fixed point in units of 1/32 of a block and its relative
// moves are int8, so a trajectory built from it is exact only to that
// resolution. See tolerance.go for what that means for a comparison.
type v1_8Rules struct{}

func init() { register(v1_8Rules{}) }

func (v1_8Rules) ProtocolID() string { return v1_8.Protocol().ID() }

// Apply is protocol 47's packet-to-motion mapping, written as one switch so a
// reader tracing a wrong trajectory can see every packet that could have
// produced it.
func (v1_8Rules) Apply(e *extractor, record mccapture.Record, packet protocol.Packet) (bool, error) {
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
		return true, e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, value.Flags, e.playerOnGround())

	// What the player did between corrections is only ever reported by the
	// player. Walking, sprinting, jumping, and falling all arrive here.
	case *v1_8.PlayServerboundPosition:
		return true, e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, 0, value.OnGround)

	case *v1_8.PlayServerboundPositionLook:
		return true, e.playerAt(record, Vec3{X: value.X, Y: value.Y, Z: value.Z}, 0, value.OnGround)

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
		return true, e.absolute(record, value.EntityID, fixed(value.X, value.Y, value.Z), value.OnGround)

	case *v1_8.PlayClientboundRelEntityMove:
		return true, e.relative(record, value.EntityID, delta(value.DX, value.DY, value.DZ), value.OnGround)

	case *v1_8.PlayClientboundEntityMoveLook:
		return true, e.relative(record, value.EntityID, delta(value.DX, value.DY, value.DZ), value.OnGround)

	case *v1_8.PlayClientboundEntityVelocity:
		return true, e.velocity(value.EntityID, velocity(value.VelocityX, value.VelocityY, value.VelocityZ))

	case *v1_8.PlayClientboundEntityDestroy:
		for _, id := range value.EntityIds {
			e.close(id)
		}

	default:
		return false, nil
	}

	return true, nil
}

// The bits protocol 47's teleport sets to mean "add this to where you already
// are" rather than "you are here". The rotation bits are ignored: a trace holds
// no rotation, so reading them would only invite the reader to think it does.
const (
	positionRelativeX int8 = 0x01
	positionRelativeY int8 = 0x02
	positionRelativeZ int8 = 0x04
)

// fixed converts protocol 47's fixed-point position to blocks.
func fixed(x, y, z int32) Vec3 {
	return Vec3{X: float64(x) / positionScale, Y: float64(y) / positionScale, Z: float64(z) / positionScale}
}

// delta converts a relative move, which uses the same scale in one byte.
func delta(dx, dy, dz int8) Vec3 {
	return Vec3{X: float64(dx) / positionScale, Y: float64(dy) / positionScale, Z: float64(dz) / positionScale}
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
