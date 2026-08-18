package trace_test

import (
	"errors"
	"testing"

	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v26_1"

	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

func TestTheTolerancesDifferByVersion(t *testing.T) {
	t.Parallel()

	old, err := trace.ToleranceFor(v1_8.Protocol().ID())
	if err != nil {
		t.Fatalf("ToleranceFor 47: %v", err)
	}
	modern, err := trace.ToleranceFor(v26_1.Protocol().ID())
	if err != nil {
		t.Fatalf("ToleranceFor 775: %v", err)
	}

	if old.Absolute != 1.0/32.0 {
		t.Fatalf("47 Absolute = %v, want 1/32; its positions are fixed point", old.Absolute)
	}
	if modern.Absolute != 0 {
		t.Fatalf("775 Absolute = %v, want 0; its position packets carry float64", modern.Absolute)
	}
	if modern.Relative >= old.Relative {
		t.Fatalf("775 Relative = %v, 47 Relative = %v; 775's wider delta must not "+
			"buy a looser comparison", modern.Relative, old.Relative)
	}
	if modern.Why == "" || old.Why == "" {
		t.Fatal("a tolerance with no recorded derivation is a magic number")
	}
}

func TestAnUnknownProtocolHasNoTolerance(t *testing.T) {
	t.Parallel()

	if _, err := trace.ToleranceFor("java/0.0"); !errors.Is(err, trace.ErrUnsupportedProtocol) {
		t.Fatal("ToleranceFor returned a tolerance for an unknown protocol; a " +
			"comparison must not fall back to another version's number")
	}
}

// TestEveryReadableVersionHasATolerance closes the gap between the two
// registries. A version this package can extract but cannot state a tolerance
// for would be compared at whatever the caller guessed.
func TestEveryReadableVersionHasATolerance(t *testing.T) {
	t.Parallel()

	for _, id := range trace.SupportedProtocols() {
		if _, err := trace.ToleranceFor(id); err != nil {
			t.Errorf("ToleranceFor(%q): %v", id, err)
		}
	}
}
