package trace_test

import (
	"slices"
	"testing"

	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"

	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

func TestProtocol47IsRegistered(t *testing.T) {
	t.Parallel()

	got := trace.SupportedProtocols()
	if !slices.Contains(got, v1_8.Protocol().ID()) {
		t.Fatalf("SupportedProtocols() = %v, want it to contain %q",
			got, v1_8.Protocol().ID())
	}
}

func TestSupportedProtocolsIsSortedAndDeduplicated(t *testing.T) {
	t.Parallel()

	// The conformance harness enumerates this to decide which lanes a
	// scenario must declare, and it prints it in errors. An unstable order
	// makes those errors and any golden output flap.
	got := trace.SupportedProtocols()
	if !slices.IsSorted(got) {
		t.Fatalf("SupportedProtocols() = %v, want sorted", got)
	}
	if len(slices.Compact(slices.Clone(got))) != len(got) {
		t.Fatalf("SupportedProtocols() = %v, want no duplicates", got)
	}
}
