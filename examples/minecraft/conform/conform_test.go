package conform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-theft-craft/relay/examples/minecraft/conform"
	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

// stubComparer stands in for the kernel. This package must not import
// minecraft-simulation — an oracle that depends on the thing it verifies would
// reproduce a wrong constant on both sides and cancel it out — so the tests
// supply trajectories rather than computing them.
type stubComparer struct {
	byProtocol map[string][]trace.Trace
}

func (s stubComparer) Trajectories(_ context.Context, protocolID string) ([]trace.Trace, error) {
	return s.byProtocol[protocolID], nil
}

// stubCaptures stands in for the recordings. Run reads a lane's file through a
// Loader so a test can state what the capture held instead of shipping one:
// a .mccap in testdata would carry a real session's UUIDs and usernames, which
// is the reason recordings are not committed anywhere in this repository.
type stubCaptures map[string][]trace.Trace

func (s stubCaptures) Load(_ context.Context, lane conform.Lane) ([]trace.Trace, error) {
	return s[lane.Recording], nil
}

func line(id int32, family string, xs ...float64) trace.Trace {
	samples := make([]trace.Sample, 0, len(xs))
	for index, x := range xs {
		samples = append(samples, trace.Sample{
			Sequence: uint64(index),
			Position: trace.Vec3{X: x, Y: 64, Z: 0},
		})
	}

	return trace.Trace{EntityID: id, Family: family, Samples: samples}
}

// twoLaneScenario is a scenario with a real lane per registered version, used
// by the tests that care about what Run does rather than what it rejects.
func twoLaneScenario(t *testing.T) conform.Scenario {
	t.Helper()

	return conform.Scenario{
		Name: "dropped item falls",
		Lanes: []conform.Lane{
			{ProtocolID: "java/1.8.9", Recording: "item-47.mccap"},
			{ProtocolID: "java/26.1", Recording: "item-775.mccap"},
		},
	}
}

func TestAScenarioMissingAVersionIsAnError(t *testing.T) {
	t.Parallel()

	// The whole point. A scenario that names only 47 must not quietly report
	// "pass" while saying nothing about 775.
	s := conform.Scenario{
		Name:  "dropped item falls",
		Lanes: []conform.Lane{{ProtocolID: "java/1.8.9", Recording: "item.mccap"}},
	}

	_, err := conform.Run(context.Background(), s, stubComparer{}, stubCaptures{})
	if err == nil {
		t.Fatal("Run accepted a scenario that checks one version and claims nothing " +
			"about the other")
	}
	if !strings.Contains(err.Error(), "java/26.1") {
		t.Fatalf("err = %v, want it to name the version with no lane", err)
	}
}

func TestAnAbsentMechanicIsReportedRatherThanSkipped(t *testing.T) {
	t.Parallel()

	s := conform.Scenario{
		Name: "attack cooldown",
		Lanes: []conform.Lane{
			{ProtocolID: "java/1.8.9", AbsentReason: "the attack cooldown arrived in 1.9"},
			{ProtocolID: "java/26.1", Recording: "cooldown.mccap"},
		},
	}

	report, err := conform.Run(context.Background(), s, stubComparer{}, stubCaptures{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := report.Outcome("java/1.8.9")
	if got.Status != conform.Absent {
		t.Fatalf("Status = %v, want Absent", got.Status)
	}
	if got.Detail == "" {
		t.Fatal("an absent mechanic with no recorded reason is indistinguishable " +
			"from one nobody checked")
	}
}

func TestALaneWithNeitherRecordingNorReasonIsRejected(t *testing.T) {
	t.Parallel()

	s := conform.Scenario{
		Name: "empty lane",
		Lanes: []conform.Lane{
			{ProtocolID: "java/1.8.9"},
			{ProtocolID: "java/26.1", Recording: "x.mccap"},
		},
	}

	if _, err := conform.Run(context.Background(), s, stubComparer{}, stubCaptures{}); err == nil {
		t.Fatal("Run accepted a lane that neither checks anything nor says why not")
	}
}

func TestALaneForAVersionNothingCanReadIsRejected(t *testing.T) {
	t.Parallel()

	// A lane naming a version with no extractor would compare at no tolerance
	// at all. It is rejected before anything runs, like a missing lane, rather
	// than reported as a failure of the thing under test.
	s := twoLaneScenario(t)
	s.Lanes = append(s.Lanes, conform.Lane{ProtocolID: "java/0.0.0", Recording: "x.mccap"})

	if _, err := conform.Run(context.Background(), s, stubComparer{}, stubCaptures{}); err == nil {
		t.Fatal("Run accepted a lane for a version no extractor reads")
	}
}

func TestEachLaneUsesItsOwnVersionTolerance(t *testing.T) {
	t.Parallel()

	s := twoLaneScenario(t)
	report, err := conform.Run(context.Background(), s, stubComparer{}, stubCaptures{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	old := report.Outcome("java/1.8.9").Tolerance
	modern := report.Outcome("java/26.1").Tolerance
	if old.Absolute == modern.Absolute {
		t.Fatalf("both lanes used Absolute = %v; each version must use its own",
			old.Absolute)
	}
}

func TestAMatchingTrajectoryPasses(t *testing.T) {
	t.Parallel()

	// Within 775's own resolution: a quarter of a delta unit at the second
	// sample, which is a third of what one relative move may drift by.
	captured := []trace.Trace{line(7, trace.FamilyItem, 0, 1, 2)}
	predicted := []trace.Trace{line(7, trace.FamilyItem, 0, 1+1.0/16384, 2)}

	report := runOneVersion(t, "java/26.1", captured, predicted)
	got := report.Outcome("java/26.1")
	if got.Status != conform.Pass {
		t.Fatalf("Status = %v, detail %q, want Pass", got.Status, got.Detail)
	}
	if got.MaxDeviation == 0 {
		t.Fatal("MaxDeviation = 0 on a lane that differed; a pass reports how close " +
			"it came, or nobody can see it about to fail")
	}
}

func TestATrajectoryOutsideItsVersionToleranceFails(t *testing.T) {
	t.Parallel()

	captured := []trace.Trace{line(7, trace.FamilyItem, 0, 1, 2)}
	predicted := []trace.Trace{line(7, trace.FamilyItem, 0, 1.5, 2)}

	report := runOneVersion(t, "java/26.1", captured, predicted)
	got := report.Outcome("java/26.1")
	if got.Status != conform.Fail {
		t.Fatalf("Status = %v, want Fail: half a block is not the wire's resolution", got.Status)
	}
	if !strings.Contains(got.Detail, "entity 7") {
		t.Fatalf("Detail = %q, want it to name the entity that diverged", got.Detail)
	}
}

func TestADifferentNumberOfTrajectoriesFails(t *testing.T) {
	t.Parallel()

	// A kernel that produced no arrow at all would otherwise compare zero
	// samples and pass.
	captured := []trace.Trace{line(7, trace.FamilyItem, 0, 1), line(8, trace.FamilyArrow, 0, 1)}
	predicted := []trace.Trace{line(7, trace.FamilyItem, 0, 1)}

	report := runOneVersion(t, "java/26.1", captured, predicted)
	if got := report.Outcome("java/26.1"); got.Status != conform.Fail {
		t.Fatalf("Status = %v, want Fail", got.Status)
	}
}

func TestAVersionWithNoOutcomeIsMissingRatherThanPassing(t *testing.T) {
	t.Parallel()

	report := conform.Report{Scenario: "x"}
	if got := report.Outcome("java/26.1").Status; got != conform.Missing {
		t.Fatalf("Status = %v, want Missing", got)
	}
}

// runOneVersion checks one version for real and declares the other absent, so a
// test about comparison does not have to build two of everything.
func runOneVersion(t *testing.T, protocolID string, captured, predicted []trace.Trace) conform.Report {
	t.Helper()

	lanes := []conform.Lane{{ProtocolID: protocolID, Recording: "under-test.mccap"}}
	for _, id := range trace.SupportedProtocols() {
		if id != protocolID {
			lanes = append(lanes, conform.Lane{
				ProtocolID:   id,
				AbsentReason: "this test is about " + protocolID,
			})
		}
	}

	report, err := conform.Run(context.Background(),
		conform.Scenario{Name: "one version", Lanes: lanes},
		stubComparer{byProtocol: map[string][]trace.Trace{protocolID: predicted}},
		stubCaptures{"under-test.mccap": captured},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	return report
}
