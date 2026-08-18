// Package conform is the two-version gate every M9 mechanic stage runs.
//
// A mechanic is not verified until it has been checked against both a 1.8.9
// and a 26.1.2 server, and the thing that makes that enforceable rather than
// aspirational is here: a scenario that names no lane for a version this
// project can read is an error before anything runs. A mechanic silently
// checked on one version and reported as green is the failure this package
// exists to prevent.
//
// It must not import minecraft-simulation. An oracle that depended on the thing
// it verifies would reproduce a wrong constant on both sides of the comparison
// and cancel it out, which is why the thing under test arrives through the
// Comparer interface instead.
package conform

import (
	"context"
	"fmt"
	"math"

	"github.com/go-theft-craft/relay/examples/minecraft/trace"
)

// Lane is one version's run of one scenario.
type Lane struct {
	ProtocolID string
	// Recording is the captured trace this lane compares against.
	Recording string
	// AbsentReason, when non-empty, declares that the mechanic under test does
	// not exist in this version. A lane with a reason runs nothing and reports
	// Absent; a lane with neither a reason nor a recording is an error.
	AbsentReason string
}

// Scenario is one mechanic, checked on every version that has it.
type Scenario struct {
	Name  string
	Lanes []Lane
}

// Status is what became of one lane.
//
// Missing is distinct from Absent on purpose: Absent means a version does not
// have the mechanic and says why, Missing means nobody checked.
type Status int

const (
	Pass Status = iota
	Fail
	Absent
	Missing
)

func (s Status) String() string {
	switch s {
	case Pass:
		return "pass"
	case Fail:
		return "fail"
	case Absent:
		return "absent"
	case Missing:
		return "missing"
	}

	return fmt.Sprintf("status(%d)", int(s))
}

// Outcome is what one lane produced.
type Outcome struct {
	ProtocolID string
	Status     Status
	Tolerance  trace.Tolerance
	// MaxDeviation is the largest difference observed, in blocks. It is
	// reported on a pass as well as a failure, because a lane that passes at
	// 99% of its tolerance is a lane about to start failing.
	MaxDeviation float64
	Detail       string
}

// Report is every lane's outcome for one scenario.
type Report struct {
	Scenario string
	Outcomes []Outcome
}

// Outcome returns the outcome for one protocol ID, and a zero Outcome with
// Status Missing if the scenario had no lane for it.
//
// Callers that want the error should use Run's, which fires before any lane
// executes.
func (r Report) Outcome(protocolID string) Outcome {
	for _, outcome := range r.Outcomes {
		if outcome.ProtocolID == protocolID {
			return outcome
		}
	}

	return Outcome{ProtocolID: protocolID, Status: Missing}
}

// OK reports whether every lane passed or was declared absent.
func (r Report) OK() bool {
	for _, outcome := range r.Outcomes {
		if outcome.Status != Pass && outcome.Status != Absent {
			return false
		}
	}

	return len(r.Outcomes) > 0
}

// Comparer is the thing under test. M9.2 onward supply a kernel-backed one;
// the tests here supply a stub, because this package must not import
// minecraft-simulation.
type Comparer interface {
	Trajectories(ctx context.Context, protocolID string) ([]trace.Trace, error)
}

// Loader turns a lane's recording into the trajectories it captured.
//
// It is an interface rather than a call to trace.ExtractFile so that a test can
// state what a capture held without shipping one: a .mccap carries player
// UUIDs, usernames, and chat, which is why no recording is committed anywhere
// in this repository. Recordings is the implementation every real caller wants.
type Loader interface {
	Load(ctx context.Context, lane Lane) ([]trace.Trace, error)
}

// Recordings loads a lane's capture from disk.
type Recordings struct{}

// Load reads the recording the lane names. The protocol comes from the file's
// own header, so a lane that names a 47 recording under a 775 protocol ID fails
// at the comparison rather than being read by the wrong rules.
func (Recordings) Load(_ context.Context, lane Lane) ([]trace.Trace, error) {
	traces, header, err := trace.ExtractFile(lane.Recording)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", lane.Recording, err)
	}
	if header.Protocol != lane.ProtocolID {
		return nil, fmt.Errorf("%s was recorded against %s, and the lane is %s",
			lane.Recording, header.Protocol, lane.ProtocolID)
	}

	return traces, nil
}

// Run executes every lane.
//
// It returns an error, rather than a report, when a scenario names no lane for
// a version that has a registered extractor, when a lane neither checks
// anything nor says why not, or when a lane names a version nothing can read.
// Those are mistakes in the scenario, and reporting them as failures of the
// thing under test would send the next reader to the wrong place.
func Run(ctx context.Context, s Scenario, compare Comparer, load Loader) (Report, error) {
	if err := validate(s); err != nil {
		return Report{}, err
	}

	report := Report{Scenario: s.Name}
	for _, lane := range s.Lanes {
		report.Outcomes = append(report.Outcomes, run(ctx, s, lane, compare, load))
	}

	return report, nil
}

func validate(s Scenario) error {
	seen := map[string]bool{}
	for _, lane := range s.Lanes {
		if lane.Recording == "" && lane.AbsentReason == "" {
			return fmt.Errorf("conform: %s: lane %s has no recording and no absent reason",
				s.Name, lane.ProtocolID)
		}
		if _, err := trace.ToleranceFor(lane.ProtocolID); err != nil {
			return fmt.Errorf("conform: %s: %w", s.Name, err)
		}
		if seen[lane.ProtocolID] {
			return fmt.Errorf("conform: %s: two lanes for %s", s.Name, lane.ProtocolID)
		}
		seen[lane.ProtocolID] = true
	}

	for _, id := range trace.SupportedProtocols() {
		if !seen[id] {
			return fmt.Errorf(
				"conform: %s: no lane for %s; a mechanic checked on one version claims "+
					"nothing about the other", s.Name, id)
		}
	}

	return nil
}

func run(ctx context.Context, s Scenario, lane Lane, compare Comparer, load Loader) Outcome {
	outcome := Outcome{ProtocolID: lane.ProtocolID}

	// Validation has already established that every lane names a readable
	// version, so this cannot fail here.
	outcome.Tolerance, _ = trace.ToleranceFor(lane.ProtocolID)

	if lane.AbsentReason != "" {
		outcome.Status = Absent
		outcome.Detail = lane.AbsentReason

		return outcome
	}

	captured, err := load.Load(ctx, lane)
	if err != nil {
		outcome.Status = Fail
		outcome.Detail = fmt.Sprintf("read the recording: %v", err)

		return outcome
	}

	predicted, err := compare.Trajectories(ctx, lane.ProtocolID)
	if err != nil {
		outcome.Status = Fail
		outcome.Detail = fmt.Sprintf("produce trajectories for %s: %v", s.Name, err)

		return outcome
	}

	deviation, detail := diff(captured, predicted, outcome.Tolerance)
	outcome.MaxDeviation = deviation
	outcome.Detail = detail
	outcome.Status = Pass
	if detail != "" {
		outcome.Status = Fail
	}

	return outcome
}

// diff compares two trajectory lists and returns the largest deviation seen,
// with a description of the first difference that exceeded what the wire
// allows. An empty description means every sample was within tolerance.
//
// Traces are compared in order, which is the order the extractor produces:
// first appearance. Comparing by entity ID would not work — a simulation
// assigns its own — and comparing by family would not distinguish two arrows.
func diff(captured, predicted []trace.Trace, allowance trace.Tolerance) (float64, string) {
	if len(captured) != len(predicted) {
		return 0, fmt.Sprintf("the recording holds %d trajectories and the run produced %d",
			len(captured), len(predicted))
	}

	var (
		worst  float64
		detail string
	)

	for index, want := range captured {
		got := predicted[index]
		if len(want.Samples) != len(got.Samples) {
			if detail == "" {
				detail = fmt.Sprintf(
					"entity %d (%s): the recording holds %d samples and the run produced %d",
					want.EntityID, want.Family, len(want.Samples), len(got.Samples))
			}

			continue
		}

		for sample := range want.Samples {
			deviation := distance(want.Samples[sample].Position, got.Samples[sample].Position)
			worst = math.Max(worst, deviation)

			// The allowance grows with the sample index because a trace does
			// not record which of its samples came from an absolute packet and
			// which accumulated a relative move. Assuming every sample after
			// the first was relative is the loosest honest reading of the
			// tolerance, and it is stated here rather than hidden in a
			// constant.
			allowed := allowance.Absolute + allowance.Relative*float64(sample)
			if deviation > allowed && detail == "" {
				detail = fmt.Sprintf(
					"entity %d (%s) sample %d: off by %g blocks, and %g is allowed",
					want.EntityID, want.Family, sample, deviation, allowed)
			}
		}
	}

	return worst, detail
}

// distance is the largest per-axis difference rather than the Euclidean one, so
// a deviation can be read against the wire's per-axis resolution without a
// square root in the way.
func distance(a, b trace.Vec3) float64 {
	return math.Max(math.Abs(a.X-b.X), math.Max(math.Abs(a.Y-b.Y), math.Abs(a.Z-b.Z)))
}
