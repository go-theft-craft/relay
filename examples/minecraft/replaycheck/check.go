// Package replaycheck is M9.1's gate.
//
// A recording that does not reproduce itself is not an oracle. Every later
// stage of the gameplay work is judged against these files, so before any of
// them is trusted the file has to be shown to replay to the same digest it was
// written with.
//
// One replay settles that. Repeating it would test this build's determinism
// rather than the recording, and the digest is computed from the file's own
// bytes through a player carrying no state between records, so a second run
// reads the same bytes through the same code and can only agree.
package replaycheck

import (
	"context"
	"errors"
	"fmt"
	"os"

	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
	"github.com/go-theft-craft/minecraft-protocol/replay"
)

// Result is what one check learned about one recording.
type Result struct {
	Path string
	// Records is how many replayable records the file held.
	Records int
	// Digest is what replaying the file produced.
	Digest string
	// Recorded is the digest the file's own trailer carries, empty when the
	// recording has no trailer or was written under a different digest rule.
	Recorded string
	// Complete reports that the file has a trailer. A file without one was
	// never closed — which is what a capture of a killed process looks like,
	// and is a finding rather than a crash.
	Complete bool
	// Divergences are the places where this build's session proposed a
	// different state than the recording holds. They are the signal that a
	// codec change moved a connection somewhere else.
	Divergences []replay.Divergence
}

// OK reports whether the recording can be used as evidence.
func (r Result) OK() bool {
	return r.Complete && r.Recorded != "" && r.Recorded == r.Digest && len(r.Divergences) == 0
}

// Explain says why in one line, for a command that has to exit non-zero and
// tell somebody what to look at.
func (r Result) Explain() string {
	switch {
	case !r.Complete:
		return "the recording has no trailer; it was never closed"
	case r.Recorded == "":
		return "the recording carries no comparable digest"
	case r.Recorded != r.Digest:
		return fmt.Sprintf("replay produced %s, the recording claims %s", short(r.Digest), short(r.Recorded))
	case len(r.Divergences) > 0:
		return fmt.Sprintf("%d state divergences, first: %s", len(r.Divergences), r.Divergences[0])
	default:
		return "replays to its own digest"
	}
}

// Check replays a recording and reports whether it reproduced itself.
//
// A read error is returned rather than folded into the Result: a file that
// cannot be opened is a different problem from one that replays wrong, and a
// caller that cannot tell them apart will report the wrong thing.
func Check(ctx context.Context, path string) (Result, error) {
	file, err := os.Open(path)
	if err != nil {
		return Result{Path: path}, fmt.Errorf("replaycheck: open: %w", err)
	}
	defer func() { _ = file.Close() }()

	reader, err := mccapture.NewReader(file)
	if err != nil {
		return Result{Path: path}, fmt.Errorf("replaycheck: read header: %w", err)
	}

	player, err := replay.New(reader,
		replay.WithMode(replay.ModeFast),
		replay.WithResolver(replay.ResolverFunc(protocols.Resolve)),
	)
	if err != nil {
		return Result{Path: path}, fmt.Errorf("replaycheck: build the player: %w", err)
	}

	outcome, err := player.Run(ctx)
	if err != nil && !errors.Is(err, mccapture.ErrEndOfCapture) {
		// Divergences and a wrong digest are findings and belong in the Result.
		// A record that cannot be decoded at all is not: the file is damaged.
		return Result{Path: path}, fmt.Errorf("replaycheck: replay: %w", err)
	}

	result := Result{
		Path:        path,
		Records:     outcome.Records,
		Digest:      outcome.Digest,
		Divergences: outcome.Divergences,
	}

	if trailer, complete := reader.Trailer(); complete {
		result.Complete = true
		if trailer.Comparable() {
			result.Recorded = trailer.Digest
		}
	}

	return result, nil
}

func short(digest string) string {
	if len(digest) <= 12 {
		return digest
	}

	return digest[:12]
}
