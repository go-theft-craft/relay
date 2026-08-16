package relay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stubSession is a session with the fields the registry reads and nothing
// running behind it. The registry never touches the connections, so this is
// enough to test it without a pipe per case.
func stubSession(id int64, upstream string) *Session {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &Session{
		ID:     id,
		Info:   SessionInfo{UpstreamAddr: upstream},
		ctx:    ctx,
		cancel: cancel,
		meta:   map[string]any{},
	}
}

func TestRegistryTracksCounts(t *testing.T) {
	r := newRegistry()

	a := stubSession(1, "up-a")
	b := stubSession(2, "up-a")
	c := stubSession(3, "up-b")

	for _, s := range []*Session{a, b, c} {
		r.add(s)
	}

	if got := r.count(); got != 3 {
		t.Fatalf("count() = %d, want 3", got)
	}
	if got := r.upstreamCount("up-a"); got != 2 {
		t.Fatalf("upstreamCount(up-a) = %d, want 2", got)
	}
	if got := r.upstreamCount("up-b"); got != 1 {
		t.Fatalf("upstreamCount(up-b) = %d, want 1", got)
	}

	r.remove(a)

	if got := r.count(); got != 2 {
		t.Fatalf("count() after a removal = %d, want 2", got)
	}
	if got := r.upstreamCount("up-a"); got != 1 {
		t.Fatalf("upstreamCount(up-a) after a removal = %d, want 1", got)
	}
}

// TestRegistryRemoveIsIdempotent matters because a session that is closed twice
// must not drive a count negative or release the drain barrier early.
func TestRegistryRemoveIsIdempotent(t *testing.T) {
	r := newRegistry()

	s := stubSession(1, "up")
	r.add(s)

	r.remove(s)
	r.remove(s)
	r.remove(s)

	if got := r.count(); got != 0 {
		t.Fatalf("count() = %d, want 0", got)
	}
	if got := r.upstreamCount("up"); got != 0 {
		t.Fatalf("upstreamCount(up) = %d, want 0", got)
	}

	// A drain must still return, which it cannot if the WaitGroup went negative
	// or the extra removals released it more than once.
	if err := r.drain(context.Background()); err != nil {
		t.Fatalf("drain after repeated removals: %v", err)
	}
}

func TestRegistrySnapshotsAreStable(t *testing.T) {
	r := newRegistry()

	s := stubSession(1, "up")
	s.Set("player", "alice")
	r.add(s)

	snaps := r.snapshots()
	if len(snaps) != 1 {
		t.Fatalf("snapshots() returned %d entries, want 1", len(snaps))
	}

	// Later churn in the registry must not reach a slice already handed out.
	r.remove(s)
	r.add(stubSession(2, "other"))

	if len(snaps) != 1 || snaps[0].ID != 1 || snaps[0].Meta["player"] != "alice" {
		t.Fatalf("an earlier snapshot changed under the caller: %+v", snaps)
	}
}

func TestRegistryDrainClosesEverySession(t *testing.T) {
	r := newRegistry()

	sessions := make([]*Session, 4)
	for i := range sessions {
		sessions[i] = stubSession(int64(i+1), "up")
		r.add(sessions[i])
	}

	// A real session removes itself as it finishes; stand in for that by
	// watching each context and removing on cancellation.
	for _, s := range sessions {
		go func() {
			<-s.ctx.Done()
			r.remove(s)
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := r.count(); got != 0 {
		t.Fatalf("count() after drain = %d, want 0", got)
	}
}

// TestRegistryDrainHonoursItsDeadline covers the session that will not go: the
// proxy has to stop shutting down eventually and say so.
func TestRegistryDrainHonoursItsDeadline(t *testing.T) {
	r := newRegistry()
	r.add(stubSession(1, "up"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := r.drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain with a session that never finishes = %v, want DeadlineExceeded", err)
	}
}

// TestRegistryUnderContention exists because the accept path reads this while
// every finishing session writes to it.
func TestRegistryUnderContention(t *testing.T) {
	const workers = 8

	r := newRegistry()

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range 200 {
				s := stubSession(int64(w*1000+i), "up")
				r.add(s)
				_ = r.count()
				_ = r.upstreamCount("up")
				_ = r.snapshots()
				r.remove(s)
			}
		}()
	}
	wg.Wait()

	if got := r.count(); got != 0 {
		t.Fatalf("count() = %d, want 0", got)
	}
	if got := r.upstreamCount("up"); got != 0 {
		t.Fatalf("upstreamCount(up) = %d, want 0", got)
	}
}
