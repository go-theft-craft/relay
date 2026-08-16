package relay

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock drives the TTL without sleeping. A probe TTL that can only be
// exercised by sleeping makes the health tests slow and flaky at once.
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *fakeClock {
	return &fakeClock{at: time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.at
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.at = c.at.Add(d)
}

// probeFunc adapts a function to Prober.
type probeFunc func(context.Context, string) error

func (f probeFunc) Probe(ctx context.Context, addr string) error { return f(ctx, addr) }

func newTestCache(t *testing.T, p Prober, clock *fakeClock, timeout time.Duration) *healthCache {
	t.Helper()

	cfg := minimalConfig()
	cfg.Prober = p
	cfg.ProbeTTL = 10 * time.Second
	cfg.ProbeTimeout = timeout
	cfg.now = clock.now

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	return newHealthCache(&cfg)
}

func TestHealthCacheHonoursTheTTL(t *testing.T) {
	var probes atomic.Int64
	clock := newClock()

	h := newTestCache(t, probeFunc(func(context.Context, string) error {
		probes.Add(1)

		return nil
	}), clock, time.Second)

	for range 2 {
		if got := h.check(context.Background(), []string{"a:1"}); len(got) != 1 {
			t.Fatalf("check() = %v, want one healthy address", got)
		}
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("two checks inside the TTL cost %d probes, want 1", n)
	}

	clock.advance(11 * time.Second)

	if got := h.check(context.Background(), []string{"a:1"}); len(got) != 1 {
		t.Fatalf("check() after the TTL = %v, want one healthy address", got)
	}
	if n := probes.Load(); n != 2 {
		t.Fatalf("a check past the TTL cost a total of %d probes, want 2", n)
	}
}

func TestHealthCacheCollapsesConcurrentMisses(t *testing.T) {
	const callers = 50

	var probes atomic.Int64
	release := make(chan struct{})

	h := newTestCache(t, probeFunc(func(context.Context, string) error {
		probes.Add(1)
		<-release

		return nil
	}), newClock(), 5*time.Second)

	results := make(chan []string, callers)

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			results <- h.check(context.Background(), []string{"a:1"})
		}()
	}

	// Let every caller reach the cache before the single probe is allowed to
	// finish, so the collapse is real rather than lucky.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	close(results)

	for got := range results {
		if len(got) != 1 || got[0] != "a:1" {
			t.Fatalf("a caller got %v, want [a:1]", got)
		}
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("%d callers cost %d probes, want 1", callers, n)
	}
}

// TestHealthCacheProbesConcurrently uses a prober that cannot finish until
// every address has arrived, so a serial implementation deadlocks rather than
// merely running slowly.
func TestHealthCacheProbesConcurrently(t *testing.T) {
	addrs := []string{"a:1", "b:1", "c:1", "d:1"}

	var (
		mu      sync.Mutex
		arrived int
	)
	all := make(chan struct{})

	h := newTestCache(t, probeFunc(func(context.Context, string) error {
		mu.Lock()
		arrived++
		if arrived == len(addrs) {
			close(all)
		}
		mu.Unlock()

		<-all

		return nil
	}), newClock(), 5*time.Second)

	got := h.check(context.Background(), addrs)
	if !slices.Equal(got, addrs) {
		t.Fatalf("check() = %v, want %v", got, addrs)
	}
}

// TestHealthCacheBoundsAWedgedProbe covers the upstream that accepts a probe
// and never answers. The accept path cannot wait for it.
func TestHealthCacheBoundsAWedgedProbe(t *testing.T) {
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })

	h := newTestCache(t, probeFunc(func(context.Context, string) error {
		<-stuck

		return nil
	}), newClock(), 100*time.Millisecond)

	start := time.Now()
	got := h.check(context.Background(), []string{"a:1", "b:1"})
	elapsed := time.Since(start)

	if len(got) != 0 {
		t.Fatalf("check() = %v, want no healthy addresses", got)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("check() took %v, want it bounded by the probe timeout", elapsed)
	}
}

func TestHealthCacheMarkDownWritesThrough(t *testing.T) {
	var probes atomic.Int64
	clock := newClock()

	h := newTestCache(t, probeFunc(func(context.Context, string) error {
		probes.Add(1)

		return nil
	}), clock, time.Second)

	h.markDown("a:1")

	if got := h.check(context.Background(), []string{"a:1"}); len(got) != 0 {
		t.Fatalf("check() after markDown = %v, want nothing healthy", got)
	}
	if n := probes.Load(); n != 0 {
		t.Fatalf("a dial failure cost %d probes, want 0 — it is the same cache", n)
	}

	clock.advance(11 * time.Second)

	if got := h.check(context.Background(), []string{"a:1"}); len(got) != 1 {
		t.Fatalf("check() past the TTL = %v, want the address probed again and healthy", got)
	}
}

// TestHealthCachePreservesOrder matters because FirstHealthy reads the result
// as configuration order.
func TestHealthCachePreservesOrder(t *testing.T) {
	h := newTestCache(t, probeFunc(func(_ context.Context, addr string) error {
		if addr == "b:1" {
			return context.DeadlineExceeded
		}

		return nil
	}), newClock(), time.Second)

	got := h.check(context.Background(), []string{"a:1", "b:1", "c:1", "d:1"})
	if !slices.Equal(got, []string{"a:1", "c:1", "d:1"}) {
		t.Fatalf("check() = %v, want the healthy addresses in the given order", got)
	}
}

// TestHealthCacheHandlesRepeatedAddresses covers a port configured with the
// same upstream twice, which must not claim the same entry twice and wait on
// itself.
func TestHealthCacheHandlesRepeatedAddresses(t *testing.T) {
	var probes atomic.Int64

	h := newTestCache(t, probeFunc(func(context.Context, string) error {
		probes.Add(1)

		return nil
	}), newClock(), time.Second)

	got := h.check(context.Background(), []string{"a:1", "a:1"})
	if !slices.Equal(got, []string{"a:1", "a:1"}) {
		t.Fatalf("check() = %v, want the address back twice", got)
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("a repeated address cost %d probes, want 1", n)
	}
}
