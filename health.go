package relay

import (
	"context"
	"sync"
	"time"
)

// healthCache answers "is this upstream usable" without probing on every
// connection.
//
// A result is cached for Config.ProbeTTL, so a burst of clients against one
// port costs one probe rather than one each. Concurrent misses for the same
// address collapse into a single in-flight probe and the rest wait on its
// result. And dial failures write to the same cache as probes, so passive
// signal and active probe share one state instead of disagreeing.
//
// Health is resolved lazily, when a client connects, rather than on a timer. A
// startup probe that goes stale routes traffic at a server that is no longer
// there, which is a worse failure than the cost this structure exists to make
// affordable.
type healthCache struct {
	prober  Prober
	ttl     time.Duration
	timeout time.Duration
	now     func() time.Time

	mu      sync.Mutex
	entries map[string]*healthEntry
}

type healthEntry struct {
	healthy bool
	at      time.Time
	// done is non-nil while a probe is in flight and is closed when it lands.
	// Waiters block on it and re-read rather than probing themselves.
	done chan struct{}
}

func newHealthCache(cfg *Config) *healthCache {
	return &healthCache{
		prober:  cfg.Prober,
		ttl:     cfg.ProbeTTL,
		timeout: cfg.ProbeTimeout,
		now:     cfg.now,
		entries: make(map[string]*healthEntry),
	}
}

// fresh reports whether an entry can answer without a new probe. It must be
// called with mu held.
func (h *healthCache) fresh(e *healthEntry) bool {
	return e != nil && e.done == nil && h.now().Sub(e.at) < h.ttl
}

// check returns the healthy subset of addrs, in the order they were given,
// because FirstHealthy depends on that order meaning configuration order.
func (h *healthCache) check(ctx context.Context, addrs []string) []string {
	misses, waits := h.claim(addrs)

	if len(misses) > 0 {
		h.probeAll(ctx, misses)
	}

	for _, done := range waits {
		select {
		case <-done:
		case <-ctx.Done():
			// A caller that gave up gets the answers that did land; the probe
			// itself still completes and populates the cache for the next client.
			return nil
		}
	}

	return h.healthySubset(addrs)
}

// claim splits the addresses into the ones this caller will probe and the ones
// another caller is already probing.
func (h *healthCache) claim(addrs []string) (misses []string, waits []chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()

	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}

		entry := h.entries[addr]
		if h.fresh(entry) {
			continue
		}
		if entry != nil && entry.done != nil {
			waits = append(waits, entry.done)

			continue
		}

		h.entries[addr] = &healthEntry{done: make(chan struct{})}
		misses = append(misses, addr)
	}

	return misses, waits
}

// probeAll probes every miss concurrently under one shared deadline.
//
// The prober is never called with mu held. Probing under the lock would
// serialise the fan-out and turn one slow upstream into a stall on every
// accept, which is the mistake this whole structure exists to avoid.
func (h *healthCache) probeAll(ctx context.Context, misses []string) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, addr := range misses {
		wg.Add(1)

		go func() {
			defer wg.Done()

			h.record(addr, h.prober.Probe(ctx, addr) == nil)
		}()
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-ctx.Done():
		// A Prober that ignores its deadline cannot be cancelled, only
		// abandoned — and this runs on the accept path, so waiting for it would
		// turn one wedged upstream into a stall on every connection. Whatever
		// is still outstanding is recorded down, which also releases the callers
		// waiting on it. If the probe ever does return, it overwrites this with
		// the truth.
		h.recordPendingDown(misses)
	}
}

// recordPendingDown fails the addresses whose probes never landed, and leaves
// alone the ones that answered in time.
func (h *healthCache) recordPendingDown(addrs []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, addr := range addrs {
		entry := h.entries[addr]
		if entry == nil || entry.done == nil {
			continue
		}

		entry.healthy = false
		entry.at = h.now()
		close(entry.done)
		entry.done = nil
	}
}

// record publishes a result and releases anyone waiting on it.
func (h *healthCache) record(addr string, healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entry := h.entries[addr]
	if entry == nil {
		entry = &healthEntry{}
		h.entries[addr] = entry
	}

	entry.healthy = healthy
	entry.at = h.now()

	if entry.done != nil {
		close(entry.done)
		entry.done = nil
	}
}

// healthySubset assembles the answer in the caller's order.
func (h *healthCache) healthySubset(addrs []string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if entry := h.entries[addr]; entry != nil && entry.healthy {
			out = append(out, addr)
		}
	}

	return out
}

// markDown records a dial failure.
//
// A dial that was refused is better evidence than a probe that succeeded a
// moment ago, so it writes to the same cache and starts the same TTL: the next
// client does not retry an upstream this one just found dead.
func (h *healthCache) markDown(addr string) { h.record(addr, false) }
