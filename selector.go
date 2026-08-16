package relay

import (
	"context"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
)

// Upstream is one server a port may route to.
type Upstream struct {
	Addr string
	// Weight is reserved for weighted selectors. The built-ins ignore it; it is
	// here so adding one later is not a breaking change to PortConfig.
	Weight int
}

// Selector chooses among the upstreams that passed their health check.
//
// It receives the client connection because sticky routing needs the client
// address. Nothing else uses it.
//
// The candidates are given in configuration order with the unhealthy ones
// removed, and a selector must return ErrNoHealthyUpstream for an empty list
// rather than a zero Upstream.
type Selector interface {
	Pick(ctx context.Context, port int, up []Upstream, c net.Conn) (Upstream, error)
}

// SelectorFunc adapts a function to Selector.
type SelectorFunc func(ctx context.Context, port int, up []Upstream, c net.Conn) (Upstream, error)

// Pick implements Selector.
func (f SelectorFunc) Pick(ctx context.Context, port int, up []Upstream, c net.Conn) (Upstream, error) {
	return f(ctx, port, up, c)
}

// FirstHealthy picks the first candidate in configuration order, which makes it
// a primary-with-fallbacks policy: traffic only moves off the first upstream
// when that upstream stops answering. It is the default.
func FirstHealthy() Selector {
	return SelectorFunc(func(_ context.Context, _ int, up []Upstream, _ net.Conn) (Upstream, error) {
		if len(up) == 0 {
			return Upstream{}, ErrNoHealthyUpstream
		}

		return up[0], nil
	})
}

// RoundRobin cycles through the healthy candidates, keeping one counter per
// port.
//
// The counter is taken modulo the current candidate count on every call rather
// than being reset when the list changes, because an index that outlives the
// slice it indexed is the bug this selector invites.
func RoundRobin() Selector {
	var counters sync.Map // port -> *atomic.Uint64

	return SelectorFunc(func(_ context.Context, port int, up []Upstream, _ net.Conn) (Upstream, error) {
		if len(up) == 0 {
			return Upstream{}, ErrNoHealthyUpstream
		}

		value, _ := counters.LoadOrStore(port, new(atomic.Uint64))
		counter, ok := value.(*atomic.Uint64)
		if !ok {
			return up[0], nil
		}

		next := counter.Add(1) - 1

		return up[next%uint64(len(up))], nil
	})
}

// LeastConn picks the candidate with the fewest live sessions, breaking ties
// towards the earlier one so a cold start still fills in configuration order.
//
// It reads the per-upstream counts the registry keeps, which is why the
// registry keeps them: a walk of every live session on every accept is not
// something a proxy holding thousands of them can afford.
func LeastConn() Selector { return &leastConn{} }

// leastConn is a type rather than a closure because the proxy has to hand it
// the registry after New has built both.
type leastConn struct {
	reg atomic.Pointer[registry]
}

// attach gives the selector the registry to count against. A selector that was
// never attached falls back to configuration order rather than failing: it is
// still a working proxy, just not a balanced one.
func (l *leastConn) attach(r *registry) { l.reg.Store(r) }

// Pick implements Selector.
func (l *leastConn) Pick(_ context.Context, _ int, up []Upstream, _ net.Conn) (Upstream, error) {
	if len(up) == 0 {
		return Upstream{}, ErrNoHealthyUpstream
	}

	reg := l.reg.Load()
	if reg == nil {
		return up[0], nil
	}

	best, fewest := up[0], reg.upstreamCount(up[0].Addr)
	for _, candidate := range up[1:] {
		if n := reg.upstreamCount(candidate.Addr); n < fewest {
			best, fewest = candidate, n
		}
	}

	return best, nil
}

// StickyByClientIP sends every connection from one client IP to the same
// upstream, so a client that reconnects from a new ephemeral port lands where
// it did before.
//
// The hash is FNV-1a from the standard library. The property wanted here is
// stability across restarts, not cryptographic strength, and a stdlib hash
// keeps the require block empty.
func StickyByClientIP() Selector {
	return SelectorFunc(func(_ context.Context, _ int, up []Upstream, c net.Conn) (Upstream, error) {
		if len(up) == 0 {
			return Upstream{}, ErrNoHealthyUpstream
		}

		ip := clientIP(c)
		if ip == "" {
			// An address that does not parse is not a reason to refuse a
			// connection; it only means this one cannot be sticky.
			return up[0], nil
		}

		sum := fnv.New32a()
		_, _ = sum.Write([]byte(ip))

		return up[sum.Sum32()%uint32(len(up))], nil
	})
}

// clientIP returns the address's IP without its port, or "" when there is
// nothing usable to hash.
func clientIP(c net.Conn) string {
	if c == nil {
		return ""
	}

	addr := c.RemoteAddr()
	if addr == nil {
		return ""
	}

	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		// Not every net.Addr is host:port — net.Pipe's is neither. Hashing the
		// whole string is still stable, which is all this needs.
		return addr.String()
	}

	return host
}
