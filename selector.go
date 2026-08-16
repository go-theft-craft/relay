package relay

import (
	"context"
	"net"
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
