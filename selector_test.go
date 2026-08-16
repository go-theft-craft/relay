package relay

import (
	"context"
	"errors"
	"net"
	"testing"
)

// stubAddr stands in for a client address without a socket behind it.
type stubAddr string

func (stubAddr) Network() string  { return "tcp" }
func (a stubAddr) String() string { return string(a) }

// stubConn is a net.Conn that only knows its remote address, which is all any
// selector reads.
type stubConn struct {
	net.Conn
	remote net.Addr
}

func (c stubConn) RemoteAddr() net.Addr { return c.remote }

func connFrom(addr string) net.Conn { return stubConn{remote: stubAddr(addr)} }

func upstreams(addrs ...string) []Upstream {
	out := make([]Upstream, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, Upstream{Addr: a})
	}

	return out
}

func TestFirstHealthyPicksTheFirst(t *testing.T) {
	s := FirstHealthy()
	up := upstreams("a:1", "b:1", "c:1")

	for range 5 {
		got, err := s.Pick(context.Background(), 1, up, nil)
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		if got.Addr != "a:1" {
			t.Fatalf("Pick = %q, want a:1 every time", got.Addr)
		}
	}
}

func TestRoundRobinCycles(t *testing.T) {
	s := RoundRobin()
	up := upstreams("a:1", "b:1", "c:1")

	want := []string{"a:1", "b:1", "c:1", "a:1", "b:1", "c:1"}
	for i, w := range want {
		got, err := s.Pick(context.Background(), 1, up, nil)
		if err != nil {
			t.Fatalf("Pick %d: %v", i, err)
		}
		if got.Addr != w {
			t.Fatalf("Pick %d = %q, want %q", i, got.Addr, w)
		}
	}
}

// TestRoundRobinSurvivesAShrinkingList is the bug this selector invites: an
// index that outlives the slice it indexed.
func TestRoundRobinSurvivesAShrinkingList(t *testing.T) {
	s := RoundRobin()

	for range 3 {
		if _, err := s.Pick(context.Background(), 1, upstreams("a:1", "b:1", "c:1"), nil); err != nil {
			t.Fatalf("Pick: %v", err)
		}
	}

	// Two upstreams have just failed their probes.
	for range 4 {
		got, err := s.Pick(context.Background(), 1, upstreams("a:1"), nil)
		if err != nil {
			t.Fatalf("Pick against a shrunk list: %v", err)
		}
		if got.Addr != "a:1" {
			t.Fatalf("Pick = %q, want a:1", got.Addr)
		}
	}
}

func TestRoundRobinCountsPerPort(t *testing.T) {
	s := RoundRobin()
	up := upstreams("a:1", "b:1")

	first, _ := s.Pick(context.Background(), 1, up, nil)
	other, _ := s.Pick(context.Background(), 2, up, nil)

	if first.Addr != "a:1" || other.Addr != "a:1" {
		t.Fatalf("ports shared a counter: got %q and %q, want both a:1", first.Addr, other.Addr)
	}
}

func TestLeastConnPicksTheQuietestUpstream(t *testing.T) {
	reg := newRegistry()
	reg.add(stubSession(1, "a:1"))
	reg.add(stubSession(2, "a:1"))
	reg.add(stubSession(3, "b:1"))

	s := LeastConn()
	s.(*leastConn).attach(reg)

	got, err := s.Pick(context.Background(), 1, upstreams("a:1", "b:1", "c:1"), nil)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Addr != "c:1" {
		t.Fatalf("Pick = %q, want c:1 — it has no sessions at all", got.Addr)
	}
}

func TestLeastConnBreaksTiesTowardsTheEarlier(t *testing.T) {
	reg := newRegistry()

	s := LeastConn()
	s.(*leastConn).attach(reg)

	got, err := s.Pick(context.Background(), 1, upstreams("a:1", "b:1"), nil)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Addr != "a:1" {
		t.Fatalf("Pick = %q, want a:1 on a tie", got.Addr)
	}
}

// TestLeastConnWithoutARegistry covers the selector a caller built by hand and
// never attached: still a working proxy, just not a balanced one.
func TestLeastConnWithoutARegistry(t *testing.T) {
	got, err := LeastConn().Pick(context.Background(), 1, upstreams("a:1", "b:1"), nil)
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Addr != "a:1" {
		t.Fatalf("Pick = %q, want a:1", got.Addr)
	}
}

func TestStickyByClientIPIsStable(t *testing.T) {
	s := StickyByClientIP()
	up := upstreams("a:1", "b:1", "c:1", "d:1")

	first, err := s.Pick(context.Background(), 1, up, connFrom("10.0.0.7:51000"))
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	// The same client reconnecting from a new ephemeral port must land where it
	// did before, which is the whole property.
	again, err := s.Pick(context.Background(), 1, up, connFrom("10.0.0.7:62000"))
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if again.Addr != first.Addr {
		t.Fatalf("a reconnect from a new port moved from %q to %q", first.Addr, again.Addr)
	}

	// And the port the proxy listens on is not part of the key either.
	onAnotherPort, err := s.Pick(context.Background(), 2, up, connFrom("10.0.0.7:51000"))
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if onAnotherPort.Addr != first.Addr {
		t.Fatalf("the listening port changed the answer: %q became %q", first.Addr, onAnotherPort.Addr)
	}
}

func TestStickyByClientIPSpreadsClients(t *testing.T) {
	s := StickyByClientIP()
	up := upstreams("a:1", "b:1", "c:1", "d:1")

	seen := map[string]int{}
	for i := range 64 {
		got, err := s.Pick(context.Background(), 1, up, connFrom(net.JoinHostPort(
			"10.0.0."+string(rune('0'+i%10)), "51000",
		)))
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		seen[got.Addr]++
	}

	if len(seen) < 2 {
		t.Fatalf("every client hashed to the same upstream: %v", seen)
	}
}

// TestStickyByClientIPFallsBackOnAnUnparseableAddress covers the connection
// whose address is not host:port. It must not panic, and it must still route.
func TestStickyByClientIPFallsBackOnAnUnparseableAddress(t *testing.T) {
	s := StickyByClientIP()
	up := upstreams("a:1", "b:1")

	for _, c := range []net.Conn{nil, connFrom("pipe"), stubConn{}} {
		got, err := s.Pick(context.Background(), 1, up, c)
		if err != nil {
			t.Fatalf("Pick with %v: %v", c, err)
		}
		if got.Addr == "" {
			t.Fatal("Pick returned a zero upstream")
		}
	}
}

func TestEverySelectorRefusesAnEmptyList(t *testing.T) {
	selectors := map[string]Selector{
		"FirstHealthy":     FirstHealthy(),
		"RoundRobin":       RoundRobin(),
		"LeastConn":        LeastConn(),
		"StickyByClientIP": StickyByClientIP(),
	}

	for name, s := range selectors {
		t.Run(name, func(t *testing.T) {
			_, err := s.Pick(context.Background(), 1, nil, connFrom("10.0.0.1:1"))
			if !errors.Is(err, ErrNoHealthyUpstream) {
				t.Fatalf("Pick with no candidates = %v, want ErrNoHealthyUpstream", err)
			}
		})
	}
}
