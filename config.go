package relay

import (
	"fmt"
	"log/slog"
	"time"
)

// Default values filled in by Config.validate. They are named rather than
// inlined because the documentation quotes them and a drift between the two is
// the kind of thing nobody notices.
const (
	defaultReadBufferSize = 4096
	defaultMaxMessageSize = 2 << 20
	defaultProbeTTL       = 10 * time.Second
	defaultProbeTimeout   = 2 * time.Second
	defaultDialTimeout    = 5 * time.Second
	defaultDrainGrace     = 5 * time.Second
)

// OverflowPolicy is what an accept does when MaxSessions is already reached.
type OverflowPolicy uint8

const (
	// OverflowClose closes a connection that arrives with no session slot free,
	// so a client that cannot be served learns so immediately.
	OverflowClose OverflowPolicy = iota
	// OverflowWait holds the connection until a slot frees or the proxy stops.
	OverflowWait
)

// PortConfig maps one listening port to the upstreams it may route to.
type PortConfig struct {
	// Port is the TCP port to listen on. Zero binds an ephemeral port, which
	// Proxy.Addrs reports back.
	Port      int
	Upstreams []Upstream
}

// Config describes a proxy. Every fault in it is reported from New, rather than
// as a nil dereference on the first connection, which is the whole reason
// validation is a step rather than a scattering of nil checks.
type Config struct {
	// Ports is the set of listeners and their candidate upstreams. At least one
	// is required, each needs at least one upstream, and a port number may not
	// repeat.
	Ports []PortConfig

	// Framer or NewFramer is required. Everything else on this line is
	// optional: a Codec
	// makes decoded packets visible to hooks and sinks, a Prober replaces the
	// default TCP dial with a health check that speaks the protocol, a Sink
	// records what crossed the wire, and a Selector chooses among healthy
	// upstreams.
	Framer   Framer
	Codec    Codec
	Prober   Prober
	Sink     Sink
	Selector Selector

	// NewCodec builds a Codec per session, for the common case where decoding
	// carries connection state.
	//
	// Codec above is one instance shared by every session, which only suits a
	// codec that is a pure function of its bytes. Most are not: a protocol with
	// connection states has a state machine per connection, and often a decoder
	// per direction as well, so one shared instance would have every client
	// advancing everyone else's state. Set this or Codec, not both.
	NewCodec func() (Codec, error)

	// NewFramer builds a Framer per session and per direction, for a protocol
	// whose message boundaries are not a pure function of the bytes.
	//
	// Framer above is one instance shared by every session and by both
	// directions, which is right for a length prefix and wrong for anything
	// that has to know what it is looking at. A protocol that finds a boundary
	// by decoding — no length on the wire, so the packet's own shape ends it —
	// needs the same per-connection state a Codec does, and needs it per
	// direction too, because the same leading byte means different things
	// arriving from each peer.
	//
	// It is called twice per session, once per direction, and the returned
	// framer handles messages travelling that way: the ToServer framer reads
	// the client connection and writes the upstream one. Set this or Framer,
	// not both. A session whose framer cannot be built is reported to
	// OnSessionError and dropped, since there is no framing without it.
	NewFramer func(Direction) (Framer, error)

	// CaptureRaw records every byte crossing the client connection to
	// Sink.RawChunk, below any framing and below any mid-stream transform.
	//
	// It is off by default because it costs a copy per socket read and write,
	// and because a proxy that is not recording should not pay for one that is.
	// It requires a Sink: capture with nowhere to put it is a mistake worth
	// reporting rather than a no-op worth hiding.
	CaptureRaw bool

	// Hooks run in order on every relayed message. PreFrame runs once, on the
	// opening bytes of a client connection, before any framing.
	Hooks    []Hook
	PreFrame PreFrame

	// ReadBufferSize sizes each direction's raw read buffer. At ten thousand
	// sessions the default is worth roughly 80 MiB across the process, which is
	// what makes it a knob rather than a constant.
	//
	// There is deliberately no write-side counterpart. Writes go straight to the
	// socket so that a message is on the wire when WriteMessage returns; a write
	// buffer would need a flush at every message boundary to preserve that, and
	// a knob whose only correct setting is "flushed immediately" is not a knob.
	ReadBufferSize int
	// MaxMessageSize bounds what a Framer may hand back, so a hostile or buggy
	// length prefix cannot allocate without limit.
	MaxMessageSize int
	// MaxSessions bounds live sessions. Zero means unlimited: opting in to a
	// bound is the caller's decision, but an unbounded accept loop is the first
	// thing to fall over.
	MaxSessions int
	Overflow    OverflowPolicy

	// ProbeTTL is how long a health result is trusted, so a burst of clients
	// against one port costs one probe rather than one each.
	ProbeTTL time.Duration
	// ProbeTimeout bounds a probe, and with it the accept path.
	ProbeTimeout time.Duration
	DialTimeout  time.Duration
	// DrainGrace is how long a closing session is given to finish an in-flight
	// write. It is one write, not a lingering session.
	DrainGrace time.Duration

	// OnSessionError receives every per-session fault. Run returns only fatal
	// faults, so this is where the rest go; with thousands of sessions there is
	// nowhere else they can go.
	OnSessionError func(*Session, error)
	Logger         *slog.Logger

	// now is the clock the probe cache reads. Tests set it; nothing else does,
	// because a probe TTL that can only be exercised by sleeping makes the
	// health tests slow and flaky at the same time.
	now func() time.Time
}

// validate reports every configuration fault as ErrInvalidConfig and fills the
// defaults in place.
func (c *Config) validate() error {
	if c.Framer == nil && c.NewFramer == nil {
		return fmt.Errorf("%w: Framer or NewFramer is required", ErrInvalidConfig)
	}
	if c.Framer != nil && c.NewFramer != nil {
		return fmt.Errorf("%w: set Framer or NewFramer, not both", ErrInvalidConfig)
	}
	if len(c.Ports) == 0 {
		return fmt.Errorf("%w: at least one port is required", ErrInvalidConfig)
	}

	seen := make(map[int]struct{}, len(c.Ports))
	for _, p := range c.Ports {
		if p.Port < 0 || p.Port > 65535 {
			return fmt.Errorf("%w: port %d is out of range", ErrInvalidConfig, p.Port)
		}
		if _, dup := seen[p.Port]; dup && p.Port != 0 {
			return fmt.Errorf("%w: port %d is configured twice", ErrInvalidConfig, p.Port)
		}
		seen[p.Port] = struct{}{}

		if len(p.Upstreams) == 0 {
			return fmt.Errorf("%w: port %d has no upstreams", ErrInvalidConfig, p.Port)
		}
		for _, u := range p.Upstreams {
			if u.Addr == "" {
				return fmt.Errorf("%w: port %d has an upstream with no address", ErrInvalidConfig, p.Port)
			}
		}
	}

	if c.CaptureRaw && c.Sink == nil {
		return fmt.Errorf("%w: CaptureRaw needs a Sink to record to", ErrInvalidConfig)
	}

	if c.Codec != nil && c.NewCodec != nil {
		return fmt.Errorf("%w: set Codec or NewCodec, not both", ErrInvalidConfig)
	}

	if c.Prober == nil {
		c.Prober = DialProber{}
	}
	if c.Sink == nil {
		c.Sink = nopSink{}
	}
	if c.Selector == nil {
		c.Selector = FirstHealthy()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.now == nil {
		c.now = time.Now
	}

	if c.ReadBufferSize <= 0 {
		c.ReadBufferSize = defaultReadBufferSize
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = defaultMaxMessageSize
	}
	if c.MaxSessions < 0 {
		return fmt.Errorf("%w: MaxSessions cannot be negative", ErrInvalidConfig)
	}
	if c.ProbeTTL <= 0 {
		c.ProbeTTL = defaultProbeTTL
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = defaultProbeTimeout
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.DrainGrace <= 0 {
		c.DrainGrace = defaultDrainGrace
	}

	if c.OnSessionError == nil {
		logger := c.Logger
		c.OnSessionError = func(s *Session, err error) {
			logger.Warn(
				"relay: session ended with an error",
				slog.String("client", s.Info.ClientAddr),
				slog.String("upstream", s.Info.UpstreamAddr),
				slog.Any("err", err),
			)
		}
	}

	return nil
}
