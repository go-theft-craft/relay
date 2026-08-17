package relay

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
)

// Proxy accepts connections on many ports and relays them to upstreams.
//
// It is built by New, driven by Run, and stopped by Shutdown or by cancelling
// Run's context.
type Proxy struct {
	cfg    Config
	health *healthCache
	reg    *registry

	// slots bounds live sessions when Config.MaxSessions is set. An unbounded
	// accept loop is the first thing to fall over.
	slots chan struct{}

	mu        sync.Mutex
	listeners map[int]net.Listener
	addrs     map[int]net.Addr
	// upstreams is keyed by the port actually bound, which is how a port-zero
	// entry finds its candidates again once the kernel has named it.
	upstreams map[int][]Upstream

	stop     context.CancelFunc
	stopOnce sync.Once
}

// New validates a configuration and builds the proxy it describes. Every
// configuration fault is reported here rather than on the first connection.
func New(cfg Config) (*Proxy, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	p := &Proxy{
		cfg:       cfg,
		reg:       newRegistry(),
		listeners: make(map[int]net.Listener, len(cfg.Ports)),
		addrs:     make(map[int]net.Addr, len(cfg.Ports)),
		upstreams: make(map[int][]Upstream, len(cfg.Ports)),
	}
	p.health = newHealthCache(&p.cfg)

	if cfg.MaxSessions > 0 {
		p.slots = make(chan struct{}, cfg.MaxSessions)
	}

	// LeastConn counts against the registry, which only exists now.
	if lc, ok := p.cfg.Selector.(*leastConn); ok {
		lc.attach(p.reg)
	}

	return p, nil
}

// Run binds every configured port and serves until ctx is cancelled or a
// listener fails fatally.
//
// Every port is bound, including ports whose upstreams are all dead: health is
// resolved lazily, so startup does not know. A socket and a goroutine per port
// cost nothing next to the staleness of a startup probe, and the visible cost —
// a client on a dead port sees connect-then-drop rather than connection
// refused — is the better failure.
//
// It returns only fatal faults. Per-session errors reach Config.OnSessionError,
// because with thousands of sessions there is nowhere else they can go.
func (p *Proxy) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.mu.Lock()
	p.stop = cancel
	p.mu.Unlock()

	if err := p.listen(ctx); err != nil {
		p.closeListeners()

		return err
	}

	var wg sync.WaitGroup

	p.mu.Lock()
	for port, ln := range p.listeners {
		wg.Add(1)

		go func() {
			defer wg.Done()

			p.accept(ctx, port, ln)
		}()
	}
	p.mu.Unlock()

	<-ctx.Done()
	p.closeListeners()
	wg.Wait()

	// Sessions that outlive the listeners get the same grace a Shutdown would
	// give them, so a cancelled Run is not a harder stop than an orderly one.
	drainCtx, drainCancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.DrainGrace*2)
	defer drainCancel()

	_ = p.reg.drain(drainCtx)

	return nil
}

// listen binds every configured port, undoing the ones it bound if any fail. A
// proxy half-listening is a worse state to hand back than an error.
func (p *Proxy) listen(ctx context.Context) error {
	var lc net.ListenConfig

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, pc := range p.cfg.Ports {
		ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", pc.Port))
		if err != nil {
			return fmt.Errorf("relay: listen on port %d: %w", pc.Port, err)
		}

		port := pc.Port
		if port == 0 {
			// An ephemeral port is keyed by what it actually got, so Addrs and
			// the per-port upstream lookup agree.
			if tcp, ok := ln.Addr().(*net.TCPAddr); ok {
				port = tcp.Port
			}
		}

		p.listeners[port] = ln
		p.addrs[port] = ln.Addr()
		p.upstreams[port] = pc.Upstreams
	}

	return nil
}

func (p *Proxy) closeListeners() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for port, ln := range p.listeners {
		_ = ln.Close()
		delete(p.listeners, port)
	}
}

// Shutdown stops accepting, then gives live sessions until ctx expires to
// finish. Listeners close first so no new session starts while the old ones are
// draining.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.closeListeners()

	err := p.reg.drain(ctx)

	p.stopOnce.Do(func() {
		p.mu.Lock()
		stop := p.stop
		p.mu.Unlock()

		if stop != nil {
			stop()
		}
	})

	return err
}

// Addrs reports what each configured port actually bound, keyed by the port
// number in use. It is how a test that configured port 0 finds out where to
// connect.
func (p *Proxy) Addrs() map[int]net.Addr {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make(map[int]net.Addr, len(p.addrs))
	for port, addr := range p.addrs {
		out[port] = addr
	}

	return out
}

// Sessions lists the live sessions.
func (p *Proxy) Sessions() []SessionSnapshot { return p.reg.snapshots() }

// SessionCount reports how many sessions are live.
func (p *Proxy) SessionCount() int { return p.reg.count() }

// UpstreamCount reports how many live sessions are joined to one upstream.
func (p *Proxy) UpstreamCount(addr string) int { return p.reg.upstreamCount(addr) }

// accept serves one port until its listener closes.
func (p *Proxy) accept(ctx context.Context, port int, ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener is how shutdown reaches this loop, not a fault.
			return
		}

		go p.serve(ctx, port, conn)
	}
}

// serve takes one accepted connection all the way to a running session.
func (p *Proxy) serve(ctx context.Context, port int, client net.Conn) {
	release, ok := p.acquireSlot(ctx)
	if !ok {
		_ = client.Close()

		return
	}

	served := false
	defer func() {
		if !served {
			release()
			_ = client.Close()
		}
	}()

	info := SessionInfo{
		ClientAddr: client.RemoteAddr().String(),
		Port:       port,
		OpenedAt:   time.Now(),
	}

	// The capture wraps the connection before anything reads from it, so the
	// bytes a pre-frame hook consumes are recorded too. It holds them until
	// there is a sink session to attach them to.
	var capture *captureConn
	if p.cfg.CaptureRaw {
		capture = newCaptureConn(ctx, client)
		client = capture
	}

	// The session exists before the upstream does, so a PreFrame hook and an
	// upstream failure both have something to be reported against.
	s := newSession(ctx, &p.cfg, client, nil, info, 0)

	if p.cfg.NewCodec != nil {
		codec, err := p.cfg.NewCodec(s)
		if err != nil {
			p.cfg.OnSessionError(s, fmt.Errorf("relay: build the session codec: %w", err))

			return
		}

		s.codec = codec
	}

	if p.cfg.NewFramer != nil {
		for _, dir := range []Direction{ToServer, ToClient} {
			framer, err := p.cfg.NewFramer(s, dir)
			if err != nil {
				p.cfg.OnSessionError(s, fmt.Errorf("relay: build the %s session framer: %w", dir, err))

				return
			}
			if framer == nil {
				p.cfg.OnSessionError(s, fmt.Errorf("relay: NewFramer returned no %s framer", dir))

				return
			}

			s.framers[dir] = framer
		}
	}

	if p.cfg.PreFrame != nil {
		result, err := p.cfg.PreFrame.OnConnect(ctx, s, s.clientSide.PreFrameReader())
		if err != nil {
			p.cfg.OnSessionError(s, fmt.Errorf("relay: pre-frame hook: %w", err))

			return
		}
		if result == Handled {
			// The hook answered the connection itself. No upstream is dialled,
			// which is the whole point of returning Handled.
			return
		}
	}

	upstream, addr, err := p.resolve(ctx, port, client)
	if err != nil {
		p.cfg.OnSessionError(s, err)

		return
	}

	s.joinUpstream(upstream, addr)

	// The sink is told about the session once it is a session: two connections
	// joined.
	//
	// Opening the record earlier — before the upstream is resolved, which is the
	// obvious reading of the accept order — means every row a sink ever writes
	// has an empty upstream address, because that is genuinely not known yet. It
	// also puts OpenSession and CloseSession on different paths, so a connection
	// rejected at the pre-frame hook opens a record that nothing closes. Pairing
	// them here costs the recording of connections that never found an upstream;
	// those reach Config.OnSessionError instead, which is where a failure to
	// route belongs.
	sinkID, err := p.cfg.Sink.OpenSession(ctx, s.Info)
	if err != nil {
		p.cfg.OnSessionError(s, fmt.Errorf("relay: open sink session: %w", err))
		_ = upstream.Close()

		return
	}

	s.sinkID = sinkID

	if capture != nil && capture.activate(s.sink, sinkID) {
		p.cfg.OnSessionError(s, fmt.Errorf(
			"relay: raw capture dropped bytes before the session opened; over %d were buffered",
			capturePendingLimit,
		))
	}

	p.reg.add(s)
	s.onFinish = func() {
		p.reg.remove(s)
		release()
	}

	served = true
	s.run()
}

// acquireSlot enforces Config.MaxSessions under the configured overflow policy.
func (p *Proxy) acquireSlot(ctx context.Context) (release func(), ok bool) {
	if p.slots == nil {
		return func() {}, true
	}

	free := func() {
		<-p.slots
	}

	if p.cfg.Overflow == OverflowWait {
		select {
		case p.slots <- struct{}{}:
			return free, true
		case <-ctx.Done():
			return nil, false
		}
	}

	select {
	case p.slots <- struct{}{}:
		return free, true
	default:
		return nil, false
	}
}

// resolve picks a healthy upstream and dials it, failing over on a dial error.
//
// Dial failover applies underneath whichever selector is configured: a dial
// error marks the upstream down in the same cache the probes write to and the
// selector is asked again with the remaining candidates.
func (p *Proxy) resolve(ctx context.Context, port int, client net.Conn) (net.Conn, string, error) {
	candidates := p.candidates(port)
	if len(candidates) == 0 {
		return nil, "", fmt.Errorf("%w: port %d has no upstreams", ErrNoHealthyUpstream, port)
	}

	addrs := make([]string, 0, len(candidates))
	for _, u := range candidates {
		addrs = append(addrs, u.Addr)
	}

	healthy := p.health.check(ctx, addrs)
	remaining := filterByAddr(candidates, healthy)

	dialer := net.Dialer{Timeout: p.cfg.DialTimeout}
	for len(remaining) > 0 {
		pick, err := p.cfg.Selector.Pick(ctx, port, remaining, client)
		if err != nil {
			return nil, "", err
		}

		conn, err := dialer.DialContext(ctx, "tcp", pick.Addr)
		if err == nil {
			return conn, pick.Addr, nil
		}

		p.health.markDown(pick.Addr)
		remaining = slices.DeleteFunc(remaining, func(u Upstream) bool { return u.Addr == pick.Addr })
	}

	return nil, "", fmt.Errorf("%w: port %d", ErrNoHealthyUpstream, port)
}

// candidates returns the upstreams configured for the port that is actually
// bound.
func (p *Proxy) candidates(port int) []Upstream {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.upstreams[port]
}

// filterByAddr keeps the upstreams whose addresses survived the health check,
// in configuration order.
func filterByAddr(up []Upstream, keep []string) []Upstream {
	out := make([]Upstream, 0, len(up))
	for _, u := range up {
		if slices.Contains(keep, u.Addr) {
			out = append(out, u)
		}
	}

	return out
}
