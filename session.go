package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// messagePool recycles the per-message struct. The bytes inside it come from
// the Framer; what is pooled is the envelope, which is allocated once per
// message and would otherwise be the most frequent allocation in the process.
var messagePool = sync.Pool{New: func() any { return new(Message) }}

// sessionIDs numbers sessions for the lifetime of the process.
var sessionIDs atomic.Int64

// SessionSnapshot is a session as a listing sees it: identity, addresses, and a
// copy of whatever metadata the consumer attached.
type SessionSnapshot struct {
	ID   int64
	Info SessionInfo
	Meta map[string]any
}

// Session is one client connection and the upstream it was joined to.
//
// It is passed to every hook and sink call, and is the handle a consumer uses
// to inject messages, attach metadata, swap a mid-stream transform, or end the
// connection.
//
// Two goroutines run per session, one read pump per direction. At ten thousand
// sessions that is twenty thousand goroutines; four per session would double
// the stack floor before a single buffer is allocated, which is why a queue
// between the pumps was not worth its decoupling.
type Session struct {
	// ID is unique for the lifetime of the process. It is the proxy's own
	// identifier, distinct from whatever a Sink assigns.
	ID int64
	// Client and Upstream are the raw connections. They are exported for
	// addresses and deadlines; writing to them directly bypasses the writer lock
	// that makes injection safe, so relay through Inject instead.
	Client   net.Conn
	Upstream net.Conn
	// Info describes the session as it opened. It is the same value the Sink was
	// handed, so a log line and a stored row agree.
	Info SessionInfo

	cfg    *Config
	sinkID int64

	// clientSide and upstreamSide are the byte layers over the two connections.
	// A message travelling ToClient is written to clientSide and a message
	// travelling ToServer is read from it, which is why a pump reads from the
	// conduit its direction is *not* named after.
	clientSide   *Conduit
	upstreamSide *Conduit

	// One-slot semaphores rather than mutexes, because a write must be able to
	// lose a race with the session context and give up rather than block until a
	// dead peer's write times out.
	clientWrite   chan struct{}
	upstreamWrite chan struct{}

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu   sync.RWMutex
	meta map[string]any

	// decodeErrLogged keeps a codec that cannot parse this session's traffic
	// from reporting once per message. The first one is the useful one.
	decodeErrLogged atomic.Bool

	// onFinish is what the registry hangs its removal on. It is a field rather
	// than a registry reference so a session can be built and run in a test with
	// no proxy in sight.
	onFinish func()
}

// newSession joins two connections. The caller has already dialled the upstream
// and opened the sink record.
func newSession(parent context.Context, cfg *Config, client, upstream net.Conn, info SessionInfo, sinkID int64) *Session {
	ctx, cancel := context.WithCancelCause(parent)

	return &Session{
		ID:            sessionIDs.Add(1),
		Client:        client,
		Upstream:      upstream,
		Info:          info,
		cfg:           cfg,
		sinkID:        sinkID,
		clientSide:    NewConduit(client, cfg.ReadBufferSize),
		upstreamSide:  NewConduit(upstream, cfg.ReadBufferSize),
		clientWrite:   make(chan struct{}, 1),
		upstreamWrite: make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		meta:          make(map[string]any),
	}
}

// Context returns the session context. It is cancelled when the session closes,
// so a hook that starts work should hang it here.
func (s *Session) Context() context.Context { return s.ctx }

// Set attaches consumer metadata. The framework never reads the values; the
// registry only copies them into a snapshot.
func (s *Session) Set(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.meta[key] = value
}

// Get returns metadata previously attached with Set.
func (s *Session) Get(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.meta[key]

	return value, ok
}

// Snapshot copies the session's identity and metadata.
//
// The map is copied rather than shared: handing out the live one turns a
// listing into a data race against every hook that calls Set.
func (s *Session) Snapshot() SessionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SessionSnapshot{ID: s.ID, Info: s.Info, Meta: maps.Clone(s.meta)}
}

// Close ends the session. It is safe to call from anywhere and any number of
// times; the connections are closed by the session's own shutdown path once
// in-flight writes have had their grace period.
func (s *Session) Close() { s.cancel(ErrSessionClosed) }

// Swap installs a mid-stream transform on one of the session's two
// connections.
//
// The direction names the connection the same way it does everywhere else in
// this API: the one a message travelling that way is written to. ToClient is
// the client connection and ToServer is the upstream one. A Transform covers
// both halves of that connection, because a cipher negotiated on a link
// encodes it in both directions — and a proxy stands on two links, so a cipher
// negotiated by the client needs its own swap on ToClient and the upstream's
// needs a separate one on ToServer, with separate keystreams.
//
// It is safe from any goroutine, including from a hook running on the other
// direction's pump — which is the common case, since the pump reading the
// connection being swapped is parked inside a socket read at that moment. It
// returns ErrSwapPending when that connection still holds unread bytes from
// before the boundary; see Conduit.Swap for why that is an error rather than
// something to absorb.
func (s *Session) Swap(dir Direction, t Transform) error {
	return s.conduit(dir).Swap(t)
}

// conduit returns the byte layer a message travelling dir is written to.
func (s *Session) conduit(dir Direction) *Conduit {
	if dir == ToClient {
		return s.clientSide
	}

	return s.upstreamSide
}

// readSide returns the byte layer a message travelling dir is read from, which
// is the opposite connection from the one it will be written to.
func (s *Session) readSide(dir Direction) *Conduit {
	if dir == ToClient {
		return s.upstreamSide
	}

	return s.clientSide
}

// write sends one framed message to a peer.
//
// The one-slot channel is what makes injection safe: every write to a peer,
// relayed or injected, passes through here, so no two goroutines hold the same
// writer and an injected message can never land inside a relayed one. A
// framework that handed hooks a raw net.Conn could not make that promise.
func (s *Session) write(dir Direction, raw []byte) error {
	lock := s.upstreamWrite
	if dir == ToClient {
		lock = s.clientWrite
	}

	select {
	case lock <- struct{}{}:
		defer func() { <-lock }()
	case <-s.ctx.Done():
		return ErrSessionClosed
	}

	return s.cfg.Framer.WriteMessage(s.conduit(dir), raw)
}

// Inject sends a message to one peer as though the other had sent it.
//
// It acquires the same writer lock relaying uses, so an injected message never
// interleaves inside a relayed one — the guarantee that makes injection worth
// having, and the one a framework handing out a raw net.Conn cannot make.
//
// Injected messages do not run the hook chain. A hook that wants to see what it
// injected can see it at the point it injected it, and re-entering the chain
// invites a hook that injects on every message to recurse.
//
// They are recorded to the Sink, because a capture that omits what the proxy
// itself sent is a capture that cannot be replayed.
func (s *Session) Inject(dir Direction, raw []byte) error {
	select {
	case <-s.ctx.Done():
		return ErrSessionClosed
	default:
	}

	if err := s.write(dir, raw); err != nil {
		return err
	}

	s.cfg.Sink.Message(s.ctx, s.sinkID, MessageRecord{Dir: dir, Raw: raw, At: time.Now()})

	return nil
}

// InjectDecoded encodes through the configured Codec and injects the result.
//
// It returns ErrInvalidConfig when no codec is configured, because the
// alternative is a silent no-op on a call that read like it sent something.
func (s *Session) InjectDecoded(dir Direction, value any) error {
	if s.cfg.Codec == nil {
		return fmt.Errorf("%w: InjectDecoded needs a Codec", ErrInvalidConfig)
	}

	raw, err := s.cfg.Codec.Encode(value)
	if err != nil {
		return fmt.Errorf("relay: encode injected message: %w", err)
	}

	select {
	case <-s.ctx.Done():
		return ErrSessionClosed
	default:
	}

	if err := s.write(dir, raw); err != nil {
		return err
	}

	s.cfg.Sink.Message(s.ctx, s.sinkID, MessageRecord{Dir: dir, Raw: raw, Decoded: value, At: time.Now()})

	return nil
}

// run drives the session until either direction fails, then shuts it down.
func (s *Session) run() {
	defer s.finish()

	go s.closeWhenDone()

	var (
		wg     sync.WaitGroup
		once   sync.Once
		reason error
	)
	for _, dir := range []Direction{ToServer, ToClient} {
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := s.pump(dir)
			once.Do(func() { reason = err })
			// The first pump to fail cancels the session, which is what stops the
			// other one: a read parked on a healthy socket has nothing else to
			// wake it.
			s.cancel(err)
		}()
	}
	wg.Wait()

	s.report(reason)
}

// closeWhenDone gives in-flight writes their grace period once the session is
// cancelled, then closes both connections — which is what unparks a read pump.
func (s *Session) closeWhenDone() {
	<-s.ctx.Done()
	s.drain()

	_ = s.Client.Close()
	_ = s.Upstream.Close()
}

// drain waits for both writers to fall idle, up to Config.DrainGrace, and then
// keeps them, so a session that is closing finishes the write it started and
// begins no other.
func (s *Session) drain() {
	timer := time.NewTimer(s.cfg.DrainGrace)
	defer timer.Stop()

	for _, lock := range []chan struct{}{s.clientWrite, s.upstreamWrite} {
		select {
		case lock <- struct{}{}:
		case <-timer.C:
			return
		}
	}
}

// finish closes the sink record exactly once, on the one path every session
// leaves through.
func (s *Session) finish() {
	// The parent context is already cancelled by the time a shutdown-triggered
	// session ends, and a sink still has to be told the session closed.
	s.cfg.Sink.CloseSession(context.WithoutCancel(s.ctx), s.sinkID)

	if s.onFinish != nil {
		s.onFinish()
	}
}

// report hands a session-ending fault to the consumer. A clean peer close and a
// deliberate shutdown are not faults and are not reported.
func (s *Session) report(err error) {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, ErrSessionClosed) {
		return
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return
	}

	s.cfg.OnSessionError(s, err)
}

// pump reads one direction to exhaustion, relaying each message to the peer.
//
// It blocks while writing to a slow peer, which propagates TCP backpressure to
// the origin instead of buffering. A queue between the pumps would decouple
// something nobody asked to decouple, at the cost of a goroutine per direction.
func (s *Session) pump(dir Direction) error {
	src := s.readSide(dir)

	for {
		raw, err := s.cfg.Framer.ReadMessage(src)
		if err != nil {
			return err
		}
		if len(raw) > s.cfg.MaxMessageSize {
			return fmt.Errorf("%w: %d bytes, limit %d", ErrMessageTooLarge, len(raw), s.cfg.MaxMessageSize)
		}

		if err := s.relay(dir, raw); err != nil {
			return err
		}
	}
}

// relay decodes, hooks, re-encodes, records, and forwards one message.
func (s *Session) relay(dir Direction, raw []byte) error {
	m, ok := messagePool.Get().(*Message)
	if !ok {
		m = new(Message)
	}
	defer func() {
		m.reset()
		messagePool.Put(m)
	}()

	m.Dir = dir
	m.Raw = raw

	s.decode(m)

	action, err := s.runHooks(s.ctx, m)
	if err != nil {
		return err
	}
	if action == Drop {
		return nil
	}

	out, err := s.finalBytes(m)
	if err != nil {
		return err
	}

	s.cfg.Sink.Message(s.ctx, s.sinkID, MessageRecord{
		Dir:     dir,
		Desc:    m.Desc,
		Raw:     out,
		Decoded: m.Decoded,
		At:      time.Now(),
	})

	return s.write(dir, out)
}

// decode fills in the typed half of a message when a codec is configured.
//
// A decode error is not fatal. The message is forwarded as opaque bytes with a
// zero Descriptor, because a proxy that refuses to relay what it cannot parse
// is less useful than the connection it replaced. It is reported once per
// session: a codec that cannot read this traffic will fail on every message,
// and the first report is the only informative one.
func (s *Session) decode(m *Message) {
	if s.cfg.Codec == nil {
		return
	}

	value, desc, err := s.cfg.Codec.Decode(m.Dir, m.Raw)
	if err != nil {
		if s.decodeErrLogged.CompareAndSwap(false, true) {
			s.cfg.OnSessionError(s, fmt.Errorf("relay: decode %s: %w (relaying opaquely from here)", m.Dir, err))
		}

		return
	}

	m.Decoded, m.Desc = value, desc
}

// finalBytes decides what actually goes on the wire.
//
// Re-encoding happens once, after the whole chain, and only when a hook changed
// the decoded value. Raw bytes win over a decoded change, because a hook that
// wrote bytes was more specific than one that wrote a value.
func (s *Session) finalBytes(m *Message) ([]byte, error) {
	if m.RawChanged() || !m.DecodedChanged() {
		return m.Raw, nil
	}

	if s.cfg.Codec == nil {
		return nil, fmt.Errorf("%w: a hook set a decoded value but no Codec is configured", ErrHook)
	}

	encoded, err := s.cfg.Codec.Encode(m.Decoded)
	if err != nil {
		return nil, fmt.Errorf("%w: re-encode: %w", ErrHook, err)
	}

	return encoded, nil
}

// runHooks walks the chain and returns the action the chain settled on.
//
// Drop stops the walk, because a message that will never be sent has nothing
// left to decide. Replace does not, so a later hook sees the edit rather than
// the original.
func (s *Session) runHooks(ctx context.Context, m *Message) (Action, error) {
	for _, h := range s.cfg.Hooks {
		action, err := s.callHook(ctx, h, m)
		if err != nil {
			return Drop, err
		}
		if action == Drop {
			return Drop, nil
		}
	}

	return Forward, nil
}

// callHook isolates one hook so a panic ends this session and no other.
//
// This is a deliberate divergence from our protocol library's router, which
// does not recover handler panics: for a library driving one connection,
// burying a bug puts the report far from its cause. A proxy holds thousands of
// sessions, and one malformed message reaching a buggy hook must not take the
// unrelated ones down with it. The stack is carried on the error rather than
// discarded, so the report still lands near the cause.
//
// A hook that returns an error also ends its session. A hook that meant to
// rewrite a message and failed has left the stream in a state neither peer
// agreed to, and forwarding anyway corrupts it quietly.
func (s *Session) callHook(ctx context.Context, h Hook, m *Message) (action Action, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrHook, r, debug.Stack())
			action = Drop
		}
	}()

	action, err = h.OnMessage(ctx, s, m)
	if err != nil {
		return Drop, fmt.Errorf("%w: %w", ErrHook, err)
	}

	return action, nil
}
