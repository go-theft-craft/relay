package relay

import (
	"context"
	"sync"
)

// registry tracks the live sessions.
//
// It keeps a per-upstream count alongside the session set so LeastConn is a map
// lookup on the accept path rather than a walk of every live session. That
// makes it contended by design: the accept loop reads it while every finishing
// session writes to it.
type registry struct {
	mu       sync.RWMutex
	sessions map[int64]*Session
	upstream map[string]int

	// wg tracks live sessions so drain can wait for the last one.
	wg sync.WaitGroup
}

func newRegistry() *registry {
	return &registry{
		sessions: make(map[int64]*Session),
		upstream: make(map[string]int),
	}
}

// add registers a session and takes a reference drain will wait on.
func (r *registry) add(s *Session) {
	r.wg.Add(1)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.sessions[s.ID] = s
	r.upstream[s.Info.UpstreamAddr]++
}

// remove drops a session. It is idempotent, because a session that was never
// added must not decrement a count it never incremented.
func (r *registry) remove(s *Session) {
	r.mu.Lock()

	_, present := r.sessions[s.ID]
	if present {
		delete(r.sessions, s.ID)

		addr := s.Info.UpstreamAddr
		if r.upstream[addr] <= 1 {
			delete(r.upstream, addr)
		} else {
			r.upstream[addr]--
		}
	}

	r.mu.Unlock()

	if present {
		r.wg.Done()
	}
}

// count reports how many sessions are live.
func (r *registry) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.sessions)
}

// upstreamCount reports how many live sessions are joined to one upstream.
func (r *registry) upstreamCount(addr string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.upstream[addr]
}

// snapshots lists the live sessions.
//
// The result is a stable slice of copies: a listing that handed back live
// sessions would race every hook that calls Set for as long as the caller held
// it.
func (r *registry) snapshots() []SessionSnapshot {
	r.mu.RLock()
	live := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		live = append(live, s)
	}
	r.mu.RUnlock()

	// Snapshot outside the registry lock: each session takes its own, and
	// holding both at once orders two locks that nothing else orders.
	out := make([]SessionSnapshot, 0, len(live))
	for _, s := range live {
		out = append(out, s.Snapshot())
	}

	return out
}

// drain closes every live session and waits for the last one to finish, or for
// ctx to expire. Sessions get their own DrainGrace on top of this to finish an
// in-flight write, so ctx should allow for it.
func (r *registry) drain(ctx context.Context) error {
	r.mu.RLock()
	for _, s := range r.sessions {
		s.Close()
	}
	r.mu.RUnlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
