package minecraft

import (
	"context"
	"sync"

	"github.com/go-theft-craft/relay"
)

// MultiSink fans one session out to several sinks.
//
// relay takes one Sink, and a proxy that both stores rows and writes a
// recording needs two. Composing them here rather than in the core is
// deliberate: each sink hands back its own session identifier, so a fan-out
// has to keep a translation table, and the core has no reason to carry one for
// consumers that use a single sink.
//
// A sink that fails to open a session is dropped for that session rather than
// failing the connection. The proxy's job is to relay; losing a recording is
// worth reporting, not worth disconnecting a player over. OpenSession's error
// is returned only when every sink refused.
type MultiSink struct {
	sinks []relay.Sink

	mu  sync.Mutex
	ids map[int64][]sessionID
	// next numbers the sessions this sink hands back, so the identifier a
	// caller sees is never one of the children's.
	next int64
}

// sessionID pairs a child sink with the identifier that child issued.
type sessionID struct {
	sink relay.Sink
	id   int64
}

// NewMultiSink composes sinks in the order given.
func NewMultiSink(sinks ...relay.Sink) *MultiSink {
	return &MultiSink{sinks: sinks, ids: make(map[int64][]sessionID)}
}

// OpenSession implements relay.Sink.
func (m *MultiSink) OpenSession(ctx context.Context, info relay.SessionInfo) (int64, error) {
	opened := make([]sessionID, 0, len(m.sinks))

	var firstErr error
	for _, sink := range m.sinks {
		id, err := sink.OpenSession(ctx, info)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		opened = append(opened, sessionID{sink: sink, id: id})
	}

	if len(opened) == 0 {
		return 0, firstErr
	}

	m.mu.Lock()
	m.next++
	id := m.next
	m.ids[id] = opened
	m.mu.Unlock()

	return id, nil
}

// Message implements relay.Sink.
func (m *MultiSink) Message(ctx context.Context, id int64, record relay.MessageRecord) {
	for _, child := range m.lookup(id) {
		child.sink.Message(ctx, child.id, record)
	}
}

// RawChunk implements relay.Sink.
func (m *MultiSink) RawChunk(ctx context.Context, id int64, dir relay.Direction, chunk []byte) {
	for _, child := range m.lookup(id) {
		child.sink.RawChunk(ctx, child.id, dir, chunk)
	}
}

// CloseSession implements relay.Sink.
func (m *MultiSink) CloseSession(ctx context.Context, id int64) {
	m.mu.Lock()
	children := m.ids[id]
	delete(m.ids, id)
	m.mu.Unlock()

	for _, child := range children {
		child.sink.CloseSession(ctx, child.id)
	}
}

func (m *MultiSink) lookup(id int64) []sessionID {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ids[id]
}

var _ relay.Sink = (*MultiSink)(nil)
