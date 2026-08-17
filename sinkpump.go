package relay

import (
	"context"
	"sync/atomic"
	"time"
)

// sinkCallKind names which Sink method a queued record is waiting to make.
type sinkCallKind uint8

const (
	sinkCallMessage sinkCallKind = iota
	sinkCallRawChunk
	sinkCallClose
)

// sinkCall is one queued Sink call, carried by value so the queue holds nothing
// that points back into a session's buffers.
type sinkCall struct {
	kind   sinkCallKind
	record MessageRecord
	dir    Direction
	chunk  []byte
}

// sinkPump makes one session's Sink calls on a goroutine of its own.
//
// It exists because Sink says no method may block and the core has no way to
// check. A sink that blocks anyway stalls the read pump that called it and,
// through backpressure, the peer — which is what this repository's own capture
// sink did for three releases while the rule sat in a doc comment above the
// interface.
//
// It is per session rather than per proxy because ordering only means anything
// within a session, and because one slow session should not delay another's
// records. Messages and raw chunks share the one queue, so the interleaving a
// replay sees is the interleaving that crossed the wire.
//
// There is no batching here, and that part of Sink's comment stands unchanged: a
// sink knows its own storage and can size a batch for it, and a core that
// batched would be guessing on its behalf.
//
// It exists only under SinkOverflowDrop and SinkOverflowEndSession. Under
// SinkOverflowBlock, which is the default, there is no pump and no queue and the
// session calls the sink inline exactly as it always did.
type sinkPump struct {
	sink   Sink
	policy SinkOverflowPolicy
	grace  time.Duration

	// ctx and id are what every queued call is made with. They are set at start,
	// because the sink identifier is not assigned until the upstream is joined
	// and the session already exists by then.
	ctx context.Context
	id  int64

	queue chan sinkCall
	// done is closed by the pump goroutine as it leaves. abandon is closed by
	// stop when it has waited long enough, so a pump whose terminating record
	// never arrived does not park on the queue forever.
	done    chan struct{}
	abandon chan struct{}

	dropped atomic.Uint64

	// onOverflow ends the session under SinkOverflowEndSession. It is a callback
	// rather than a session pointer because the pump has no other reason to know
	// what a session is.
	onOverflow func()
	ended      atomic.Bool
}

func newSinkPump(sink Sink, policy SinkOverflowPolicy, depth int, grace time.Duration, onOverflow func()) *sinkPump {
	return &sinkPump{
		sink:       sink,
		policy:     policy,
		grace:      grace,
		queue:      make(chan sinkCall, depth),
		done:       make(chan struct{}),
		abandon:    make(chan struct{}),
		onOverflow: onOverflow,
	}
}

// start binds the pump to a session's context and sink identifier and runs it.
//
// Records may be queued before this: a raw capture flushes whatever it held
// while the upstream was being dialled, and that happens before the session
// runs. They wait in the queue until there is a goroutine to drain them.
func (p *sinkPump) start(ctx context.Context, id int64) {
	// Records are made with a context that does not carry the session's
	// cancellation. A queued record describes something that already crossed the
	// wire, and the session ending afterwards is not a reason for the sink to
	// discard it — which is what passing the cancelled context would invite.
	p.ctx = context.WithoutCancel(ctx)
	p.id = id

	go p.run()
}

func (p *sinkPump) run() {
	defer close(p.done)

	for {
		select {
		case call := <-p.queue:
			switch call.kind {
			case sinkCallMessage:
				p.sink.Message(p.ctx, p.id, call.record)
			case sinkCallRawChunk:
				p.sink.RawChunk(p.ctx, p.id, call.dir, call.chunk)
			case sinkCallClose:
				// CloseSession is the last call a sink gets, so it is also what
				// ends the pump. Reaching it through the queue rather than
				// beside it is what puts it after every record that belongs
				// before it.
				p.sink.CloseSession(p.ctx, p.id)

				return
			}
		case <-p.abandon:
			return
		}
	}
}

// OpenSession implements Sink by delegating inline, always.
//
// It returns an error, it assigns the identifier every later call is keyed by,
// and it runs on the accept path before there is a session to queue against. A
// sink that wants it off that path does what the SQLite example already does:
// assign from a counter and insert later.
func (p *sinkPump) OpenSession(ctx context.Context, info SessionInfo) (int64, error) {
	return p.sink.OpenSession(ctx, info)
}

// Message implements Sink by queueing a copy.
//
// The copy is not avoidable and it is the price of this policy:
// MessageRecord.Raw is borrowed for the duration of the call, so returning
// before the sink has read it means owning the bytes. Under SinkOverflowBlock no
// copy is made, because no call outlives its arguments.
func (p *sinkPump) Message(_ context.Context, _ int64, record MessageRecord) {
	record.Raw = append([]byte(nil), record.Raw...)

	p.enqueue(sinkCall{kind: sinkCallMessage, record: record})
}

// RawChunk implements Sink by queueing the chunk it was handed.
//
// It does not copy, and that is not an oversight. The only caller is
// captureConn, which allocates a fresh chunk per socket call before it records
// anything, because the buffer underneath belongs to whoever called Read. The
// slice arriving here is therefore already owned, and copying it again would buy
// nothing.
func (p *sinkPump) RawChunk(_ context.Context, _ int64, dir Direction, chunk []byte) {
	p.enqueue(sinkCall{kind: sinkCallRawChunk, dir: dir, chunk: chunk})
}

// CloseSession implements Sink by queueing the terminating record.
//
// It waits for room rather than dropping on a full queue, because it is the one
// call whose loss a sink cannot recover from: a recording with no closing record
// is indistinguishable from a capture of a killed process. The wait is bounded
// all the same — a teardown that can hang on a wedged sink has moved the stall
// rather than removed it.
func (p *sinkPump) CloseSession(context.Context, int64) {
	timer := time.NewTimer(p.grace)
	defer timer.Stop()

	select {
	case p.queue <- sinkCall{kind: sinkCallClose}:
	case <-timer.C:
		p.dropped.Add(1)
	}
}

// enqueue offers one record to the queue and applies the policy if it will not
// fit. It never blocks, which is the whole point of the pump.
func (p *sinkPump) enqueue(call sinkCall) {
	select {
	case p.queue <- call:
	default:
		p.overflowed()
	}
}

// overflowed counts a record the queue had no room for, and ends the session if
// that is what the consumer asked for.
func (p *sinkPump) overflowed() {
	p.dropped.Add(1)

	if p.policy != SinkOverflowEndSession {
		return
	}

	// Once, because every record arriving behind the first would otherwise end
	// the same session again and report the same fault again.
	if p.ended.CompareAndSwap(false, true) && p.onOverflow != nil {
		p.onOverflow()
	}
}

// stop waits for the pump to finish the records it holds.
//
// The queue is never closed. A session's writers are gone by the time this runs,
// but an injection racing a disconnect is not, and sending on a closed channel
// panics — a library that can be made to panic by a consumer calling Inject at
// an unlucky moment has traded one fault for a worse one. The terminating record
// ends the goroutine instead, and abandon is what ends it when that record never
// arrives.
func (p *sinkPump) stop() {
	timer := time.NewTimer(p.grace)
	defer timer.Stop()

	select {
	case <-p.done:
	case <-timer.C:
		close(p.abandon)
	}
}

var _ Sink = (*sinkPump)(nil)
