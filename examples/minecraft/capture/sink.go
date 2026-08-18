package capture

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"

	"github.com/go-theft-craft/relay"
)

// Options configures a Recorder.
type Options struct {
	// Dir is where recordings are written. One file per session.
	Dir string
	// Descriptor names the protocol a replay will resolve, and answers which
	// frames carry secret material.
	Descriptor protocol.Protocol
	// Limits are the bounds in force while recording. They are stamped into
	// the header, so a reader allocates against the same ceiling.
	Limits protocol.Limits
	// Framer rebuilds the complete frame, length prefix included, for the raw
	// records. relay hands a sink the frame payload, and a raw record is
	// defined as the bytes that crossed the transport.
	Framer protocol.Framer
	// Note is free text stamped into the header. The upstream address is
	// appended to it, because a trace that cannot say what it recorded against
	// is not evidence.
	Note string
	// QueueDepth bounds the records waiting to be written for one session.
	// Zero means DefaultQueueDepth.
	//
	// It is a bound rather than a buffer because of what a full queue means
	// here: the recorder is losing to the disk, and the only outcomes are a
	// recording with a hole in it or a session that ends. This sink chooses the
	// second, so the depth is how much burst it absorbs before making that
	// choice, and the memory it may hold is roughly depth × frame size.
	QueueDepth int
	// CloseGrace bounds how long CloseSession waits for the records still queued
	// when a session ends. Zero means DefaultCloseGrace.
	//
	// Waiting is what makes a returned CloseSession mean a finished file. The
	// bound is there because one write's duration is not bounded by anything: a
	// proxy that can hang its session teardown on a wedged disk has moved the
	// stall rather than removed it.
	CloseGrace time.Duration
	// OpenSink builds one session's destination. Zero means a capture file named
	// after the session in Dir, which is what a recording is for.
	//
	// It is here because the queue below made the destination's speed matter: a
	// bound that ends sessions has to be testable against a destination that
	// holds a write open, and a real file never does.
	OpenSink func(name string, header mccapture.Header) (RecordSink, error)
	// OnError receives a fault that a Sink method cannot return. Recording
	// failures must be loud: a truncated oracle that reports nothing is worse
	// than no oracle, because it still looks like evidence.
	OnError func(error)
}

// RecordSink is one session's destination: records in order, then a trailer.
//
// mccapture.FileSink is the implementation Options builds by default.
// protocol.ObservationSink is the half minecraft-protocol names; Close is the
// half that makes a recording complete, because the trailer is what gives a
// capture a digest to replay against.
type RecordSink interface {
	protocol.ObservationSink
	Close() error
}

// Recorder writes one capture file per session, in the format
// minecraft-protocol's replay and digest tooling already reads.
//
// It is a relay.Sink, which is a process-wide interface rather than a
// per-session one, so the file bookkeeping lives here: a proxy holds many
// sessions at once and each needs its own recording, its own frame numbering,
// and its own clock origin.
//
// It keeps its own queue even though relay.Config.SinkOverflowEndSession now
// offers one that does the same job, and the reason is that the two are not
// quite the same job. The core's queue holds relay.MessageRecord values and can
// only end the session; this one holds finished capture records — bytes copied,
// sequence assigned, transition applied — so what it absorbs is the disk being
// slow rather than the pump being fast, and its CloseGrace is what makes a
// returned CloseSession mean a finished file.
//
// Running both is two bounded queues in a row, and that question is settled:
// the core's belongs in front of a sink you do not control, not in front of this
// one. Because Message never blocks — it copies, sends, and fails the recording
// rather than parking the caller — a slow disk fills this queue and never
// reaches the core's, so the core's can only fire on a burst that outruns a
// channel send, at a copy per message per sink. And SinkOverflowBlock is the
// default, so a consumer who configures nothing gets this queue and nothing
// else: a capture's guarantee cannot rest on a flag somebody else remembered to
// set. docs/verification/2026-08-18-sink-policy-live.md is the run both
// mechanisms were put through, including the one where the core's drop policy
// lost 783 records and the replay gate still called the file ok.
type Recorder struct {
	opts Options

	mu       sync.Mutex
	next     int64
	sessions map[int64]*recording
}

// recording is one session's file and the counters that describe it.
//
// The mutex is per recording rather than per Recorder: both read pumps of one
// session record concurrently, but two sessions share nothing. It covers the
// counters and the state machine below, which run on the pump; it does not cover
// sink, which the writer goroutine owns from the header to the trailer and which
// nothing else touches.
type recording struct {
	// queue carries finished records to the writer goroutine, which is what
	// keeps the file's write off the read pump. Records are complete by the time
	// they are queued — bytes copied, sequence assigned — so the writer only
	// writes.
	queue chan protocol.Observation
	// stop asks the writer to drain and finish; done reports that it has.
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	// ctx is the session context with its cancellation removed. The writer
	// outlives the session by whatever is still queued at close, and a recording
	// that stopped writing because the client hung up is exactly the truncated
	// file this sink exists to avoid.
	ctx context.Context
	// end closes the session feeding this recording, once Bind's hook has seen
	// it. It is nil until then, and stays nil for a consumer that never wired
	// the hook.
	end atomic.Pointer[func()]

	mu       sync.Mutex
	sink     RecordSink
	started  time.Time
	sequence uint64
	frame    uint64
	// state is the last connection state a decoded packet reported. An
	// undecodable frame inherits it, which is what lets the redaction check
	// below answer for a frame nobody could parse.
	state protocol.State
	// sensitive answers whether a frame's bytes must be withheld, and
	// sensitiveSession is the same object under the interface it needs for
	// setting the state the question is asked in. It decodes nothing and
	// relays nothing: it is asked one question per frame.
	sensitive        protocol.SensitiveFrames
	sensitiveSession protocol.Session
	// secrets answers the same question about a decoded packet, which is how
	// this recording knows the key exchange has completed. See opaque.
	secrets protocol.SensitivePackets
	// opaque latches when the session stops carrying frames, which happens once
	// and never unwinds. Everything after it is bytes: see Recorder.Message.
	opaque bool

	// failed is atomic because either side can set it: the pump when the queue
	// overflows, the writer when the file refuses a record.
	failed atomic.Bool
}

// DefaultQueueDepth is the per-session record queue used when Options leaves
// QueueDepth zero. A frame produces at most two records, so this absorbs a few
// hundred frames of burst.
const DefaultQueueDepth = 1024

// DefaultCloseGrace is how long CloseSession waits for a recording's queued
// records when Options leaves CloseGrace zero.
const DefaultCloseGrace = 30 * time.Second

// NewRecorder validates the options and prepares the output directory.
func NewRecorder(opts Options) (*Recorder, error) {
	if opts.Dir == "" && opts.OpenSink == nil {
		return nil, fmt.Errorf("capture: no output directory")
	}
	if opts.Descriptor == nil {
		return nil, fmt.Errorf("capture: no protocol descriptor")
	}
	if opts.Framer == nil {
		return nil, fmt.Errorf("capture: no framer to rebuild frames with")
	}
	if opts.OnError == nil {
		opts.OnError = func(err error) { slog.Default().Error("capture", slog.Any("err", err)) }
	}
	if opts.QueueDepth <= 0 {
		opts.QueueDepth = DefaultQueueDepth
	}
	if opts.CloseGrace <= 0 {
		opts.CloseGrace = DefaultCloseGrace
	}

	if opts.OpenSink == nil {
		dir := opts.Dir
		opts.OpenSink = func(name string, header mccapture.Header) (RecordSink, error) {
			return mccapture.NewFileSink(filepath.Join(dir, name), header)
		}

		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("capture: create %s: %w", dir, err)
		}
	}

	return &Recorder{opts: opts, sessions: make(map[int64]*recording)}, nil
}

// OpenSession implements relay.Sink. It creates the session's file and writes
// the header before any record can reach it.
func (r *Recorder) OpenSession(ctx context.Context, info relay.SessionInfo) (int64, error) {
	started := info.OpenedAt
	if started.IsZero() {
		started = time.Now()
	}

	r.mu.Lock()
	r.next++
	id := r.next
	r.mu.Unlock()

	name := fmt.Sprintf("%s-%03d.mccap", started.UTC().Format("20060102-150405"), id)

	sink, err := r.opts.OpenSink(name, mccapture.Header{
		Protocol:          r.opts.Descriptor.ID(),
		FrameBytes:        r.opts.Limits.FrameBytes(),
		DecompressedBytes: r.opts.Limits.DecompressedBytes(),
		Created:           started.UTC().Format(time.RFC3339),
		Note:              r.note(info),
	})
	if err != nil {
		return 0, fmt.Errorf("capture: open %s: %w", name, err)
	}

	entry := &recording{
		queue:   make(chan protocol.Observation, r.opts.QueueDepth),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		ctx:     context.WithoutCancel(ctx),
		sink:    sink,
		started: started,
		state:   protocol.State("handshaking"),
	}

	// The sensitivity oracle is optional: a protocol that declares no sensitive
	// frames leaves it nil and every frame records its bytes.
	if session, err := r.opts.Descriptor.NewSession(protocol.RoleServer, r.opts.Limits); err == nil {
		if frames, ok := session.(protocol.SensitiveFrames); ok {
			entry.sensitive = frames
			entry.sensitiveSession = session
		}

		if packets, ok := session.(protocol.SensitivePackets); ok {
			entry.secrets = packets
		}
	}

	r.mu.Lock()
	r.sessions[id] = entry
	r.mu.Unlock()

	go r.write(entry)

	return id, nil
}

// note describes what the recording was taken against.
func (r *Recorder) note(info relay.SessionInfo) string {
	note := fmt.Sprintf("relay capture; upstream %s; client port %d", info.UpstreamAddr, info.Port)
	if r.opts.Note != "" {
		note = r.opts.Note + "; " + note
	}

	return note
}

// Message implements relay.Sink.
//
// Every relayed frame produces a raw record, and one that decoded produces a
// packet record as well. That is the same pair a real endpoint's stream emits,
// and it is what keeps an unparseable frame in the recording: relay hands the
// sink a zero descriptor rather than dropping the message, and those are
// precisely the frames worth having later.
func (r *Recorder) Message(ctx context.Context, id int64, record relay.MessageRecord) {
	entry := r.lookup(id)
	if entry == nil {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.failed.Load() {
		return
	}

	dir := direction(record.Dir)
	elapsed := record.At.Sub(entry.started)
	if elapsed < 0 {
		elapsed = 0
	}

	// Past the key exchange there is nothing here to record. What arrives is
	// ciphertext in chunks the transport chose, not frames, and the file format
	// has no record that means "some bytes": a raw record claims to be one
	// complete frame, and writing chunks into one produces a file that reads
	// back and cannot replay. The secret record written at the switch is what
	// says where this recording stopped being able to see, which is the honest
	// thing a capture can say and the reason it is not silent about it.
	if entry.opaque {
		return
	}

	entry.frame++
	before := entry.beforeState(record)

	frame, err := r.opts.Framer.BuildFrame(record.Raw)
	if err != nil {
		r.fail(entry, fmt.Errorf("capture: rebuild frame: %w", err))

		return
	}

	wire := frame.WireBytes()

	// The sensitivity question is asked before the transition is applied, and
	// the order is load-bearing rather than incidental. A frame that changes the
	// pipeline has to be judged under the pipeline it arrived on: a set
	// compression packet travels uncompressed and enables compression for
	// everything after it, so asking afterwards asks about an envelope the frame
	// does not wear, the read fails, and the check fails closed on a frame that
	// is not a secret and that the capture cannot do without.
	redacted := entry.withhold(before, dir, record.Raw)

	after := entry.advance(record, before)

	raw := protocol.Observation{
		Frame:       entry.frame,
		Direction:   dir,
		Stage:       protocol.ObservationRawFrame,
		Elapsed:     elapsed,
		Before:      protocol.NewSnapshot(before, nil),
		After:       protocol.NewSnapshot(after, nil),
		OriginalLen: len(wire),
		Redacted:    redacted,
	}
	// The bytes are copied because the record outlives this call now: relay
	// borrows Raw for the duration of Message, and the frame the Framer rebuilt
	// is no safer, since one Framer serves every session.
	if !redacted {
		raw.Bytes = append([]byte(nil), wire...)
	}

	r.observe(ctx, entry, raw)

	// A decoded value earns a packet record, whether or not the protocol could
	// name it. The format documents an empty name as "a packet the capturing
	// session could not name", so keying this off the name instead would drop
	// exactly those records and leave a recording that decodes to nothing.
	if record.Decoded != nil {
		packet := protocol.Observation{
			Frame:     entry.frame,
			Direction: dir,
			Stage:     protocol.ObservationPacket,
			Elapsed:   elapsed,
			Before:    protocol.NewSnapshot(before, nil),
			After:     protocol.NewSnapshot(after, nil),
			Packet: &protocol.PacketMetadata{
				State:     after,
				Direction: dir,
				ID:        record.Desc.ID,
				Name:      record.Desc.Name,
			},
			OriginalLen: len(record.Raw),
			Redacted:    redacted,
		}
		if !redacted {
			packet.Bytes = append([]byte(nil), record.Raw...)
		}

		r.observe(ctx, entry, packet)
	}

	entry.state = after

	// The frame just recorded may have been the last one this recording can
	// read. A key exchange ends with a sensitive packet from the client, and
	// everything after it is enciphered.
	//
	// This recording works that out for itself rather than being told by the
	// codec, which is the same independence the rest of this file rests on: it
	// keeps its own session, applies its own transitions, and asks its own
	// sensitivity questions, so that a recording is a second opinion about a
	// connection rather than a copy of the first. The protocol answers which
	// packet it is, so no packet identifier is written down here.
	if !entry.opaque && entry.completesAKeyExchange(dir, record) {
		entry.opaque = true

		r.markEncrypted(ctx, entry, dir, before, after, elapsed)
	}
}

// markEncrypted writes the record that says where this recording went dark.
//
// The format has a record for exactly this and it is the reason a capture is
// allowed to end early: a secret record marks the switch, carries no material
// unless the writer discloses, and is skipped by replay and excluded from the
// digest. Without it a reader could not tell an online-mode login from a
// recorder that stopped for a reason nobody wrote down, and those two files
// must not look alike.
//
// It must be called with the recording's mutex held.
func (r *Recorder) markEncrypted(
	ctx context.Context,
	entry *recording,
	dir protocol.Direction,
	before, after protocol.State,
	elapsed time.Duration,
) {
	r.observe(ctx, entry, protocol.Observation{
		Frame:     entry.frame,
		Direction: dir,
		Stage:     protocol.ObservationSecret,
		Elapsed:   elapsed,
		Before:    protocol.NewSnapshot(before, nil),
		After:     protocol.NewSnapshot(after, nil),
		// The material is the one thing this must never carry. The proxy never
		// held it — it relayed the exchange without standing in it — so there
		// is nothing to disclose even under a disclosing writer.
		Redacted: true,
	})
}

// completesAKeyExchange reports whether the packet just recorded was the last
// one either peer sends in the clear.
//
// The protocol names the packets that carry key material; the direction is what
// separates the request from the answer to it. A server asks and a client
// answers, and it is the answer that switches both ciphers on.
//
// It must be called with the recording's mutex held.
func (e *recording) completesAKeyExchange(dir protocol.Direction, record relay.MessageRecord) bool {
	if e.secrets == nil || dir != protocol.DirectionServerbound {
		return false
	}

	packet, ok := record.Decoded.(protocol.Packet)
	if !ok {
		return false
	}

	return e.secrets.Sensitive(packet)
}

// RawChunk implements relay.Sink and deliberately records nothing.
//
// A chunk is a socket read, not a frame: its boundaries fall wherever the
// kernel put them. Writing chunks into a capture as though they were frames
// would produce a file that reads back but cannot replay, which is the one
// failure an oracle must not have. Frames arrive through Message, including the
// ones no codec could parse, so nothing is lost by declining here.
func (*Recorder) RawChunk(context.Context, int64, relay.Direction, []byte) {}

// CloseSession implements relay.Sink. It writes the trailer, which is what
// makes a recording complete and gives it a digest to replay against.
//
// This is the one method here that waits, and it is the one that can: relay
// calls it from the session's finish path, after both read pumps have stopped,
// so what it holds up is a session that is already over rather than traffic.
// Waiting is what lets it promise that a returned CloseSession means a finished
// file — the trailer has to land after the records still queued, not in front of
// them.
//
// The wait has a grace period all the same. What is queued is bounded by
// QueueDepth, but how long one write takes is not, and a proxy whose session
// teardown can hang on a wedged disk has traded a stalled pump for a stalled
// shutdown. Past the grace the writer is left to finish and close the file on its
// own, and the fault is reported.
func (r *Recorder) CloseSession(_ context.Context, id int64) {
	r.mu.Lock()
	entry := r.sessions[id]
	delete(r.sessions, id)
	r.mu.Unlock()

	if entry == nil {
		return
	}

	entry.stopOnce.Do(func() { close(entry.stop) })

	timer := time.NewTimer(r.opts.CloseGrace)
	defer timer.Stop()

	select {
	case <-entry.done:
	case <-timer.C:
		r.opts.OnError(fmt.Errorf(
			"capture: recording still writing %s after its session closed; left to finish on its own",
			r.opts.CloseGrace,
		))
	}
}

// Bind returns a hook that tells the recorder which session feeds which
// recording.
//
// A relay.Sink is handed a session identifier and never the session, which is
// right for something whose job is to record. This recorder has one thing it
// must do to the session it records: end it, when the connection outruns the
// disk and the alternative is a recording with a hole in it. A hook is the one
// place a consumer holds both halves — relay.Session.SinkID names the recording,
// and Close ends the session — so wiring this hook is what turns the overflow
// path from a log line into an outcome.
//
// It is optional. A recorder without it still refuses to write a torn recording;
// it just cannot stop the session that tore it.
func (r *Recorder) Bind() relay.Hook {
	return relay.HookFunc(func(_ context.Context, session *relay.Session, _ *relay.Message) (relay.Action, error) {
		r.Attach(session.SinkID(), session.Close)

		return relay.Forward, nil
	})
}

// Attach names what ends the session feeding one recording. Bind is the wiring
// that calls it with a relay session; this is the seam a test can reach.
//
// The first call wins and later ones are ignored, because Bind's hook runs on
// every message and only the first has anything to say.
func (r *Recorder) Attach(id int64, end func()) {
	entry := r.lookup(id)
	if entry == nil || end == nil || entry.end.Load() != nil {
		return
	}

	entry.end.CompareAndSwap(nil, &end)
}

// observe hands one record to the writer goroutine, or ends the session.
//
// Sequence numbering and the queueing happen together, under the recording's
// mutex, because the file's records must arrive in the order they were numbered
// and both read pumps are numbering. The send itself never blocks — that is the
// entire point of the queue — so holding the mutex across it costs nothing.
//
// It must be called with the recording's mutex held.
func (r *Recorder) observe(_ context.Context, entry *recording, observation protocol.Observation) {
	entry.sequence++
	observation.Sequence = entry.sequence

	select {
	case entry.queue <- observation:
	default:
		// A dropped record would be the cheap way out here, and it is the wrong
		// one: the SQLite sink beside this one drops and counts because a gap in
		// a telemetry table is a number to look at, while a gap in a recording is
		// a file that will not replay and therefore is not evidence. Losing the
		// session is the honest way to say the recorder lost.
		r.fail(entry, fmt.Errorf(
			"capture: recorder fell behind the connection: %d queued records unwritten, ending session",
			cap(entry.queue),
		))
	}
}

// write is one recording's writer goroutine: the only place the file is written
// while the session is live.
//
// It exists because relay calls Message on the read pump, before forwarding, so
// a synchronous write parks the connection for as long as the disk takes. That
// held while the page cache absorbed it and stops holding on a full disk, a slow
// disk, or a network filesystem.
func (r *Recorder) write(entry *recording) {
	defer close(entry.done)

	for {
		select {
		case observation := <-entry.queue:
			r.persist(entry, observation)
		case <-entry.stop:
			// Drain before leaving. What is queued at close belongs in the file,
			// and the trailer has to come after it.
			for {
				select {
				case observation := <-entry.queue:
					r.persist(entry, observation)
				default:
					r.finish(entry)

					return
				}
			}
		}
	}
}

// finish writes the trailer, which is what makes a recording complete and gives
// it a digest to replay against.
//
// It belongs to the writer rather than to CloseSession because the two must not
// both touch the file, and because the writer is the one that knows the records
// are all in. CloseSession normally waits for this; when it gives up waiting,
// this still runs.
func (r *Recorder) finish(entry *recording) {
	if err := entry.sink.Close(); err != nil && !entry.failed.Load() {
		r.opts.OnError(fmt.Errorf("capture: close recording: %w", err))
	}
}

// persist writes one record, or reports the first failure and stops recording.
func (r *Recorder) persist(entry *recording, observation protocol.Observation) {
	if entry.failed.Load() {
		return
	}

	if err := entry.sink.Observe(entry.ctx, observation); err != nil {
		r.fail(entry, fmt.Errorf("capture: write record: %w", err))
	}
}

// fail reports once, stops recording this session, and ends the session if it
// can reach it.
//
// A recording that lost a record in the middle is not evidence, and continuing
// to append to it would hide which part is missing. Ending the session says the
// same thing to the client: this connection is no longer being recorded, so it
// stops rather than running on unrecorded. It needs Bind's hook to have run; with
// no way back to the session, the loud error is all this can do.
func (r *Recorder) fail(entry *recording, err error) {
	if entry.failed.Swap(true) {
		return
	}

	r.opts.OnError(err)

	if end := entry.end.Load(); end != nil {
		(*end)()
	}
}

func (r *Recorder) lookup(id int64) *recording {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.sessions[id]
}

// beforeState reports the state this frame belonged to.
//
// A decoded packet carries it by definition; the recorder's own running state is
// the fallback for a frame nobody could decode.
//
// It must be called with the recording's mutex held.
func (e *recording) beforeState(record relay.MessageRecord) protocol.State {
	packet, ok := record.Decoded.(protocol.Packet)
	if !ok || packet.State == "" {
		return e.state
	}

	return packet.State
}

// advance applies whatever transition this frame implies and reports the state
// on its far side.
//
// The question asked is the same one the relay's codec asks, so a login success
// records the move into play rather than claiming the connection never left
// login. Replay follows the recorded transitions, so getting this wrong produces
// a file that will not replay through the very packets that matter.
//
// It is separate from beforeState because it mutates: applying a transition can
// change the pipeline as well as the state, and the sensitivity check has to run
// against the session as the frame found it. Both halves used to be one call,
// and the set compression frame was redacted out of every capture as a result.
//
// It must be called with the recording's mutex held.
func (e *recording) advance(record relay.MessageRecord, before protocol.State) protocol.State {
	packet, ok := record.Decoded.(protocol.Packet)
	if !ok || packet.State == "" || e.sensitiveSession == nil {
		return before
	}

	e.sensitiveSession.SetState(before)

	transition, proposed, err := e.sensitiveSession.ProposeTransition(packet)
	if err != nil || !proposed {
		return before
	}

	if e.sensitiveSession.ValidateTransition(transition) == nil {
		e.sensitiveSession.ApplyTransition(transition)
	}
	if transition.State != nil {
		return *transition.State
	}

	return before
}

// withhold reports whether this frame's bytes must be kept out of the file.
//
// The protocol answers, rather than a list of packet identifiers kept here: the
// key exchange is the material at stake, and minecraft-protocol already knows
// which frames carry it. The check is asked in the last state a decoded packet
// reported, so an opaque frame is judged in the state it actually arrived in
// rather than assumed harmless.
//
// The session this asks does follow the connection's pipeline, because advance
// applies compression to it exactly when the wire enables it — so the bytes
// handed here are read under the envelope they actually wear. What makes that
// work is that the question is asked before the frame's own transition is
// applied. A set compression packet travels uncompressed and turns compression
// on behind itself; judged afterwards it cannot be read, and the fail-closed
// branch below withholds the one field replay needs.
//
// It must be called with the recording's mutex held.
func (e *recording) withhold(state protocol.State, dir protocol.Direction, payload []byte) bool {
	if e.sensitive == nil {
		return false
	}

	e.sensitiveSession.SetState(state)

	return e.sensitive.SensitiveFrame(dir, payload)
}

func direction(dir relay.Direction) protocol.Direction {
	if dir == relay.ToClient {
		return protocol.DirectionClientbound
	}

	return protocol.DirectionServerbound
}

var _ relay.Sink = (*Recorder)(nil)
