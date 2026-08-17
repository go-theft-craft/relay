package capture

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
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
	// OnError receives a fault that a Sink method cannot return. Recording
	// failures must be loud: a truncated oracle that reports nothing is worse
	// than no oracle, because it still looks like evidence.
	OnError func(error)
}

// Recorder writes one capture file per session, in the format
// minecraft-protocol's replay and digest tooling already reads.
//
// It is a relay.Sink, which is a process-wide interface rather than a
// per-session one, so the file bookkeeping lives here: a proxy holds many
// sessions at once and each needs its own recording, its own frame numbering,
// and its own clock origin.
type Recorder struct {
	opts Options

	mu       sync.Mutex
	next     int64
	sessions map[int64]*recording
}

// recording is one session's file and the counters that describe it.
//
// The mutex is per recording rather than per Recorder: both read pumps of one
// session record concurrently, but two sessions share nothing.
type recording struct {
	mu       sync.Mutex
	sink     *mccapture.FileSink
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
	failed           bool
}

// NewRecorder validates the options and prepares the output directory.
func NewRecorder(opts Options) (*Recorder, error) {
	if opts.Dir == "" {
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

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("capture: create %s: %w", opts.Dir, err)
	}

	return &Recorder{opts: opts, sessions: make(map[int64]*recording)}, nil
}

// OpenSession implements relay.Sink. It creates the session's file and writes
// the header before any record can reach it.
func (r *Recorder) OpenSession(_ context.Context, info relay.SessionInfo) (int64, error) {
	started := info.OpenedAt
	if started.IsZero() {
		started = time.Now()
	}

	r.mu.Lock()
	r.next++
	id := r.next
	r.mu.Unlock()

	name := fmt.Sprintf("%s-%03d.mccap", started.UTC().Format("20060102-150405"), id)

	sink, err := mccapture.NewFileSink(filepath.Join(r.opts.Dir, name), mccapture.Header{
		Protocol:          r.opts.Descriptor.ID(),
		FrameBytes:        r.opts.Limits.FrameBytes(),
		DecompressedBytes: r.opts.Limits.DecompressedBytes(),
		Created:           started.UTC().Format(time.RFC3339),
		Note:              r.note(info),
	})
	if err != nil {
		return 0, fmt.Errorf("capture: open %s: %w", name, err)
	}

	entry := &recording{sink: sink, started: started, state: protocol.State("handshaking")}

	// The sensitivity oracle is optional: a protocol that declares no sensitive
	// frames leaves it nil and every frame records its bytes.
	if session, err := r.opts.Descriptor.NewSession(protocol.RoleServer, r.opts.Limits); err == nil {
		if frames, ok := session.(protocol.SensitiveFrames); ok {
			entry.sensitive = frames
			entry.sensitiveSession = session
		}
	}

	r.mu.Lock()
	r.sessions[id] = entry
	r.mu.Unlock()

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

	if entry.failed {
		return
	}

	dir := direction(record.Dir)
	elapsed := record.At.Sub(entry.started)
	if elapsed < 0 {
		elapsed = 0
	}

	entry.frame++
	before, after := entry.states(record)

	frame, err := r.opts.Framer.BuildFrame(record.Raw)
	if err != nil {
		r.fail(entry, fmt.Errorf("capture: rebuild frame: %w", err))

		return
	}

	wire := frame.WireBytes()
	redacted := entry.withhold(before, dir, record.Raw)

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
	if !redacted {
		raw.Bytes = wire
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
			packet.Bytes = record.Raw
		}

		r.observe(ctx, entry, packet)
	}

	entry.state = after
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
func (r *Recorder) CloseSession(_ context.Context, id int64) {
	r.mu.Lock()
	entry := r.sessions[id]
	delete(r.sessions, id)
	r.mu.Unlock()

	if entry == nil {
		return
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if err := entry.sink.Close(); err != nil && !entry.failed {
		r.opts.OnError(fmt.Errorf("capture: close recording: %w", err))
	}
}

// observe writes one record and reports the first failure. It must be called
// with the recording's mutex held.
func (r *Recorder) observe(ctx context.Context, entry *recording, observation protocol.Observation) {
	entry.sequence++
	observation.Sequence = entry.sequence

	if err := entry.sink.Observe(ctx, observation); err != nil {
		r.fail(entry, fmt.Errorf("capture: write record: %w", err))
	}
}

// fail reports once and stops recording this session. A recording that lost a
// record in the middle is not evidence, and continuing to append to it would
// hide which part is missing.
func (r *Recorder) fail(entry *recording, err error) {
	if entry.failed {
		return
	}

	entry.failed = true
	r.opts.OnError(err)
}

func (r *Recorder) lookup(id int64) *recording {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.sessions[id]
}

// states reports the connection state either side of this frame.
//
// A decoded packet carries the state it belonged to, which is the before state
// by definition — the recorder's own running state is a fallback for frames
// nobody could decode. The after state comes from asking the protocol what the
// packet implies, the same question the relay's codec asks, so a login success
// records the move into play rather than claiming the connection never left
// login. Replay follows those recorded transitions, so getting this wrong
// produces a file that will not replay through the very packets that matter.
//
// It must be called with the recording's mutex held.
func (e *recording) states(record relay.MessageRecord) (before, after protocol.State) {
	packet, ok := record.Decoded.(protocol.Packet)
	if !ok || packet.State == "" {
		return e.state, e.state
	}

	before = packet.State
	after = before

	if e.sensitiveSession == nil {
		return before, after
	}

	e.sensitiveSession.SetState(before)

	transition, proposed, err := e.sensitiveSession.ProposeTransition(packet)
	if err != nil || !proposed {
		return before, after
	}

	if e.sensitiveSession.ValidateTransition(transition) == nil {
		e.sensitiveSession.ApplyTransition(transition)
	}
	if transition.State != nil {
		after = *transition.State
	}

	return before, after
}

// withhold reports whether this frame's bytes must be kept out of the file.
//
// The protocol answers, rather than a list of packet identifiers kept here: the
// key exchange is the material at stake, and minecraft-protocol already knows
// which frames carry it. The check is asked in the last state a decoded packet
// reported, so an opaque frame is judged in the state it actually arrived in
// rather than assumed harmless.
//
// The oracle session never has compression applied, which is safe only because
// the sensitive frames all precede the packet that enables it. A protocol that
// compressed before it exchanged keys would need the control mirrored here, and
// the check would silently start answering about the wrong bytes.
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
