package capture_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	mccapture "github.com/go-theft-craft/minecraft-protocol/capture"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft/capture"
)

func testLimits(t *testing.T) protocol.Limits {
	t.Helper()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	return limits
}

func newRecorder(t *testing.T) (*capture.Recorder, string) {
	t.Helper()

	dir := t.TempDir()
	limits := testLimits(t)

	framer, err := java.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	recorder, err := capture.NewRecorder(capture.Options{
		Dir:        dir,
		Descriptor: protocols.Default(),
		Limits:     limits,
		Framer:     framer,
		OnError:    func(err error) { t.Errorf("recorder reported: %v", err) },
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	return recorder, dir
}

func openSession(t *testing.T, r *capture.Recorder) int64 {
	t.Helper()

	id, err := r.OpenSession(t.Context(), relay.SessionInfo{
		ClientAddr:   "127.0.0.1:51000",
		UpstreamAddr: "127.0.0.1:25565",
		Port:         25565,
		OpenedAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	return id
}

// readAll reads back the one recording the directory holds. A test that had to
// be told which file to read could not catch a recorder that wrote none.
func readAll(t *testing.T, dir string) ([]mccapture.Record, mccapture.Header) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("found %d recordings in %s, want 1", len(matches), dir)
	}

	file, err := os.Open(matches[0])
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()

	reader, err := mccapture.NewReader(file)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var records []mccapture.Record
	for {
		record, err := reader.Next()
		if err != nil {
			break
		}

		records = append(records, record)
	}

	if !reader.Complete() {
		t.Error("the recording has no trailer; it was not closed cleanly")
	}

	return records, reader.Header()
}

// message builds what relay hands a sink for one relayed frame.
//
// A descriptor and a decoded value arrive together or not at all: the relay
// fills both from one Codec.Decode call, so a record carrying a name but no
// value is a shape that never occurs on a real connection. A zero descriptor
// and a nil value is what an undecodable frame arrives with, which is the case
// the oracle cares most about.
func message(dir relay.Direction, desc relay.Descriptor, payload []byte) relay.MessageRecord {
	record := relay.MessageRecord{Dir: dir, Desc: desc, Raw: payload, At: time.Now()}
	if desc.Name != "" {
		record.Decoded = protocol.Packet{State: protocol.State("play"), ID: desc.ID, Name: desc.Name}
	}

	return record
}

func TestARecordingKeepsDirectionAndOrder(t *testing.T) {
	t.Parallel()

	recorder, dir := newRecorder(t)
	id := openSession(t, recorder)

	recorder.Message(t.Context(), id, message(relay.ToServer, relay.Descriptor{ID: 0, Name: "handshaking/set_protocol"}, []byte{0x00, 0x01}))
	recorder.Message(t.Context(), id, message(relay.ToClient, relay.Descriptor{ID: 2, Name: "login/success"}, []byte{0x02, 0x03}))
	recorder.Message(t.Context(), id, message(relay.ToServer, relay.Descriptor{ID: 4, Name: "play/position"}, []byte{0x04, 0x05}))

	recorder.CloseSession(t.Context(), id)

	records, _ := readAll(t, dir)

	packets := packetRecords(records)
	if len(packets) != 3 {
		t.Fatalf("recorded %d packet records, want 3", len(packets))
	}

	if packets[1].Direction == packets[0].Direction {
		t.Error("a clientbound record kept the serverbound direction")
	}
	if packets[0].Direction != protocol.DirectionServerbound {
		t.Errorf("first record direction = %v, want serverbound", packets[0].Direction)
	}

	for i, want := range []string{"handshaking/set_protocol", "login/success", "play/position"} {
		if packets[i].Name != want {
			t.Errorf("record %d is %q, want %q — order was not preserved", i, packets[i].Name, want)
		}
	}
}

// TestAnUndecodableFrameIsStillRecorded is the property that makes a recording
// an oracle rather than a decode log. relay forwards what it cannot parse and
// hands the sink a zero descriptor; a recorder that dropped those would lose
// exactly the packets worth investigating.
func TestAnUndecodableFrameIsStillRecorded(t *testing.T) {
	t.Parallel()

	recorder, dir := newRecorder(t)
	id := openSession(t, recorder)

	opaque := []byte{0x7f, 0xde, 0xad, 0xbe, 0xef}
	recorder.Message(t.Context(), id, message(relay.ToClient, relay.Descriptor{}, opaque))
	recorder.CloseSession(t.Context(), id)

	records, _ := readAll(t, dir)

	var found bool
	for _, record := range records {
		if record.Kind != mccapture.KindRawFrame {
			continue
		}
		if bytes.Contains(record.Payload, opaque) {
			found = true
		}
	}

	if !found {
		t.Fatal("the recording holds no raw frame carrying the undecodable bytes")
	}
}

// TestTheHeaderNamesTheProtocolAndTheUpstream keeps a recording self-describing.
// A trace file that cannot say what it recorded against is not evidence.
func TestTheHeaderNamesTheProtocolAndTheUpstream(t *testing.T) {
	t.Parallel()

	recorder, dir := newRecorder(t)
	id := openSession(t, recorder)
	recorder.CloseSession(t.Context(), id)

	_, header := readAll(t, dir)

	if header.Protocol != protocols.Default().ID() {
		t.Errorf("header protocol = %q, want %q", header.Protocol, protocols.Default().ID())
	}
	if header.Redaction != mccapture.RedactionEnforced {
		t.Errorf("header redaction = %q, want %q", header.Redaction, mccapture.RedactionEnforced)
	}
	if !bytes.Contains([]byte(header.Note), []byte("127.0.0.1:25565")) {
		t.Errorf("header note = %q, want it to name the upstream", header.Note)
	}
}

// TestEachSessionGetsItsOwnRecording covers the shape relay actually has: one
// Sink for the whole proxy, many concurrent sessions through it.
func TestEachSessionGetsItsOwnRecording(t *testing.T) {
	t.Parallel()

	recorder, dir := newRecorder(t)

	first := openSession(t, recorder)
	second := openSession(t, recorder)

	recorder.Message(t.Context(), first, message(relay.ToServer, relay.Descriptor{Name: "one"}, []byte{0x01}))
	recorder.Message(t.Context(), second, message(relay.ToServer, relay.Descriptor{Name: "two"}, []byte{0x02}))

	recorder.CloseSession(t.Context(), first)
	recorder.CloseSession(t.Context(), second)

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("two sessions produced %d recordings, want 2", len(matches))
	}
}

// TestARecordingNeverHoldsTheKeyExchangeInTheClear is the one property that
// cannot be fixed after the fact.
//
// minecraft-protocol's M5 found this exact defect and fixed it inside its own
// stream, but a proxy builds its observations by hand, so the fix does not come
// along for free: relay hands the sink raw frames and would happily record a
// key exchange byte for byte. The protocol is asked which frames carry secret
// material rather than a list of packet identifiers being kept here.
func TestARecordingNeverHoldsTheKeyExchangeInTheClear(t *testing.T) {
	t.Parallel()

	recorder, dir := newRecorder(t)
	id := openSession(t, recorder)

	// A login-state packet moves the recorder into login, which is the state
	// the sensitivity question is asked in.
	recorder.Message(t.Context(), id, decoded(relay.ToServer, "login/login_start", 0, protocol.State("login"), []byte{0x00, 0x06, 't', 'e', 's', 't', 'e', 'r'}))

	secret := []byte{0x01, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe}
	recorder.Message(t.Context(), id, decoded(relay.ToClient, "login/encryption_begin", 1, protocol.State("login"), secret))

	recorder.CloseSession(t.Context(), id)

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}

	raw, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if bytes.Contains(raw, secret[1:]) {
		t.Fatal("the recording holds the key exchange body in the clear")
	}

	// Withheld is not the same as absent: the record must still say a frame
	// crossed, or the recording claims a gap that did not happen.
	var withheld bool
	for _, record := range readRecords(t, matches[0]) {
		if record.Redacted {
			withheld = true
		}
	}
	if !withheld {
		t.Error("no record is marked redacted; the frame was dropped rather than withheld")
	}
}

// decoded builds what relay hands a sink for a frame the codec understood.
func decoded(dir relay.Direction, name string, id int32, state protocol.State, payload []byte) relay.MessageRecord {
	return relay.MessageRecord{
		Dir:     dir,
		Desc:    relay.Descriptor{ID: id, Name: name},
		Raw:     payload,
		Decoded: protocol.Packet{State: state, ID: id, Name: name},
		At:      time.Now(),
	}
}

func readRecords(t *testing.T, path string) []mccapture.Record {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()

	reader, err := mccapture.NewReader(file)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var records []mccapture.Record
	for {
		record, err := reader.Next()
		if err != nil {
			break
		}

		records = append(records, record)
	}

	return records
}

func packetRecords(records []mccapture.Record) []mccapture.Record {
	var packets []mccapture.Record
	for _, record := range records {
		if record.Kind == mccapture.KindPacket {
			packets = append(packets, record)
		}
	}

	return packets
}
