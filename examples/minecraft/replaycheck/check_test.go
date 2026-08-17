package replaycheck_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft/capture"
	"github.com/go-theft-craft/relay/examples/minecraft/replaycheck"
)

// recordFixtureSession writes one recording through the real sink, so the file
// under test was produced the way a captured session produces one.
func recordFixtureSession(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	limits, err := protocol.NewLimits()
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	framer, err := java.NewFramer(limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	recorder, err := capture.NewRecorder(capture.Options{
		Dir:        dir,
		Descriptor: v1_8.Protocol(),
		Limits:     limits,
		Framer:     framer,
		OnError:    func(err error) { t.Errorf("recorder reported: %v", err) },
	})
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}

	id, err := recorder.OpenSession(t.Context(), relay.SessionInfo{
		ClientAddr:   "127.0.0.1:51000",
		UpstreamAddr: "127.0.0.1:25565",
		OpenedAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	server, err := v1_8.Protocol().NewSession(protocol.RoleServer, limits)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	server.SetState(protocol.State("play"))

	for i, value := range []any{
		&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32},
		&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16},
		&v1_8.PlayClientboundEntityTeleport{EntityID: 7, X: 32, Y: 64 * 32, Z: 32},
	} {
		identified, ok := value.(interface{ PacketID() int32 })
		if !ok {
			t.Fatalf("%T has no PacketID", value)
		}

		packet := protocol.Packet{
			State:     protocol.State("play"),
			Direction: protocol.DirectionClientbound,
			ID:        identified.PacketID(),
			Value:     value,
		}

		payload, err := server.EncodeFrame(packet)
		if err != nil {
			t.Fatalf("EncodeFrame: %v", err)
		}

		recorder.Message(t.Context(), id, relay.MessageRecord{
			Dir:     relay.ToClient,
			Desc:    relay.Descriptor{ID: packet.ID, Name: packet.Name},
			Raw:     payload,
			Decoded: packet,
			At:      time.Now().Add(time.Duration(i) * 50 * time.Millisecond),
		})
	}

	recorder.CloseSession(t.Context(), id)

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("Glob = %v, %v; want one recording", matches, err)
	}

	return matches[0]
}

func TestARecordingReplaysToItsOwnDigest(t *testing.T) {
	t.Parallel()

	path := recordFixtureSession(t)

	first, err := replaycheck.Check(t.Context(), path)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !first.OK() {
		t.Fatalf("the recording does not replay: %s", first.Explain())
	}
	if first.Records == 0 {
		t.Fatal("the replay saw no records")
	}

	second, err := replaycheck.Check(t.Context(), path)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if first.Digest != second.Digest {
		t.Errorf("two replays of one recording produced %s and %s", first.Digest, second.Digest)
	}
}

// TestAnUnclosedRecordingIsNotEvidence covers the failure that actually
// happens: a proxy killed mid-session leaves a file with records and no
// trailer. It reads back perfectly well, which is the danger — nothing about it
// looks wrong until something asks it for a digest it does not have.
//
// Byte corruption is deliberately not tested here. Every record carries a CRC,
// so a flipped byte fails the read long before the digest is consulted; a test
// that flipped one would be testing the checksum. The digest catches the other
// thing: a build whose decoding moved, replaying the same bytes to a different
// answer.
func TestAnUnclosedRecordingIsNotEvidence(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(recordFixtureSession(t))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Cut the tail off, which is what a process that died mid-write leaves.
	truncated := raw[:len(raw)-32]

	path := filepath.Join(t.TempDir(), "killed.mccap")
	if err := os.WriteFile(path, truncated, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	result, err := replaycheck.Check(t.Context(), path)
	if err != nil {
		// A cut that lands mid-record fails the read, which is also a
		// rejection and is equally fine. The point is that it is never OK.
		return
	}

	if result.OK() {
		t.Fatal("a recording with no trailer passed the gate")
	}
	if result.Complete {
		t.Error("a truncated recording reported itself complete")
	}
	if result.Explain() == "" {
		t.Error("the result rejects the recording without saying why")
	}
}
