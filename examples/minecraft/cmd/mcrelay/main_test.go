package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	capturesink "github.com/go-theft-craft/relay/examples/minecraft/capture"
)

// recordFixture writes one recording holding a spawn and two moves, through the
// same sink the proxy uses.
func recordFixture(t *testing.T) string {
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

	recorder, err := capturesink.NewRecorder(capturesink.Options{
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

	for _, value := range []any{
		&v1_8.PlayClientboundNamedEntitySpawn{EntityID: 7, X: 100 * 32, Y: 64 * 32, Z: 200 * 32},
		&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16},
		&v1_8.PlayClientboundRelEntityMove{EntityID: 7, DX: 16},
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
			At:      time.Now(),
		})
	}

	recorder.CloseSession(t.Context(), id)

	matches, err := filepath.Glob(filepath.Join(dir, "*.mccap"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("Glob = %v, %v; want one recording", matches, err)
	}

	return matches[0]
}

func TestVerifyExitsZeroOnAGoodRecording(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	if code := dispatch([]string{"verify", recordFixture(t)}, &out, io.Discard); code != 0 {
		t.Fatalf("verify exited %d on a recording that replays: %s", code, out.String())
	}
	if !strings.Contains(out.String(), "ok ") {
		t.Errorf("verify printed %q, want a line saying which recording passed", out.String())
	}
}

// TestVerifyExitsNonZeroOnARecordingThatCannotReplay is the property that makes
// the gate usable from CI: a bad recording has to fail the process, not just
// print something.
func TestVerifyExitsNonZeroOnARecordingThatCannotReplay(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(recordFixture(t))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	path := filepath.Join(t.TempDir(), "killed.mccap")
	if err := os.WriteFile(path, raw[:len(raw)-32], 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if code := dispatch([]string{"verify", path}, io.Discard, io.Discard); code == 0 {
		t.Error("verify exited zero on a recording that was never closed")
	}
}

func TestVerifyWithNoArgumentsIsAnError(t *testing.T) {
	t.Parallel()

	if code := dispatch([]string{"verify"}, io.Discard, io.Discard); code == 0 {
		t.Error("verify with no recordings exited zero")
	}
}

// TestHelpExitsZero covers every mode, because each one owns its own flag set
// and would have to get this right separately. A caller that runs -h to find
// out what the tool does should not be told the tool failed.
func TestHelpExitsZero(t *testing.T) {
	t.Parallel()

	for _, mode := range [][]string{{"-h"}, {"trace", "-h"}, {"verify", "-h"}} {
		if code := dispatch(mode, io.Discard, io.Discard); code != 0 {
			t.Errorf("%v exited %d, want 0", mode, code)
		}
	}
}

func TestTraceWritesTrajectories(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "traces.json")

	if code := dispatch([]string{"trace", "-in", recordFixture(t), "-out", out}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("trace exited %d", code)
	}

	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var document struct {
		Recording string `json:"recording"`
		Protocol  string `json:"protocol"`
		Traces    []struct {
			EntityID int32  `json:"EntityID"`
			Family   string `json:"Family"`
			Samples  []struct {
				Position struct{ X, Y, Z float64 }
			} `json:"Samples"`
		} `json:"traces"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("the written document is not JSON: %v", err)
	}

	if document.Protocol != v1_8.Protocol().ID() {
		t.Errorf("document protocol = %q, want %q", document.Protocol, v1_8.Protocol().ID())
	}
	if len(document.Traces) != 1 {
		t.Fatalf("document holds %d traces, want 1", len(document.Traces))
	}
	if got := len(document.Traces[0].Samples); got != 3 {
		t.Fatalf("the trace holds %d samples, want the spawn and two moves", got)
	}

	// The two moves are half a block each, from 100.
	if got := document.Traces[0].Samples[2].Position.X; got < 100.9 || got > 101.1 {
		t.Errorf("the trace ends at X %.4f, want about 101", got)
	}
}

func TestTraceWithoutAnInputIsAnError(t *testing.T) {
	t.Parallel()

	if code := dispatch([]string{"trace"}, io.Discard, io.Discard); code == 0 {
		t.Error("trace with no -in exited zero")
	}
}
