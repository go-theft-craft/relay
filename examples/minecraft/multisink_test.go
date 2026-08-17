package minecraft_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
)

// countingSink records what it was told and, crucially, under which identifier.
// Every sink numbers its own sessions, so a fan-out that passed its own
// identifier through would address the wrong session in every child that did
// not happen to agree.
type countingSink struct {
	first    int64
	openErr  error
	opened   []relay.SessionInfo
	messages []int64
	chunks   []int64
	closed   []int64
}

func (s *countingSink) OpenSession(_ context.Context, info relay.SessionInfo) (int64, error) {
	if s.openErr != nil {
		return 0, s.openErr
	}

	s.opened = append(s.opened, info)

	return s.first + int64(len(s.opened)), nil
}

func (s *countingSink) Message(_ context.Context, id int64, _ relay.MessageRecord) {
	s.messages = append(s.messages, id)
}

func (s *countingSink) RawChunk(_ context.Context, id int64, _ relay.Direction, _ []byte) {
	s.chunks = append(s.chunks, id)
}

func (s *countingSink) CloseSession(_ context.Context, id int64) {
	s.closed = append(s.closed, id)
}

func TestMultiSinkAddressesEachChildByItsOwnIdentifier(t *testing.T) {
	t.Parallel()

	// Deliberately disjoint numbering: a fan-out that forwarded the caller's
	// identifier, or one child's, would pass this test only by coincidence if
	// both children started at the same number.
	first := &countingSink{first: 100}
	second := &countingSink{first: 5000}

	multi := minecraft.NewMultiSink(first, second)

	id, err := multi.OpenSession(t.Context(), relay.SessionInfo{ClientAddr: "127.0.0.1:1", OpenedAt: time.Now()})
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	multi.Message(t.Context(), id, relay.MessageRecord{Dir: relay.ToServer})
	multi.RawChunk(t.Context(), id, relay.ToClient, []byte{0x01})
	multi.CloseSession(t.Context(), id)

	for _, child := range []*countingSink{first, second} {
		want := child.first + 1

		if len(child.messages) != 1 || child.messages[0] != want {
			t.Errorf("child %d saw messages %v, want [%d]", child.first, child.messages, want)
		}
		if len(child.chunks) != 1 || child.chunks[0] != want {
			t.Errorf("child %d saw chunks %v, want [%d]", child.first, child.chunks, want)
		}
		if len(child.closed) != 1 || child.closed[0] != want {
			t.Errorf("child %d saw closes %v, want [%d]", child.first, child.closed, want)
		}
	}
}

// TestMultiSinkSurvivesOneSinkRefusing keeps a recording failure from becoming
// a disconnection. The proxy's job is to relay; a sink that cannot open is
// worth reporting, not worth dropping a player over.
func TestMultiSinkSurvivesOneSinkRefusing(t *testing.T) {
	t.Parallel()

	broken := &countingSink{openErr: errors.New("disk full")}
	working := &countingSink{first: 7}

	multi := minecraft.NewMultiSink(broken, working)

	id, err := multi.OpenSession(t.Context(), relay.SessionInfo{})
	if err != nil {
		t.Fatalf("OpenSession: %v — one failing sink must not fail the session", err)
	}

	multi.Message(t.Context(), id, relay.MessageRecord{})

	if len(working.messages) != 1 {
		t.Errorf("the working sink saw %d messages, want 1", len(working.messages))
	}
	if len(broken.messages) != 0 {
		t.Errorf("the refusing sink saw %d messages, want none", len(broken.messages))
	}
}

// TestMultiSinkReportsEverySinkRefusing draws the other half of the line. If
// nothing is recording, the caller asked for something that is not happening.
func TestMultiSinkReportsEverySinkRefusing(t *testing.T) {
	t.Parallel()

	multi := minecraft.NewMultiSink(
		&countingSink{openErr: errors.New("disk full")},
		&countingSink{openErr: errors.New("read only")},
	)

	if _, err := multi.OpenSession(t.Context(), relay.SessionInfo{}); err == nil {
		t.Fatal("every sink refused and OpenSession reported success")
	}
}
