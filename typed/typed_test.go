package typed_test

import (
	"context"
	"testing"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/typed"
)

type chatPacket struct{ Text string }

type movePacket struct{ X, Y int }

func TestOnFiresForItsOwnType(t *testing.T) {
	var seen string

	hook := typed.On(func(_ context.Context, _ *relay.Session, p chatPacket, _ *relay.Message) (relay.Action, error) {
		seen = p.Text

		return relay.Forward, nil
	})

	m := &relay.Message{Dir: relay.ToServer, Decoded: chatPacket{Text: "hello"}}

	action, err := hook.OnMessage(context.Background(), nil, m)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if action != relay.Forward {
		t.Fatalf("action = %v, want Forward", action)
	}
	if seen != "hello" {
		t.Fatalf("the hook saw %q, want hello", seen)
	}
}

// TestOnSkipsOtherMessages is the property that makes a typed hook a filter
// rather than a gate: a hook watching for one packet must never be the reason
// another was dropped.
func TestOnSkipsOtherMessages(t *testing.T) {
	cases := map[string]any{
		"another decoded type": movePacket{X: 1, Y: 2},
		"undecoded":            nil,
		"raw bytes":            []byte("opaque"),
	}

	for name, decoded := range cases {
		t.Run(name, func(t *testing.T) {
			ran := false

			hook := typed.On(func(_ context.Context, _ *relay.Session, _ chatPacket, _ *relay.Message) (relay.Action, error) {
				ran = true

				return relay.Drop, nil
			})

			m := &relay.Message{Dir: relay.ToServer, Decoded: decoded}

			action, err := hook.OnMessage(context.Background(), nil, m)
			if err != nil {
				t.Fatalf("OnMessage: %v", err)
			}
			if ran {
				t.Fatal("the hook ran for a message that is not its type")
			}
			if action != relay.Forward {
				t.Fatalf("action = %v, want Forward — a typed hook filters, it does not gate", action)
			}
		})
	}
}

func TestOnMutationReachesTheCore(t *testing.T) {
	hook := typed.On(func(_ context.Context, _ *relay.Session, p chatPacket, m *relay.Message) (relay.Action, error) {
		p.Text = "rewritten"
		m.SetDecoded(p)

		return relay.Replace, nil
	})

	m := &relay.Message{Dir: relay.ToServer, Decoded: chatPacket{Text: "original"}}

	action, err := hook.OnMessage(context.Background(), nil, m)
	if err != nil {
		t.Fatalf("OnMessage: %v", err)
	}
	if action != relay.Replace {
		t.Fatalf("action = %v, want Replace", action)
	}
	if !m.DecodedChanged() {
		t.Fatal("the core was not told the decoded value changed, so it would not re-encode")
	}
	if got, _ := m.Decoded.(chatPacket); got.Text != "rewritten" {
		t.Fatalf("Decoded = %+v, want the rewritten text", m.Decoded)
	}
}

func TestOnPropagatesErrors(t *testing.T) {
	want := context.DeadlineExceeded

	hook := typed.On(func(_ context.Context, _ *relay.Session, _ chatPacket, _ *relay.Message) (relay.Action, error) {
		return relay.Forward, want
	})

	m := &relay.Message{Decoded: chatPacket{}}

	if _, err := hook.OnMessage(context.Background(), nil, m); err != want {
		t.Fatalf("OnMessage error = %v, want it passed through unchanged", err)
	}
}

func TestOnIDNarrowsToOnePacket(t *testing.T) {
	ran := 0

	hook := typed.OnID(7, func(_ context.Context, _ *relay.Session, _ chatPacket, _ *relay.Message) (relay.Action, error) {
		ran++

		return relay.Forward, nil
	})

	matching := &relay.Message{Decoded: chatPacket{}, Desc: relay.Descriptor{ID: 7, Name: "chat"}}
	other := &relay.Message{Decoded: chatPacket{}, Desc: relay.Descriptor{ID: 8, Name: "chat_other_state"}}

	for _, m := range []*relay.Message{matching, other, matching} {
		if _, err := hook.OnMessage(context.Background(), nil, m); err != nil {
			t.Fatalf("OnMessage: %v", err)
		}
	}

	if ran != 2 {
		t.Fatalf("the hook ran %d times, want 2 — the same Go type in two states is two packets", ran)
	}
}
