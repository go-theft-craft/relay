// Package typed removes the type assertion from a hook that only cares about
// one decoded packet type.
//
// The core carries Message.Decoded as any so that nothing in the framework has
// a type parameter and a consumer relaying opaque bytes writes no ceremony.
// That trade would be a bad one if typed use cost an assertion in every hook,
// which is what this package exists to prevent.
package typed

import (
	"context"

	"github.com/go-theft-craft/relay"
)

// On returns a relay.Hook that runs fn only when the message decoded to a P.
//
// Any other message, and any message no codec decoded, is forwarded untouched.
// A typed hook filters; it does not gate, so a hook watching for one packet
// never becomes the reason another one was dropped.
func On[P any](fn func(context.Context, *relay.Session, P, *relay.Message) (relay.Action, error)) relay.Hook {
	return relay.HookFunc(func(ctx context.Context, s *relay.Session, m *relay.Message) (relay.Action, error) {
		value, ok := m.Decoded.(P)
		if !ok {
			return relay.Forward, nil
		}

		return fn(ctx, s, value, m)
	})
}

// OnID is On narrowed further, to one packet identifier.
//
// Two packets in different connection states routinely decode to the same Go
// type, so a hook that wants exactly one of them needs the descriptor as well
// as the type.
func OnID[P any](id int32, fn func(context.Context, *relay.Session, P, *relay.Message) (relay.Action, error)) relay.Hook {
	return On(func(ctx context.Context, s *relay.Session, value P, m *relay.Message) (relay.Action, error) {
		if m.Desc.ID != id {
			return relay.Forward, nil
		}

		return fn(ctx, s, value, m)
	})
}

// Inject encodes value through the session's codec and sends it to one peer.
//
// It is a thin wrapper over relay.Session.InjectDecoded that exists so a typed
// call site reads the same way the typed hooks do.
func Inject[P any](s *relay.Session, dir relay.Direction, value P) error {
	return s.InjectDecoded(dir, value)
}
