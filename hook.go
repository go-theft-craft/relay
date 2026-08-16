package relay

import (
	"bufio"
	"context"
)

// Hook observes and may alter one message.
//
// The Message it receives is valid only for the duration of the call. A hook
// that wants the bytes afterwards must copy them.
type Hook interface {
	OnMessage(context.Context, *Session, *Message) (Action, error)
}

// HookFunc adapts a function to Hook.
type HookFunc func(context.Context, *Session, *Message) (Action, error)

// OnMessage implements Hook.
func (f HookFunc) OnMessage(ctx context.Context, s *Session, m *Message) (Action, error) {
	return f(ctx, s, m)
}

// PreFrameResult is what a pre-frame hook decided.
type PreFrameResult uint8

const (
	// Continue proceeds to normal framed relaying.
	Continue PreFrameResult = iota
	// Handled means the hook consumed the connection itself. The session ends
	// without dialling an upstream.
	Handled
)

// PreFrame inspects the opening bytes of a client connection before any framing
// happens.
//
// It exists so a session can recognise a protocol it should answer directly
// rather than relay — a legacy ping, a health check, a protocol probe. The
// reader it receives is the same one the read pump will use, so bytes the hook
// consumes are gone from the stream and bytes it only peeks are not.
type PreFrame interface {
	OnConnect(context.Context, *Session, *bufio.Reader) (PreFrameResult, error)
}

// PreFrameFunc adapts a function to PreFrame.
type PreFrameFunc func(context.Context, *Session, *bufio.Reader) (PreFrameResult, error)

// OnConnect implements PreFrame.
func (f PreFrameFunc) OnConnect(ctx context.Context, s *Session, br *bufio.Reader) (PreFrameResult, error) {
	return f(ctx, s, br)
}
