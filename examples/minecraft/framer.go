// Package minecraft is the worked example: a relay consumer built against a
// real protocol rather than a toy one.
//
// It implements three of the four seams — Framer, Codec, and Prober — over a
// protocol that was not designed with this framework in mind, which is the
// only way to find out whether the seams are real.
//
// It deliberately does not stand between an encrypted login. Doing that as a
// third party means running two key exchanges and holding the client's session
// credentials, which is a project in itself and teaches nothing about the
// framework. Decoding stops at that point and the session continues as opaque
// passthrough; see Codec.
package minecraft

import (
	"fmt"
	"io"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
)

// Framer adapts the Java edition frame envelope to relay.Framer.
//
// A relay message is one frame payload: the length prefix is framing and
// belongs here, and everything inside it is the codec's problem. The
// interesting content is in the protocol library's wire package, not in this
// file — this is an adapter and nothing more.
type Framer struct {
	inner  protocol.Framer
	limits protocol.Limits
}

// NewFramer builds a framer bound to limits, which is what bounds the length a
// frame prefix may claim.
func NewFramer(limits protocol.Limits) (*Framer, error) {
	inner, err := java.NewFramer(limits)
	if err != nil {
		return nil, fmt.Errorf("minecraft: build the framer: %w", err)
	}

	return &Framer{inner: inner, limits: limits}, nil
}

// ReadMessage implements relay.Framer.
//
// The payload is copied because Frame.Payload returns a borrowed view into the
// frame's own buffer, and relay.Framer promises the caller a slice the framer
// will not reuse.
//
// The error is returned unwrapped so io.EOF survives: the relay reads a clean
// peer close as io.EOF and anything else as a fault, and the underlying framer
// already draws that line in the right place — an EOF before the first byte of
// a length prefix is a clean close, and an EOF anywhere after it is a truncated
// frame reported as io.ErrUnexpectedEOF.
func (f *Framer) ReadMessage(r relay.Reader) ([]byte, error) {
	frame, err := f.inner.ReadFrame(r)
	if err != nil {
		return nil, err
	}

	payload := frame.Payload()

	return append([]byte(nil), payload...), nil
}

// WriteMessage implements relay.Framer.
//
// It builds the frame rather than writing the payload directly, because the
// length prefix is not this file's to guess.
func (f *Framer) WriteMessage(w io.Writer, raw []byte) error {
	frame, err := f.inner.BuildFrame(raw)
	if err != nil {
		return fmt.Errorf("minecraft: build frame: %w", err)
	}

	if err := f.inner.WriteFrame(w, frame); err != nil {
		return fmt.Errorf("minecraft: write frame: %w", err)
	}

	return nil
}
