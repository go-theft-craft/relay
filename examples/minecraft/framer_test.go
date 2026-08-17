package minecraft_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/wire/java"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
	"github.com/go-theft-craft/relay/relaytest"
)

func testLimits(t *testing.T, options ...protocol.LimitOption) protocol.Limits {
	t.Helper()

	limits, err := protocol.NewLimits(options...)
	if err != nil {
		t.Fatalf("NewLimits: %v", err)
	}

	return limits
}

func newFramer(t *testing.T, limits protocol.Limits) *minecraft.Framer {
	t.Helper()

	f, err := minecraft.NewFramer(nil, limits)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}

	return f
}

// TestFramerContract is the conformance harness earning its place. It was
// written against a newline framer; this is the first time it meets a
// length-prefixed one.
func TestFramerContract(t *testing.T) {
	limits := testLimits(t)

	relaytest.FramerContract(
		t,
		func() relay.Framer { return newFramer(t, limits) },
		[][]byte{
			// One byte, so the length prefix is a single VarInt byte.
			{0x00},
			// A short packet body.
			[]byte("\x00\x0bhello world"),
			// Long enough that the length needs a multi-byte VarInt, and long
			// enough to cross a small read buffer.
			bytes.Repeat([]byte{0x7f}, 300),
			bytes.Repeat([]byte{0x41}, 9000),
		},
	)
}

// TestFramerRejectsAnOversizeLengthPrefix is the case the harness cannot reach:
// a frame whose prefix claims more than the limits allow must fail at the
// prefix, before anything is allocated for it.
func TestFramerRejectsAnOversizeLengthPrefix(t *testing.T) {
	const limit = 512

	f := newFramer(t, testLimits(t, protocol.MaxFrameBytes(limit)))

	// A prefix one byte over the limit, followed by nothing. A framer that
	// allocated first and checked later would sit here waiting for bytes that
	// are never coming.
	var prefix [5]byte
	n := java.PutVarInt(prefix[:], int32(limit+1))

	_, err := f.ReadMessage(bufio.NewReader(bytes.NewReader(prefix[:n])))
	if err == nil {
		t.Fatal("a frame one byte over the limit was accepted")
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("the length was not checked before the payload was read: %v", err)
	}
}

// TestFramerRejectsAnOversizeWrite is the mirror on the write side. Sending a
// frame the peer's own limits will refuse is a desynchronisation waiting to
// happen, so it fails here instead.
func TestFramerRejectsAnOversizeWrite(t *testing.T) {
	const limit = 512

	f := newFramer(t, testLimits(t, protocol.MaxFrameBytes(limit)))

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, bytes.Repeat([]byte{0x00}, limit+1)); err == nil {
		t.Fatal("a frame one byte over the limit was written")
	}
	if buf.Len() != 0 {
		t.Fatalf("the rejected frame still put %d bytes on the wire", buf.Len())
	}
}

// TestFramerCleanEOFSurvives pins the distinction the relay branches on: an EOF
// at a frame boundary is a peer closing normally, and the session must see it
// as io.EOF rather than as a fault.
func TestFramerCleanEOFSurvives(t *testing.T) {
	f := newFramer(t, testLimits(t))

	_, err := f.ReadMessage(bufio.NewReader(strings.NewReader("")))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadMessage at a clean close = %v, want io.EOF", err)
	}
}

// TestFramerTruncationIsNotACleanClose is the other half of the same
// distinction, and the one that corrupts a stream when it is got wrong: an EOF
// part-way through a frame is a truncated message, not a polite goodbye.
func TestFramerTruncationIsNotACleanClose(t *testing.T) {
	f := newFramer(t, testLimits(t))

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, []byte("\x00truncate me")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	encoded := buf.Bytes()

	_, err := f.ReadMessage(bufio.NewReader(bytes.NewReader(encoded[:len(encoded)-1])))
	if err == nil {
		t.Fatal("a truncated frame was returned as a message")
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("a truncated frame reported io.EOF, which the relay reads as a clean close: %v", err)
	}
}
