// Package relaytest checks a consumer's Framer against the contract the relay
// depends on.
//
// A Framer is the easiest part of the framework to get subtly wrong: partial
// reads, short writes, and a buffer reused after it was handed over all produce
// corruption a long way from their cause. Running this harness turns those into
// a test failure in the consumer's own suite.
package relaytest

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/go-theft-craft/relay"
)

// FramerContract exercises newFramer against every message in messages.
//
// newFramer is called per case rather than once, so a stateful framer is tested
// from a known start each time.
func FramerContract(t *testing.T, newFramer func() relay.Framer, messages [][]byte) {
	t.Helper()

	for _, want := range messages {
		roundTrip(t, newFramer(), want)
		oneByteAtATime(t, newFramer(), want)
		truncated(t, newFramer(), want)
		bufferIsOwned(t, newFramer(), want)
	}

	backToBack(t, newFramer(), messages)
}

func roundTrip(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, want); err != nil {
		t.Errorf("WriteMessage(%q): %v", want, err)

		return
	}

	got, err := f.ReadMessage(bufio.NewReader(&buf))
	if err != nil {
		t.Errorf("ReadMessage after writing %q: %v", want, err)

		return
	}
	if !bytes.Equal(got, want) {
		t.Errorf("round trip gave %q, want %q", got, want)
	}
	if buf.Len() != 0 {
		t.Errorf("ReadMessage left %d bytes unconsumed after %q", buf.Len(), want)
	}
}

// oneByteAtATime proves the framer reads to a boundary rather than to whatever
// one Read happened to return.
func oneByteAtATime(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, want); err != nil {
		t.Errorf("WriteMessage(%q): %v", want, err)

		return
	}

	got, err := f.ReadMessage(bufio.NewReader(&oneByteReader{data: buf.Bytes()}))
	if err != nil {
		t.Errorf("ReadMessage over a one-byte reader for %q: %v", want, err)

		return
	}
	if !bytes.Equal(got, want) {
		t.Errorf("one-byte read gave %q, want %q", got, want)
	}
}

// truncated proves an incomplete frame is an error rather than a short message.
// Silently returning what arrived is the corruption this catches.
func truncated(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	if err := f.WriteMessage(&buf, want); err != nil {
		t.Errorf("WriteMessage(%q): %v", want, err)

		return
	}

	encoded := buf.Bytes()
	if len(encoded) < 2 {
		return
	}

	got, err := f.ReadMessage(bufio.NewReader(bytes.NewReader(encoded[:len(encoded)-1])))
	if err == nil {
		t.Errorf("ReadMessage returned %q from a truncated frame, want an error", got)
	}
}

// bufferIsOwned proves the framer does not hand back a buffer it will reuse.
//
// Comparing the two returned slices byte for byte is not enough on its own,
// because reading the same message twice refills a reused buffer with the same
// contents and the violation hides. So the check is on identity: two live
// messages that share a backing array are the same buffer handed out twice,
// whatever they happen to contain.
func bufferIsOwned(t *testing.T, f relay.Framer, want []byte) {
	t.Helper()

	var buf bytes.Buffer
	for range 2 {
		if err := f.WriteMessage(&buf, want); err != nil {
			t.Errorf("WriteMessage(%q): %v", want, err)

			return
		}
	}

	br := bufio.NewReader(&buf)
	first, err := f.ReadMessage(br)
	if err != nil {
		t.Errorf("first ReadMessage(%q): %v", want, err)

		return
	}
	held := append([]byte(nil), first...)

	second, err := f.ReadMessage(br)
	if err != nil {
		t.Errorf("second ReadMessage(%q): %v", want, err)

		return
	}
	if !bytes.Equal(first, held) {
		t.Errorf("the second read overwrote the first message: %q became %q", held, first)
	}
	if len(first) > 0 && len(second) > 0 && &first[0] == &second[0] {
		t.Errorf("ReadMessage handed out the same buffer twice for %q; it must not reuse the slice it returns", want)
	}
}

// backToBack proves boundaries hold when messages arrive in one buffer.
func backToBack(t *testing.T, f relay.Framer, messages [][]byte) {
	t.Helper()

	var buf bytes.Buffer
	for _, want := range messages {
		if err := f.WriteMessage(&buf, want); err != nil {
			t.Errorf("WriteMessage(%q): %v", want, err)

			return
		}
	}

	br := bufio.NewReader(&buf)
	for _, want := range messages {
		got, err := f.ReadMessage(br)
		if err != nil {
			t.Errorf("ReadMessage for %q in a back-to-back stream: %v", want, err)

			return
		}
		if !bytes.Equal(got, want) {
			t.Errorf("back-to-back read gave %q, want %q", got, want)
		}
	}

	if _, err := f.ReadMessage(br); !errors.Is(err, io.EOF) {
		t.Errorf("ReadMessage at end of stream returned %v, want io.EOF", err)
	}
}

// oneByteReader yields one byte per Read, which is what a real socket does
// under a small MTU and what a framer that trusts one Read gets wrong.
type oneByteReader struct {
	data []byte
	at   int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.at >= len(r.data) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}

	p[0] = r.data[r.at]
	r.at++

	return 1, nil
}
