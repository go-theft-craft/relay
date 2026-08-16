package relay

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// flip inverts every byte in place. It is its own inverse, which makes it a
// stand-in for a stream cipher that a test can assert against without a key
// exchange.
func flip(p []byte) {
	for i := range p {
		p[i] = ^p[i]
	}
}

func flipped(b []byte) []byte {
	out := append([]byte(nil), b...)
	flip(out)

	return out
}

func flipTransform() Transform {
	return Transform{
		Read:  flip,
		Write: flipped,
	}
}

func pipePair(t *testing.T) (client, server net.Conn) {
	t.Helper()

	client, server = net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	return client, server
}

// TestConduitSwapWhileParked is the case the hand-out ordering exists for: the
// pump is blocked inside a socket read when the swap lands, and the bytes that
// arrive afterwards must come out transformed.
func TestConduitSwapWhileParked(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	f := lineFramer{}

	type result struct {
		msg []byte
		err error
	}
	done := make(chan result, 1)

	go func() {
		msg, err := f.ReadMessage(c)
		done <- result{msg: msg, err: err}
	}()

	// Let the reader park inside the pipe read with nothing buffered.
	time.Sleep(50 * time.Millisecond)

	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap while parked: %v", err)
	}

	if _, err := client.Write(flipped([]byte("late\n"))); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("ReadMessage after a swap: %v", got.err)
		}
		if string(got.msg) != "late" {
			t.Fatalf("message = %q, want late", got.msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadMessage never returned")
	}
}

// TestConduitSwapRefusesBufferedBytes covers the one refusal. Bytes already
// buffered arrived before the switch, so transforming them on the way out would
// corrupt the next message with nothing to point at afterwards.
func TestConduitSwapRefusesBufferedBytes(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	f := lineFramer{}

	go func() { _, _ = client.Write([]byte("trigger\nextra\n")) }()

	first, err := f.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(first) != "trigger" {
		t.Fatalf("first message = %q, want trigger", first)
	}
	if c.Buffered() == 0 {
		t.Fatal("the test needs bytes left buffered; there were none")
	}

	if err := c.Swap(flipTransform()); !errors.Is(err, ErrSwapPending) {
		t.Fatalf("Swap with %d bytes buffered returned %v, want ErrSwapPending", c.Buffered(), err)
	}
}

func TestConduitReadTransformsOnHandout(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	go func() { _, _ = client.Write(flipped([]byte("hello\n"))) }()

	got, err := lineFramer{}.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("message = %q, want hello", got)
	}
}

func TestConduitWriteTransform(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	go func() { _ = lineFramer{}.WriteMessage(c, []byte("out")) }()

	got := make([]byte, 4)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, flipped([]byte("out\n"))) {
		t.Fatalf("wrote %q, want the flipped form of out\\n", got)
	}
}

// TestConduitWriteDoesNotMutateCaller matters because the message the relay is
// writing is the same slice a sink or a hook may still be holding.
func TestConduitWriteDoesNotMutateCaller(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("Swap: %v", err)
	}

	payload := []byte("keepme")
	held := append([]byte(nil), payload...)

	go func() { _, _ = io.CopyN(io.Discard, client, int64(len(payload))) }()

	if _, err := c.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !bytes.Equal(payload, held) {
		t.Fatalf("Write mutated the caller's buffer: %q became %q", held, payload)
	}
}

// TestConduitWriteReportsCallerBytes pins the io.Writer contract for a
// transform that changes length. Returning the transformed count would make
// io.Copy and bufio believe a different number of bytes moved than the caller
// handed over, and the bug would surface far from here.
func TestConduitWriteReportsCallerBytes(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)

	// Doubling stands in for any length-changing encoding: the only property
	// that matters is that the transformed length differs from the caller's.
	err := c.Swap(Transform{Write: func(p []byte) []byte {
		return append(append([]byte(nil), p...), p...)
	}})
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}

	payload := []byte("grow")
	go func() { _, _ = io.CopyN(io.Discard, client, int64(2*len(payload))) }()

	n, werr := c.Write(payload)
	if werr != nil {
		t.Fatalf("Write: %v", werr)
	}
	if n != len(payload) {
		t.Fatalf("Write returned %d, want %d — the count must be in the caller's bytes", n, len(payload))
	}
}

func TestConduitComposesSwaps(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("first Swap: %v", err)
	}
	if err := c.Swap(flipTransform()); err != nil {
		t.Fatalf("second Swap: %v", err)
	}

	// Two flips compose to the identity, which is how the test knows the second
	// swap layered over the first rather than replacing it.
	go func() { _, _ = client.Write([]byte("plain\n")) }()

	got, err := lineFramer{}.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != "plain" {
		t.Fatalf("message = %q, want plain", got)
	}
}

// TestConduitComposesWriteSwapsInMirrorOrder pins the composition order for
// writes. Two layers that are not each other's inverse only agree with a peer
// undoing them in the opposite order, which is what the mirror composition
// produces and what a same-order composition silently gets wrong.
func TestConduitComposesWriteSwapsInMirrorOrder(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)

	// Two distinct, non-commuting write layers: one appends a byte, the other
	// flips every byte. Applied newest-first, "x" becomes flip("x" + "1").
	appendOne := Transform{Write: func(p []byte) []byte {
		return append(append([]byte(nil), p...), '1')
	}}
	if err := c.Swap(appendOne); err != nil {
		t.Fatalf("first Swap: %v", err)
	}
	if err := c.Swap(Transform{Write: flipped}); err != nil {
		t.Fatalf("second Swap: %v", err)
	}

	want := flipped([]byte("x1"))
	go func() { _, _ = c.Write([]byte("x")) }()

	got := make([]byte, len(want))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("wrote %q, want %q — write transforms must compose newest-first", got, want)
	}
}

// TestConduitReadPropagatesEOF keeps a clean peer close distinguishable from a
// fault, which is what the session path branches on.
func TestConduitReadPropagatesEOF(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	_ = client.Close()

	if _, err := c.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("ReadByte after a clean close returned %v, want io.EOF", err)
	}
}

// TestConduitWriteOnlySwapIgnoresBufferedBytes is what lets a caller arm the
// two halves of a transform at different moments.
//
// A proxy needs that: the read side has to be armed before the peer is told to
// switch, because the peer may answer in the new encoding immediately, while
// the write side must stay in the old encoding until the message that tells it
// has actually gone out. Refusing a write-only swap for bytes sitting in the
// read buffer would make that ordering impossible to express.
func TestConduitWriteOnlySwapIgnoresBufferedBytes(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	f := lineFramer{}

	go func() { _, _ = client.Write([]byte("first\nleftover\n")) }()

	if _, err := f.ReadMessage(c); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if c.Buffered() == 0 {
		t.Fatal("the test needs bytes left buffered; there were none")
	}

	if err := c.Swap(Transform{Write: flipped}); err != nil {
		t.Fatalf("a write-only swap with %d bytes buffered: %v", c.Buffered(), err)
	}

	// The read side is untouched, so the buffered message still reads plainly.
	got, err := f.ReadMessage(c)
	if err != nil {
		t.Fatalf("ReadMessage after a write-only swap: %v", err)
	}
	if string(got) != "leftover" {
		t.Fatalf("buffered message = %q, want leftover", got)
	}

	// And the write side did change.
	go func() { _ = f.WriteMessage(c, []byte("out")) }()

	wire := make([]byte, 4)
	if _, err := io.ReadFull(client, wire); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(wire, flipped([]byte("out\n"))) {
		t.Fatalf("wrote %q, want the flipped form", wire)
	}
}

// TestConduitReadSwapStillRefusesBufferedBytes keeps the original refusal in
// place: it is the read side the buffered bytes would corrupt.
func TestConduitReadSwapStillRefusesBufferedBytes(t *testing.T) {
	client, server := pipePair(t)

	c := NewConduit(server, 4096)
	f := lineFramer{}

	go func() { _, _ = client.Write([]byte("first\nleftover\n")) }()

	if _, err := f.ReadMessage(c); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if err := c.Swap(Transform{Read: flip, Write: flipped}); !errors.Is(err, ErrSwapPending) {
		t.Fatalf("a read swap with bytes buffered = %v, want ErrSwapPending", err)
	}
}
