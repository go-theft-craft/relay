package relay

import (
	"bufio"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// splitFramer writes a message in two parts with a scheduling point between
// them. A relay that did not hold a writer lock across the whole message would
// let an injected message land in the gap, and this framer makes that
// interleaving happen every run rather than one run in a thousand.
type splitFramer struct{ lineFramer }

func (f splitFramer) WriteMessage(w io.Writer, raw []byte) error {
	if len(raw) < 2 {
		return f.lineFramer.WriteMessage(w, raw)
	}

	if _, err := w.Write(raw[:len(raw)/2]); err != nil {
		return err
	}

	runtime.Gosched()

	if _, err := w.Write(raw[len(raw)/2:]); err != nil {
		return err
	}

	_, err := w.Write([]byte("\n"))

	return err
}

// TestInjectNeverInterleaves is the whole reason injection is designed in
// rather than added later.
func TestInjectNeverInterleaves(t *testing.T) {
	const (
		rounds   = 300
		relayed  = "AAAAAAAAAAAAAAAA"
		injected = "BBBBBBBBBBBBBBBB"
	)

	h := newHarness(t, Config{Framer: splitFramer{}})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for range rounds {
			writeLine(t, h.client, relayed)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for range rounds {
			if err := h.session.Inject(ToServer, []byte(injected)); err != nil {
				t.Errorf("Inject: %v", err)

				return
			}
		}
	}()

	upstream := bufio.NewReader(h.server)
	for i := range 2 * rounds {
		line := readLine(t, upstream)
		if line != relayed && line != injected {
			t.Fatalf("line %d was interleaved: %q", i, line)
		}
		if strings.ContainsAny(line, "AB") && strings.Count(line, line[:1]) != len(line) {
			t.Fatalf("line %d mixed two messages: %q", i, line)
		}
	}

	wg.Wait()
}

func TestInjectOnAClosedSessionReportsIt(t *testing.T) {
	h := newHarness(t, Config{})

	h.session.Close()

	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the session never finished")
	}

	done := make(chan error, 1)
	go func() { done <- h.session.Inject(ToServer, []byte("too late")) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("Inject after close = %v, want ErrSessionClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Inject on a closed session blocked instead of reporting")
	}
}

func TestInjectIsRecordedToTheSink(t *testing.T) {
	sink := &recordingSink{}
	h := newHarness(t, Config{Sink: sink})

	// The pipe is synchronous, so the injection only completes once the peer
	// reads it.
	sent := make(chan error, 1)
	go func() { sent <- h.session.Inject(ToClient, []byte("from the proxy")) }()

	if got := readLine(t, bufio.NewReader(h.client)); got != "from the proxy" {
		t.Fatalf("client received %q, want from the proxy", got)
	}
	if err := <-sent; err != nil {
		t.Fatalf("Inject: %v", err)
	}

	records := sink.recorded()
	if len(records) != 1 {
		t.Fatalf("recorded %d messages, want 1 — a capture that omits what the proxy sent cannot be replayed", len(records))
	}
	if records[0].Dir != ToClient || string(records[0].Raw) != "from the proxy" {
		t.Fatalf("record = %v %q, want to_client from the proxy", records[0].Dir, records[0].Raw)
	}
}

func TestInjectDecodedNeedsACodec(t *testing.T) {
	h := newHarness(t, Config{})

	err := h.session.InjectDecoded(ToServer, "anything")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("InjectDecoded without a codec = %v, want ErrInvalidConfig", err)
	}
}

func TestInjectDecodedEncodesAndSends(t *testing.T) {
	codec := &countingCodec{}
	h := newHarness(t, Config{Codec: codec})

	sent := make(chan error, 1)
	go func() { sent <- h.session.InjectDecoded(ToServer, "typed") }()

	if got := readLine(t, bufio.NewReader(h.server)); got != "typed" {
		t.Fatalf("upstream received %q, want typed", got)
	}
	if err := <-sent; err != nil {
		t.Fatalf("InjectDecoded: %v", err)
	}
	if n := codec.encodes.Load(); n != 1 {
		t.Fatalf("Encode ran %d times, want 1", n)
	}
}
