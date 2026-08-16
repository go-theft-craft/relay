package relay

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"testing"
)

// lineFramer frames on a newline. It is the core's stand-in for a real
// protocol: enough structure to have boundaries, no dependency to acquire.
type lineFramer struct{}

func (lineFramer) ReadMessage(r Reader) ([]byte, error) {
	var line []byte
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			return line, nil
		}

		line = append(line, b)
	}
}

func (lineFramer) WriteMessage(w io.Writer, raw []byte) error {
	if _, err := w.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
		return err
	}

	return nil
}

func TestLineFramerRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	f := lineFramer{}

	if err := f.WriteMessage(&buf, []byte("hello")); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	got, err := f.ReadMessage(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("ReadMessage = %q, want hello", got)
	}
}

// TestNopSinkIsInert pins the one property the default sink has: every method
// is safe to call and none of them reports anything, so the session path never
// has to branch on a nil sink.
func TestNopSinkIsInert(t *testing.T) {
	var s Sink = nopSink{}

	id, err := s.OpenSession(context.Background(), SessionInfo{})
	if err != nil || id != 0 {
		t.Fatalf("OpenSession = (%d, %v), want (0, nil)", id, err)
	}

	s.Message(context.Background(), 0, MessageRecord{})
	s.RawChunk(context.Background(), 0, ToServer, nil)
	s.CloseSession(context.Background(), 0)
}

func TestDialProberReportsADeadAddress(t *testing.T) {
	// Port 1 on the loopback interface is not listening in any environment this
	// suite runs in, which is what makes it a stable "down" address.
	if err := (DialProber{}).Probe(context.Background(), "127.0.0.1:1"); err == nil {
		t.Fatal("Probe of a dead address returned nil, want an error")
	}
}
