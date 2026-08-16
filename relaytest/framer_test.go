package relaytest_test

import (
	"io"
	"testing"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/relaytest"
)

type lineFramer struct{}

func (lineFramer) ReadMessage(r relay.Reader) ([]byte, error) {
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
	_, err := w.Write(append(append([]byte(nil), raw...), '\n'))

	return err
}

func TestFramerContractAcceptsACorrectFramer(t *testing.T) {
	relaytest.FramerContract(t, func() relay.Framer { return lineFramer{} }, [][]byte{
		[]byte("a"),
		[]byte("hello world"),
	})
}

// shortWriteFramer reports success after writing one byte, which is the failure
// the harness exists to catch.
type shortWriteFramer struct{ lineFramer }

func (shortWriteFramer) WriteMessage(w io.Writer, raw []byte) error {
	_, _ = w.Write(raw[:1])

	return nil
}

func TestFramerContractRejectsAShortWrite(t *testing.T) {
	fake := &testing.T{}
	relaytest.FramerContract(fake, func() relay.Framer { return shortWriteFramer{} }, [][]byte{
		[]byte("hello"),
	})
	if !fake.Failed() {
		t.Fatal("FramerContract passed a framer that writes one byte and claims success")
	}
}

// sharedBufferFramer hands back a buffer it reuses on the next read, which is
// the ownership violation bufferIsOwned exists to catch.
type sharedBufferFramer struct {
	lineFramer
	buf []byte
}

func (f *sharedBufferFramer) ReadMessage(r relay.Reader) ([]byte, error) {
	f.buf = f.buf[:0]
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '\n' {
			return f.buf, nil
		}

		f.buf = append(f.buf, b)
	}
}

func TestFramerContractRejectsAReusedBuffer(t *testing.T) {
	fake := &testing.T{}
	relaytest.FramerContract(fake, func() relay.Framer {
		return &sharedBufferFramer{buf: make([]byte, 0, 64)}
	}, [][]byte{
		[]byte("hello"),
	})
	if !fake.Failed() {
		t.Fatal("FramerContract passed a framer that reuses the buffer it hands back")
	}
}
