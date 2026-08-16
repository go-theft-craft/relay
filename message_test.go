package relay

import (
	"bytes"
	"testing"
)

func TestDirectionString(t *testing.T) {
	if got := ToServer.String(); got != "to_server" {
		t.Fatalf("ToServer.String() = %q, want to_server", got)
	}
	if got := ToClient.String(); got != "to_client" {
		t.Fatalf("ToClient.String() = %q, want to_client", got)
	}
}

func TestDirectionOpposite(t *testing.T) {
	if ToServer.Opposite() != ToClient {
		t.Fatal("ToServer.Opposite() is not ToClient")
	}
	if ToClient.Opposite() != ToServer {
		t.Fatal("ToClient.Opposite() is not ToServer")
	}
}

func TestMessageTracksMutation(t *testing.T) {
	m := &Message{Dir: ToServer, Raw: []byte("abc")}

	if m.RawChanged() || m.DecodedChanged() {
		t.Fatal("a fresh message reports itself modified")
	}

	m.SetRaw([]byte("defg"))
	if !m.RawChanged() {
		t.Fatal("SetRaw did not mark the message modified")
	}
	if !bytes.Equal(m.Raw, []byte("defg")) {
		t.Fatalf("Raw = %q, want defg", m.Raw)
	}
	if m.DecodedChanged() {
		t.Fatal("SetRaw marked the decoded value modified")
	}

	m.SetDecoded("value")
	if !m.DecodedChanged() {
		t.Fatal("SetDecoded did not mark the decoded value modified")
	}
	if m.Decoded != "value" {
		t.Fatalf("Decoded = %v, want value", m.Decoded)
	}
}

func TestMessageResetClearsEverything(t *testing.T) {
	m := &Message{Dir: ToClient, Raw: []byte("abc"), Desc: Descriptor{ID: 7, Name: "n"}}
	m.SetDecoded("v")

	m.reset()

	if m.Raw != nil || m.Decoded != nil || m.Desc != (Descriptor{}) {
		t.Fatalf("reset left state behind: %+v", m)
	}
	if m.RawChanged() || m.DecodedChanged() {
		t.Fatal("reset left the modification flags set")
	}
}
