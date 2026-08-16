package minecraft_test

import (
	"bytes"
	"errors"
	"testing"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/protocols"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
)

func newCodec(t *testing.T) *minecraft.Codec {
	t.Helper()

	c, err := minecraft.NewCodec(protocols.Default(), testLimits(t))
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	return c
}

// peerSession builds a session standing in for a real endpoint, so the wire
// bytes a test feeds the codec were produced the way a peer would produce them
// rather than by the codec itself.
func peerSession(t *testing.T, role protocol.Role) protocol.Session {
	t.Helper()

	session, err := protocols.Default().NewSession(role, testLimits(t))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	return session
}

func encodeWith(t *testing.T, session protocol.Session, packet protocol.Packet) []byte {
	t.Helper()

	raw, err := session.EncodeFrame(packet)
	if err != nil {
		t.Fatalf("EncodeFrame(%s): %v", packet.Name, err)
	}

	return raw
}

func handshakeBytes(t *testing.T, nextState int32) []byte {
	t.Helper()

	packet, err := protocols.Handshake(protocols.Default(), "example.test", 25565, nextState)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	return encodeWith(t, peerSession(t, protocol.RoleClient), packet)
}

func TestCodecDecodesAHandshake(t *testing.T) {
	c := newCodec(t)

	value, desc, err := c.Decode(relay.ToServer, handshakeBytes(t, 1))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	packet, ok := value.(protocol.Packet)
	if !ok {
		t.Fatalf("Decode returned %T, want protocol.Packet — Encode needs the identity back", value)
	}

	// The handshake's ID is legitimately zero, so the descriptor's name is what
	// proves a codec ran at all. Asserting a non-zero ID here would be asserting
	// something about this packet rather than about the codec.
	if desc.Name == "" {
		t.Fatalf("descriptor = %+v, want a name", desc)
	}
	if desc.ID != packet.ID || desc.Name != packet.Name {
		t.Fatalf("descriptor %+v does not match the packet it came from (%d, %q)", desc, packet.ID, packet.Name)
	}

	fields, ok := protocols.ReadHandshake(packet)
	if !ok {
		t.Fatal("the decoded packet is not readable as a handshake")
	}
	if fields.ServerHost != "example.test" || fields.ServerPort != 25565 || fields.NextState != 1 {
		t.Fatalf("handshake fields = %+v, want the ones it was built with", fields)
	}
}

// TestCodecRoundTripsByteIdentically is what makes a hook that inspects without
// editing invisible on the wire.
func TestCodecRoundTripsByteIdentically(t *testing.T) {
	c := newCodec(t)

	want := handshakeBytes(t, 1)

	value, _, err := c.Decode(relay.ToServer, want)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	got, err := c.Encode(value)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("round trip changed the bytes:\n got %x\nwant %x", got, want)
	}
}

// TestCodecAdvancesBothDecoders covers the per-direction state machine.
//
// The plan this example was built from expected the handshake to move only the
// serverbound decoder. It moves both, and it has to: the server's very next
// packet is a clientbound status response, so a clientbound decoder still
// sitting in the handshaking state could not decode the reply to the very
// handshake that was just read. There is no clientbound packet in the
// handshaking state for it to be waiting on.
func TestCodecAdvancesBothDecoders(t *testing.T) {
	c := newCodec(t)

	if _, _, err := c.Decode(relay.ToServer, handshakeBytes(t, 1)); err != nil {
		t.Fatalf("Decode handshake: %v", err)
	}

	// A real server answers in the status state; the clientbound decoder has to
	// be there already.
	response, err := protocols.StatusResponse(protocols.Default(), `{"description":"stub"}`)
	if err != nil {
		t.Fatalf("StatusResponse: %v", err)
	}

	raw := encodeWith(t, statusServerSession(t), response)

	value, desc, err := c.Decode(relay.ToClient, raw)
	if err != nil {
		t.Fatalf("Decode the status response: %v — the clientbound decoder did not follow the handshake", err)
	}
	if desc.Name == "" {
		t.Fatalf("descriptor = %+v, want a name", desc)
	}

	packet, ok := value.(protocol.Packet)
	if !ok {
		t.Fatalf("Decode returned %T, want protocol.Packet", value)
	}
	if packet.Direction != protocol.DirectionClientbound {
		t.Fatalf("packet direction = %v, want clientbound", packet.Direction)
	}
}

// statusServerSession is a server-role session already moved into the status
// state, which is what a real server's session looks like when it answers.
func statusServerSession(t *testing.T) protocol.Session {
	t.Helper()

	session := peerSession(t, protocol.RoleServer)
	session.SetState(protocol.State("status"))

	return session
}

// TestCodecReportsAnUndecodableBody proves a malformed body is an error rather
// than a half-filled packet. The relay treats a decode error as "forward
// opaquely", which is only safe if it can tell the two apart.
func TestCodecReportsAnUndecodableBody(t *testing.T) {
	c := newCodec(t)

	// Packet ID 0 in the handshaking state is the handshake, and it is followed
	// by fields that are not there.
	_, desc, err := c.Decode(relay.ToServer, []byte{0x00})
	if err == nil {
		t.Fatal("a truncated handshake was accepted")
	}
	if desc != (relay.Descriptor{}) {
		t.Fatalf("descriptor = %+v, want the zero value on a decode failure", desc)
	}
}

// TestCodecForwardsAnUnknownPacketID records a deliberate non-failure. An ID
// with no entry in the current state decodes to an unknown packet rather than
// an error, which is the behaviour a proxy wants: a version skew between the
// example and the server should degrade to passthrough, not sever the session.
func TestCodecForwardsAnUnknownPacketID(t *testing.T) {
	c := newCodec(t)

	value, _, err := c.Decode(relay.ToServer, []byte{0x7f, 0x00, 0x00})
	if err != nil {
		t.Fatalf("Decode an unknown packet ID: %v", err)
	}

	packet, ok := value.(protocol.Packet)
	if !ok {
		t.Fatalf("Decode returned %T, want protocol.Packet", value)
	}
	if packet.ID != 0x7f {
		t.Fatalf("packet ID = %d, want 0x7f preserved", packet.ID)
	}
}

// TestCodecStopsAtEncryption is the deliberate limit. Once the key exchange
// completes the example decodes nothing further and the relay falls back to
// opaque passthrough.
func TestCodecStopsAtEncryption(t *testing.T) {
	c := newCodec(t)

	if _, _, err := c.Decode(relay.ToServer, handshakeBytes(t, 2)); err != nil {
		t.Fatalf("Decode handshake: %v", err)
	}

	raw := encryptionResponseBytes(t)

	// The response itself is the last packet in the clear, so it still decodes.
	if _, _, err := c.Decode(relay.ToServer, raw); err != nil {
		t.Fatalf("Decode the encryption response: %v", err)
	}

	// Everything after it does not.
	for _, dir := range []relay.Direction{relay.ToServer, relay.ToClient} {
		if _, _, err := c.Decode(dir, raw); !errors.Is(err, minecraft.ErrEncrypted) {
			t.Fatalf("Decode %s after the key exchange = %v, want ErrEncrypted", dir, err)
		}
	}
}

// encryptionResponseBytes builds the serverbound packet that completes the key
// exchange. The contents do not matter; its identity does.
func encryptionResponseBytes(t *testing.T) []byte {
	t.Helper()

	descriptor := protocols.Default()

	factory, ok := descriptor.(protocol.PacketFactory)
	if !ok {
		t.Skip("this protocol cannot build packet values")
	}

	value, known := factory.NewPacketValue(protocol.State("login"), protocol.DirectionServerbound, 1)
	if !known {
		t.Skip("this protocol has no login packet 1")
	}

	session := peerSession(t, protocol.RoleClient)
	session.SetState(protocol.State("login"))

	return encodeWith(t, session, protocol.Packet{
		State:     protocol.State("login"),
		Direction: protocol.DirectionServerbound,
		ID:        1,
		Value:     value,
	})
}
