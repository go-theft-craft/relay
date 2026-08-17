package minecraft_test

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/generated/java/v1_8"
	"github.com/go-theft-craft/minecraft-protocol/protocols"

	"github.com/go-theft-craft/relay"
	"github.com/go-theft-craft/relay/examples/minecraft"
)

// The packet identities these tests name directly. Login success and set
// compression carry the same identity in both protocols; the rest are protocol
// 47's and are only used by the tests pinned to it.
const (
	loginSuccessID   int32 = 0x02
	setCompressionID int32 = 0x03
	v1_8KeepAliveID  int32 = 0x00
	v1_8PositionID   int32 = 0x08
	// v1_8SetCompressionID is set compression in the play state, where a
	// threshold that changes after the login arrives. It is a different branch
	// of the protocol's transition table from the login packet above, and it is
	// 47's alone: the default protocol has no play-state set compression.
	v1_8SetCompressionID int32 = 0x46
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

// serverPeer is a server-role session that commits its own transitions, which
// is what makes it a stand-in for a real endpoint rather than an encoder.
//
// A real server moves state after it writes login success and starts
// compressing after it writes set compression. A test peer that skips those
// steps produces bytes no server would ever send, and a codec that only ever
// sees those bytes looks correct while being unable to read the real thing.
type serverPeer struct {
	t       *testing.T
	session protocol.Session
}

func newServerPeer(t *testing.T, descriptor protocol.Protocol, state protocol.State) *serverPeer {
	t.Helper()

	session, err := descriptor.NewSession(protocol.RoleServer, testLimits(t))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session.SetState(state)

	return &serverPeer{t: t, session: session}
}

// send encodes one packet and then commits whatever transition it implies, in
// that order, because the packet itself crosses under the old settings.
func (p *serverPeer) send(packet protocol.Packet) []byte {
	p.t.Helper()

	raw := encodeWith(p.t, p.session, packet)

	transition, ok, err := p.session.ProposeTransition(packet)
	if err != nil {
		p.t.Fatalf("ProposeTransition(%s): %v", packet.Name, err)
	}
	if ok {
		p.session.ApplyTransition(transition)
	}

	return raw
}

// clientboundPacket builds a packet by identity, leaving its fields zero. These
// tests care where a packet moves the session, not what it carries.
func clientboundPacket(t *testing.T, descriptor protocol.Protocol, state protocol.State, id int32) protocol.Packet {
	t.Helper()

	factory, ok := descriptor.(protocol.PacketFactory)
	if !ok {
		t.Skip("this protocol cannot build packet values")
	}

	value, known := factory.NewPacketValue(state, protocol.DirectionClientbound, id)
	if !known {
		t.Skipf("this protocol has no clientbound packet %d in state %q", id, state)
	}

	return protocol.Packet{
		State:     state,
		Direction: protocol.DirectionClientbound,
		ID:        id,
		Value:     value,
	}
}

// TestCodecFollowsTheLoginIntoPlay is the difference between a relay and an
// oracle.
//
// A codec that stays in the login state is still a working relay — every frame
// is forwarded — but it decodes nothing after the login, so a capture taken
// through it holds a whole session of opaque bytes with no packet identities to
// extract a trace from.
//
// It runs on protocol 47 rather than the default, because the packet that ends
// a login is version-specific: 47 moves to play on the clientbound success,
// while 775 waits for a serverbound acknowledgement and a configuration state
// that 47 does not have. That difference is the argument against hand-writing
// these transitions at all, and 47 is the protocol M9.1 captures.
func TestCodecFollowsTheLoginIntoPlay(t *testing.T) {
	descriptor := v1_8.Protocol()

	c, err := minecraft.NewCodec(descriptor, testLimits(t))
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	handshake, err := protocols.Handshake(descriptor, "example.test", 25565, 2)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	client, err := descriptor.NewSession(protocol.RoleClient, testLimits(t))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, _, err := c.Decode(relay.ToServer, encodeWith(t, client, handshake)); err != nil {
		t.Fatalf("Decode handshake: %v", err)
	}

	server := newServerPeer(t, descriptor, protocol.State("login"))

	success := clientboundPacket(t, descriptor, protocol.State("login"), loginSuccessID)
	if _, _, err := c.Decode(relay.ToClient, server.send(success)); err != nil {
		t.Fatalf("Decode login success: %v", err)
	}

	value, desc, err := c.Decode(relay.ToClient, server.send(clientboundPacket(t, descriptor, protocol.State("play"), v1_8KeepAliveID)))
	if err != nil {
		t.Fatalf("Decode a play packet after login success: %v — the codec is still in the login state", err)
	}
	if desc.Name == "" {
		t.Fatalf("descriptor = %+v, want a name", desc)
	}

	packet, ok := value.(protocol.Packet)
	if !ok {
		t.Fatalf("Decode returned %T, want protocol.Packet", value)
	}
	if packet.State != protocol.State("play") {
		t.Fatalf("packet state = %q, want play", packet.State)
	}
}

// TestCodecFollowsSetCompression covers the other half of the same gap, and
// this half is version-neutral: both protocols set compression with the same
// clientbound login packet.
//
// A vanilla server compresses at a threshold of 256 by default, and the
// envelope sits inside the frame the framer hands over. A codec that has not
// applied the control reads the compressed body as a packet ID, from the first
// packet after the login rather than from any visible boundary.
//
// It covers the threshold arriving once. The test below covers it changing,
// which is 47's alone and so cannot live here.
func TestCodecFollowsSetCompression(t *testing.T) {
	descriptor := protocols.Default()
	c := newCodec(t)

	if _, _, err := c.Decode(relay.ToServer, handshakeBytes(t, 2)); err != nil {
		t.Fatalf("Decode handshake: %v", err)
	}

	server := newServerPeer(t, descriptor, protocol.State("login"))

	// A threshold of zero compresses everything, so the next frame is
	// unambiguously enveloped. Vanilla's 256 would leave small packets
	// uncompressed and let an unapplied control pass by luck.
	compress := clientboundPacket(t, descriptor, protocol.State("login"), setCompressionID)
	setThreshold(t, compress, 0)

	if _, _, err := c.Decode(relay.ToClient, server.send(compress)); err != nil {
		t.Fatalf("Decode set compression: %v", err)
	}

	// Vanilla's order: set compression, then success, which is the first packet
	// to cross under the new setting.
	success := clientboundPacket(t, descriptor, protocol.State("login"), loginSuccessID)
	if _, desc, err := c.Decode(relay.ToClient, server.send(success)); err != nil {
		t.Fatalf("Decode login success under compression: %v — the codec did not apply the control", err)
	} else if desc.Name == "" {
		t.Fatalf("descriptor = %+v, want a name", desc)
	}
}

// TestCodecFollowsAThresholdThatChanges is the half of compression that had
// never run here.
//
// Compression is the one negotiated setting in this protocol family that is
// reversible. A threshold arrives again in play, and a negative one turns
// compression off behind the very frame that carries it — which travels
// compressed, the mirror image of the frame that turns compression on and
// travels in the clear. Every conclusion in this tree about the disable path
// rests on that ordering, and reading the code is not running it.
//
// It is pinned to protocol 47 because the play-state set compression packet is
// 47's; the version-neutral half stays in the test above.
func TestCodecFollowsAThresholdThatChanges(t *testing.T) {
	descriptor := v1_8.Protocol()

	c, err := minecraft.NewCodec(descriptor, testLimits(t))
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}

	client, err := descriptor.NewSession(protocol.RoleClient, testLimits(t))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	handshake, err := protocols.Handshake(descriptor, "example.test", 25565, 2)
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}

	if _, _, err := c.Decode(relay.ToServer, encodeWith(t, client, handshake)); err != nil {
		t.Fatalf("Decode handshake: %v", err)
	}

	server := newServerPeer(t, descriptor, protocol.State("login"))

	// Enable at zero, so every frame after this one is genuinely deflated
	// rather than passing by luck.
	compress := clientboundPacket(t, descriptor, protocol.State("login"), setCompressionID)
	setThreshold(t, compress, 0)

	if _, _, err := c.Decode(relay.ToClient, server.send(compress)); err != nil {
		t.Fatalf("Decode set compression: %v", err)
	}

	// Login success is the first frame under the new envelope, and it is what
	// puts both halves in play, where every later change arrives.
	success := clientboundPacket(t, descriptor, protocol.State("login"), loginSuccessID)
	if _, _, err := c.Decode(relay.ToClient, server.send(success)); err != nil {
		t.Fatalf("Decode login success under compression: %v — the codec did not apply the control", err)
	}

	assertAPlayPacketDecodesAsItself(t, c, server, "enabled at threshold 0")

	for _, change := range []struct {
		name      string
		threshold int32
	}{
		{"re-enabled at vanilla's default", 256},
		{"disabled", -1},
	} {
		next := clientboundPacket(t, descriptor, protocol.State("play"), v1_8SetCompressionID)
		setThreshold(t, next, change.threshold)

		if _, _, err := c.Decode(relay.ToClient, server.send(next)); err != nil {
			t.Fatalf("Decode set compression, %s: %v", change.name, err)
		}

		assertAPlayPacketDecodesAsItself(t, c, server, change.name)
	}
}

// assertAPlayPacketDecodesAsItself sends one play packet under whatever
// envelope is currently in force and insists the codec read that packet rather
// than some other one.
//
// The position is the probe because it has a non-zero ID and a body, so a
// misread moves the identity as well as the fields. A stale threshold makes
// this loud — the codec either refuses an uncompressed body it expected to be
// deflated, or tries to inflate a raw one — and both are caught by the error
// check alone.
//
// The identity check is here for the quiet failure, which is the one the live
// procedure actually found: a codec that believes compression is off reads the
// zero-length data prefix of an uncompressed frame as a packet ID and returns a
// perfectly valid packet of the wrong identity, with no error at all. Nothing
// in this test provokes that, and asserting only "no error" is what would let
// it through if something later did.
func assertAPlayPacketDecodesAsItself(t *testing.T, c *minecraft.Codec, server *serverPeer, stage string) {
	t.Helper()

	const x = 8.5

	value, desc, err := c.Decode(relay.ToClient, server.send(protocol.Packet{
		State:     protocol.State("play"),
		Direction: protocol.DirectionClientbound,
		ID:        v1_8PositionID,
		Value:     &v1_8.PlayClientboundPosition{X: x, Y: 65, Z: -4.5},
	}))
	if err != nil {
		t.Fatalf("%s: decode a play packet: %v — the codec is reading the wrong envelope", stage, err)
	}
	if desc.ID != v1_8PositionID {
		t.Fatalf("%s: decoded packet %d, want %d — the frame was read under the wrong envelope", stage, desc.ID, v1_8PositionID)
	}

	packet, ok := value.(protocol.Packet)
	if !ok {
		t.Fatalf("%s: Decode returned %T, want protocol.Packet", stage, value)
	}

	position, ok := packet.Value.(*v1_8.PlayClientboundPosition)
	if !ok {
		t.Fatalf("%s: decoded a %T, want a position", stage, packet.Value)
	}
	if position.X != x {
		t.Fatalf("%s: position X = %v, want %v", stage, position.X, x)
	}
}

// setThreshold fills the one field these tests care about, by reflection,
// because the field lives on a generated type this version-neutral example does
// not otherwise import.
func setThreshold(t *testing.T, packet protocol.Packet, threshold int32) {
	t.Helper()

	field := reflect.ValueOf(packet.Value).Elem().FieldByName("Threshold")
	if !field.IsValid() || !field.CanSet() {
		t.Skipf("packet %d in state %q has no settable Threshold field", packet.ID, packet.State)
	}

	field.SetInt(int64(threshold))
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
