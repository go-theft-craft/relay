package minecraft

import (
	"errors"
	"fmt"
	"sync"

	protocol "github.com/go-theft-craft/minecraft-protocol"

	"github.com/go-theft-craft/relay"
)

// The states and packet identifiers this file names directly. Both protocol
// versions agree on them, which is what keeps this codec version-neutral: the
// alternative is importing a generated package and pinning the example to one
// version of the game.
const (
	stateStatus = protocol.State("status")
	stateLogin  = protocol.State("login")

	// nextStateStatus is the value a handshake carries to ask for status. It is
	// a protocol constant, not a choice this example makes.
	nextStateStatus int32 = 1

	// encryptionResponseID is the serverbound login packet that completes the
	// key exchange. Every byte after it is enciphered.
	encryptionResponseID int32 = 1
)

// ErrEncrypted reports that decoding stopped because the session enabled
// encryption. It is not a failure: the relay forwards the message as opaque
// bytes and the proxy keeps working.
//
// It keeps working because the framer stops too. This error on its own only says
// the typed half is gone; a framer still hunting for length prefixes in
// ciphertext will find them, and the session stops without ever reporting
// anything further. See Framer.ReadMessage.
var ErrEncrypted = errors.New("minecraft: the session is encrypted; relaying opaquely")

// Codec decodes frame payloads into typed packets.
//
// It holds two protocol sessions, not one. A protocol.Session fixes its inbound
// direction from the role it was built with, and a proxy reads both directions,
// so a single session could only ever decode half the traffic. The serverbound
// session is built with RoleServer because the proxy is what the client is
// talking to; the clientbound session is built with RoleClient for the mirror
// reason.
//
// Coding state is per direction for the same reason it is per session on a real
// endpoint, but the connection state the two share is not this file's to invent:
// advance asks the protocol what each packet implies and applies the answer to
// both. See advance for why both.
//
// A Codec is called from both read pumps at once, so everything it touches is
// under one mutex. A protocol.Session is documented as unsafe for concurrent
// use, and the two sessions share a state machine besides.
type Codec struct {
	mu       sync.Mutex
	toServer protocol.Session
	toClient protocol.Session

	// link carries the one thing this codec learns that the framers beside it
	// need: that the key exchange has completed. It latches once and never
	// clears, because a stream cipher does not go back.
	link *link
}

// NewCodec builds the two sessions one proxy connection needs.
//
// The relay session may be nil, for a codec built outside a relay — a test, or
// a tool that decodes bytes it got from somewhere else. When it is not nil, the
// codec publishes what it learns about encryption there, which is how the two
// framers of the same session find out that they must stop framing. See
// Encrypted.
func NewCodec(session *relay.Session, descriptor protocol.Protocol, limits protocol.Limits) (*Codec, error) {
	// The proxy is the server the client talks to, so its serverbound decoder
	// takes the server role.
	toServer, err := descriptor.NewSession(protocol.RoleServer, limits)
	if err != nil {
		return nil, fmt.Errorf("minecraft: serverbound session: %w", err)
	}

	toClient, err := descriptor.NewSession(protocol.RoleClient, limits)
	if err != nil {
		return nil, fmt.Errorf("minecraft: clientbound session: %w", err)
	}

	return &Codec{toServer: toServer, toClient: toClient, link: linkFor(session)}, nil
}

// Decode implements relay.Codec.
//
// The whole packet is carried as the decoded value rather than just its Value
// field, because Encode needs the identity back and recovering it through a
// second dispatch would be waste on the hot path. A hook that wants the typed
// body reads one field.
func (c *Codec) Decode(dir relay.Direction, raw []byte) (any, relay.Descriptor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.link.encrypted.Load() {
		return nil, relay.Descriptor{}, ErrEncrypted
	}

	session := c.sessionFor(dir)

	packet, err := session.DecodeFrame(raw)
	if err != nil {
		return nil, relay.Descriptor{}, fmt.Errorf("minecraft: decode %s: %w", dir, err)
	}

	c.advance(dir, packet)

	return packet, relay.Descriptor{ID: packet.ID, Name: packet.Name}, nil
}

// Encode implements relay.Codec.
func (c *Codec) Encode(value any) ([]byte, error) {
	packet, ok := value.(protocol.Packet)
	if !ok {
		return nil, fmt.Errorf("minecraft: cannot encode %T, want protocol.Packet", value)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Encoding uses the *other* session from the one that decoded, which is the
	// least obvious thing in this file.
	//
	// A session's role fixes both directions: the serverbound decoder was built
	// with RoleServer, so its inbound is serverbound and its outbound is
	// clientbound. Re-encoding a serverbound packet therefore needs the session
	// whose outbound is serverbound — the RoleClient one. That is not a
	// workaround, it is what a proxy is: it reads serverbound traffic as the
	// server the client dialled, and writes it as a client of the upstream.
	session := c.toClient
	if packet.Direction == protocol.DirectionClientbound {
		session = c.toServer
	}

	// Encode against the state the packet was decoded in, not the state the
	// session has reached since.
	//
	// This matters because the relay re-encodes after the whole hook chain,
	// which is necessarily after Decode advanced the state machine. A handshake
	// decodes in the handshaking state and moves both sessions to status or
	// login on the way out, so by the time a hook that edited it returns, the
	// session would refuse the very packet it just produced. Every packet
	// carries the state it belongs to, so the fix is to honour it.
	restore := session.State()
	if packet.State != restore {
		session.SetState(packet.State)
		defer session.SetState(restore)
	}

	raw, err := session.EncodeFrame(packet)
	if err != nil {
		return nil, fmt.Errorf("minecraft: encode %s: %w", packet.Name, err)
	}

	return raw, nil
}

// sessionFor picks the decoder for a direction. It must be called with mu held.
func (c *Codec) sessionFor(dir relay.Direction) protocol.Session {
	if dir == relay.ToClient {
		return c.toClient
	}

	return c.toServer
}

// advance moves both decoders in step with the connection. It must be called
// with mu held.
//
// The transitions are the protocol's to decide, not this file's. A session can
// already report what a packet implies — a handshake selects the next state,
// and depending on the version a login ends on a clientbound success or on a
// serverbound acknowledgement through a configuration state that older versions
// do not have — so asking is both shorter and correct on versions this example
// was never tested against. Hand-writing the rules here produced a codec that
// followed the handshake and then silently stopped following anything, which is
// invisible in a relay and fatal in a capture.
//
// Both decoders take every transition, because both describe one connection.
// The pair exists to track two directions of coding, not two conversations: a
// login that ends puts both peers in play, and a compression threshold applies
// to every frame on the link regardless of who sent it.
func (c *Codec) advance(dir relay.Direction, packet protocol.Packet) {
	// The serverbound encryption response completes the key exchange. It is the
	// last packet either side sends in the clear, so this one still decoded and
	// nothing after it will.
	//
	// Standing between an encrypted login as a third party means running two key
	// exchanges and holding the client's session credentials. That is a project
	// in itself and it teaches nothing about the framework seam, so the example
	// stops here on purpose. relay.Transform, which is what a consumer that did
	// want to do it would reach for, has its own worked example in
	// examples/cipher.
	//
	// Latching here rather than in the framers is what makes the two agree: this
	// is the only place in the example that can see the exchange happen, because
	// seeing it means decoding a packet, and by the next frame there is nothing
	// left to decode. The framers read the latch and stop looking for length
	// prefixes that are no longer there; see Framer.ReadMessage.
	//
	// An offline server never sends the request that provokes this response, so
	// a capture taken against one — which is what the vanilla-behaviour work
	// needs — never reaches this line at all.
	if dir == relay.ToServer && packet.State == stateLogin && packet.ID == encryptionResponseID {
		c.link.encrypted.Store(true)
	}

	transition, ok, err := c.sessionFor(dir).ProposeTransition(packet)
	if err != nil || !ok {
		// A proposal that errors means the packet does not belong in the state
		// the session is in. That is worth neither severing the session nor
		// guessing about: the relay's job is to forward, and the next packet
		// that does belong will be decoded normally.
		return
	}

	for _, session := range []protocol.Session{c.toServer, c.toClient} {
		if err := session.ValidateTransition(transition); err != nil {
			continue
		}

		session.ApplyTransition(transition)
	}
}
