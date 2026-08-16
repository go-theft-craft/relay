package minecraft

import (
	"errors"
	"fmt"
	"sync"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/protocols"

	"github.com/go-theft-craft/relay"
)

// The states and packet identifiers this file names directly. Both protocol
// versions agree on them, which is what keeps this codec version-neutral: the
// alternative is importing a generated package and pinning the example to one
// version of the game.
const (
	stateHandshaking = protocol.State("handshaking")
	stateStatus      = protocol.State("status")
	stateLogin       = protocol.State("login")

	// nextStateStatus and nextStateLogin are the values a handshake carries.
	// They are protocol constants, not choices this example makes.
	nextStateStatus int32 = 1
	nextStateLogin  int32 = 2

	// encryptionResponseID is the serverbound login packet that completes the
	// key exchange. Every byte after it is enciphered.
	encryptionResponseID int32 = 1
)

// ErrEncrypted reports that decoding stopped because the session enabled
// encryption. It is not a failure: the relay forwards the message as opaque
// bytes and the proxy keeps working.
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
// Connection state is per direction for the same reason it is per session on a
// real endpoint: a handshake moves the serverbound decoder into status or
// login, and the clientbound decoder follows only when the protocol says it
// should. Advancing both on one packet without thinking about it is the bug
// this structure exists to prevent — here they do move together, and the state
// machine below says why in each case.
//
// A Codec is called from both read pumps at once, so everything it touches is
// under one mutex. A protocol.Session is documented as unsafe for concurrent
// use, and the two sessions share a state machine besides.
type Codec struct {
	mu       sync.Mutex
	toServer protocol.Session
	toClient protocol.Session

	// encrypted latches once the key exchange completes. It never clears,
	// because a stream cipher does not go back.
	encrypted bool
}

// NewCodec builds the two sessions one proxy connection needs.
func NewCodec(descriptor protocol.Protocol, limits protocol.Limits) (*Codec, error) {
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

	return &Codec{toServer: toServer, toClient: toClient}, nil
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

	if c.encrypted {
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

// advance is the state machine, written out rather than hidden in a helper
// because a reader tracing a bug here will want to see every transition at
// once. It must be called with mu held.
func (c *Codec) advance(dir relay.Direction, packet protocol.Packet) {
	switch {
	// The handshake is the only packet in the handshaking state, and it names
	// where the connection goes next. Both decoders move together here, because
	// the server's very first reply is already in the new state: there is no
	// clientbound packet in the handshaking state for the client decoder to
	// still be waiting on.
	case dir == relay.ToServer && packet.State == stateHandshaking:
		fields, ok := protocols.ReadHandshake(packet)
		if !ok {
			return
		}

		switch fields.NextState {
		case nextStateStatus:
			c.toServer.SetState(stateStatus)
			c.toClient.SetState(stateStatus)
		case nextStateLogin:
			c.toServer.SetState(stateLogin)
			c.toClient.SetState(stateLogin)
		}

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
	case dir == relay.ToServer && packet.State == stateLogin && packet.ID == encryptionResponseID:
		c.encrypted = true
	}
}
