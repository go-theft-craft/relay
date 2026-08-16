// Package cipher is the worked example for relay.Transform: a proxy that
// relays in the clear, then switches both of its links onto an AES-CTR
// keystream partway through the session.
//
// The protocol is newline-delimited lines and the key is a constant, because
// neither is the point. The point is the shape of the swap, which is the same
// shape every real mid-stream cipher needs: a trigger message that crosses in
// the clear, a swap applied to the link the hook is running on, and a second
// swap applied to the other link, whose read pump is parked inside a socket
// read at that moment.
//
// A proxy stands on two links, not one, and each is enciphered independently.
// That is why there are four keystreams here rather than two: an AES-CTR
// keystream is position-dependent, so the client-side and upstream-side streams
// cannot share one. Sharing any of them produces a proxy that works for exactly
// one message and then desynchronises.
package cipher

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"

	"github.com/go-theft-craft/relay"
)

// Trigger is the message that marks the boundary. Both endpoints have to agree
// on which message is the last unenciphered one, and the only message both can
// name is the trigger itself.
const Trigger = "START-CIPHER"

// key and the two initialisation vectors are constants because this example is
// about the swap, not about key exchange. A real consumer negotiates all three.
var (
	key = []byte("0123456789abcdef0123456789abcdef")

	// Each direction of a link gets its own keystream, so an endpoint's send
	// stream and its peer's receive stream stay in step while the two directions
	// stay independent.
	ivToServer = []byte("relay-to-server!")
	ivToClient = []byte("relay-to-client!")
)

// Role is which end of a link an endpoint is.
type Role uint8

const (
	// RoleClient is the end that initiates: it sends with the to-server
	// keystream and receives with the to-client one.
	RoleClient Role = iota
	// RoleServer is the mirror.
	RoleServer
)

// Streams returns the two keystreams one end of one link needs. Every call
// returns fresh streams, because two links must never share position.
func Streams(role Role) (send, recv cipher.Stream, err error) {
	sendIV, recvIV := ivToServer, ivToClient
	if role == RoleServer {
		sendIV, recvIV = ivToClient, ivToServer
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, fmt.Errorf("cipher: build the block: %w", err)
	}

	return cipher.NewCTR(block, sendIV), cipher.NewCTR(block, recvIV), nil
}

// transformFor builds the Transform one end of one link installs.
//
// Read transforms in place, which suits XORKeyStream exactly — it takes the
// same slice as source and destination. Write allocates, because the caller
// still owns the message it passed and a hook or a sink may hold a view of it.
func transformFor(role Role) (relay.Transform, error) {
	send, recv, err := Streams(role)
	if err != nil {
		return relay.Transform{}, err
	}

	return relay.Transform{
		Read: func(p []byte) { recv.XORKeyStream(p, p) },
		Write: func(p []byte) []byte {
			out := make([]byte, len(p))
			send.XORKeyStream(out, p)

			return out
		},
	}, nil
}

// LineFramer frames on a newline. The protocol is deliberately trivial so that
// everything this example demonstrates is about the swap.
type LineFramer struct{}

// ReadMessage implements relay.Framer.
func (LineFramer) ReadMessage(r relay.Reader) ([]byte, error) {
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

// WriteMessage implements relay.Framer.
func (LineFramer) WriteMessage(w io.Writer, raw []byte) error {
	_, err := w.Write(append(append([]byte(nil), raw...), '\n'))

	return err
}

// Hook returns the hook that installs the keystreams when the trigger crosses.
func Hook() relay.Hook { return relay.HookFunc(onTrigger) }

// onTrigger installs the keystreams when it sees the negotiation message.
//
// The ordering is the whole lesson. The trigger is forwarded first, and
// forwarded in the clear: swapping first would encipher it, and the peer — still
// reading plaintext — could not read it. Forwarding through Inject rather than
// by returning Forward is what makes that ordering explicit, because a relayed
// message is written after the hook returns, which is too late.
//
// The ToServer swap lands on the upstream link, whose read pump is parked
// inside a socket read at that moment. That is safe, and it is the whole reason
// the conduit transforms bytes as it hands them out rather than as it buffers
// them; see relay.Conduit.
func onTrigger(_ context.Context, s *relay.Session, m *relay.Message) (relay.Action, error) {
	if string(m.Raw) != Trigger {
		return relay.Forward, nil
	}

	if err := s.Inject(relay.ToServer, m.Raw); err != nil {
		return relay.Drop, fmt.Errorf("cipher: forward the trigger: %w", err)
	}

	// The proxy is the server end of the client link and the client end of the
	// upstream link, which is what keeps each keystream paired with the endpoint
	// on the other side of it.
	//
	// The client link is swapped first because it is the only one that can be
	// holding unread bytes here: this hook runs on the pump that reads it, and a
	// client that sent past the boundary has already buffered them. Doing it
	// first means that refusal lands before the upstream link has been changed,
	// so a session that fails fails with both links still agreeing.
	clientSide, err := transformFor(RoleServer)
	if err != nil {
		return relay.Drop, err
	}
	if err := s.Swap(relay.ToClient, clientSide); err != nil {
		return relay.Drop, fmt.Errorf("cipher: swap the client link: %w", err)
	}

	upstreamSide, err := transformFor(RoleClient)
	if err != nil {
		return relay.Drop, err
	}
	if err := s.Swap(relay.ToServer, upstreamSide); err != nil {
		return relay.Drop, fmt.Errorf("cipher: swap the upstream link: %w", err)
	}

	// The trigger has already been sent, so the relay must not send it again.
	return relay.Drop, nil
}

// New builds a proxy that listens on one port and enciphers from the trigger
// onwards. Port 0 binds an ephemeral port, which Proxy.Addrs reports back.
func New(port int, upstream string) (*relay.Proxy, error) {
	return relay.New(relay.Config{
		Framer: LineFramer{},
		Ports:  []relay.PortConfig{{Port: port, Upstreams: []relay.Upstream{{Addr: upstream}}}},
		Hooks:  []relay.Hook{Hook()},
	})
}
