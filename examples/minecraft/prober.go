package minecraft

import (
	"context"
	"fmt"
	"net"
	"time"

	protocol "github.com/go-theft-craft/minecraft-protocol"
	"github.com/go-theft-craft/minecraft-protocol/protocols"
)

// statusRequestID is the serverbound status request. It is empty and its ID is
// zero in every protocol version, which is why this needs no per-version
// exchange the way a login would.
const statusRequestID int32 = 0

// Prober reports an upstream healthy only when it answers a status request.
//
// This is the second place, after Framer, where the example shows the seam is
// real. The core's default probe is a TCP dial, which can only tell that
// something holds the port open — a server wedged before it answers protocol
// traffic passes it. This one gets an answer or reports nothing.
type Prober struct {
	// Descriptor is the protocol version to speak. A zero value uses the
	// library's default.
	Descriptor protocol.Protocol
	Timeout    time.Duration
}

// Probe implements relay.Prober.
//
// The whole exchange respects ctx, because this runs on the accept path: a
// probe that ignores its deadline turns one wedged upstream into a stall on
// every connection. The connection is closed on every path out.
func (p Prober) Probe(ctx context.Context, addr string) error {
	descriptor := p.Descriptor
	if descriptor == nil {
		descriptor = protocols.Default()
	}

	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	host, port, err := splitHostPort(addr)
	if err != nil {
		return err
	}

	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("minecraft: dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	return exchangeStatus(ctx, descriptor, conn, host, port)
}

// exchangeStatus runs the handshake-then-status flow and reports whether the
// server answered.
func exchangeStatus(
	ctx context.Context,
	descriptor protocol.Protocol,
	conn net.Conn,
	host string,
	port uint16,
) error {
	limits, err := protocol.NewLimits()
	if err != nil {
		return fmt.Errorf("minecraft: limits: %w", err)
	}

	// A probe is an ordinary client, so it takes the client role.
	session, err := descriptor.NewSession(protocol.RoleClient, limits)
	if err != nil {
		return fmt.Errorf("minecraft: session: %w", err)
	}

	stream, err := protocol.NewStream(session, protocol.Transport{
		Reader: conn,
		Writer: conn,
		// Interrupt is what lets a cancelled context unblock a parked read.
		Interrupt: conn.Close,
	})
	if err != nil {
		return fmt.Errorf("minecraft: stream: %w", err)
	}
	defer func() { _ = stream.Close() }()

	if err := stream.Start(ctx); err != nil {
		return fmt.Errorf("minecraft: start stream: %w", err)
	}

	handshake, err := protocols.Handshake(descriptor, host, port, nextStateStatus)
	if err != nil {
		return fmt.Errorf("minecraft: build handshake: %w", err)
	}
	if err := stream.Write(ctx, handshake); err != nil {
		return fmt.Errorf("minecraft: write handshake: %w", err)
	}

	request, err := statusRequest(descriptor)
	if err != nil {
		return err
	}
	if err := stream.Write(ctx, request); err != nil {
		return fmt.Errorf("minecraft: write status request: %w", err)
	}

	if _, err := stream.Read(ctx); err != nil {
		return fmt.Errorf("minecraft: read status response: %w", err)
	}

	return nil
}

// statusRequest builds the empty packet that asks a server to describe itself.
func statusRequest(descriptor protocol.Protocol) (protocol.Packet, error) {
	factory, ok := descriptor.(protocol.PacketFactory)
	if !ok {
		return protocol.Packet{}, fmt.Errorf("minecraft: protocol %s cannot build packet values", descriptor.ID())
	}

	value, known := factory.NewPacketValue(stateStatus, protocol.DirectionServerbound, statusRequestID)
	if !known {
		return protocol.Packet{}, fmt.Errorf("minecraft: protocol %s has no status request", descriptor.ID())
	}

	return protocol.Packet{
		State:     stateStatus,
		Direction: protocol.DirectionServerbound,
		ID:        statusRequestID,
		Value:     value,
	}, nil
}

// splitHostPort separates the address the handshake has to carry. The server
// address a client sends is part of the protocol, not decoration: virtual hosts
// route on it.
func splitHostPort(addr string) (host string, port uint16, err error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("minecraft: upstream address %q must be host:port: %w", addr, err)
	}

	number, err := net.LookupPort("tcp", portText)
	if err != nil {
		return "", 0, fmt.Errorf("minecraft: upstream port in %q: %w", addr, err)
	}

	return host, uint16(number), nil
}
