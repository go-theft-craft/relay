# relay

```
go get github.com/go-theft-craft/relay
```

A TCP proxy framework that does not know what protocol it is proxying.

Supply message boundaries through a `Framer` and you have a working proxy: it
accepts connections on many ports, resolves an upstream per connection through
a lazily-probed health cache and a pluggable selector, and relays framed
messages in both directions. Supplying a `Codec` additionally makes decoded
packets visible to hooks and sinks, and supplying a `Prober` replaces the
default TCP-dial health check with one that speaks the protocol.

This module imports nothing outside the standard library. Its `go.mod` has no
`require` line, and CI fails if one appears — which is what makes the claim
checkable rather than aspirational.

## The smallest working proxy

```go
package main

import (
	"context"
	"io"
	"log"

	"github.com/go-theft-craft/relay"
)

// lines frames on a newline. Everything else in this file is configuration.
type lines struct{}

func (lines) ReadMessage(r relay.Reader) ([]byte, error) {
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

func (lines) WriteMessage(w io.Writer, raw []byte) error {
	_, err := w.Write(append(append([]byte(nil), raw...), '\n'))

	return err
}

func main() {
	proxy, err := relay.New(relay.Config{
		Framer: lines{},
		Ports: []relay.PortConfig{{
			Port:      8080,
			Upstreams: []relay.Upstream{{Addr: "127.0.0.1:9000"}},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(proxy.Run(context.Background()))
}
```

## The seam

`Framer` is required. Everything else is optional, and each one buys one thing.

| Interface | What adding it buys |
| --- | --- |
| `Framer` | message boundaries — the one thing the core cannot guess |
| `Codec` | typed packets in hooks and sinks, and re-encoding after an edit |
| `Prober` | health that means the server answered, not that a port is open |
| `Sink` | a record of what crossed the wire, including what the proxy injected |
| `Config.CaptureRaw` | every raw byte of the client connection, below framing |
| `Hook` | inspect, rewrite, drop, or inject a message |
| `PreFrame` | answer or divert a connection before any framing happens |

`Codec` comes in two forms and you set exactly one. `Config.Codec` is a single
instance shared by every session, which suits a codec that is a pure function of
its bytes. `Config.NewCodec` builds one per session, which is what a protocol
with connection states needs — a handshake advances a state machine, and one
shared instance would have every client advancing everyone else's.

Hooks see `Message.Decoded` as `any` next to a `Descriptor`, so nothing in the
framework carries a type parameter and a bytes-only consumer writes no ceremony.
[`typed`](typed/) is the other half of that trade: `typed.On[P]` runs a callback
only for messages that decoded to a `P`, so typed use costs no hand-written
assertions either.

## Injection

A hook can send a message to either peer with `Session.Inject`. Every write —
relayed or injected — goes through the same per-peer lock, so an injected
message never lands inside a relayed one. That guarantee is why injection is
part of the design rather than something bolted on: a framework that handed
hooks a raw `net.Conn` could never make it.

## Mid-stream transforms

Framing is not always constant for the life of a connection. Real protocols
negotiate a cipher partway through, and from an agreed byte onwards every
subsequent byte is enciphered against a keystream that does not restart.
`Session.Swap` installs a `Transform` on one of the session's two connections,
from a message boundary onwards.

**Compression is not this**, and the two get grouped together constantly. A
negotiated compression threshold compresses each message independently inside
the frame envelope: a per-frame header, then a self-contained payload. Nothing
carries from one frame to the next, so it is a framing concern and belongs to
your `Framer`, which already owns the envelope. A stream cipher is the opposite —
its keystream is continuous, so the boundary is a property of the byte stream
rather than of any one message, and no `Framer` can express it. Reaching for
`Transform` to do compression produces something that works until the first
frame boundary lands mid-buffer.

Two things the swap guarantees, and one it does not:

- It is safe from any goroutine, including from a hook running on the other
  direction's pump — which is the common case, since the connection being
  swapped usually has a read parked on it at that moment.
- It refuses, with `ErrSwapPending`, when that connection still holds unread
  bytes from before the boundary. Those bytes belong to the old encoding, and
  transforming them on the way out would corrupt the next message with nothing
  to point at afterwards.
- It takes effect at a message boundary in the stream, **not** at a byte offset
  agreed with the peer. Every protocol that renegotiates mid-stream already
  forbids sending across the switch, because both endpoints have the identical
  problem. A proxy that swaps at the same message an endpoint would is exactly
  as correct as that endpoint, and no more.

## Raw capture

`Config.CaptureRaw` records every byte crossing the client connection to
`Sink.RawChunk`, below any framing and below any mid-stream transform — so what
is stored is the conversation as it appeared on the wire, not as the codec
understood it. It is off by default, because it costs a copy per socket read and
write.

Only the client connection is wrapped. What a capture is for is replaying the
session a client had; the upstream link carries the same messages, possibly
under a different encoding, and recording both would double the storage to say
the same thing twice.

Capture starts before the sink has a session to attach it to — a `PreFrame` hook
reads from the socket well before an upstream is joined — so those opening bytes
are held and flushed once the session opens, under a bound. A capture that
simply started later would be missing exactly the bytes that opened the
conversation.

## Upstreams and health

A port maps to a set of upstreams. `Selector` chooses among the healthy ones —
`FirstHealthy()`, `RoundRobin()`, `LeastConn()`, and `StickyByClientIP()` ship —
and dial failover applies underneath whichever you pick.

Health is resolved **lazily, when a client connects**, not on a timer. A result
is cached for `ProbeTTL`, concurrent misses for one address collapse into a
single in-flight probe, and dial failures write to the same cache as probes.

The visible consequence: every configured port is bound, including one whose
upstreams are all dead, so a client there sees connect-then-drop rather than
connection refused. That is the accepted trade. A socket and a goroutine per
port cost nothing next to a startup probe that goes stale and routes traffic at
a server which is no longer there.

## Errors

`Run` returns only fatal faults — invalid configuration, a listener that cannot
bind. Per-session errors reach `Config.OnSessionError`, because with thousands
of sessions there is nowhere else they can go.

Sentinels: `ErrInvalidConfig`, `ErrNoHealthyUpstream`, `ErrSessionClosed`,
`ErrMessageTooLarge`, `ErrSwapPending`, and `ErrHook` wrapping whatever a hook
returned, so a caller distinguishes hook failure from transport failure without
a type switch.

Hook panics are recovered at the session boundary and converted to `ErrHook`
carrying the stack, ending only that session. This is a deliberate divergence
from our protocol library's router, which does not recover handler panics: that
is right for a library driving one connection, where burying a bug puts the
report far from its cause. A proxy holds thousands, and one malformed message
reaching a buggy hook must not take every unrelated session down with it.

## Testing your Framer

```go
relaytest.FramerContract(t, func() relay.Framer { return myFramer{} }, messages)
```

A `Framer` is the easiest part of this to get subtly wrong — partial reads,
short writes, EOF mid-frame, a buffer reused after it was handed over — and each
of those produces corruption a long way from its cause.
[`relaytest`](relaytest/) turns them into a failure in your own suite.

## Examples

[`examples/`](examples/) is a **separate module**, so every third-party
dependency lives there and none of it reaches a consumer of the core.

- [`examples/cipher`](examples/cipher/) — `Transform`'s worked consumer: a real
  AES-CTR keystream installed mid-session, on both links, with the ordering
  rules the tests exist to pin.
- [`examples/minecraft`](examples/minecraft/) — the seam against a protocol that
  was not designed for it: a `Framer` over a length-prefixed envelope, a `Codec`
  with per-direction state, a status-ping `Prober`, and a batched SQLite `Sink`.

## Development

```
devbox run -- task verify
```

The design is written up in
[`docs/2026-08-16-relay-proxy-framework-design.md`](docs/2026-08-16-relay-proxy-framework-design.md).
