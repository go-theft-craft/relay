# `relay`: a protocol-agnostic proxy framework

- Status: Draft for review
- Date: 2026-08-16
- Repository: `relay` (new, public)
- Related: `minecraft-protocol`, used by relay's worked example and depended on
  by neither module in either direction

## Context

Two proxies in this organization need the same core and share none of it. Both
accept client connections on a range of ports, pick an upstream, relay framed
messages in both directions, let code inspect and rewrite those messages, inject
messages of their own, and record what crossed the wire. Only the framing and
the packet types differ.

Today that core lives inside one private repository, entangled with the game
logic it was written for: the session type reaches into a bot registry, a shared
world, and an order queue, and the packet logger takes a concrete packet type.
Separating the reusable half is worth doing on its own terms — the result is a
general Go framework, and our proxies become its first two consumers.

`minecraft-protocol` already ships `router`, `middleware`, `capture`, and
`replay`, all written over `protocol.Packet`. Those solve a neighbouring problem
— dispatch and recording for *one* connection — and they stay where they are.
`relay` owns the part above them: listeners, upstream selection, session
lifecycle, and bidirectional relay. It does not depend on `minecraft-protocol`,
and `minecraft-protocol` does not depend on it.

## Goals

- A framework that proxies any length-prefixed or delimited byte protocol, given
  one interface implementation.
- Optional decoding, so hooks and sinks can see typed packets without the core
  ever naming a packet type.
- Injection of messages from hook code with a stated ordering guarantee.
- Many upstreams across many ports, with pluggable selection and health that
  reflects reality at connect time rather than at startup.
- Enough clients that per-session cost is a design constraint, not an
  afterthought.
- A core module whose `go.mod` requires nothing outside the standard library —
  checked in CI, not merely intended.

## Non-goals

- Migrating either existing proxy onto the framework. That is separate work in
  the repositories that own those proxies, planned once the API has been proven
  against the worked example here.
- Protocol decoding, state machines, or game logic of any kind.
- UDP, QUIC, or datagram transports. Stream transports only.
- Metrics or tracing integrations. The hook and callback surface is where a
  consumer attaches its own.
- A general-purpose L7 HTTP proxy. Nothing forbids it; nothing is designed for
  it either.

## Module layout

```
github.com/go-theft-craft/relay        core module, zero non-stdlib requires
  relay.go        Proxy, Config, Run
  listen.go       listeners, probe cache, upstream selection and failover
  session.go      session lifecycle, read pumps, writer locks
  conduit.go      per-direction byte layer and mid-stream transform swaps
  message.go      Message, Direction, Action, Descriptor
  hook.go         Hook, HookFunc, PreFrame
  framer.go       Framer, Codec, Prober
  sink.go         Sink, SessionInfo, MessageRecord
  registry.go     live session tracking, snapshots, drain
  typed/          generic wrappers over the untyped core
  relaytest/      Framer conformance harness for consumers

  examples/                              separate go.mod
    cipher/     Transform's worked example: a real AES-CTR mid-stream swap
    minecraft/
      main.go     runnable proxy: config, wiring, graceful stop
      framer.go   Framer over minecraft-protocol framing
      codec.go    Codec over its packets
      prober.go   status-ping Prober, replacing the TCP-dial default
      store/      async batched SQLite sink
      proxy_test.go
```

Two modules. The core's dependency-free `go.mod` is the enforceable form of
"protocol-agnostic": a CI step asserts the require block is empty. The examples
module depends on `minecraft-protocol` and `modernc.org/sqlite` freely, and
nothing it imports reaches a consumer of the core.

`Transform` gets its own worked example rather than a corner of the Minecraft
one. The Minecraft example cannot demonstrate it honestly: its compression is a
framing concern, and its cipher is negotiated during a login this example
deliberately does not stand between. So `examples/cipher` proves the swap
against a real AES-CTR keystream over a toy line protocol — small enough to read
in one sitting, and exercising the cross-direction swap that is the pattern
every real consumer will need. An example that demonstrated `Transform` with an
identity function would be worse than none.

The Minecraft example proxies handshake, status, and the opening of login with
decoding active, and falls back to opaque passthrough once a session enables
encryption. Standing between an encrypted login as a third party means running
two key exchanges and holding the client's session credentials, which is a
project in itself and teaches nothing about the framework seam. The example's job is to
prove `Framer`, `Codec`, and `Prober` against a real protocol, and it does that
before the first encrypted byte.

The SQLite sink is a package inside the one example rather than a separate
example, because it is part of that example's story; it is a package rather than
a file because several hundred lines of SQL and batching in the same package as
the framer would bury what a reader came for.

## The seam

Four interfaces. Only `Framer` is required.

```go
type Framer interface {
	ReadMessage(Reader) ([]byte, error) // Reader is io.Reader + io.ByteReader
	WriteMessage(io.Writer, []byte) error
}

type Codec interface {
	Decode(Direction, []byte) (any, Descriptor, error)
	Encode(any) ([]byte, error)
}

type Hook interface {
	OnMessage(context.Context, *Session, *Message) (Action, error)
}

type Sink interface {
	OpenSession(context.Context, SessionInfo) (int64, error)
	Message(context.Context, int64, MessageRecord)
	RawChunk(context.Context, int64, Direction, []byte)
	CloseSession(context.Context, int64)
}
```

`Decode` returns the descriptor alongside the value rather than offering a
separate `Describe` call: the decoder already knows the ID and the name, and
recovering them through a second dispatch is waste on the hot path.

Only `OpenSession` returns an error, and the documented contract is that no
`Sink` method may block. Batching and asynchrony belong to the implementation,
which can size its own queue; a core that owned that goroutine could not tune it
for anyone.

The core is not generic. `Message.Decoded` is `any`, carried next to a
`Descriptor{ID, Name}`, so `Proxy`, `Session`, and `Hook` have no type parameter
and a bytes-only consumer writes no ceremony. `typed/` provides
`TypedHook[P]` and friends, which assert once and hand the consumer a typed
callback, so typed use costs no hand-written assertions either.

```go
type Message struct {
	Dir     Direction
	Raw     []byte     // pooled; valid only for the duration of the hook call
	Decoded any        // nil unless a codec ran
	Desc    Descriptor // zero if undecoded
}
```

`Action` is `Forward`, `Drop`, or `Replace`. Re-encoding happens once, after the
whole hook chain, and only if some hook modified `Decoded`.

## Session lifecycle

Accept → optional raw-capture wrap → `Sink.OpenSession` → pre-frame hook →
resolve upstream → relay.

The pre-frame hook receives the buffered reader before any framing, so a session
can sniff the first bytes and either continue normally or take the connection
over entirely and end the session. It exists in v1 because retrofitting it means
reshaping the read path.

Each session runs **two** goroutines: one read pump per direction. Each peer's
writer is guarded by a one-slot semaphore channel rather than owned by a third
goroutine:

```go
select {
case s.toServer <- struct{}{}:
	defer func() { <-s.toServer }()
case <-ctx.Done():
	return ctx.Err()
}
err := s.framer.WriteMessage(s.serverConn, raw)
```

Every write to a peer — relayed or injected — acquires that lock, so no two
goroutines ever hold the same writer and an injected message never interleaves
inside a relayed one. That guarantee is the reason injection is designed in
rather than added later: a framework that hands hooks a raw `net.Conn` can never
make it.

A read pump blocks while writing to a slow peer, which propagates TCP
backpressure to the origin without buffering. A queue between the pumps would
add decoupling nobody asked for, at the cost of a goroutine per direction.

Shutdown: the first pump to fail cancels the session context; the peers are
given `Config.DrainGrace` to finish an in-flight write, then both connections
close, `Sink.CloseSession` runs, and the registry entry drops. Proxy shutdown
closes listeners first, then applies the same grace to live sessions.

`Session` carries `Set`/`Get` for consumer metadata and a `Snapshot()`, which is
how the registry lists live sessions without the framework knowing what the
metadata means.

## Mid-stream transforms

Framing is not always constant for the life of a connection. Real protocols
negotiate a symmetric cipher partway through, and from an agreed byte onwards
every subsequent byte on that stream is enciphered against a keystream that does
not restart. A framework that hands a `Framer` one `*bufio.Reader` at session
start and never revisits it cannot express that, and both proxies need it.

**Compression is not this.** It is worth saying because the two get grouped
together and they do not belong together. A negotiated compression threshold, in
this protocol family and in the legacy one, compresses each packet
independently inside the frame envelope — a per-frame header declaring a
decompressed length, then a self-contained zlib payload. Nothing carries from
one frame to the next, so it is a framing concern and belongs to the `Framer`,
which already owns the envelope. A stream cipher is the opposite: its keystream
is continuous, so the boundary is a property of the byte stream rather than of
any one message, and no `Framer` can express it. `Transform` exists for that
second case only. Reaching for it to do compression produces something that
works until the first frame boundary lands mid-buffer.

So each direction owns a **conduit**: the byte layer between the socket and the
`Framer`, holding a transform a hook can change while the session runs.

```go
type Transform struct {
	Read  func([]byte)          // nil leaves the read side alone
	Write func([]byte) []byte   // nil leaves the write side alone
}

func (s *Session) Swap(dir Direction, t Transform) error
```

`minecraft-protocol` already solved this once, in its own `Conduit`, and the
ordering it found is the whole trick: **buffer raw bytes, and transform them as
they are handed out rather than as they are buffered.** Everything difficult
about a mid-stream switch dissolves under that ordering.

```go
func (c *Conduit) Read(p []byte) (int, error) {
	n, err := c.buffered.Read(p)

	// The lock is taken after the read, never around it, so a socket read that
	// blocks forever cannot stop a hook from swapping.
	c.mu.Lock()
	if n > 0 && c.read != nil {
		c.read(p[:n])
	}
	c.pending = c.buffered.Buffered()
	c.mu.Unlock()

	return n, err
}
```

Because the buffer holds untransformed bytes, a swap does not have to reach into
it, rebuild it, or carry anything across. Because the lock is never held around
the read, a swap does not have to interrupt a parked pump with a read deadline,
and no reader is ever mutated from outside the goroutine that owns it. A swap is
a two-field assignment under a mutex, and it is safe from any goroutine — which
matters, because a cipher negotiated in one direction almost always has to be
installed on the other.

**The one refusal.** `Swap` fails when the read buffer still holds unread bytes.
Those bytes arrived before the switch, so they belong to the old encoding, and
transforming them on the way out would corrupt the very next message with
nothing to point at afterwards. Failing at the switch names the cause at the
cause. In practice the buffer is empty exactly when it should be: every protocol
that renegotiates mid-stream requires the peer to stop sending across the
boundary, because both endpoints have the identical problem and solve it the
identical way. A non-empty buffer means the peer broke that rule, or the
consumer swapped at the wrong message — both worth an error rather than silent
corruption.

**What this costs `Framer`.** The conduit cannot hand out a `*bufio.Reader`,
because a reader above it would buffer transformed bytes and put the emptiness
check on the wrong buffer. So `ReadMessage` takes an interface instead:

```go
type Reader interface {
	io.Reader
	io.ByteReader
}

type Framer interface {
	ReadMessage(Reader) ([]byte, error)
	WriteMessage(io.Writer, []byte) error
}
```

`*bufio.Reader` satisfies it, which is what lets `relaytest` exercise a framer
over a plain buffer with no session in sight, and `*Conduit` satisfies it in
production. `Peek` is deliberately absent: peeking past a transform means
transforming bytes without consuming them, and a stream cipher cannot be asked
to do that. The pre-frame hook, which genuinely needs `Peek`, runs before any
swap can have happened and gets the raw buffered reader directly.

**What the framework does not promise.** A swap takes effect at a message
boundary in the stream, not at a byte offset agreed with the peer. If the
opposite peer is still sending pre-boundary bytes when the swap lands, those
bytes are decoded with the wrong transform. Every protocol that renegotiates
mid-stream already forbids this, because both endpoints have the identical
problem and solve it the identical way: the peer stops sending, the boundary
message crosses, and traffic resumes under the new encoding. A proxy that swaps
at the same message the endpoint would is exactly as correct as the endpoint.
Stating the limit is the point — a consumer inventing a protocol should know the
constraint it has to honour.

## Upstreams

A port maps to a set of upstream addresses. Two decisions were previously
implicit in "try them in order":

**Selection** is an interface with built-ins.

```go
type Selector interface {
	Pick(ctx context.Context, port int, up []Upstream, c net.Conn) (Upstream, error)
}
```

Shipped: `FirstHealthy()`, `RoundRobin()`, `LeastConn()`, `StickyByClientIP()`.
Dial failover applies underneath whichever selector is configured — a dial error
marks the upstream unhealthy and moves to the next candidate.

**Health is resolved lazily, when a client connects**, not on a timer.

```go
type Prober interface {
	Probe(ctx context.Context, addr string) error // nil means healthy
}
```

The default probes with a TCP dial, which is all an agnostic core can assume.
The example implements a protocol status ping instead, so "healthy" means the
server answered rather than something holds the port open. That makes `Prober`
the second place, after `Framer`, where the worked example demonstrates the seam
is real.

Three properties keep per-connection probing viable under load:

- A probe result is cached for `Config.ProbeTTL`, so a burst of clients against
  one port costs one probe rather than one each.
- Concurrent misses for the same address collapse into a single in-flight probe;
  the rest wait on its result.
- On a miss, a port's candidates are probed concurrently under a shared
  deadline.

Dial failures write to the same cache, so passive signal and active probe share
one state.

**Consequence.** Startup no longer knows which ports have live servers, so relay
listens on every configured port. A port whose upstreams are all dead accepts a
connection and closes it once the probe returns empty, where the earlier
behaviour was to never open the listener — a client sees connect-then-drop
instead of connection refused. This is accepted: a socket and a goroutine per
port cost nothing, and a startup probe that goes stale is a real bug.

## Scale

Targets are many clients per process, so per-session cost is part of the design
rather than a later optimization.

- Two goroutines per session. At ten thousand sessions that is twenty thousand
  goroutines and roughly 160 MiB of stack floor; four per session would double
  it before a single buffer is allocated.
- `Message.Raw` is drawn from a `sync.Pool` and is valid only for the duration
  of a hook call. This is the same ownership contract
  `minecraft-protocol/middleware` documents, so the two repositories read
  consistently.
- `bufio` sizes are configurable. At ten thousand sessions the default is worth
  roughly 80 MiB, which makes it a knob rather than a constant.
- `Config.MaxSessions` is enforced by an accept semaphore with a stated overflow
  policy (close immediately, or wait for a slot). An unbounded accept loop is
  the first thing to fall over.
- Listeners cost one goroutine per port, independent of client count.

These figures are arithmetic from Go's initial stack size and the configured
buffer sizes, not measurements. Benchmarks belong to implementation.

## Errors

`Proxy.Run` returns only fatal faults: invalid configuration, a listener that
cannot bind. It cannot return per-session errors when there are thousands of
sessions, so those reach `Config.OnSessionError(*Session, error)`, which
defaults to a `slog` line.

Sentinels: `ErrInvalidConfig`, `ErrNoHealthyUpstream`, `ErrSessionClosed`,
`ErrMessageTooLarge`, and `ErrHook` wrapping whatever a hook returned, so a
caller distinguishes hook failure from transport failure without a type switch.

A hook that returns an error ends its session. A hook that meant to rewrite a
message and failed has left the stream in a state neither peer agreed to, and
forwarding anyway corrupts it quietly.

`io.EOF` from a `Framer` is a clean close. Any other read error ends the
session. `Config.MaxMessageSize` bounds what a `Framer` may hand back, so a
hostile or buggy length prefix cannot allocate without limit.

**Panics.** `minecraft-protocol/router` deliberately does not recover handler
panics, on the grounds that a panic is a bug and burying it puts the report far
from the cause. That is right for a library driving one connection. A proxy
holds thousands, and one malformed message reaching a buggy hook must not take
down every unrelated session. `relay` therefore recovers at the session
boundary, converts the panic to `ErrHook` carrying the stack, and ends only that
session. The divergence is deliberate and documented in both places.

## Testing

The core module tests itself with a newline framer defined in `_test.go`, so the
zero-dependency guarantee survives its own test suite. `net.Pipe` covers session
mechanics; loopback TCP covers accept, probe, and shutdown. Everything runs
under `-race`: the writer lock and the single-flight probe cache are the two
places a race would cost data.

Mid-stream swaps are tested with a reversible byte transform rather than a real
cipher: a stream that flips every byte after the boundary proves the swap landed
on the right message, without importing anything or making the test's
correctness depend on a key exchange. Two cases carry the design — a swap
issued while the owning pump is parked in a read, which must not race, and a
swap issued with bytes still buffered, which must be refused rather than
absorbed.

Two things need help to be deterministic. `ProbeTTL` reads an injectable clock —
an unexported `Config` field set by tests — rather than sleeping. Injection
ordering is asserted with a framer that writes each message in two parts with a
scheduling point between them, proving a relayed message never lands inside an
injected one.

`relaytest.FramerContract(t, f)` is shipped for consumers. A `Framer` is the
easiest thing in the framework to get subtly wrong — partial reads, short
writes, EOF mid-frame — and a table-driven conformance harness turns that into a
bug the consumer catches rather than one we field.

The examples module runs the end-to-end case: a client through relay to a stub
upstream, asserting the expected rows land in SQLite.

CI runs both modules separately, plus the assertion that the core's require
block is empty.

## Open questions

None blocking. The two calls most worth revisiting after the example is written
are whether `Selector` needs the `net.Conn` (it is there for sticky routing, and
nothing else uses it) and whether `Sink.RawChunk` earns its place once capture
is wired through a real consumer.
