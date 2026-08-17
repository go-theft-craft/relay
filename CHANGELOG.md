# Changelog

All notable changes to this module are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module uses
[semantic versioning](https://semver.org/spec/v2.0.0.html) — while the major
version is `0`, a minor bump may break the API.

## [Unreleased]

### Added

- `Session.SinkID`, which reports the identifier a `Sink` assigned the session at
  `OpenSession`.

  A `Sink` is handed an int64 and never a `*Session`, deliberately: a sink
  records, it does not steer. That leaves a sink with no way back to the session
  it is recording, and one sink needs it — a recorder whose storage cannot keep
  up has to end the session rather than write a recording with a hole in it,
  because a recording that does not replay is not evidence and still looks like a
  file. A hook holds both halves, so `SinkID` is what lets a consumer pair them.

  The `Sink` interface is unchanged: nothing in the core acts on this, and a sink
  that only records never needs it.

- `Config.SinkOverflow` and `Config.SinkQueueDepth`, which let the core enforce
  the `Sink` contract instead of only documenting it. `SinkOverflowDrop` puts a
  bounded queue and one goroutine per session in front of the sink and counts
  what will not fit; `SinkOverflowEndSession` ends the session with the new
  `ErrSinkOverflow` instead of recording one it cannot record completely.
  `Session.SinkDropped` reports the count, because a silent drop is not an
  observability story.

  **The default did not change, and that is the finding rather than an
  omission.** `SinkOverflowBlock` still calls the sink inline on the read pump,
  because a queued call has to outlive its arguments and `MessageRecord.Raw` is
  borrowed — so enforcement means copying every message for every sink,
  including sinks that never read it. Measured on the relay path, that copy adds
  25% to a 100-byte message and 154% to a 1500-byte one, and doubles the
  allocation count at both sizes. `docs/2026-08-17-enforce-the-sink-contract.md`
  carries the benchmark and the reasoning; `sink_bench_test.go` reproduces it.

  The rule was worth enforcing for somebody and not for everybody: this
  repository's own capture sink blocked on the read pump for three releases, and
  `MultiSink` fanned that stall out to the sink beside it. A consumer running a
  sink it does not control can now buy the guarantee; one that is not paying for
  a problem it does not have keeps today's cost exactly.

  `OpenSession` is called inline under every policy, and ordering within a
  session is preserved under all of them. Raw chunks share the message queue, so
  a sink parked in `RawChunk` no longer couples the two directions through
  `captureConn`'s mutex — the one stall a per-direction queue could not have
  confined.

## [0.3.0] — 2026-08-17

### Added

- `Config.NewFramer`, which builds a `Framer` per session and per direction.
  `Config.Framer` stays as it was: one instance shared by every session and both
  directions, which is right for a length prefix and wrong for a protocol that
  finds a boundary by decoding. Set one or the other, not both.

  Two things forced it, both found by migrating a real proxy onto this API. A
  protocol with no length on the wire ends a message by its own shape, so
  framing carries the same per-connection state decoding does — and needs it per
  direction too, because the same leading byte means different packets arriving
  from each peer.

### Fixed

- `examples/minecraft/capture` withheld the frame that enables compression as
  though it carried key material, so every recording taken against a vanilla
  server — which compresses by default — lost the threshold and would not replay.
  The sensitivity question is now asked before the frame's own transition is
  applied.

  The frame travels uncompressed and turns compression on behind itself, so
  asking afterwards asked about an envelope it does not wear; the read failed and
  the check fails closed, on the one field replay cannot reconstruct. Everything
  past the threshold then sat in the file wearing an envelope no replay knew
  about, and the first packet after it decoded as a different packet entirely.

  Nothing caught this because the stub upstream the end-to-end tests speak to
  only answers a status ping: until a real 1.8.9 server was put behind the proxy,
  nothing in this repository had ever negotiated compression. The live procedure
  in `docs/verification/2026-08-17-capture-oracle.md` found it on its first run,
  and the same document records the fix replaying at vanilla's default threshold.

  The stub now logs in and negotiates a threshold, so this class of defect fails
  in CI rather than waiting for somebody with a Minecraft client.

## [0.2.1] — 2026-08-17

### Fixed

- `Conduit.Swap` refused any swap while unread bytes were buffered. It now
  refuses only swaps that change the read side, because buffered read bytes have
  nothing to do with a write-only swap.

  This is what makes the ordering a proxy actually needs expressible: arm the
  read side before forwarding the message that moves the boundary — the peer may
  answer in the new encoding immediately — and the write side after it has gone
  out. An endpoint never faces this, because it switches both halves of its own
  stream at once. `examples/cipher` had the race and CI caught it.

## [0.2.0] — 2026-08-16

Additive: nothing in `v0.1.0` changed behaviour, and the minor bump is because
`Config` grew a field rather than because anything broke.

### Added

- `Config.CaptureRaw`, which wires `Sink.RawChunk`: every byte crossing the
  client connection is recorded, below any framing and below any mid-stream
  transform. Off by default. It requires a `Sink` — capture with nowhere to put
  it is reported as `ErrInvalidConfig` rather than silently doing nothing.

  Bytes read before the session has a sink identifier — everything a `PreFrame`
  hook consumes — are held and flushed once it does, under a bound, so a capture
  is not missing the bytes that opened the conversation.

  This closes the `v0.1.0` note below: `Sink.RawChunk` is no longer dead.

- `examples/minecraft/cmd/mcrelay` grew a `-capture` flag, and the SQLite sink's
  `raw_chunks` table is populated when it is set.

## [0.1.0] — 2026-08-16

The initial API. A proxy framework for any length-prefixed or delimited byte
protocol, with a `go.mod` that requires nothing outside the standard library.

### Added

- `Proxy`, built by `New` and driven by `Run`, accepting on many ports and
  relaying framed messages with two goroutines per session. `Shutdown` closes
  listeners first, then gives live sessions their drain grace.
- `Framer`, the one interface a consumer must implement, over a `Reader` that is
  `io.Reader` plus `io.ByteReader`.
- `Codec`, in two forms: `Config.Codec` for a stateless decoder shared by every
  session, and `Config.NewCodec` for one built per session, which is what a
  protocol with connection states needs.
- `Hook` and `HookFunc` for inspecting, rewriting, dropping, or injecting
  messages; `PreFrame` for answering or diverting a connection before any
  framing. Hook panics are recovered at the session boundary and reported as
  `ErrHook` carrying the stack, ending only that session.
- `Session.Inject` and `Session.InjectDecoded`, which take the same per-peer
  writer lock relaying does, so an injected message never lands inside a relayed
  one.
- `Transform` and `Session.Swap` for mid-stream encoding changes, with
  `ErrSwapPending` when the connection still holds bytes from before the
  boundary. `Conduit` buffers raw bytes and transforms them as it hands them
  out, which is what makes a swap safe against a parked read pump.
- `Sink`, `SessionInfo`, and `MessageRecord` for recording what crossed the
  wire, including what the proxy itself injected. No `Sink` method may block.
- Lazy upstream health: a single-flight probe cache with a TTL, concurrent
  fan-out under a shared deadline, and dial failures writing through to the same
  cache. `Prober` and the default `DialProber`.
- `Selector` with `FirstHealthy`, `RoundRobin`, `LeastConn`, and
  `StickyByClientIP`, and dial failover underneath whichever is configured.
- Session registry: `Proxy.Sessions`, `SessionCount`, `UpstreamCount`, and
  `Session.Set`/`Get`/`Snapshot` for consumer metadata.
- `Config.MaxSessions` with `OverflowClose` and `OverflowWait`.
- `typed.On`, `typed.OnID`, and `typed.Inject` — generic wrappers so typed use
  costs no hand-written assertions, over a core that carries no type parameter.
- `relaytest.FramerContract`, a conformance harness for a consumer's own suite.
- Sentinel errors: `ErrInvalidConfig`, `ErrNoHealthyUpstream`,
  `ErrSessionClosed`, `ErrMessageTooLarge`, `ErrSwapPending`, `ErrHook`.

### Notes

- Every configured port is bound, including one whose upstreams are all dead, so
  a client there sees connect-then-drop rather than connection refused. This is
  the accepted consequence of resolving health lazily rather than at startup.
- `Sink.RawChunk` is part of the interface but nothing in this version calls
  it: the accept path never wraps a connection for byte-level capture. Wired in
  `0.2.0` above.
- `examples/` is a separate module. Nothing it imports reaches a consumer of the
  core.
