# Changelog

All notable changes to this module are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module uses
[semantic versioning](https://semver.org/spec/v2.0.0.html) — while the major
version is `0`, a minor bump may break the API.

## Unreleased

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
  the next release; see Unreleased above.
- `examples/` is a separate module. Nothing it imports reaches a consumer of the
  core.
