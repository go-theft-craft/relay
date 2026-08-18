# Changelog

All notable changes to this module are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module uses
[semantic versioning](https://semver.org/spec/v2.0.0.html) — while the major
version is `0`, a minor bump may break the API.

## [0.4.1] — 2026-08-18

Everything here is `examples/` and the documents beside it: the core module is
byte for byte `0.4.0`. The tag exists so the example proxy's recordings and the
run behind them have a version to name.

### Added

- `mcrelay` can run the core's sink policy: `-sink-overflow` picks `block`,
  `drop`, or `end-session`, `-sink-queue` sizes the queue the last two use, and
  `-record-queue` sizes the recorder's own. They exist because the policy added
  in `0.4.0` had never been run against a real server, and a policy nobody can
  run from a command line is a claim rather than a feature.

  It also reports drops now. `relay.Session.SinkDropped` is a counter that
  nothing reads on a consumer's behalf — a `Sink` is handed an int64, and
  `SessionSnapshot` does not carry the count — so under `drop` the loss was
  silent until a hook asked. The first run of that arm lost 783 records and said
  nothing at all.

### Changed

- The capture recorder keeps its own queue, and the core's stays out of the
  capture path. `docs/2026-08-17-enforce-the-sink-contract.md` parked this
  pending a run against a real server;
  `docs/verification/2026-08-18-sink-policy-live.md` is that run, in front of
  vanilla 1.8.9 with a headless client.

  Both mechanisms were made to fire and both left a recording that replays to
  its own digest, so the deciding facts were the other ones: the recorder never
  blocks, which means a slow disk fills its queue and never reaches the core's,
  leaving the core's able to fire only on a burst that outruns a channel send —
  at a copy per message per sink. And `SinkOverflowBlock` is the default, so a
  consumer who configures nothing gets the recorder's queue and nothing else. A
  capture's guarantee should not rest on a flag somebody else remembered to set.

  Nothing in the core changed. `mcrelay` warns when both queues are configured,
  because the smaller one silently decides which is running.

- `examples/README.md` says what the replay gate is not. The digest covers what
  was written, so a recording missing records still replays to its own digest:
  the live run lost 783 records under `SinkOverflowDrop` and `verify` called the
  file ok. What caught it was `trace`, refusing movement for an entity that
  never spawned — which is luck about what the queue happened to drop, not a
  check.

### Fixed

- `examples/` pinned `minecraft-protocol` v0.2.0, three releases behind, whose
  codec refuses a schema switch carrying a value the schema names no case for.
  Protocol 775 supplies them in ordinary play — a real client's command tree
  carries `minecraft:loot_predicate`, and a player taking damage draws a
  `damage_indicator` particle — and either one ended decoding for the rest of
  the session. The proxy kept relaying opaquely, so the connection survived and
  the recording did not: everything after that point failed to replay.

  v0.5.0 reads an unnamed switch value as absent rather than as an error, which
  is what the ProtoDef compiler ships and what node-minecraft-protocol runs. Two
  recordings that had failed replay under it, including a 36023-record session
  from a real 26.1.2 client that used to stop at sequence 97.

## [0.4.0] — 2026-08-18

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

### Fixed

- `examples/minecraft` did not relay an encrypted session, though three places
  said it did. The codec stopped decoding at the key exchange as documented, and
  the framer went on reading length prefixes out of ciphertext: it took a phantom
  13-byte frame, then read a prefix announcing 1.7 MB, and parked waiting for a
  frame nobody had sent. Under the frame limit, so nothing was refused — no
  error, and the session's only log line was the codec's own notice that it had
  stopped decoding, which reads like the documented behaviour working.

  The codec now latches on the session and both framers read it, stopping at the
  same point the codec does: `minecraft.NewCodec` and `minecraft.NewFramer` take
  the `*relay.Session`, and the framer is built per session and per direction
  through `Config.NewFramer`. The latch is checked after the first byte of a read
  as well as before it, because the clientbound pump is parked on that byte when
  the exchange completes on the other pump. The write side follows the last read
  instead of the latch, since the frame that completes the exchange is read as a
  frame and written after the latch is set — keying on the latch drops its length
  prefix and the upstream never switches its own cipher.

- `examples/minecraft/capture` recorded ciphertext as though it were frames: it
  rebuilt the length prefix relay strips, putting a number in the file that was
  never on the wire, and asked the sensitivity question about payloads that are
  ciphertext — which eventually produces a packet identifier that matches and
  withholds a record for a reason nobody can reconstruct. A recording now ends at
  the key exchange with a secret record marking the switch, which is what the
  capture format has that record for: replay skips it, the digest excludes it,
  and a reader can tell an online-mode login from a recorder that stopped for a
  reason nobody wrote down.

  Nothing caught either of these because no test here had ever run a key
  exchange. `examples/minecraft/encryption_test.go` now does, and
  `docs/2026-08-17-the-encryption-remainder.md` records what the first run found.

- `examples/cipher` armed the client link's write side after forwarding the
  trigger, so the upstream's acknowledgement raced it. The acknowledgement
  crosses on the other pump — deciphered, hooked, and written to the client —
  which can all happen while the hook is still executing, and a client that has
  already switched then reads a plaintext line as bytes with no line ending in
  them and waits for one that cannot arrive.

  It is armed before the trigger goes out now, for the mirror of the reason the
  upstream read side already was. This is what had been failing CI roughly one
  run in three, in two different tests of that package, on a ten-second read
  deadline; no local run ever reproduced it. `onTrigger` carries the manual
  falsifier, because the window is inside the hook and nothing outside it can be
  made to widen it reliably.

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
