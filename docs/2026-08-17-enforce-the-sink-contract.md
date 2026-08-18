# Plan: enforce the `Sink` contract in the core

> **For agentic workers:** steps use checkbox (`- [ ]`) syntax for tracking. Run
> every command as `devbox run -- task <name>`.

**Goal:** decide, and then implement, whether `relay` calls sinks through a queue
it owns rather than documenting a no-blocking rule it does not enforce. This is
option 3 of `docs/2026-08-17-after-the-plan.md`.

**Why now:** the documented-not-enforced approach has failed once already, inside
this repository. `sink.go` says no method may block; `examples/minecraft/capture`
blocked on the read pump for three releases, and `MultiSink` fanned that stall
out to the sink beside it. The capture sink now owns a queue and ends the session
when it fills, which fixes that sink and leaves the contract exactly as
unenforced as it was for the next one.

**What is not in scope:** the `Sink` interface itself. Every option below leaves
`OpenSession`/`Message`/`RawChunk`/`CloseSession` as they are, so no consumer has
to change to keep working.

## The argument against, stated first

`sink.go:28` makes the case for leaving this alone, and it is not a weak one:

> batching and asynchrony belong to the implementation, which can size its own
> queue for its own storage. A core that owned that goroutine could not tune it
> for anyone.

That is true of the *policy* and false of the *mechanism*. A core queue does not
have to choose what a full queue means — it can carry the choice as
configuration, which is what the design below does. What the core cannot avoid
choosing is who copies the bytes, and that is the real cost:
`MessageRecord.Raw` is borrowed for the duration of the call, so a queued call
means the core copies every message for every sink, including sinks that never
look at `Raw`. Today `examples/minecraft/store` and the capture sink each copy
once for themselves, so for them a core copy is a wash; for a counting sink it is
a new allocation per message that buys nothing.

So the deciding question is measured, not argued: **what does one copy per
message per sink cost on the relay path, and is the default worth changing?**
Task 1 answers it before anything else is built.

## Design

Three pieces, in the order the code would grow them.

1. **A per-session sink pump.** One goroutine per session drains a bounded
   channel and makes the sink calls in order. It is per session rather than per
   proxy because ordering is only meaningful within a session, and because one
   slow session should not delay another's records.
2. **A policy, chosen by the consumer.** `Config.SinkOverflow` names what a full
   queue means, because the tree already contains both right answers: a telemetry
   store wants the record dropped and counted, a capture wants the session ended.
   - `SinkOverflowBlock` — today's behaviour, and the default. The call happens
     inline on the pump, nothing is queued, nothing is copied.
   - `SinkOverflowDrop` — the record is discarded and counted.
   - `SinkOverflowEndSession` — the session ends, cause `ErrSinkOverflow`, and
     the reason reaches `Config.OnSessionError`.
3. **`OpenSession` stays inline, always.** It returns an error, it assigns the
   identifier every later call is keyed by, and it runs on the accept path before
   there is a session to queue against. A sink that wants it off the accept path
   does what the SQLite sink already does: assign from a counter and insert
   later.

`RawChunk` deserves a note of its own. It is called from inside
`captureConn.Read`/`Write` while `captureConn.mu` is held, which couples both
directions — a stall there is the one case that is not confined to one pump.
Queueing raw chunks removes that coupling regardless of which policy a consumer
picks, so it is worth doing even if the default stays `Block`.

## Tasks

### Task 1: Measure what the copy costs

**Files:** create `sink_bench_test.go`.

- [x] **Step 1:** Benchmark the relay path as it is today: a session, a framer, a
      sink that ignores its arguments. Report ns/op and B/op per relayed message.
- [x] **Step 2:** Benchmark the same path with one `append([]byte(nil), raw...)`
      added per message, which is the floor for any queued design.
- [x] **Step 3:** Write the two numbers into this document. If the copy is inside
      the noise on a 100-byte frame, the default becomes arguable; if it is not,
      `SinkOverflowBlock` stays the default and the rest of this plan is opt-in
      only. Either way the number is recorded, not remembered.

#### The measurement

`sink_bench_test.go`, Go 1.26.6, AMD Ryzen 9 9950X, `-count=6`, medians:

| Path | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| 100 B relayed, `Raw` borrowed | 117.1 | 2 | 1 |
| 100 B relayed, one copy added | 146.4 | 114 | 2 |
| 1500 B relayed, `Raw` borrowed | 117.5 | 2 | 1 |
| 1500 B relayed, one copy added | 298.3 | 1540 | 2 |
| the copy alone, 100 B | 20.6 | 112 | 1 |
| the copy alone, 1500 B | 208.1 | 1536 | 1 |

**The copy is not inside the noise.** It adds 25% to a 100-byte message and
154% to a 1500-byte one, and it doubles the allocation count at every size. The
relay path's own cost barely moves with message size — 117 ns either way, since
nothing on it touches the bytes — so a per-message copy is the first thing on
that path that scales with what is being relayed.

Two things the numbers do not say. The benchmark writes to a discarding
connection, so a real socket write is not in the denominator; the copy's share
of a whole message on the wire is smaller than the percentages above. And it
calls `relay` directly rather than driving a read pump, because `net.Pipe` is
synchronous and two goroutine handoffs per message would bury a 20 ns
difference. Both choices make the copy look **more** expensive than it is
relative to real work, which is the safe direction for a number used to justify
leaving a default alone.

The allocation is the part that does not shrink with a faster socket: one per
message per sink, unconditional, including for sinks that never read `Raw`.

**Decision: `SinkOverflowBlock` stays the default, and the rest of this plan is
opt-in.** A consumer that wants the core to guarantee the contract asks for it
and pays the copy knowingly; one that does not keeps today's cost exactly.

### Task 2: The queue and the policy

**Files:** modify `config.go`, `sink.go`, `session.go`, `errors.go`; create
`sinkpump.go`.

- [x] **Step 1:** Add `Config.SinkQueueDepth` (records, per session) and
      `Config.SinkOverflow`, with validation and defaults in `validate`. Depth is
      meaningless under `SinkOverflowBlock`; reject the combination rather than
      silently ignoring it.
- [x] **Step 2:** Add `ErrSinkOverflow` to `errors.go`.
- [x] **Step 3:** Write the pump: a bounded channel, one goroutine, records
      carried by value with `Raw` copied on enqueue. Order within a session is
      the guarantee; there is no batching, because batching is the sink's job and
      that part of `sink.go`'s comment stands.
- [x] **Step 4:** Own the lifecycle from the session. The pump starts when the
      session starts and stops on the finish path, after both read pumps have
      ended; `CloseSession` is the last call it makes, so it must not be enqueued
      ahead of records that belong before it. A drain grace bounds the wait, for
      the same reason `capture` bounds its own: teardown that can hang on a
      wedged sink has moved the stall rather than removed it.

### Task 3: Wire it, and prove each policy

**Files:** modify `session.go`, `capture.go`; create tests in `session_test.go`.

- [x] **Step 1:** Route `Message`, `RawChunk`, and `CloseSession` through the
      pump when the policy is not `Block`, and leave the inline path untouched
      when it is. `Raw` is owned by the sink once queued — document that, because
      it is the one place a sink's obligations differ between policies.
- [x] **Step 2:** Test that a sink parked in `Message` does not stall a pump
      under `Drop` or `EndSession`, and does under `Block`. The last assertion is
      the honest one: it documents the default as a choice.
- [x] **Step 3:** Test that `EndSession` ends the session with `ErrSinkOverflow`
      reaching `OnSessionError`, and that `Drop` counts. Expose the count — a
      silent drop is not an observability story, which is the argument the SQLite
      sink's `Dropped()` already makes.
- [x] **Step 4:** Test `RawChunk` specifically: a sink parked there must not
      couple the two directions, which is the failure the `captureConn` mutex
      makes possible.

### Task 4: Decide what the examples do with it

**Files:** modify `examples/minecraft/capture/sink.go`,
`examples/minecraft/store/sqlite.go`, `examples/minecraft/cmd/mcrelay/main.go`.

- [x] **Step 1:** Leave the SQLite sink's own queue alone. It exists for
      batching, not for the contract, and a core queue in front of it would add a
      hop without removing a reason.
- [x] **Step 2:** Decide whether the capture sink keeps its queue or leans on
      `SinkOverflowEndSession`. Keeping it means two bounded queues in a row;
      leaning on the core means the recorder no longer needs `Bind`, `Attach`, or
      `Session.SinkID` for this purpose. Recommendation: keep the sink's queue
      until the core's policy has run against a real server, then remove one.

      *Run and decided on 2026-08-18: the recorder keeps its queue, and the
      core's stays out of the capture path.
      `docs/verification/2026-08-18-sink-policy-live.md` has the eight arms. The
      short of it is that the recorder never blocks, so a slow disk fills its
      queue and never reaches the core's — which leaves the core's able to fire
      only on a burst that outruns a channel send, at a copy per message per
      sink. Both mechanisms were made to fire and both left a recording that
      replays; `SinkOverflowDrop` in front of the recorder left one that does
      not, and passes the gate anyway.*
- [x] **Step 3:** Whichever way step 2 goes, `MultiSink`'s serial fan-out is
      unchanged and still means one slow child delays its siblings. Say so in its
      doc comment, or give it a goroutine per child and a note about ordering.

### Task 5: Document the decision

**Files:** modify `sink.go`, `CHANGELOG.md`, `README.md`.

- [x] **Step 1:** Rewrite the `Sink` comment. It currently states a contract and
      an argument for not enforcing it; it should state which parts the core
      enforces, which the sink still owns, and what the default is.
- [x] **Step 2:** Changelog entry under 0.4.0, naming the default and why it did
      not change if it did not.

## Open questions, answered

- **Should `Block` stay the default?** Yes. Task 1 measured the copy at 25% of a
  100-byte message and 154% of a 1500-byte one, with an extra allocation at both
  sizes, so enforcement is real work charged to every consumer including the ones
  with no problem to solve. It is opt-in. `TestSinkOverflowBlockStallsTheReadPump`
  pins what the default actually does, because a default that enforces nothing
  should at least be a documented choice rather than an unexamined one.
- **Does the pump belong to the session or the sink wrapper?** The session. The
  asymmetry the question names is the deciding one: a wrapper could queue and
  could drop, but it could not end the session, and ending the session is the
  policy that motivated the exercise. A recorder that would rather stop than
  write a file with a hole in it needs to reach `Close`, and a `Sink` is handed
  an int64 precisely so it cannot.

  Two things fell out of that choice which a wrapper would have got wrong.
  `CloseSession` goes *through* the queue rather than beside it, so it still
  means "everything before this" — a wrapper closing over a sink has no way to
  know the session is done. And `captureConn` is handed the session's sink rather
  than the configured one, which is what uncouples the two directions: raw chunks
  are recorded under a mutex held across the call, so a sink parked there stalls
  both directions at once. That is the one stall a per-direction queue could not
  have confined, and it is confined now.

## What was built

- `sinkpump.go`: one bounded queue and one goroutine per session, implementing
  `Sink` so every call site in the session is written once and the policy is
  decided in one place. The queue is never closed — an injection racing a
  disconnect would otherwise panic on a send to a closed channel — so the
  terminating record ends the goroutine and a bounded abandon ends it when that
  record never arrives.
- `Config.SinkOverflow`, `Config.SinkQueueDepth`, `Session.SinkDropped`,
  `ErrSinkOverflow`.
- Tests in `sinkpump_test.go` covering each policy, including the two that
  document the default: under `Block` a parked sink holds the message short of
  the upstream, and both "does not stall" tests hang rather than fail if the
  policy is taken away.

The capture sink keeps its own queue, as the recommendation in Task 4 said it
should — and keeps it for good now. The core's policy went through a real server
on 2026-08-18 and the two queues turned out to absorb different failures, only
one of which this sink can actually suffer:
`docs/verification/2026-08-18-sink-policy-live.md`.
