# The two questions the plan parked

`docs/2026-08-16-relay-proxy-framework.md` ends by naming two things to watch on
the first real migration, and deliberately leaves both open:

> - Whether `Transform` composing in place is enough for a protocol that needs to
>   *remove* a layer as well as add one. Nothing here supports un-swapping,
>   because nothing needed it yet. `examples/cipher` only ever adds.
> - Whether the `Sink` no-blocking contract holds up when a consumer wires a sink
>   with real storage behind it. The contract is stated; it is not enforced, and a
>   sink that blocks stalls a read pump and, through backpressure, its peer.

This is what looking found. One question has an answer that costs nothing; the
other has an answer that is already wrong in this repository.

## The `Sink` contract does not hold, and the offender is ours

The contract is at `sink.go:28`:

> no method may block: batching and asynchrony belong to the implementation,
> which can size its own queue for its own storage … A sink that blocks stalls a
> session's read pump and, through backpressure, its peer.

Nothing enforces it. `Sink.Message` is called inline at `session.go:428` on the
read pump goroutine, *before* the message is forwarded at `session.go:436`, so a
sink that parks parks the connection. `OpenSession` is called on the accept path
at `relay.go:315`, and `RawChunk` from inside `captureConn.Read`/`Write` at
`capture.go:89`, holding `captureConn.mu` — which couples both directions while
it is held.

Of the two sinks in this tree, one honours the contract and one does not.

`examples/minecraft/store` is the reference implementation. `Message` copies the
buffer and hands it to `enqueue` (`sqlite.go:285`), which is a non-blocking
`select` with a `default` that drops and counts. Every SQL transaction happens on
its own goroutine, batched. `OpenSession` even assigns IDs from an atomic counter
rather than inserting a row, to keep the accept path off the disk. There is a test
for it — `TestMessageDoesNotBlock` wedges the writer mid-batch, submits fifty
queues' worth, and asserts the submitter returns and that `Dropped()` rose.

`examples/minecraft/capture` breaks it. `Message` takes the recording's mutex and
writes to the file synchronously on the pump goroutine (`sink.go:163` through the
`Observe` call at `sink.go:287`). There is no queue, no batch, and no writer
goroutine anywhere in the file; `OpenSession` writes the header and
`CloseSession` the trailer, both synchronously. It works today because the page
cache absorbs it — the live oracle wrote a 22 MB recording through it without a
stall — and it will stop working on a full disk, a slow disk, or a network
filesystem, at which point the proxy stops relaying. Worth being precise about
the blast radius: the stalled pump is one direction of one session, so the other
direction keeps flowing — unless the stall is in `RawChunk`, which holds
`captureConn.mu` and therefore couples both.

`MultiSink` makes it worse rather than better: it fans out to children serially
on the caller's goroutine (`multisink.go:76`), so the capture sink blocking also
starves the SQLite sink beside it. Its only isolation is at `OpenSession`, where a
child that *errors* is dropped for that session — which handles failure, not
slowness.

So the plan's question has an answer: **no, the contract does not survive real
storage, and it was already broken by the second sink written against it.** The
first consumer to wire a sink did not misread the contract; the repository's own
example did.

### Why this is not a one-line fix

The obvious repair — copy the store's enqueue-and-drop — is wrong here, and the
reason is worth stating before anybody applies it.

A dropped record is acceptable for a telemetry store: the SQLite sink exists so
somebody can query what happened, and a gap under load is a counter to look at.
It is not acceptable for a capture. A recording with a frame missing does not
replay, and a recording that does not replay is not evidence — which is the whole
premise of `docs/verification/2026-08-17-capture-oracle.md`. Silently dropping
would convert a stalled proxy into a corrupt oracle, and the corrupt oracle is
the worse failure: it looks like a file.

That leaves three honest options, none of them free:

1. **Bounded queue, and end the session when it fills.** The capture stays whole
   or stops existing, and the session that outran its recorder dies with an error
   naming why. This preserves the oracle's premise and keeps the pump free.
2. **Say that this sink may block, and take it off the pump.** Give the recorder
   its own goroutine per session inside the sink, with the queue unbounded in
   principle and bounded by memory in practice. Trades a stall for a heap.
3. **Enforce the contract in the core** rather than documenting it: call sinks
   through a bounded queue owned by the session, and let the core decide what a
   full queue means. This is what the contract's own comment argues against —
   "a core that owned that goroutine could not tune it for anyone" — and that
   argument is weaker now that the tree has one sink that got it right and one
   that did not.

Recommendation: option 1 for the capture sink, because a capture's contract with
its reader is stricter than a proxy's contract with its client, and losing the
session is the honest way to say so. Option 3 deserves a second look regardless,
since the documented-not-enforced approach has now failed once inside a
repository whose author wrote the document.

### What was done

Option 1, in the capture sink. Records are built on the pump as before — the
ordering the `withhold`-before-`advance` split depends on is not something to move
— and handed to one writer goroutine per session through a bounded queue, so the
file's write no longer happens on the connection. A full queue fails the
recording and ends the session rather than dropping a record, which needed a way
back from a sink identifier to its session: `Session.SinkID` plus a hook the
recorder supplies, since a `Sink` is handed an int64 and never a `*Session`.

`CloseSession` still waits, because a returned `CloseSession` should mean a
finished file, but the wait has a grace period now: what is queued is bounded by
`QueueDepth`, and how long one write takes is not.

Option 3 is planned rather than dropped:
`docs/2026-08-17-enforce-the-sink-contract.md`. It opens with the measurement the
decision actually turns on — what one copy per message per sink costs — because
the argument in `sink.go`'s comment is about who copies the bytes, not about who
owns the goroutine.

## Un-swapping: not needed, and the reason is better than "not yet"

First, a correction to the premise. The sibling `proxy` repository holds no Go
code at all — one plan document, `2026-08-15-drop-server-dependency.md`, and
nothing to migrate. `mcgl-proxy` is the only other consumer, and it installs an
XOR cipher once at session setup rather than the AES-CFB8 the plan had in mind.
So "the first migration" has produced one consumer, not two, and the reading
below is drawn from it and from `examples/cipher`.

Neither removes a layer. Both install one and leave it: `mcgl-proxy` sets its
cipher once during setup, and `examples/cipher` swaps in CFB8 after the key
exchange. `minecraft-protocol`'s own conduit goes further and makes removal
impossible on purpose — a second `EnableEncryption` returns `ErrEncryptionEnabled`,
and there is no `DisableEncryption` to pair with it. `relay.Conduit.Swap` is
append-only in the same spirit: it composes read transforms oldest-first and
write transforms newest-first, and no branch ever unwraps one.

The interesting part is compression, which looks like the counter-example and
isn't. A threshold of `-1` genuinely does arrive mid-session and genuinely does
turn compression off — `compressionForThreshold` returns a control with `Enabled`
false, and the per-message codecs branch on it. But that is a config struct being
replaced, not a layer being torn down, because compression was never a layer here
in the first place. `conduit.go:18` says so outright:

> Compression is deliberately not this. A negotiated compression threshold
> compresses each message independently inside the frame envelope, so nothing
> carries from one frame to the next and it belongs to the `Framer`, which already
> owns the envelope.

So the one negotiated thing in this protocol family that *is* reversible was
deliberately kept out of `Transform`, and the one thing `Transform` exists for is
a keystream, which cannot be reversed mid-connection because it does not restart.
That is a structural answer rather than a survey result: **un-swapping is not
needed, and the design already routes the reversible case somewhere it is free.**

When this was written the disable path had never run here, so the claim rested on
reading `compressionForThreshold` and `conduit.go:18`. It runs now.
`codec_test.go` takes a threshold from 0 up to 256 and then to −1, through
protocol 47's play-state set compression, asserting after each change that a
packet decodes as itself rather than merely without error. `login_test.go` drives
the same two changes through the proxy into a recording and puts each recording
through the replay gate, and a companion test corrupts a recorded threshold to
show the gate can refuse. The conclusion is unchanged; what backs it is no longer
"the code says".

The thing worth keeping: if a protocol ever does hand a layer back, the hard part
is not `Transform`. It is the guard `0.2.1` added — a swap that changes the read
side is refused while buffered bytes from before the boundary remain — because a
removal has to satisfy exactly the same ordering, in the same place, with the
buffer running the other way.
