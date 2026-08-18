# relay examples

Worked consumers of [`relay`](../), each built to prove one part of the seam
against something real.

This is a **separate module**. Every third-party dependency lives here, so the
core's `go.mod` stays empty and nothing an example imports can reach a consumer
of the framework. The `replace` directive at the top of `go.mod` points at the
working tree above; `task deps:check` fails the build if it ever drifts into the
core.

```
devbox run -- task test:examples
```

## `cipher` — mid-stream transforms

A proxy that relays in the clear, then switches both of its links onto an
AES-CTR keystream partway through the session.

`relay.Transform` needs a worked consumer and the Minecraft example cannot be
one: that protocol's compression is a framing concern rather than a stream
transform, and its cipher is negotiated inside a login the example deliberately
does not stand between. Demonstrating a transform with an identity function
would be worse than not demonstrating it, so it gets its own example — small,
real, and about one thing.

The protocol is newline-delimited lines and the key is a constant, because
neither is the point. What the example is for:

- **A proxy stands on two links, not one.** There are four keystreams here, not
  two. An AES-CTR keystream is position-dependent, so the client-side and
  upstream-side streams cannot share one; sharing any of them produces a proxy
  that works for exactly one message and then desynchronises.
- **The trigger crosses in the clear.** Both endpoints have to agree on which
  message is the last unenciphered one, and the only message both can name is
  the trigger itself. The hook forwards it through `Session.Inject` rather than
  by returning `Forward`, because a relayed message is written *after* the hook
  returns, which is too late to swap behind it.
- **The cross-direction swap.** One of the two swaps lands on the link whose
  read pump is parked inside a socket read at that moment. That is safe, and it
  is the whole reason `relay.Conduit` transforms bytes as it hands them out
  rather than as it buffers them.

It depends on nothing outside the standard library — `crypto/aes` and
`crypto/cipher` are all it needs. It lives here anyway, because the core's
`go.mod` stays empty and this is where anything runnable belongs.

The test suite is the documentation for the ordering rules. In particular
`TestSwapWithBytesBufferedIsRefused` sends past the boundary on purpose and
asserts the swap is refused with `relay.ErrSwapPending` rather than silently
corrupting the stream.

| Symbol | What it is |
| --- | --- |
| `Trigger` | the message that marks the boundary |
| `Streams(Role)` | the two keystreams one end of one link needs |
| `LineFramer` | `relay.Framer` over newline-delimited messages |
| `Hook()` | the hook that installs the keystreams |
| `New(port, upstream)` | a runnable proxy |

## `minecraft` — the seam against a real protocol, and the capture oracle

This is where the seam gets tested against a protocol that was not designed for
it, which is the only way to find out whether the seams are real. It builds on
[`minecraft-protocol`](https://github.com/go-theft-craft/minecraft-protocol).

It is also the capture tool the gameplay-verification work depends on: put it in
front of a vanilla server and it records what vanilla did, in a format that
replays. Recording belongs to a proxy rather than to either endpoint, because an
endpoint only ever sees its own half — and one of the endpoints is the thing
being judged.

| Package | What it is |
| --- | --- |
| `Framer` | `relay.Framer` over the Java frame envelope |
| `Codec` | two protocol sessions, one per direction, following the protocol's own transitions |
| `Prober` | health that means the server answered a status ping |
| `MultiSink` | fans one session out to several sinks |
| `capture/` | a `relay.Sink` writing one replayable `.mccap` per session |
| `trace/` | per-entity trajectories extracted from a recording |
| `replaycheck/` | the gate: does a recording reproduce itself |
| `store/` | an async batched SQLite sink |
| `cmd/mcrelay` | the runnable proxy, plus `trace` and `verify` |

```
mcrelay -upstream 127.0.0.1:25565 -record ./recordings   # relay and record
mcrelay verify ./recordings/*.mccap                      # does it replay?
mcrelay trace -in ./recordings/session.mccap -out t.json  # trajectories
```

`-sink-overflow` picks the core's policy for a sink that cannot keep up —
`block`, `drop`, or `end-session` — with `-sink-queue` sizing the queue the last
two use. Both sinks here honour the contract on their own, so the default leaves
the core out of it; the flags exist because a policy nobody can run is a claim.
Use `drop` on a recording proxy and the capture will be missing frames it does
not know are missing.

### What the parts are for

- **`Framer`** adapts the Java edition frame envelope. A relay message is one
  frame payload: the length prefix is framing and belongs here, everything
  inside it is the codec's problem. It copies the payload, because
  `Frame.Payload` returns a borrowed view and `relay.Framer` promises the caller
  a slice it will not reuse.
- **`Codec`** holds **two** protocol sessions, not one. A `protocol.Session`
  fixes its inbound direction from the role it was built with, and a proxy reads
  both directions, so a single session could only ever decode half the traffic.
  It does not hand-write its state machine: it asks the protocol what each
  packet implies and applies the answer to both decoders. The triggers are
  version-specific — protocol 47 moves to play on the clientbound login success,
  while 775 waits for a serverbound acknowledgement through a configuration
  state 47 does not have — so hand-writing them is a standing bug rather than a
  shortcut.
- **`capture`** records every relayed frame as a raw record, and every frame
  that decoded as a packet record besides. That is the pair a real endpoint's
  stream emits. It records what it cannot parse, and it withholds the key
  exchange: the fix `minecraft-protocol` made inside its own stream does not
  come along for free when a proxy assembles observations by hand, so the sink
  asks the protocol which frames carry secret material.
- **`trace`** accumulates relative moves onto the last absolute position, so a
  consumer gets positions rather than deltas. A move for an entity that never
  spawned is an error, not a trace starting at the origin. It reads both
  directions, because the connecting player's own trajectory is in neither the
  spawn packets nor the server's movement packets — a server does not spawn a
  client to itself, and after its opening teleport the client reports where it
  walked rather than being told. Protocol 47 only:
  the packets, their fixed-point scales, and the spawn set are all
  version-specific, and decoding 775 with 47's rules would produce plausible
  numbers that are wrong.
- **`replaycheck`** replays a recording and compares the digest with the one the
  file's own trailer carries. A recording that does not reproduce itself is not
  evidence. It is not a completeness check, and the difference matters: the
  digest covers what was written, so a recording missing records replays to its
  own digest and passes. A live run lost 783 records to
  `relay.SinkOverflowDrop` and this gate called the file ok —
  `docs/verification/2026-08-18-sink-policy-live.md`.

**Decoding still stops at encryption, and so does framing.** Once a session
completes a key exchange the codec returns `ErrEncrypted` and both framers stop
looking for length prefixes, because an enciphered stream carries none a third
party can read. Half of that is not optional: a framer that keeps parsing finds
lengths in ciphertext, and the first one that asks for more bytes than were sent
parks the session with no error to point at. The codec sets a latch on the
session and the framers read it — `relay.Config.NewFramer` documents session
metadata as the place for exactly this — and
`examples/minecraft/encryption_test.go` runs a key exchange through the proxy to
hold both to it.

A recording ends there too. The two frames carrying key material are withheld,
a secret record marks the switch, and nothing after it is filed as a frame —
ciphertext is not frames, and a capture that says so is still a capture that
replays.

Standing between an encrypted login as a third party means running two key
exchanges and holding the client's session credentials. Capture does not need
it: vanilla only exchanges keys in online mode, and an oracle wants a server
whose behaviour is vanilla rather than one whose accounts are verified. A
consumer that did want to do it would reach for `relay.Transform`, which is what
`cipher` above demonstrates.
