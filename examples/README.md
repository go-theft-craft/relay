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

## `minecraft` — the seam against a real protocol

**In progress.** The framer and the codec are written; the prober, the sink, the
runnable proxy, and the tests are not. Do not read this as finished work yet.

This is where the seam gets tested against a protocol that was not designed for
it, which is the only way to find out whether the seams are real. It builds on
[`minecraft-protocol`](https://github.com/go-theft-craft/minecraft-protocol).

- `Framer` adapts the Java edition frame envelope. A relay message is one frame
  payload: the length prefix is framing and belongs here, everything inside it
  is the codec's problem. It copies the payload, because `Frame.Payload` returns
  a borrowed view and `relay.Framer` promises the caller a slice it will not
  reuse.
- `Codec` holds **two** protocol sessions, not one. A `protocol.Session` fixes
  its inbound direction from the role it was built with, and a proxy reads both
  directions, so a single session could only ever decode half the traffic.
  Connection state is per direction for the same reason it is per session on a
  real endpoint.

**Decoding stops at encryption.** Once a session completes its key exchange the
codec returns `ErrEncrypted` and the relay falls back to opaque passthrough.
Standing between an encrypted login as a third party means running two key
exchanges and holding the client's session credentials, which is a project in
itself and teaches nothing about the framework seam. A consumer that did want to
do it would reach for `relay.Transform`, which is what `cipher` above
demonstrates.

### Still to build

- `prober.go` — a `relay.Prober` that performs a real status ping, so "healthy"
  means the server answered rather than that something holds the port open.
- `store/` — an async batched SQLite sink. It is a package rather than a file
  because several hundred lines of SQL and batching beside the framer would bury
  what a reader came for.
- `main.go` — flags, wiring, and a graceful stop.
- `framer_test.go`, `codec_test.go`, `proxy_test.go` — including
  `relaytest.FramerContract` against the real length-prefixed framer, which is
  the first time that harness meets anything other than a newline.
