# Plan: the encryption remainder

> **For agentic workers:** steps use checkbox (`- [ ]`) syntax for tracking. Run
> every command as `devbox run -- task <name>` and
> `devbox run -- task test:examples`.

**Goal:** run an online-mode login through the proxy and hold the three seams to
what this tree already claims about it — that the codec stops decoding, that the
session continues as opaque passthrough, and that the recorder records honestly.

**Why now:** `docs/verification/2026-08-17-capture-oracle.md` names encryption as
the honest remainder, and the compression plan parked it as "a different plan".
This is that plan. The gap was never that the behaviour was undecided; it was
that nothing had ever watched it happen. `codec.go`, `framer.go`, and
`examples/README.md` each state the passthrough claim, and an offline login —
which is every login this repository had ever run, in a test or against the real
server — never reaches the branch.

What existed, and what did not:

- `examples/minecraft/codec_test.go:226` feeds the codec an encryption response
  and then asserts `ErrEncrypted` on the next frame, in both directions. That
  covers the latch and nothing around it: the bytes are hand-built, no proxy is
  running, and nothing is enciphered.
- Nothing anywhere ran a key exchange. No stub asked for a key, no client
  answered with one, and no byte in this repository had ever been enciphered by
  the mode the protocol actually uses.

**The defect class this covers:** every part that keeps working by reading the
stream, in a session where the stream has stopped being readable. The codec is
the only one that announces it stopped. A framer, a recorder, and a hook all go
on doing their jobs against bytes that no longer mean what they are being read
to mean, and none of them has a reason to say so.

> **Executed 2026-08-17. The premise held and the claim did not.** The session
> did not continue as opaque passthrough: the first run of the round-trip test
> below hung until the client's deadline and the login never completed. The codec
> stopped decoding exactly as documented, and the framer went on looking for a
> length prefix, found one in ciphertext, and waited for a frame nobody had sent.
> Details under Task 1. Two seams needed the fix; a third — the recorder — was
> filing ciphertext as frames and is fixed here too.

## What the first run found

The framer reads a VarInt length and then that many bytes. Ciphertext gives it
numbers, so it frames on them.

With the test's fixed shared secret, the server's first two enciphered frames
(login success and the opening teleport, 82 bytes of plaintext) come out as
ciphertext beginning `0d af 40 33 eb a4 92 da`. The framer reads `0x0d` and takes
a 13-byte "frame". The next byte is `0x95`, which starts a multi-byte VarInt:
`95 d4 69` decodes to **1,731,093 bytes**. That is under the 2 MiB frame limit,
so nothing is refused — the pump parks waiting for 1.7 MB that will never arrive,
holding the rest of the login behind it.

That is the worst shape a failure can have here. No error is returned, the one
log line the session emits is the codec's `ErrEncrypted` notice — which reads as
the documented behaviour working — and the session simply stops. The number is
not luck either: half of all bytes are a valid one-byte length, so a proxy in
this state chops the stream into phantom frames until one prefix asks for more
than has arrived, which for a real session is a matter of a few frames.

Byte preservation is not the problem, and that is worth saying because it is what
makes the failure quiet. A frame read as `length + payload` and written back the
same way reproduces the input exactly, so every byte that gets through is
correct. What breaks is *when*: the proxy will not release a byte until the
phantom frame it invented is complete.

## Tasks

### Task 1: A login that exchanges keys

**Files:** add `examples/minecraft/encryption_test.go`.

- [x] **Step 1:** Write the two halves of an online-mode login as stubs — a server
      that sends `encryption_begin` with a real RSA public key and a verify token,
      and a client that answers with both encrypted under it — and switch both
      transports to AES-CFB8 afterwards. Generate the RSA key per run; a private
      key in a repository is a finding whatever it protects, and `task secrets`
      would say so.

      *CFB8 is written out in the test rather than taken from the standard
      library, which offers full-block CFB only. The difference is the point:
      CFB8 is byte-granular, so a frame's ciphertext is exactly as long as its
      plaintext and leaves as soon as it is written, which is what puts a framing
      proxy in front of a length prefix it cannot read.*
- [x] **Step 2:** Fix the shared secret. Every byte after the switch is a function
      of it, and so is where a proxy that cannot read the stream decides one
      message ends — a random secret makes the outcome a coin flip per run rather
      than a result.
- [x] **Step 3:** Assert the round trip rather than the absence of an error: two
      packets decoded by the client and one by the server, across the switch.
      CFB8 feeds every byte into the next byte's keystream, so anything added,
      dropped, or reordered turns the rest of the stream into noise — which makes
      a completed round trip a proof that the proxy passed the bytes through
      exactly.

### Task 2: Make the passthrough claim true

**Files:** modify `examples/minecraft/framer.go`, `examples/minecraft/codec.go`;
add `examples/minecraft/encryption.go`.

- [x] **Step 1:** Give the session a latch the codec sets and the framers read.
      The codec is the only part that can see the exchange happen, because seeing
      it means decoding a packet; the framers are the only parts that can act on
      it. `relay.Config.NewFramer` already documents session metadata as the place
      for exactly this, so this is the seam being used rather than a new one.
- [x] **Step 2:** Stop framing when it latches: hand up whatever arrived, and
      write it back with no prefix. Read the first byte in `ReadMessage` and check
      the latch *after* it, because the clientbound pump is parked on that byte
      when the exchange completes on the other pump — a check only on entry is a
      check that pump never reaches.
- [x] **Step 3:** Key the write side off the last read rather than off the latch.
      The frame that completes the exchange is read as a frame and written after
      the latch is already set, so keying on the latch drops its length prefix and
      the upstream never learns to switch its own cipher. Confirmed by making that
      exact change: the round trip fails with the upstream closing.
- [x] **Step 4:** Wire the per-session framer through `mcrelay` and the test
      proxies, since a framer that can stop framing cannot be one instance shared
      by every session and both directions.

### Task 3: A recording that says what it could not see

**Files:** modify `examples/minecraft/capture/sink.go`.

- [x] **Step 1:** Stop rebuilding a frame around bytes that never had one. The
      recorder rebuilds the length prefix relay stripped, which for an opaque
      chunk puts a number in the file that was never on the wire — found by
      reading the capture the first passing round trip produced.
- [x] **Step 2:** Stop asking the sensitivity question about ciphertext. It reads
      a packet identifier out of the payload, and ciphertext eventually produces
      one that matches — the first capture inspected had withheld a chunk for that
      reason, which is a redaction nobody could reconstruct a cause for.
- [x] **Step 3:** Mark the switch instead of recording past it. The format has a
      secret record for exactly this: it carries no material unless the writer
      discloses, replay skips it, and the digest excludes it. Without it a reader
      cannot tell an online-mode login from a recorder that stopped for a reason
      nobody wrote down, and those two files must not look alike.
- [x] **Step 4:** Assert the file, not just the run: both halves of the exchange
      withheld in their packet *and* raw records, exactly one secret record
      carrying nothing, no record of any kind after it, and the capture still
      passing the replay gate. A capture that stops early has to still be a
      recording.

      *Asserted twice, at both altitudes. `encryption_test.go` reads back the
      file a real proxy run produced; `capture/sink_test.go` drives the recorder
      directly, beside the tests for the redaction rules it sits next to. Each
      fails on its own when the transition is removed.*

### Task 4: Say what is still live-only

**Files:** modify `docs/verification/2026-08-17-capture-oracle.md`,
`examples/README.md`, `CHANGELOG.md`.

- [x] **Step 1:** Replace the "honest remainder" section with what the stub now
      covers and what it still does not.
- [x] **Step 2:** Correct the passthrough claim wherever it is made. It was true
      of the codec and false of the session, and it is now true of both.

## Not in scope

- **Standing between an encrypted login.** Decrypting means running two key
  exchanges and holding the client's session credentials, which `codec.go`
  declines on purpose. Nothing here changes that: the proxy relays the exchange,
  it does not take part in it.
- **Authentication.** The stub server checks its verify token and no more. A real
  online-mode server also asks Mojang whether the account is who it says, which
  is a network call to somebody else's service and tests nothing about these
  seams.
- **A recording that can be decrypted later.** The proxy never holds the key, so
  there is nothing it could store that would make one possible.
