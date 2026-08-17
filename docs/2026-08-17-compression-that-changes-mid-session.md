# Plan: cover compression that changes mid-session

> **For agentic workers:** steps use checkbox (`- [ ]`) syntax for tracking. Run
> every command as `devbox run -- task <name>` and
> `devbox run -- task test:examples`.

**Goal:** put a threshold change — including the one that turns compression off —
under test at the codec, the recorder, and the replay gate, so the compression
argument this repository rests on is verified rather than read.

**Why now:** compression is the one negotiated setting in this protocol family
that is reversible, and two conclusions in this tree depend on that.
`docs/2026-08-17-after-the-plan.md` closes the un-swapping question structurally
by saying the reversible case was deliberately kept out of `Transform` and routed
to the framer, where reversing it is free. `conduit.go:18` states the same. Both
claims are about the disable path, and the disable path has never run here.

What exists today, and what does not:

- `examples/minecraft/codec_test.go:372` covers compression being *enabled*
  mid-login, at thresholds 0 and vanilla's 256.
- `examples/minecraft/login_test.go:45` covers a stub login that negotiates a
  threshold and produces a capture that replays, at thresholds 256 and 1.
- Nothing sends a second `set compression`. Nothing sends a negative threshold.
  `compressionForThreshold` in minecraft-protocol's 1.8 descriptor turns a
  negative threshold into a control with `Enabled` false, and protocol 47 has a
  play-state `set compression` as well as the login-state one, so a change after
  the login is reachable on the wire and unreached by any test.

**The defect class this covers:** every place that caches the envelope rather than
asking the session for it. The bug that motivated `login_test.go` was exactly
that shape one direction on — a frame judged under the wrong envelope — and it
survived because no test had ever changed the envelope. A threshold that goes
back to −1 hits the same code from the other side: the frame carrying it travels
*compressed* and turns compression off behind itself, which is the mirror image
of the set-compression frame that travels uncompressed and turns it on. That
ordering is what `capture/sink.go`'s `withhold`-before-`advance` split exists for,
and the mirror case has no test.

> **Executed 2026-08-17. One premise above did not survive.** The last sentence
> is wrong about the recorder: a play-state threshold change never reaches the
> `withhold`-before-`advance` split, because `SensitiveFrame` answers false
> outside the login state without reading the frame, so a recorder holding a
> stale envelope through play has nothing to be wrong about. Deleting the
> transition from `capture.advance` leaves every test added here passing. The
> split is load-bearing during the login and only there, which is where the
> existing test already pins it.
>
> The defect class is still real and now covered — deleting the same transition
> from `Codec.advance` fails the codec test and both new login cases — but it
> lands on the codec, the replay gate, and the extractor rather than on the
> recorder. `login_test.go` says so at the test it belongs to.

## Tasks

### Task 1: The codec follows a threshold that changes

**Files:** modify `examples/minecraft/codec_test.go`.

- [x] **Step 1:** Extend the existing set-compression test into a table: enable at
      0, then re-enable at 256, then disable at −1, asserting a packet decodes
      after each change. Reuse `setThreshold`, which already reaches the generated
      field by reflection.

      *Done as a second test, `TestCodecFollowsAThresholdThatChanges`, rather than
      by extending the first. Step 2 pins the sequence to protocol 47, and
      `TestCodecFollowsSetCompression` is deliberately version-neutral — folding
      them together would have skipped the enable case on the default protocol,
      which is the one it exists to cover.*
- [x] **Step 2:** Do the second and third changes in the play state, through
      `PlayClientboundSetCompression`, because that is where a change after login
      actually arrives and because it exercises a different transition branch than
      the login packet does.
- [x] **Step 3:** Assert the failure mode explicitly, not just success: after a
      disable, a frame that would have been read as compressed must decode as
      itself. A test that only checks "no error" passes against a codec that
      guessed right for the wrong reason.

### Task 2: The stub login changes its mind

**Files:** modify `examples/minecraft/login_test.go`.

- [x] **Step 1:** Teach `loginStub` to send a play-state `set compression` after
      the join, driven by a per-case script rather than a constant, so one stub
      serves "enable and stay", "enable then re-enable higher", and "enable then
      disable".
- [x] **Step 2:** Add the two new cases to the existing table. The play frames
      after the change are the assertion that matters: the same two frames in each
      direction the current cases send, decoded from the recording afterwards.
- [x] **Step 3:** Keep `assertCompressionWasRecorded` honest across a change. It
      currently proves the frame that enables compression kept its body; extend it
      to prove every threshold-carrying frame kept its body, since the last one is
      the one a replay needs to arrive at the right envelope.

### Task 3: The recording replays across the change

**Files:** modify `examples/minecraft/login_test.go`, and
`examples/minecraft/replaycheck/check.go` only if the gate cannot already answer.

- [x] **Step 1:** Run the same replay gate the live procedure uses over each new
      case's recording. A capture whose envelope changes twice and still replays is
      the whole point; anything weaker is a decode log.
- [x] **Step 2:** Confirm the gate fails when it should. Corrupt the recorded
      threshold in a copy of one capture and assert the check refuses it, the way
      `replaycheck` already refuses a recording whose play packets never decoded.
      A gate that cannot fail is not evidence.

### Task 4: Say what is still live-only

**Files:** modify `docs/verification/2026-08-17-capture-oracle.md`, and
`docs/2026-08-17-after-the-plan.md`.

- [x] **Step 1:** Update the verification procedure to name what the stub now
      covers, so a person running the live procedure knows which steps are
      regression-checked and which are the reason they are holding a Minecraft
      client. Encryption is the honest remainder: an online-mode login puts the
      codec into `ErrEncrypted` and the recorder into opaque frames, and no stub
      here does a key exchange.
- [x] **Step 2:** Amend the un-swapping section of `after-the-plan.md` to cite the
      new tests rather than the source. Its conclusion does not change; its
      evidence does, from "the code says" to "the disable path runs here".

## Not in scope

- **Encryption.** Standing between an encrypted login as a third party means
  running two key exchanges, which `codec.go:182` declines on purpose. Testing
  the encrypted path means testing that the proxy relays opaquely and that the
  recorder records honestly, which is a different plan.
- **Compression settings other than the threshold.** The policy — algorithm,
  decompressed ceiling — is stamped into the capture header and does not change
  mid-session.
