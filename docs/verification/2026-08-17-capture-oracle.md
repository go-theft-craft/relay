# Capture oracle: live verification procedure

**Status: RUN 2026-08-17. One defect found, fixed, and the fix confirmed on the
wire.** The procedure was executed against a real vanilla server; the results are
at the bottom. Captures taken from a server with compression enabled — which is
every vanilla server at its default settings — did not replay, because the frame
that enables compression was being withheld as though it were key material. With
that fixed, a fresh capture at vanilla's default threshold replays to its own
digest and traces. Recordings taken **before** the fix stay unreadable; the
files this document names as failures cannot be repaired after the fact.

## Why an automated pass is not enough

Every test behind M9.1 uses a stub upstream whose packets this repository
generated. That proves the recorder agrees with our own encoder, which is
exactly the agreement an oracle cannot rely on: a shared misunderstanding of the
wire format passes every one of those tests and produces recordings that are
confidently wrong.

Only a real vanilla server and a real client settle it. This is the same split
M8.1 used for its transcribed constants, and the same one `minecraft-protocol`
M4 hit when a defect invisible to every automated test showed up against a
vanilla client.

## What to run

Requirements: a pinned vanilla 1.8.9 server with `online-mode=false`, a real
1.8.9 client, and a build of `mcrelay` from this tree.

1. Start the vanilla server on a port that is not 25565.

2. Start the proxy in front of it:

   ```
   mcrelay -protocol java/1.8.9 -upstream 127.0.0.1:<server port> \
           -listen 25565 -record ./recordings
   ```

   `-protocol` is not optional here. The default is 775, and a 47 session
   recorded under a 775 header will not replay.

3. Connect the real client to `127.0.0.1:25565` and perform, in one session:
   walk, sprint, jump, fall from a height, collide with a wall, drop an item,
   and shoot an arrow. The last two are what M9.2 is verified against.

4. Disconnect cleanly, so the recording gets its trailer.

5. Run the gate:

   ```
   mcrelay verify ./recordings/*.mccap
   ```

   Expected: `ok`, with a record count and "replays to its own digest".

6. Extract the trajectories:

   ```
   mcrelay trace -in ./recordings/<file>.mccap -out traces.json
   ```

   Expected: a trace for the player, one for the dropped item, and one for the
   arrow, each with samples whose positions move the way the session looked.

   The player's trace is built from what the client reported, not from what the
   server narrated — a server neither spawns a client to itself nor describes
   its walking back to it. So expect that trace to be dense while the player is
   moving and to hold the server's teleports as well, and expect the other two
   to be sampled only as often as the server sent movement for them.

7. Repeat step 2 against our own `server` instead of the vanilla one, and
   capture one login and one walk. This proves the second use of the same
   binary — the packet log `server` therefore never has to grow — and costs a
   few minutes.

## What would count as a failure

- `verify` reporting anything but `ok`.
- A trace whose positions do not match what the session looked like, beyond one
  thirty-second of a block. Protocol 47 sends fixed-point positions, so that is
  the finest resolution a capture can carry; a disagreement larger than it is a
  wrong scale or a wrong axis order, not drift.
- Any recording holding a login body that should have been withheld. An offline
  login exchanges no keys, so this should be vacuous — if it is not, the
  redaction path has a hole the automated test could not reach.
- A trace for an entity nobody spawned, which the extractor reports as an error
  rather than producing.
- `trace` exiting zero with an empty trace list. This one is here because the
  run below found it and neither this list nor any automated test named it: a
  recording whose every record failed to decode produced a clean exit and no
  trajectories, which reads as "nothing moved" when it means "nothing was read".

- Note while judging the two above that a capture can be unreadable while a
  handful of its frames still parse. `arm_animation` carries no fields in
  protocol 47, so a mis-parse of it cannot fail; 413 of them decoded out of a
  wholly unreadable session. "Some frames decoded" is not evidence that a file
  was read.

## Results

Run 2026-08-17. The defect was found against commit `f6d07be` and the fix is the
change this document ships with; every "fixed" row below was recorded with it.

### What was on the wire

- Server: vanilla 1.8.9, `server.jar` sha1 `b58b2ceb36e01bcd8dbf49c8fb66c55a9f0676cd`
  as named by Mojang's own version manifest, on OpenJDK 8u502, `online-mode=false`,
  port 25566, a throwaway world at seed `oracle1889`.
- Proxy: `mcrelay` built from this tree, `-protocol java/1.8.9`, listening on
  25575 rather than the 25565 the procedure names, because another server was
  already bound there.
- Clients: two of them. A real 1.8.9 client for sessions 002, 007, and fixed/003,
  which walked, dug, dropped, and shot a bow. The pinned third-party Node
  `minecraft-protocol` 1.66.2 driven by a script for the rest, which is not a
  real client but is not our encoder either — it has never read our codec, which
  is the property the oracle actually needs.

### The defect

A capture taken from a server with compression enabled does not replay:

```
FAIL 20260817-125838-002.mccap: sequence 7: decode: decode java/1.8.9 packet
disconnect: field disconnect: trailing bytes: 50 unread
```

The recorder withholds the `compress` packet as though it were key material, so
the capture loses the threshold. `Recorder.Message` computes the state pair
before it asks whether to withhold, and `states` applies the transition to the
same session `withhold` then consults — so the set-compression frame is judged
with compression already on, cannot be decoded under an envelope it does not
carry, and `SensitiveFrame` fails closed on the grounds that a login frame it
cannot read cannot be shown to be harmless. Both the raw record and the packet
record come out empty.

Everything after it is stored still wearing the compression envelope while the
replaying session believes no compression was negotiated. Sequence 7 is the
`success` frame: its leading data-length `0x00` is read as a packet ID, which in
login state clientbound is `disconnect`, and the 50 bytes that follow the string
field are the rest of the real packet.

Both arms were run to settle it:

| Session | Client | `network-compression-threshold` | `verify` |
| --- | --- | --- | --- |
| 002 | real 1.8.9 client | 256 (vanilla default) | FAIL, sequence 7 |
| 003 | scripted Node client | 256 (vanilla default) | FAIL, sequence 7 |
| 004 | scripted Node client | -1 (disabled) | ok, 4474 records, digest `0210deb4…` |
| 005 | scripted Node client | -1 (disabled) | ok, 4226 records |
| 007 | real 1.8.9 client | -1 (disabled) | ok, 2596 records |
| fixed/003 | real 1.8.9 client | 256, **recorder fixed** | ok, 5304 records |
| fixed/001 | scripted Node client | 256, **recorder fixed** | ok, 5560 records |

A decode census over 002 puts a number on the damage: 2055
`entity_head_rotation` and 1584 `rel_entity_move` fail, along with every other
packet that carries a field, each mis-read as `keep_alive` — play's ID zero — for
the same reason. Only bodiless packets survive, which matters later.

`trace` on that same file exits **zero** and writes `"traces": []`. Every record
was undecodable, and an undecodable packet is deliberately not fatal there, so
15916 records skipped one at a time produce a successful run with no
trajectories in it. `ExtractFile` already says a truncated trajectory must never
look like a complete one; this is that failure through a different door.

### What passed, with compression disabled

Against sessions 004 and 005, on commit `f6d07be`:

- `verify` reported `ok` on both, replaying each to its own digest.
- The player trajectory reproduces the doubles the client sent, exactly: the ten
  walk steps of 0.2, the sprint steps of 0.3, the jump arc, the descent, and a
  final position of (-103.1, 67, 226.5) matching what the client itself reported
  as its last position. No axis is transposed and no scale is off.
- Fixed-point positions convert correctly. Every object spawn matches the
  position the third-party client computed independently, to the bit —
  -126.5625 is -4050/32 — so the divide-by-32 and the axis order are right and
  the resolution the doc asks about is not being lost.
- The dropped item traces: it spawns at the player's position plus eye height
  (-88.406, 68.312, 226.500 against a player at -88.4, 67, 226.5), then falls to
  y=63 and flies west, which is where yaw 90 was pointing.
- No trace was produced for an entity nobody spawned, and no login body was
  recorded that should have been withheld. The second is vacuous as predicted —
  an offline login exchanges no keys — and the redaction path erred in the
  opposite direction, withholding something it needed to keep.

### The arrow

M9.2's other target has real-client evidence. Session 007 was driven by a real
1.8.9 client, verifies at 2596 records, and holds one arrow, entity 1024:

```
(-127.844, 65.500, 230.812)  launch, eye height above a player at y=64
(-125.531, 64.688, 229.125)  d=(+2.312, -0.812, -1.688)
(-113.125, 57.625, 220.156)  d=(+12.406, -7.062, -8.969)
(-113.125, 56.031, 220.156)  impact, then still
```

The drop accelerates, the horizontal track is straight, the flight ends against
something and stops, and every sample lies exactly on the 1/32 grid — which is
the resolution the failure list above says is the finest a capture can carry. The
scripted client cannot produce this: vanilla does not accept its bow draw, so
sessions 004, 005, and fixed/001 spawn no arrow. This one needed the real client.

A second real-client bow shot, `fixed/003`, was recorded after the fix with the
threshold at its default 256 — which makes it the only session that is a real
client, under compression, through the fixed recorder, all three at once. It
verifies at 5304 records and traces 86 entities including two arrows. Entity 831:

```
(-128.625, 65.500, 230.031)  launch at eye height
(-125.812, 64.500, 230.125)
(-113.469, 57.562, 230.531)
(-113.469, 57.031, 230.531)  impact, then four identical samples while the
                             server keeps teleporting an embedded arrow
```

Every value on the grid again: 65.5 is 2096/32 and -128.625 is -4116/32.

### The fix, confirmed on the wire

The recorder now asks whether to withhold a frame before applying that frame's
own transition, so set compression is judged under the pipeline it arrived on.
Capture `fixed/001` was taken afterwards, against the same vanilla server with
`network-compression-threshold` back at its default 256 — the exact
configuration that had failed an hour earlier:

- `verify`: ok, 5560 records, replays to its own digest.
- `trace`: 135 traces, the player among them with 59 samples ending at
  (-73.7, 67, 226.5), which is the position the client itself reported as its
  last. Ground truth and extraction agree to the last digit.

A unit test pins the ordering: it asserts the set compression record keeps its
body, and it fails if the two calls are swapped back.

### The second defect, and why counting decodes could not close it

`trace` exited zero with an empty trace list on an unreadable capture. The first
attempt at a gate errored only when no play packet decoded at all, and that
never happens: over the unreadable 002, 7954 play records were offered and 431
decoded — 413 of them `arm_animation`, which carries no fields in protocol 47 and
so cannot fail to parse. Any frame landing on that ID decodes into a valid empty
packet, and 413 arm swings in fifty seconds keep the counter off zero forever.

The rule that holds is about motion rather than decoding: a capture that reached
play and produced no trajectories has failed to be read, whatever fraction of its
frames technically parsed. A play session always receives a clientbound position,
because the server places the player before they can act. Confirmed across four
real files rather than synthetic ones — `recordings/002` exits 1, while
`fixed/003`, `fixed/001`, and `recordings/007` all exit 0.

One stated consequence: a play capture too short to hold any position packet now
trips this. That is the right answer — a file that short is not evidence either.

### Still open

- **Step 7**, the same binary in front of our own `server`, was not run. It is
  the only item from the procedure above that has no result.
