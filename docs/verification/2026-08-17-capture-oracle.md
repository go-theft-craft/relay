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

## What the stub covers, and what it does not

The argument above is unchanged, but the line has moved since this procedure was
first run, and somebody about to spend an evening with a Minecraft client should
know where it now sits.

Compression is regression-checked end to end. `examples/minecraft/login_test.go`
drives a stub login through the proxy at four threshold scripts — vanilla's 256,
1, a change from 1 up to 256 after the join, and a change from 256 to −1 that
turns compression off mid-session — and runs each recording through the same
`replaycheck` gate step 5 below runs. `codec_test.go` covers the same three
transitions one layer down, asserting the identity of the packet that came back
rather than the absence of an error, because a frame read under the wrong
envelope can decode cleanly into the wrong packet. The gate's own negative case
is covered too: a recording rewritten with its last threshold flipped from
disabled back to enabled is refused, which is what says the passes above mean
something.

So steps 5 and 6 against a compressed session are now a confirmation rather than
a discovery, in both directions of the threshold.

**Encryption was the honest remainder, and is no longer.**
`examples/minecraft/encryption_test.go` runs the exchange: a stub server that
asks for a key, a stub client that answers with one, and AES-CFB8 in both
directions afterwards. The proxy stands between them without taking part, which
is what it does in front of a real online-mode server.

That run found the claim this document made about it to be false. An online-mode
login did *not* continue as opaque passthrough — the codec stopped decoding as
documented, and the framer went on reading length prefixes out of ciphertext,
took a phantom 13-byte frame, then read a prefix announcing 1.7 MB and parked
waiting for it. Under the frame limit, so nothing was refused; no error, and the
one log line was the codec's own notice that it had stopped decoding, which reads
exactly like the documented behaviour working. The login never completed. Both
seams now agree on where the stream stops being readable, and the round trip
across the switch is what holds them to it.

The recorder was wrong in a quieter way and is fixed with it: it was rebuilding a
length prefix around chunks that never had one, and withholding the occasional
chunk because ciphertext had produced a packet identifier that looked like key
material. A capture of an online-mode login is now a login, both halves of the
exchange withheld, and a secret record marking where the recording stopped being
able to see — which still replays to its own digest, because a file that stops
early has to be a recording or it is nothing.

**So an online-mode session is worth taking, and it is worth knowing what it will
look like.** `verify` will say `ok` over a handful of records, and `trace` will
report no trajectories, because the capture holds a login and a marker. That is
the correct answer rather than the empty-trace failure this document's list names
below: that rule is about a capture that *reached play*, and this one never does.
A capture whose secret record is missing, or that holds anything after it, is the
finding.

What a person with a real client is still holding one for is the
shared-misunderstanding problem the section above describes — every byte in every
test here, enciphered or not, was produced by this repository's own encoder.

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

8. Optional, and only for somebody holding a paid account: set the vanilla
   server to `online-mode=true`, log in through the proxy once, and disconnect.

   Expected: the client reaches the world normally — the proxy relays the key
   exchange without taking part in it — and the capture holds the login, both
   halves of the exchange withheld, a secret record marking the switch, and
   nothing after it. `verify` says `ok`; `trace` reports no trajectories.

   This is the one step the stub cannot stand in for, because the client is the
   half that talks to Mojang. It is optional because what it adds over the stub
   is a real client's key exchange rather than a real client's gameplay, and
   because an account is not something a procedure can require.

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

A unit test pins the ordering: it asserts every threshold-carrying frame keeps
its body — the raw record as well as the packet record, since a replay reads the
first and the two are withheld together — and it fails if the two calls are
swapped back. It asks that of the changes made in play as well as the one made
during the login, because the last threshold in a session is the one that
decides the envelope every later frame wears.

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

### Step 7: the same binary in front of our own server

Run against `server` built from its own tree, on port 25567, `-online-mode=false`,
`-generator flat`, and its default `-compression-threshold` of 256 — so this is
also a second protocol implementation negotiating compression, and a second
check on the fix rather than a repeat of the first.

The same `mcrelay` binary, unchanged and given nothing but a different
`-upstream`, recorded one login and one walk:

- `verify`: ok, 212 records, replays to its own digest.
- `trace`: one player trace, 11 samples — the server's opening teleport at
  (0.5, 5, 0.5), then exactly the ten steps of 0.2 the client sent, ending at
  (2.5, 5, 0.5). Ground truth and extraction agree to the last digit again.

The first sample carries `onGround=false` because a teleport says nothing about
footing, which is what the extractor documents it will do rather than a
disagreement.

That settles what step 7 was for: recording a session against our own server
needs no code in that server, so its packet log never has to grow.

### Where the recordings are

Not in this repository, and they must not be put here. `.gitignore` excludes
`*.mccap` because a recording holds player UUIDs, usernames, and chat, and every
file above is a real session played by a real account.

They are at `../oracle-evidence/2026-08-17-relay-capture/`, alongside a `README`
saying what each one demonstrates, a `SHA256SUMS` over all of them, the two
scripted clients, both server logs, and the `mcrelay` binary built from `83bfe15`
that the verdicts were taken with. Digests of the files this document names:

```
79d88e5faa6dbff8  recordings/20260817-125838-002.mccap  the defect
53700ff62da7ed1e  recordings/20260817-130234-003.mccap  the defect, third-party client
4c28f8ed9121af14  recordings/20260817-130323-004.mccap  control arm, compression off
5eb4478279b1422a  recordings/20260817-130923-005.mccap  the dropped item
a161df901889c8de  recordings/20260817-131254-007.mccap  the first real arrow
4d11607a65349027  fixed/20260817-132050-001.mccap       the fix on the wire
90f0bfa0091c17bb  fixed/20260817-132217-003.mccap       real client, compressed, fixed
7236f03c8c9fa679  ourserver/20260817-133017-001.mccap   step 7
```

Every verdict in this document was re-run against those files in that location
after they were moved there, and each reproduced — including both failures.

### Still open

Step 8, which was added after this run and is optional: no online-mode session
has been recorded against a real server. What it would add over
`encryption_test.go` is a real client's key exchange rather than a stub's, and it
needs a paid account. Every other step has a result above.

One thing worth stating plainly about what this evidence cannot outlive: the two
failing recordings are the only artifacts of the defect, and a capture taken
before the fix cannot be repaired. If they are lost, the defect becomes a claim
in a document rather than something anybody can re-run.

---

## Protocol 775

**Status: RUN 2026-08-18 against a pinned vanilla 26.1.2 server, with a headless
client rather than a vanilla one.** The gate passed, the relative-move scale was
measured rather than assumed, and one finding came out of it that belongs to
`minecraft-protocol`.

Everything in this section is the 775 half of M9.1b. The procedure is the one
above with two changes: `-protocol` is left at its default, since 775 is it, and
the client was `headless-minecraft`'s rather than Mojang's.

### What was running

| Piece | Identity |
| --- | --- |
| Server | vanilla 26.1.2, `server/executable.jar` prepared by `mcreference`, `sha256 4723380bd2a0a0206719b50f2e390383afdaf82b0a76a0d573baf788e6aa3e86` |
| Server configuration | offline mode, flat world, seed `orbit1889`, compression at vanilla's default 256, port 25666 |
| Proxy | `mcrelay` from `409e270`, `-protocol java/26.1 -upstream 127.0.0.1:25666 -listen 25665 -record ./recordings` |
| Client | `headless-minecraft`'s client, standing still |

The entities that carry the measurement were summoned from the server console,
so their starting positions are exact and stated by the operator rather than
inferred:

```
summon minecraft:arrow -4.5 -55.0 9.5 {Motion:[0.10d,0.0d,0.0d]}
summon minecraft:arrow -4.5 -52.0 11.5 {Motion:[0.0d,0.0d,0.05d]}
summon minecraft:item  -3.5 -55.0 9.5 {Item:{id:"minecraft:stone",count:1}}
```

### The gate

```
mcrelay verify ./recordings/*.mccap
ok 20260818-073841-001.mccap: 2276 records, replays to its own digest
ok 20260818-074339-002.mccap: 192 records, replays to its own digest
ok 20260818-074505-003.mccap: 230 records, replays to its own digest
```

That is M9.1b's gate: a 775 recording replays deterministically from a real
26.1.2 server, through the same codec, sink, and replay path the 47 lane uses.

### The trajectories

`mcrelay trace` on the first recording returns eight, and they are the right
eight:

```
1   player  1 sample    (-4.5, -60, 9.5)
6   living  134         (14.5, -60, 28.5)  -> (7.858, -59.879, 32.785)
27  living  146         (15.5, -60, 28.5)  -> (-3.963, -59.58, 10.941)
66  living  68          (14.5, -60, 28.5)  -> (12.647, -60, 32.08)
73  arrow   22          (-4.5, -55, 9.5)   -> (-3.155, -59.95, 9.5)
76  item    2           (-3.5, -55, 9.5)   -> (-3.5, -55, 9.5)
77  arrow   20          (-4.5, -52, 11.5)  -> (-4.5, -59.95, 12.315)
105 item    3           (-4.559, -58.68, 9.403) -> (-3.302, -60, 11.9)
```

The families come from 26.1's own entity registry, because 775 consolidated 47's
several spawn packets into one carrying a type ID. The three `living` traces are
slimes, which a flat world spawns whatever `spawn-monsters` says. The player's
own trace holds one sample because the client stood still: a 775 client that is
not moving sends the rotation-only packet, which carries no position.

### The relative-move scale, measured

The constant this section exists to settle is `relMoveScale` in
`examples/minecraft/trace/v26_1.go`. It was measured, not read out of a
document.

Both arrows fell onto the same flat surface and each reported its whole fall as
a single relative move, from starting heights three blocks apart:

```
arrow at y = -55:  DY = -20275
arrow at y = -52:  DY = -32563
```

Landing on one surface means the difference in units is the difference in
blocks:

```
(32563 - 20275) / 3 = 4096 units per block
```

The check that it is right rather than merely self-consistent: both landings
then resolve to y = -59.949951171875, the same number from two different
heights. Protocol 47's scale is 32 with an `int8` delta; reading a 775 delta at
47's scale would have shrunk every relative move by a factor of 128, which is
the kind of error that produces a trajectory of the right shape and the wrong
size.

The dump the numbers come from was produced by a throwaway decoder reading the
generated 775 packet types directly, with no involvement from the trace
extractor, so the extractor cannot have agreed with itself.

### The finding: 775 velocities did not decode, and why

**Diagnosed and fixed the same day.** The section below is what was observed,
kept as written because the shape of the defect is the point. The cause is at
the end.

Positions came out right. Velocities did not, and the evidence is strong enough
to state plainly:

- Values repeat to sixteen digits across unrelated entities minutes apart —
  `0.9942013062320698` and `0.8588170664713421` appear again and again. No real
  velocity does that.
- A `packet_entity_velocity` for a slime disagrees with the
  `packet_sync_entity_position` that follows it for the same entity, whose own
  `float64` delta is physically plausible.
- Nothing fails while this happens: every one of the 1098 play records in the
  session decoded without error.

The suspect is the `lpVec3` codec in `minecraft-protocol/wire/java/vector.go`.
Its encoder and decoder agree with each other exactly, so a round-trip test
passes while a vector written by vanilla is read as something else — the same
shape of defect as the compression bug above, and the reason this document
exists. `lpVec3` is six bytes without its continuation, and so is the three-`i16`
encoding it replaced, so a mis-specified field would consume exactly the right
number of bytes and produce a valid-looking vector.

One known-motion sample, for whoever fixes it. An arrow summoned with
`Motion:[0.10d,0.0d,0.0d]` produced this spawn packet, with the velocity field
isolated:

```
raw   00 01 49 99fc0184326f416ab6005d0b70e8a3f4 06
      c012000000000000 c04b800000000000 4023000000000000   x=-4.5 y=-55 z=9.5
      29 33 7f fe ff fe                                    velocity
      00 00 00 00                                          pitch yaw headPitch objectData
```

Our writer encodes `LPVec3{X: 0.1}` as `29 33 fe ff fe 7f` — the same first two
bytes and the last four in the opposite order. That is not a byte-order fix,
though: reading vanilla's six bytes back at every permutation of the three
component shifts, both 48-bit byte orders, and biased, unsigned, and signed
fifteen-bit mappings produces nothing near `(0.1, 0, 0)`. Whatever vanilla is
doing is a different packing, not our packing written backwards. A second sample
is in the same file, an arrow summoned with `Motion:[0.0d,0.0d,0.05d]`, whose
velocity bytes are `f9 ff 86 64 ff fd`. The zero case does match: an item
summoned with no motion encodes as the single byte `00`, which is what
`WriteLPVec3` produces.

**The cause: byte order, in both directions at once.**
`net/minecraft/network/LpVec3.java` in the decompiled 26.1.2 server writes the
packed vector as `writeByte(buffer)`, `writeByte(buffer >> 8)`,
`writeInt(buffer >> 16)` — and Netty's `writeInt` is big endian. This project's
`wire/java` wrote and read all forty-eight bits little endian. Wrong in both
directions is why every round-trip test passed: the package agreed with itself
about a layout no server uses. The low sixteen bits happen to match, which is
why the first two bytes of our encoding of `{0.1, 0, 0}` and vanilla's were
identical and the last four were reversed.

Fixed in `minecraft-protocol` as `fix(java): read the byte order vanilla writes
an LPVec3 in`, with a test that reads the captured velocity fields above and
re-encodes them byte for byte. Re-running the extraction over the same recording
with the fix in place:

```
arrow 73  velocity {0.1, 0, 0}       summoned with Motion:[0.10d,0.0d,0.0d]
arrow 77  velocity {0, 0, 0.05}      summoned with Motion:[0.0d,0.0d,0.05d]
item 105  velocity {0.102, 0.2, 0.203}   a dropped item: vanilla tosses at y = 0.2
slime 27  velocity {-0.194, 0.333, 0.040} a jump
slime 66  velocity {-0.002, -0.078, 0.005} falling
```

Every one of those is the number the game states or a value physics explains,
where before they were repeats of 0.9942 and 0.8588.

`relay` pins released versions of `minecraft-protocol`. The run above was made
with a temporary `replace`; the fix shipped in v0.6.0 and this repository now
takes it, so the numbers above are what a build of this tree produces —
re-extracting the same recording with `mcrelay` built from the pinned version
gives the two summoned arrows exactly the motion they were summoned with.
Positions were never affected: they arrive as `float64` fields and `int16`
deltas, neither of which goes through that codec.

### What this run does not cover

No real vanilla 26.1.2 client was used, because none is installed on this
machine. The wire format is settled by the server's own frames, which is what
every number above comes from, but the packet mix a real client produces —
inventory, digging, chat, animation, and a player trajectory with more than one
sample in it — is not exercised. That is the same gap M9.1 closed for 1.8.9 by
sitting down with a client for an evening, and it stays open here.

Two smaller oddities, recorded because the next person will hit them:

- Only the first session after a server start saw entity traffic. The second and
  third connected, joined, and received about 200 records each with no spawns.
  The client logs `the session was placed and has loaded no chunk` in those
  runs. Not diagnosed; it did not block the measurement, which the first session
  carries.
- Slimes appear on a flat world with `spawn-monsters=false`. They are what the
  three `living` traces are.

### Where the recordings are

`../oracle-evidence/2026-08-18-relay-775/`, with the server log and
configuration, the raw packet dump the scale was measured from, the extractor's
JSON output, a `SHA256SUMS` over all of it, and the `mcrelay` binary built from
`409e270` that the verdicts were taken with.
