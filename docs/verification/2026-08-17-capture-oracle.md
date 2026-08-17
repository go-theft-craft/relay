# Capture oracle: live verification procedure

**Status: NOT RUN.** This file is the procedure and an empty result. Nothing
below has been executed against a real client or a real server. It is written
now because the automated work is finished and the person who runs it should not
have to reconstruct what to run, and it must not be read as evidence until the
results section is filled in.

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

## Results

Not yet run. Record here: the server build and its `online-mode` setting, the
client version, the digest `verify` reported, the record count, and anything
that had to be repeated.
