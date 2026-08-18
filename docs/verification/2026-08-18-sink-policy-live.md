# The sink policy in front of a real server

`docs/2026-08-17-enforce-the-sink-contract.md` left one thing open on purpose.
The capture recorder keeps its own bounded queue, the core grew one in `0.4.0`,
and running both is two queues in a row:

> Recommendation: keep the sink's queue until the core's policy has run against a
> real server, then remove one.

This is that run, and the answer it produced. Nothing here was decided from
reading the code: every line below is a session that happened.

## What was on the wire

- **Server:** vanilla 1.8.9, `server.jar` sha1
  `b58b2ceb36e01bcd8dbf49c8fb66c55a9f0676cd`, on OpenJDK 8u502,
  `online-mode=false`, port 25566, `network-compression-threshold=256`, seed
  `orbit1889`. The same server the 2026-08-17 oracle ran against, still up.
- **Proxy:** `mcrelay` built from this tree at `a945d55` plus the flags this run
  needed — `-sink-overflow`, `-sink-queue`, `-record-queue` — listening on 25585,
  upstream `127.0.0.1:25566`, `-protocol java/1.8.9`. Against
  `minecraft-protocol` v0.5.0, which is what `go mod tidy` resolves in the
  examples module rather than the v0.2.0 its committed `go.mod` names; the suite
  passes on either.
- **Client:** `headless-minecraft`'s `examples/orbit` at `c05b5c7`, with
  `-legacy` for the protocol 47 profile. It logs in, finds spawn, walks a circle,
  and quits when the server has corrected its movement six times — which is a
  bot limitation and not a proxy fault, and it ends the session cleanly either
  way.

Each arm is one proxy, one client session, then `mcrelay verify` over what was
recorded. `run.sh` in the evidence directory is the harness.

## The arms

| Arm | Flags | What the log said | `verify` |
| --- | --- | --- | --- |
| 1 | *(defaults)* | nothing | ok, 356 records |
| 2 | `-sink-overflow end-session -sink-queue 1024` | nothing | ok, 5864 records |
| 3 | `-sink-overflow end-session -sink-queue 1` | `session ended with an error … relay: sink queue overflow` | **ok, 16 records** |
| 4 | `-sink-overflow drop -sink-queue 1` | nothing — see below | ok, 3624 records |
| 5 | `-sink-overflow drop -sink-queue 1` | `sink dropped records … dropped=783` | **ok, 4152 records** |
| 6 | `-sink-overflow drop -sink-queue 1 -capture` | `dropped=1501` | **FAIL**: `sequence 5: decode … trailing bytes: 8 unread` |
| 7 | `-record-queue 1` *(core policy off)* | `capture: recorder fell behind the connection: 1 queued records unwritten, ending session` | **ok, 16 records** |
| 8 | `-sink-overflow end-session -capture` *(default depth)* | nothing | ok, 772 records |
| 9 | `-sink-overflow drop -sink-queue 1` *(shipped wiring)* | `the recorder brings its own queue …`, `dropped=614` | **ok, 3040 records** |

Arm 9 is arm 5 again after the flags and the warning took their shipped form,
and it reproduced: 614 records gone, `verify` still ok.

Arms 4 and 5 are the same configuration run twice. The first one is here because
of what it did not say: the drop counter is on `relay.Session`, nothing in the
core reads it for you, and `mcrelay` had no way to notice. Arm 5 is the same run
after `reportSinkDrops` was added, and it lost 783 records.

## What it settles

**The core's policy works in front of a real server, and its end-session arm
leaves a recording rather than a mess.** Arm 3 filled a one-deep queue during
login, ended the session with `ErrSinkOverflow`, and the file it left behind
replays to its own digest at 16 records. A capture that stops early is still a
capture; that is the whole premise of
`docs/verification/2026-08-17-capture-oracle.md`, and the core's policy honours
it without knowing anything about the format.

**The recorder's own queue produces the same outcome from the other side.** Arm 7
ran with the core policy off and the recorder's queue at depth 1: the recorder
failed the recording, ended the session through the hook `Bind` installs, and
left a file that replays to its own digest at 16 records, stopping at the same
point in the login arm 3 stopped at. Two mechanisms, one outcome.

**Neither fires at its default depth.** Arms 2 and 8 ran the core queue at 1024,
one of them with raw capture on and therefore two records per socket read, and
neither overflowed. The only arms that made a queue fire had depth 1, set to make
it fire.

**`SinkOverflowDrop` in front of a recorder produces a file that lies, and the
replay gate does not catch it.** This is the finding worth the run. Arm 5 lost
783 records — about 16% of the session — and `mcrelay verify` called the result
ok, because the digest in the trailer covers what was written and not what
crossed the wire. Nothing in the file says a frame is missing.

What caught it was the layer above: `mcrelay trace` refused the same recording
with `movement for an entity that never spawned: entity 54823813`, because a
trajectory needs a spawn packet the file no longer held. That is luck rather than
a check — a drop that took only movement records would have left both the gate
and the extractor clean, and the trace would have been shorter and wrong. Arm 6
failed the gate for a different reason again: with raw capture on, the dropped
chunks broke the byte stream replay decodes, so it failed at sequence 5 rather
than reporting a hole.

So the ordering the design assumed holds up: for a capture, ending the session is
honest and dropping is not, and the file cannot be trusted to tell the difference
afterwards.

## The decision

**The recorder keeps its queue. The core's queue stays out of the capture path.**

Not because the core's queue does not work — arm 3 shows it does — but because in
front of this sink it cannot fire for the failure the queue exists for. The
recorder never blocks: `Message` copies, sends on a channel, and returns, and a
send that would block fails the recording instead of parking the caller. So a
slow disk fills the recorder's queue and never reaches the core's, which is what
arm 7 shows. The core's queue can only fire on a burst arriving faster than a
channel send, which is what arm 3 had to manufacture with a depth of 1.

What it costs meanwhile is not nothing: a copy of every message for every sink,
measured in `docs/2026-08-17-enforce-the-sink-contract.md` at 25% of a 100-byte
message and 154% of a 1500-byte one, charged also to the SQLite sink beside the
recorder, which has its own queue for its own reasons.

And there is a plainer reason. `SinkOverflowBlock` is the default, so a consumer
who wires the recorder and configures nothing else gets the recorder's queue and
nothing more. A capture's guarantee cannot depend on a flag somebody else
remembered to set — which is the same argument that made the recorder queue in
the first place, now with a live run behind it.

The core's policy keeps its reason to exist: a sink you do not control, which is
where it started. `mcrelay` warns at startup when both are configured, because
the smaller queue silently decides which one you are actually running.

## What this run does not show

- **A slow disk.** Every failure here was forced by shrinking a queue, not by
  making a write slow. What the recorder's queue absorbs in production is still
  unmeasured; what is measured is what happens when it cannot absorb it.
- **More than one session at a time.** Every arm was one client. The queues are
  per session and the goroutines are per session, so contention between sessions
  is untested here.
- **A human client.** The sessions here are a bot on a flat world, a third of a
  second in the arms that were cut short and half a minute in the ones that were
  not. The five-minute human session in `2026-08-17-headless-client-capture`
  remains the largest capture taken.
- **A drop the gate could not see and nothing else caught either.** Arm 5's loss
  was caught downstream by `trace` on the packets it happened to take. The claim
  above is that the gate cannot see loss, not that loss is always visible
  somewhere else.

## Where the evidence is

`../oracle-evidence/2026-08-18-sink-policy/`, outside this repository, because
`.gitignore` excludes `*.mccap` for the reason it always has: a recording holds
usernames and UUIDs. It holds every arm's recording, relay log, client log and
database, the `run.sh` that produced them, the `mcrelay` binary they were taken
with, and `SHA256SUMS` over the lot.
