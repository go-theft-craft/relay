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

## The slow disk, measured

The arms above forced every failure by shrinking a queue, which leaves the
question the queue exists for unanswered: what does it absorb when the *disk* is
slow. So the recordings were written onto a passthrough filesystem that holds
every write open for a fixed duration — `slowfs` in the evidence directory,
about a hundred lines of FUSE — and the same live stack was run onto it. Nothing
in the proxy changed: real files, real write syscalls, and the syscalls take as
long as the mount says.

| Delay per write | Writes | Bytes | Outcome |
| --- | --- | --- | --- |
| none | 9 | 148 KB | ok, 772 records |
| 50 ms | 75 | 582 KB | ok, 6108 records |
| 500 ms `-capture` | 43 | 531 KB | ok, 5418 records |
| 200 ms `-capture` | 61 | 571 KB | ok, 5958 records |
| 1 s | 29 | 519 KB | ok, 5260 records |
| 2 s | 6 | 113 KB | **recorder ended the session**; ok, 480 records |
| 4 s | 4 | 80 KB | **recorder ended the session**; ok, 485 records |
| 2 s, core policy on | 6 | 116 KB | **recorder ended the session**; ok, 573 records |

Four things fall out of it.

**The recorder writes in flushes, not in records.** 772 records left the process
as 9 write syscalls, and a slower disk makes the writes larger rather than more
frequent — 18 KB apiece at one second, against 16 KB at fifty milliseconds. So
what a slow disk costs a recording is per flush, and the buffer underneath
absorbs the first order of it for free.

**It survives to about 15 KB/s, on a session that produces 14.** One second per
write is a disk delivering roughly 18 KB/s at these flush sizes, and the session
kept going for its full 36 seconds. Two seconds per write halves that and the
recorder gives up. The threshold is not a latency: it is the point where
sustained write bandwidth falls under what the session produces, which is what a
bounded queue is supposed to do and not a number to tune against. It is not the
general rule either — the tail arms below hold this bandwidth constant and end
sessions anyway.

**The queue buys about nine seconds, and the disk's slowness does not change
that.** Failure came 9.1 s into the session at two seconds per write and 8.5 s at
four. A queue of 1024 records absorbs a burst, and against a sustained shortfall
it only sets how long the recording lasts before the recorder says so — the
depth and the session's rate decide that, not how slow the disk is.

**The core's queue never fired.** The last arm ran `-sink-overflow end-session
-sink-queue 1024` alongside the same two-second disk: the recorder ended the
session, and the relay log holds no `sink queue overflow` at all. That is the
claim this repository's decision rests on, and it is now a run rather than an
argument — a slow disk fills the recorder's queue and never reaches the core's,
because the recorder's `Message` returns whether or not the disk is answering.

Every arm that survived ran to the bot's own exit, and every arm that failed
left a recording that replays to its own digest. A capture stopped by a slow
disk is still a capture.

## The tail, measured

A uniform delay says a disk is slow. It does not say what a slow disk is like:
real storage answers quickly nearly always and occasionally stops, and the
paragraph above ends by admitting that a tail is what usually ends sessions. So
`slowfs` grew `-tail-every`, which makes every Nth write take a long delay
instead of the short one, and the arms below all hold the *mean* at roughly one
second per write — the uniform delay that survived a full session — while
changing the shape.

| Stall | Mean | Stalls in the session | Outcome |
| --- | --- | --- | --- |
| uniform 1 s | 1.00 s | — | ok, 5260 records, full session |
| 3 s every 3rd write | 1.01 s | 7 | ok, 3608 records, full session |
| 6 s every 6th write | 1.02 s | 2 | ok, 1880 records, full session |
| 6 s every 6th, again | 1.02 s | 5 | ok, 6106 records, full session |
| 9 s every 9th write | 1.02 s | 1 | **ended inside the first stall**; ok, 941 records |
| 15 s every 15th write | 1.02 s | 1 | **ended inside the first stall**; ok, 1363 records |

**The mean is not what decides.** Every row has the same one second per write.
The uniform arm ran to the bot's own exit and so did stalls of three and six
seconds, repeatedly — seven of them in one arm, five in another, with the queue
draining in between each time. A single nine-second stall ended the session on
its first occurrence, and so did a fifteen-second one.

**What decides is whether one stall outlasts the queue.** The uniform arms put
that budget at about nine seconds of this session's production, and the tail
arms land on the same number from the other direction: six seconds is absorbed
however often it repeats, nine is not, and both fatal arms failed *during* the
stall rather than after it — 9.8 seconds into the fifteen-second one. So the
recorder's tolerance is a stall-duration budget, not a bandwidth budget, and the
bandwidth reading above is the same budget seen through a delay that never lets
the queue drain.

Restating it as a consumer would need it: `QueueDepth` buys seconds of the
session's own record rate, and what a disk has to avoid is not being slow but
stopping for longer than those seconds. A recording that ends this way is still a
recording — every failed arm replays to its own digest — and what the client sees
is the proxy closing the connection.

## Several sessions at once

The queues and the goroutines behind them are per session, which is a claim
about isolation that one client cannot test. So the same stack was run with
several clients at once — and with one more knob on the mount, `-serial`, which
puts every write behind a single lock so concurrent recorders share one device
instead of each getting their own.

A note on the client first, because it shapes what could be measured. The orbit
bot quits once the server has corrected its movement six times, and six of them
on one spawn circle collide immediately: those sessions last about a second, so
they measure a login burst and nothing after it. The arms below use
`headless-minecraft`'s `observe` instead, which watches for as long as it is
given and produced about 84 records a second per session, some 500 across the
six of them.

| Clients | Device | Latency | Sessions the recorder ended | Recordings |
| --- | --- | --- | --- | --- |
| 5 orbit | real | — | 0 | 5 replay |
| 10 orbit | real | — | 0 | 10 replay |
| 6 observe | real | — | 0 | 6 replay, ~3350 records each |
| 6 observe | one each | 2 s per write | 0 | 6 replay, ~3100 each |
| 6 observe | **shared** | 2 s per write | **4** | 6 replay |
| 6 observe | one each | 15 s every 15th write | **1** | 6 replay |
| 6 observe | **shared** | 15 s every 15th write | **4** | 6 replay |

**Concurrency on its own changes nothing.** Five, six and ten sessions at once
each got their own recording, their own frame numbering and their own clock
origin, and every recording replays to its own digest. The relay log is empty of
faults in all three. The ten-client arm is worth keeping for a second reason:
the server's own cap refused two logins, and those two recordings hold the
refusal — six records, `The server is full!`, replaying like anything else.

**Isolation holds exactly as far as the device does.** The two 15-second-stall
arms differ in nothing but who owns the disk. With a device each, one session hit
a stall long enough to outlast its queue and ended, and the other five ran to
their full forty seconds — eleven stalls happened in that arm and cost one
session. With one shared device, the same stalls ended four of the six, because a
write that stops the device stops it for whoever is queued behind it too. Sustained
slowness says the same thing: two seconds per write kills nobody when each
recorder has its own device and four of six when they share one.

So the per-session queue is isolation from the *pump* and from each other's
bursts, and it is not isolation from a disk. That is the honest shape of it: six
recorders behind one slow disk each get a sixth of it, and the queue is what
turns that into ended sessions rather than stalled proxies.

**Teardown contends too, and it is the part that can lose a trailer.**
`CloseGrace` bounds how long `CloseSession` waits for a session's remaining
records — thirty seconds, which is generous for one session and not for six
closing at once onto a shared device. Three of the six in the two-second arm
reported `recording still writing 30s after its session closed; left to finish on
its own`. In an earlier arm the proxy then exited while one of those writers was
still draining, and that recording ended up without its trailer. The gate refuses
it — `the recording has no trailer; it was never closed` — which is the right
answer and worth contrasting with the dropped-record case above, where the file
was incomplete and the gate said ok.

**The core's queue never fired in any of them.** Every concurrency arm ran the
default policy except where noted, and no arm produced a `sink queue overflow`.

## What this run does not show

- **A disk that stops answering entirely.** Every delay here returns. A write
  that never lands is what `CloseGrace` bounds, and that path is covered by the
  recorder's own tests rather than by a run.
- **A real remote filesystem.** The latency is injected per write by a FUSE
  passthrough. The tail arms above give it a shape, but a deterministic every-Nth
  stall is still not a distribution: real storage correlates its stalls with load,
  and this one does not.
- **More than ten sessions, or more than one proxy.** The concurrency arms top
  out at the server's own player cap, on one host, in one process. What a
  hundred sessions do to a shared disk is arithmetic from here rather than a
  measurement.
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
database, the `run.sh` and `run-slow.sh` that produced them, `slowfs` in source
and built, the `mcrelay` binary they were taken with, and `SHA256SUMS` over the
lot. The slow-disk arms keep their recordings in `backing/`, which is what the
mount wrote through to.
