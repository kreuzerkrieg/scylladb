# Reproducing S3 throttling from one machine (2026-08-24)

Petr Hála's `s3-slowdown-test` (branch `pehala/scylladb s3-slowdown-test`, commit `bbd2aa0f02e`)
reproduces real 503 SlowDown from a single machine. Verified here against
`ernest-il-797456418907-il-central-1-an` in `il-central-1`. This note records why it works and why
our own harness does not, since the reason was not what either of us assumed.

## It reproduces

```
write: 4000 of 4000 sstables in 27.9s = 143/s, 42 retries (42 throttling)
verdict: endpoint returned 42 throttling statuses
```

Plus 4 send-brake freezes (3363, 4619, 4115, 3296 ms) — the controller engaged.

**The reported count is wrong, and low.** The log holds **1,105** "reduce your request rate" lines
against the 42 the test printed. `throttling_probe` taps `seastar::logger::set_ostream()` and greps
the stream; it catches a fraction. Anything built on that test should read `s3_throttles` /
`s3_send_freezes` metrics instead. 26x under-count.

## Why it works: 37 billable write operations per sstable

Counted with `--logger-log-level s3=trace` over 20 sstables:

| verb | per sstable |
|---|---|
| DELETE | 19.0 |
| PUT | 14.0 |
| POST uploads | 2.0 |
| POST upload completion | 2.0 |
| HEAD | 4.0 |
| GET | 3.0 |
| **PUT+POST+DELETE** | **37.0** |

S3's per-prefix budget covers PUT/COPY/POST/DELETE together, so a real sstable write costs **37**
write operations. Components observed: `Data.db_0`, `Partitions.db_0`, `TemporaryHashes.db.tmp`,
`Statistics.db`, `Scylla.db`, `Filter.db`, `Digest.crc32`, `CompressionInfo.db` (trie format).

At 143 sstables/s that is **~5,300 write ops/s** — comfortably over the wall.

Note there *is* multipart traffic (2 `POST uploads` + 2 `PUT part` + 2 completion per sstable)
despite ~5.5 KB components, so at least one component uses `make_data_upload_sink` rather than the
plain-PUT path. `upload_sink::flush()` falls back to a single PUT below `minimum_part_size` (5 MiB),
which is why the other components are 1 request each.

## Why ours does not: 1 write op per object, and a ~1,500 op/s peak

`perf_s3_downloader --mode generate` (added this session, `f3daa165d9f`) writes a synthetic corpus;
`--mode upload` PUTs each file once. **1 write operation per object against his 37.**

Measured against the same bucket, same machine, WARP off, on one 12,000-object corpus so that
startup does not dominate:

| config | req/s | resets |
|---|---|---|
| 1 shard x 256 | **1,491** | 0 |
| 1 shard x 512 | 1,235 | 0 |
| 1 shard x 1024 | 900 | 8 |

**More concurrency is slower.** The peak is ~1,500 write ops/s at 256 in flight, and it degrades
above that. Against his ~5,300 ops/s, that is the whole gap: 3.5x fewer operations offered per
second, on the wrong side of a wall that sits somewhere between.

Earlier figures in this file's first draft (450, 592, 599, 613 req/s) were measured on 2,000-object
corpora where process startup dominated an ~4 s run. They are not a ceiling and should be ignored;
the three above replace them.

## Things ruled out

- **WARP.** 464 req/s tunneled vs 450 direct. It caused connection resets but did not cap the rate.
  I asserted it was the ceiling — that was wrong.
- **Per-request TLS handshakes.** Traced: 64 connections served 600 requests, so the pool is reused.
- **Multipart inflation as the explanation.** Components under `minimum_part_size` (5 MiB) take
  `upload_sink::flush()`'s plain-PUT fallback. Multipart is only ~6 of his 37 ops.
- **Socket count.** More sockets is monotonically worse past 256.
- **Serial-vs-parallel component writes.** His component writes are serial within one sstable
  (`write_components` -> `consume_in_thread`), so his in-flight ceiling is his fiber count, same
  shape as ours.

## Correction worth recording

Mid-investigation I read `connections_per_shard=4` in a trace log and concluded his test ran with 4
connections. **That was my own `S3_SLOWDOWN_CONCURRENCY=4`**, set for the trace run, not his
configuration. His real run used the 512 default. The log line was real; the attribution was mine
and it was wrong.

## ANSWER: the throttling comes from his DELETE burst, not from writing sstables

Per-second operation counts from his run, taken from the `s3=trace` log:

| second | ops (PUT+POST+DELETE) | throttles |
|---|---|---|
| 20:18:34 | 2,146 | 0 |
| 20:18:35 | 6,406 | 0 |
| 20:18:36 | 5,206 | 0 |
| 20:18:37 | 5,550 | 0 |
| 20:18:38 | 776 | 0 |
| **20:18:39** | **12,871** | **77** |
| 20:18:40 | 2,498 | 96 |
| 20:18:41 | 269 | 246 |

**His write phase ran at 5,200-6,400 ops/s and was never throttled.** All 419 refusals arrive in the
DELETE burst, where **12,853 individual DELETEs land in a single second**.

### Where the 19,000 DELETEs come from

19 per sstable, and they split into two groups by when they fire:

| group | count | window | source |
|---|---|---|---|
| `Partitions.db_0`, `Data.db_0`, part of `TemporaryHashes.db.tmp` | ~3,000 | :34-:38, interleaved with the PUTs | the sstable **writer** dropping its staging/temp objects |
| every real component (`Data.db`, `Index.db`, `TOC.txt`, ...) + `refs/nodes/<ref>` | ~16,000 | :39-:51 | his test's explicit cleanup |

The cleanup is `written[i]->unlink()` run through `run_concurrently(count, concurrency=512)` — the same
concurrency as the write phase. Each `unlink()` fans out into **one DELETE per component**, unbatched
(`client::delete_object` per object; there is no multi-object delete in our client). 1000 unlinks x ~16
components, issued 512-at-a-time, is what produces the 12,853/s spike.

His own comment on that loop is worth keeping in mind: *"object storage unlink() returns well before
the objects are actually deleted"*.

This invalidates the comparison I spent the afternoon on. I had been treating "37 write ops per
sstable at 143 sstables/s = 5,362 ops/s" as his throttling-inducing rate. It is not: that figure
averages the write phase and the delete burst together, and the two happen at different times. The
wall is not at 2,000-3,500 ops/s. **It is somewhere near 12,000 ops/s**, and it is reachable from one
machine only because bare DELETEs are cheap to issue — no request body, no payload to read, no file
to open.

Our uploads peak at ~1,768 PUT/s. Against a ~12,000 ops/s wall that is 7x short, and PUTs will not
get there: each one carries a body.

### Is it DELETE that trips it? Not provable from the log, but DELETE dominates the burst

The `s3 - DELETE <key>` trace is emitted **once per logical call, before** `make_request` — the retry
loop lives inside `make_request`, so retries are never re-logged. The 19,000 figure is logical calls,
not wire requests, and the `default_http_retry_strategy` 503 line carries no verb or object. **The log
cannot tell us which verb was refused.** (This is the logical-call-chokepoint caveat, applied to
someone else's instrumentation.)

What is measurable is the in-flight mix when the refusals landed: at :39 the traffic is 99.9% DELETE
(18 PUT vs 12,853 DELETE), and the first 503 lands at 20:18:39,815, mid-burst.

The right framing is not "DELETE trips throttling". AWS counts PUT/COPY/POST/DELETE against **one
shared per-prefix write budget** (~3,500/s nominal). DELETE is not cheaper to S3 — it is cheaper for
*us to issue*, having no body to transfer. So one machine can push DELETEs past the shared budget
while it physically cannot push enough PUTs to reach it.

This reconciles the local work with the fleet series:

| | rate | throttled |
|---|---|---|
| PUT, 16-node fleet | 2,346 PUT/s | yes - 33% below nominal |
| PUT, this machine | ~1,768 PUT/s | no - body-transfer-bound, ~7x short |
| DELETE, this machine (his test) | 12,853/s | yes - bodyless |

Consequence for what his test demonstrates: it reproduces *the shared write budget being exceeded*
via the cheapest verb that counts against it. That is adequate for exercising the throttling
controller, which does not care which verb filled the bucket. It is **not** the load shape of
SRE-1418, which is upload-bound.

### RETRACTION: his test reported *no* throttling on the run I compared against

The premise this whole investigation ran on -- "his test reproduces the SlowDown, ours does not" --
is false for the run I measured. His 1000-sstable run printed:

    20:18:39,383  cleanup: 1000 of 1000 sstables unlinked
    20:18:39,383  verdict: no throttling status seen (2 retries for other reasons).

The 419 throttles I kept quoting as "his result" were never his test's result. They are my own grep
of his log, counting a phase his test deliberately excludes:

    auto after_write = probe.snapshot();          // snapshot taken here
    report("write", ..., after_write);
    // "unlink() returns well before the objects are actually gone - the removal
    //  continues in the background ... so no rate is reported for it and the
    //  verdict below only covers the write phase."
    run_concurrently(count, concurrency, ...->unlink());
    if (after_write.throttled) { ... }            // verdict uses the PRE-delete snapshot

`unlink()` returned at :39.383 while DELETEs kept flowing until :51, and the first 503 landed at
:39.815 -- **432 ms after the verdict had already been printed**.

His test *can* reproduce write-phase throttling, but marginally: an earlier 4000-sstable run reported
**42** throttles at 143 sstables/s (~5,291 write ops/s), i.e. 0.03% of requests. The 1000-sstable run
got 0. So it is flaky for him too.

Corrected comparison:

| harness | phase | rate | throttles | density |
|---|---|---|---|---|
| his, 4000 sstables | write | 5,291 ops/s | 42 | 0.03% |
| his, 1000 sstables | write | ~5,400 ops/s | 0 | 0% |
| ours, 60k objects | upload (PUT) | 1,630/s | 0 | 0% |
| ours, 60k objects | delete | 2,406/s | **1,721** | **2.9%** |

Our delete phase is ~100x denser in throttling than his best write-phase run, and unlike his cleanup
burst it is actually measured and reported. **Our harness was never worse at this than his.**

Consistent with per-prefix budgeting: his 5,291 ops/s spread over 4000 `sstables/<uuid>/` prefixes
barely grazes the wall; our 2,406/s concentrated on one prefix goes straight through it.

### CONFIRMED by experiment: our harness now reproduces SlowDown on one machine

Added a delete phase to `perf_s3_downloader` (`--delete_after_upload`): after uploading, delete the
same keys at the same concurrency over the same connections. One run, two RESULT lines, so the only
variable is the verb.

Run 2026-08-24 20:48, 1 shard, 512 fibers, 512 connections, 60,000 objects, bucket
`ernest-il-...-il-central-1-an`, single prefix `deltest/`:

| phase | requests | wall | rate | slowdown | failed |
|---|---|---|---|---|---|
| upload (PUT) | 60,128 | 36.9 s | 1,630/s | **0** | 0 |
| delete (DELETE) | 60,000 | 24.9 s | 2,406/s | **1,721** | 0 |

The delete phase is bursty rather than steady — per-second samples peak at **8,109/s** and then read
0 for several seconds while all 512 fibers sit in the retry backoff:

    t= 10.0s DELETE/s=8109  slowdown+=401
    t= 11.0s DELETE/s=2991  slowdown+=13
    t= 12.0s DELETE/s= 243
    t= 19.0s DELETE/s=7043  slowdown+=420

**The mechanism is confirmed.** A bodyless request reaches 4-5x the PUT rate on the same hardware,
and that is what crosses the shared write budget. Nothing about DELETE is treated specially by S3 —
it is simply the verb we can issue fast enough to hit the wall from one box.

The convergence with the fleet series is the strongest evidence:

| measurement | rate at which throttling appears |
|---|---|
| 16-node fleet, PUT | 2,346/s |
| this machine, DELETE, sustained | 2,406/s |

Two completely different setups, same verb budget, **the same ceiling within 3%**. Sixteen EC2 nodes
were needed to push PUTs to that rate; one desktop reaches it with DELETEs.

Note `failed=0`: all 1,721 refusals were absorbed by the retry machinery, and only 2 freezes fired.
So this reproduces *throttling*, not yet *loss* — driving it to retry exhaustion needs a longer or
denser run.

### RETRACTED: "our keys share one prefix" -- they do not

Ernest caught this. The two key layouts are structurally identical, listed from the bucket:

    deltest/17f62da0-9fe6-11f1-b9e2-a6d4a5036ef7/Data.db      <- ours, ~6000 uuids
    sstables/6fdb3f12-9fe6-11f1-98bf-fe2f00457b4b/Data.db     <- his,  ~4000 uuids

Both are `<static>/<uuid>/<component>`. Prefix concentration does **not** distinguish them, and the
table claiming 1 prefix vs 4000 was wrong. Every conclusion drawn from it is withdrawn.

What actually remains unexplained, with correct facts:

| | key shape | rate | throttles |
|---|---|---|---|
| his write phase | `sstables/<uuid>/` | 5,800 ops/s sustained | 0 (3/3 runs) |
| our delete phase | `deltest/<uuid>/` | 2,406/s sustained, 8,109/s peak | 1,721 |

His *higher* rate on the *same* key shape is not refused while our lower one is. Two candidates, both
untested:

1. **Prefix warmth.** `sstables/` had been written by many runs through the day, so S3 may have split
   it across partitions and raised its ceiling. `deltest/` was created minutes before the delete
   phase and would still be a single partition. This is the same confounder that invalidated the
   PR 30846 hashed-prefix run.
2. **Burst shape.** Our deletes spike to 8,109/s in a single second then idle in backoff; his write
   phase is steady near 5,800/s. A token-bucket limiter punishes the spike, not the average.

Decisive test for (1): run the upload+delete pair against `sstables/` (warm) and against a freshly
minted prefix, same everything else. If the warm prefix absorbs it, warmth is the factor and the
"DELETE is what trips it" reading is at best incomplete.

### What this means for making our harness reproduce it

The lever is **DELETE**, not PUT. Our harness has no delete phase at all — `client::delete_object()`
exists and is unused. A cleanup phase that removes the uploaded corpus at full concurrency would
issue exactly the operation his burst does, and is also the honest thing for the harness to do: it
currently leaves every object behind (452,600 objects / 2.5 GB after one afternoon).

Estimated feasibility: our PUT ceiling of ~1,768/s is set by body transfer; a bodyless DELETE should
go far faster on the same connections. Untested, and it is the obvious next experiment.

## Where it stands: a 3x throughput gap in the same client, cause not found

Both paths measured on the same machine, same bucket, same client, single shard, 512 concurrency:

| | write ops | wall | **ops/s** | throttled |
|---|---|---|---|---|
| his test (1000 sstables) | 37,000 (14k PUT + 19k DELETE + 4k POST) | 6.9 s | **5,362** | 419 |
| our harness (60k objects) | 60,000 PUT | 37.4 s | **1,605** | 0 |

**The hardware is not the explanation.** This is a Ryzen 9 9950X, 16C/32T, 123 GB RAM, 1 Gbps NIC —
substantially more machine than Petr's, and his test reproduces on it without trouble. My earlier
"comparable hardware" wording was wrong in the other direction.

**The network is not the explanation.** `s3.il-central-1.amazonaws.com` is 4.5 ms away; a full
HTTPS round trip including TLS is 17 ms. At 512 connections that permits ~30,000 req/s. We achieve
1,605, i.e. ~310 ms per request against a 17 ms wire cost — an 18x gap that is entirely in-process.
The 57-102 ms RTTs visible in `ss` mid-run are our own queueing, not the path.

### Eliminated by measurement

| candidate | test | verdict |
|---|---|---|
| WARP tunnel | 464 tunneled vs 450 direct | no effect on rate |
| CPU saturation | 40% of one shard; 238 us CPU/request vs **his 336 us** | we are *more* CPU-efficient per request |
| connection count | 1 shard x 512 conns, 538 sockets established mid-run | 1,605 — no better than 64 conns |
| shard count | 1/2/4/8/16 shards, total in-flight held constant | ~1,500-1,700 throughout |
| fiber count | 128 / 256 / 384 / 512 / 1024 | peak at 128-384, degrades above |
| file streaming per object | added `--upload_from_memory` (PUT from a buffer, no `open_file_dma`) | **1,767 vs 1,768 — identical** |
| multipart vs plain PUT | traced: his components take the plain-PUT fallback too | only ~6 of his 37 ops are multipart |
| ops per object | arithmetic: at a fixed ops/s ceiling, composing more ops per object relabels them | cannot close a throughput gap |
| corpus on disk | corpus lives on tmpfs | not disk-bound |

### A second, separate effect: the stall

Some runs drop to ~600 ops/s and **always** carry connection resets, while clean runs of the same
config carry none:

```
1 sh x 512, 12k objects, three runs:
  597 ops/s, 108 resets, 20.3 s
  1186 ops/s,  0 resets, 10.1 s
  1196 ops/s,  0 resets, 10.0 s
```

Roughly one run in three. It is a distinct failure mode, not variance, and it is what produced my
earlier false conclusions about shards and concurrency — I read single stalled runs as trends. Cause
unknown; the reset correlation points at something local (conntrack, ephemeral ports, the uplink).

### Retractions

- **"More than one shard degrades performance."** False. 2 shards x 128 repeated: 594, 1510, 1526,
  1555. One stall, three normal.
- **"37 ops per sstable is the mechanism."** Wrong framing — our limit is ops/s, not ops/object.
- **"WARP is the ceiling."** No, it only caused resets.
- **"Comparable hardware."** This desktop is stronger than his machine.

### What is left to test

The remaining structural difference: **his 37 operations are issued back-to-back on one fiber**
(a serial chain per sstable), where ours issues one operation per fiber-iteration and then re-enters
the work loop. If `coroutine::parallel_for_each` over 512 fibers has per-iteration scheduling cost
that a straight-line chain avoids, that is the last candidate standing. Untested.

## Recommendation

For **reproducing the ceiling cheaply, his approach is right** and ours cannot currently do it: 37x
more write operations per object is exactly the multiplier that makes one machine sufficient. His
test is worth landing for that, with the probe rewired onto `s3_throttles` / `s3_send_freezes`
instead of tapping the log stream.

Our harness remains the right tool for fleet-scale throughput with real object sizes. If we want it
to provoke throttling locally, the lever is not concurrency — it is either finding the ~1,500 op/s
cap, or emitting many more operations per object.

## SOLVED: throttling with PUTs alone -- shrink the objects

The PUT ceiling on one machine is set by per-request cost, which scales with object size. Same
harness, same machine, same bucket, 1 shard, 512 fibers, 60,000 objects, only the component size
differs:

| component size | peak PUT/s | throttles | lost |
|---|---|---|---|
| 5,500 B | 1,853 | **0** | 0 |
| 100 B | **7,745** | **1,297** | 0 |

Per-second detail from the 100-byte run:

    t=  9.0s PUT/s=3024  slowdown+=27
    t= 12.0s PUT/s=5710  slowdown+=384
    t= 21.0s PUT/s=7745  slowdown+=42
    t= 22.0s PUT/s=3888  slowdown+=360

So **no deletes are needed**. At 5,500 B we sit below the endpoint's wall and never see a 503; at
100 B we go straight through it. This also explains his test without any appeal to prefixes or verbs:
his components are real sstable parts for a single-row mutation -- a few hundred bytes -- so his PUTs
reach ~4,000/s while ours crawled at 1,850/s.

The whole "DELETE is what trips it" framing was an artifact of object size. A bodyless DELETE is just
the limiting case of a small request.

Caveats:
- One run per size. The 0-vs-1,297 gap is far outside the noise seen elsewhere, but the *rate* numbers
  deserve a repeat.
- `requests_per_sec` for the 100-byte run reads 247 because retry backoff dominates the tail once
  throttling starts; the peak per-second samples are the meaningful figure.
- The mechanism behind the size sensitivity is not established. 1,853/s x 5,500 B is only 10 MB/s, far
  from network or crypto limits, so this is not raw throughput -- something in the per-object path
  scales with size worse than it should. Worth a profile.

### Why 0 loss: every throttle succeeded on the first retry

The accounting closes exactly, with nothing unexplained:

    requests (ops + retries) : 61,309
    logical objects          : 60,000
    extra wire requests      :  1,309
    slowdown (1,297) + net_reset (12) = 1,309   <- exact match

So each of the 1,297 throttled requests was retried **once** and succeeded. None needed a second
attempt, against a 10-attempt budget -- 9 attempts of headroom never touched, which is why
`throttle_exhaustions` is 0 and nothing was lost. Throttling at this density is comfortably absorbed;
it is nowhere near the loss threshold.

### Separate defect found on the way: one request hung for 263 s with no timeout

The run's 247.9 s elapsed is not backoff. Per-second samples show all 61,291 requests completing in
the first ~26 s, then **222 consecutive seconds at zero**. The latency histogram names the culprit:

    lat min/p50/p99/max = 20 / 60 / 3,973 / 263,210 ms

p50 60 ms, p99 4 s, and a single request at **263 s**. It is not retry looping (exactly one retry per
failure, exhaustions 0) and not the send freeze (the only two freezes were 3,175 ms and 4,467 ms).

`utils/s3/client.cc` sets **no per-request timeout**, and seastar's `http::client::make_request` takes
only an `abort_source` -- there is no timeout parameter. So a request left waiting on a connection that
has silently gone away is reaped by nothing except the OS TCP stack or an abort. The run reported 12
`net_reset`s, which makes a dead connection the leading hypothesis, though the hang itself is not yet
proven to that cause.

This is almost certainly the "sporadic stall mode" seen earlier in the day (~1 run in 3 landing near
600 ops/s, always alongside connection resets). It now has a signature to grep for: a p99/max latency
ratio in the tens, with the max in the hundreds of seconds.

## RETRACTION 5: the "0 throttles without unlink" runs had a blind probe

His `throttling_probe` is a **log tap**: it redirects `seastar::logger`'s ostream and counts lines
containing "AWS HTTP client request failed", sub-counting those matching "reduce your request rate" /
"SlowDown" / "Service Unavailable" / "Too Many Requests" / "Throttl". `default_aws_retry_strategy`
emits that line at **debug**, and the test does **not** raise the level itself -- it depends entirely
on the caller.

My three no-unlink runs passed `--default-log-level warn`, so the probe counted nothing:

| run | lines from `default_http_retry_strategy` | reported |
|---|---|---|
| sdrun.log (42 throttles) | 1,123 | 42 retries / 42 throttled |
| nounlink1-3 | **0** | "0 retries, 0 throttles" |

Re-run with `--logger-log-level default_http_retry_strategy=debug`, unlink still removed:

| run | rate | throttled | sstables written |
|---|---|---|---|
| 1 | 150/s | 32 | 4000 of 4000 |
| 2 | 143/s | 13 | 4000 of 4000 |
| 3 | 143/s | 17 | 4000 of 4000 |

**His write phase does throttle without any unlink**, 13-32 per run, and retries resolve all of it.
The earlier "3/3 runs, zero" is withdrawn. This is the same failure mode already recorded in
[[log-diagnostics-below-default-level-measure-nothing]] -- third occurrence.

## Does he use the retry strategy? Yes, the production one, unmodified

His test installs nothing. sstables::storage -> s3::client -> `aws::default_aws_retry_strategy`, the
same path and same 10-attempt budget our harness uses. The probe is a pure observer. So "retries
resolve the throttling" is true for both harnesses for the same reason.

Throttle density, corrected:

| | throttles / write ops | density |
|---|---|---|
| his, 4000 sstables | ~20 / ~148,000 | 0.014% |
| ours, 100-byte objects | 1,297 / 61,309 | **2.1%** |

## His doc already documented two things I re-derived

`docs/dev/object_storage_throttling.md` in his own commit:

- *"**No request timeout is plumbed through `s3::client`.** Driving raw PUTs at concurrency 1024-2048
  wedged twice: the storm wrote all but exactly one object and then stopped making progress, once with
  no log output for 16 minutes."* -- this is exactly the 263 s hang I found; credit is his, and he saw
  a worse case.
- *"The test under-reports its own throttle count. Its verdict covers the write phase only ... the test
  reported 225 statuses while the log held 765."* -- he already knew the verdict excludes the unlink
  deletes.

And the line that independently confirms the object-size finding: *"Objects are one byte each."* His
components are 1 byte; ours were 5,500. That, not the verb and not the prefix, is why his PUTs reach
~4,000/s and ours crawled at 1,850/s.
