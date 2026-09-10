# NV series results

Append one block per run. Same fleet and flow unless noted: 16 x i4i.16xlarge on-demand, 64 shards,
20-min download then 2 upload passes into warm `sstables_ewz`.

| run | date | mitigations | phase | req/s | requests | files | MB/s | throttles | lost (throttled/masked/other) |
|---|---|---|---|---|---|---|---|---|---|
| NV1 | 2026-08-04 | **none** (stock retry: 25ms*2^n, no jitter/cap/freeze/quota) | download | 6,705 | 8,055,250 | 383,619 | 18,052 | 0 | 0 |
| NV1 | | | upload p1 | 2,278 | 699,435 | 383,449 | 52,522 | 24,414 | **170** (170/0/0) |
| NV1 | | | upload p2 | 2,539 | 996,847 | 383,539 | 41,087 | 111,494 | **80** (53/0/27) |

## NV1 totals and notes

- **250 lost, 135,908 throttling responses.** 223 losses throttling, 27 transport.
- **384 retry exhaustions**, by reason (from the `Retries exhausted. Reason:` diagnostic):
  `344 Please reduce your request rate` / `27 Error in the push function` (GnuTLS) / `13 Broken pipe`.
  So **90% of exhaustions are throttling.**
- `masked = 0` and zero ETag-parse failures, i.e. the MPU cause attribution is working; without it
  the 223 throttling losses would have read as ETag parse errors.
- **Download amplification 21.0 req/object** (8.06M requests for 383,619 objects) against old
  run 17's 12.05, at slightly lower throughput (18,052 vs 19,181 MB/s). Same box, shards and corpus;
  the difference is the absent send-rate limiter, so the chunked download source runs unpaced and
  fragments into more ranged GETs. Not previously observed — the old series only ever showed the
  limiter costing throughput, never reducing amplification.

Logs: `fleet-runs/172636-*.txt` (48 files, includes ul2).
Branch at time of run: perf-test-only + `s3: report the cause when a multipart part upload fails`
and `s3: name the cause when retries are exhausted`.

Old-series reference (different branch, had the throttling code): run 12 = 29 lost / 95,632
throttles; run 17 = 0 lost / 76,219 throttles.

## NV2 -- 2026-08-04, random 16-char key root, COLD bucket

Single variable against NV1: upload key `{16-random}/{sstable_id}/{component}` in
`ernest-object-storage` instead of `sstables_ewz/{sstable_id}/{component}` in
`manager-backup-tests-us-east-1`. Same download source, fleet, flow and stock retry strategy.
Driver: `UPLOAD_BUCKET=ernest-object-storage NO_PREFIX=1 RANDOM_PREFIX=16`.

| phase | req/s | requests | files | MB/s | throttles | lost (thr/mask/other) |
|---|---|---|---|---|---|---|
| download | 6,547 | 7,872,128 | 377,903 | 17,736 | 0 | 1 (0/0/1) |
| upload p1 | 2,321 | 688,951 | 377,632 | 53,342 | 26,017 | **271** (271/0/0) |
| upload p2 | 2,572 | 970,520 | 377,782 | 42,066 | 111,968 | **121** (88/0/33) |

**Totals: 137,985 throttles, 392 lost, 753 exhaustions (696 throttling).**

| | NV1 | NV2 | delta |
|---|---|---|---|
| throttles | 135,908 | 137,985 | +1.5% (noise) |
| lost | 250 | 392 | **+57%** |
| exhaustions | 384 | 753 | **+96%** |
| p1 / p2 MB/s | 52,522 / 41,087 | 53,342 / 42,066 | +1.6% / +2.4% |

**The key layout did not reduce throttling.** Throttle count is unchanged within noise at matched
throughput and request count, so the leading-bytes hypothesis is not what limits this workload --
at least not against a cold target.

**Unresolved: losses rose 57% and exhaustions doubled while throttling stayed flat.** Same number of
503s, more requests that never got through ten attempts. Two candidates, neither established from
one run:

1. 380k keys across 380k distinct prefixes may make S3 split partitions harder, refusing individual
   requests longer at the same aggregate throttle rate.
2. **Cold bucket.** NV2 wrote to a brand-new bucket; NV1 wrote into a prefix warmed by the entire
   old run series. A cold target throttling harder already invalidated old run 8.

**Ernest's call (2026-08-04): the cold-bucket reading is accepted for now. Re-run the same NV2
configuration ~1 hour later and again the next day**, leaving the ~378k objects in place, to let S3
split the keyspace. Note every run writes *new* random prefixes, so a repeat only improves if S3 has
split by key *range* rather than warming specific prefixes -- which is what the repeat tests.

Logs: `fleet-runs/184634-*.txt`. Bucket left populated on purpose.

## NV3 -- 2026-08-04, random 16-char key root, WARMED bucket

Repeat of NV2 over the same `ernest-object-storage` bucket, now holding NV2's keys, to separate
"cold target" from "random key layout". Same fleet and flow. Logs: stamp `211812`.

| run | phase | req/s | requests | files | MB/s | throttles | lost (thr/mask/other) | exhaustions (throttling) |
|---|---|---|---|---|---|---|---|---|
| NV3 | download | 6,336 | 7,616,770 | 368,217 | 17,186 | 0 | 1 (0/0/1) | 0 |
| NV3 | upload p1 | 2,314 | 666,349 | 368,038 | 53,351 | 18,106 | **179** (179/0/0) | 452 (452) |
| NV3 | upload p2 | 2,462 | 966,388 | 368,136 | 39,209 | 119,035 | **81** (64/0/17) | 113 (72) |

### The three runs together

| | throttles | lost | exhaustions |
|---|---|---|---|
| NV1 static prefix, warm | 135,908 | 250 | 384 |
| NV2 random root, **cold** | 137,985 | 392 | 753 |
| NV3 random root, **warmed** | 137,141 | **260** | 565 |

**Headline: randomising the key root does not reduce throttling.** Throttle counts across a static
prefix, a cold random root and a warmed random root are 135,908 / 137,985 / 137,141 -- a **1.5%
spread**. The same wall in the same place regardless of key distribution. Throughput matched too
(p1 52.5-53.4 GB/s, p2 39.2-42.1 GB/s).

**NV2's +57% loss regression was the cold bucket, not the layout** -- Ernest's call at the time. A
second pass over the same bucket brought losses 392 -> 260, back to NV1's 250 within noise.

Warming shifts *when* the wall is hit, not *where*: NV3 had the **lowest** pass-1 throttling (18,106
against 24,414) and the **highest** pass-2 (119,035).

### Two things not explained

1. **Exhaustions stay elevated**: 565 on NV3 against NV1's 384, at the same throttle count and with
   losses back to parity. More requests burning all 10 attempts, yet fewer becoming lost objects. No
   explanation from three runs.
2. **NV3 moved 4% less data** (368,038 files against NV1's 383,449) because the download phase listed
   fewer objects, so the loss counts are not perfectly volume-matched. Throttle-per-object is still
   flat.

Numbers above were recomputed from the archived RESULT lines and log greps, not transcribed.

## NV4 -- 2026-08-06, random 16-char key root, HEAVILY warmed bucket

Same fleet, flow, binary and upload layout as NV3. Single variable: the bucket had absorbed a day of
sustained read+write traffic from a single-node warm loop (see the warm-loop note), so it started at
**2,051,890 objects / 89.49 TB** against NV3's ~755k objects. Window 10:12:40-10:50:05 (+03:00).
Binary unchanged: HEAD `02fcc15a651`, built 2026-08-04 20:25, the same one NV1-NV3 ran.
Logs: stamp `105005` (48 files), under the collected `fleet-runs/`.

| phase | req/s | requests | files | MB/s | throttles | lost (thr/mask/other) | exhaustions |
|---|---|---|---|---|---|---|---|
| download | 6,521 | 7,836,940 | 376,803 | 17,682 | 0 | 0 | 0 |
| upload p1 | 2,358 | 686,186 | 376,668 | 54,191 | 23,222 | **135** (135/0/0) | 298 |
| upload p2 | 2,457 | 995,554 | 376,726 | 39,005 | 118,095 | **77** (58/0/19) | 97 |

**Totals: 141,317 throttles, 212 lost, 395 exhaustions** (359 throttling = 91%, 32 GnuTLS push,
4 broken pipe). Download amplification 20.8 req/object.

### All four runs

| | bucket at start | throttles | 5xxErrors (CloudWatch) | lost | exhaustions |
|---|---|---|---|---|---|
| NV1 static prefix, warm | other bucket | 135,908 | not enabled | 250 | 384 |
| NV2 random root, **cold** | 0 | 137,985 | not enabled | 392 | 753 |
| NV3 random root, warmed 1 pass | ~755k obj | 137,141 | 345,853 | 260 | 565 |
| NV4 random root, **warmed a day** | 2.05M obj / 89 TB | **141,317** | **364,626** | **212** | 395 |

**Headline: warming the bucket does not move the throttling ceiling either.** Across a static
prefix, a cold random root, a lightly warmed one and one holding 2M objects after a day of sustained
traffic, throttling spans 135,908-141,317 -- a **4.0% spread**, with NV4 the *highest*. Combined with
NV1-NV3, neither key layout nor bucket warmth is what limits this workload. Throughput matched
throughout (p1 52.5-54.2 GB/s, p2 39.0-42.1 GB/s), so this is not a slower run being throttled less.

Losses fell to 212, the lowest of the series, but 250/392/260/212 has no trend separable from the
cold-bucket effect already identified in NV2.

### The harness undercounts throttling ~2.5x -- now reproduced twice

CloudWatch request metrics (`FilterId=requests`) give S3's own count, which no log level can hide:

| | harness upload requests | CloudWatch PutRequests | harness throttles | CloudWatch 5xxErrors | ratio |
|---|---|---|---|---|---|
| NV3 | 1,632,737 | 1,630,823 | 137,141 | 345,853 | 2.52x |
| NV4 | 1,681,740 | 1,680,121 | 141,317 | 364,626 | 2.58x |

Request counts agree to **0.1%** in both runs, so the pairing is right and the throttle gap is not a
windowing artifact. `4xxErrors` was 0 and `GetRequests` 0 (downloads read a different bucket), so the
5xx are all upload-path. Not yet explained: CloudWatch counts every 5xx HTTP response including
repeats against one logical request, while the harness counter may sit above the retry layer. Worth
settling in code -- it rescales every absolute throttle number in the whole series, though not the
relative comparisons above.

**Capture the CloudWatch window on every future run.** It is free, server-side, and immune to the
log-level trap that invalidated the Retry-After finding three times.

## NV5 -- 2026-08-09, EXACT repeat of NV4 (the error bar)

Ernest chose a byte-identical repeat over the static-prefix cell. It was the right call: no run in
the series had ever been repeated, so the series had **no estimate of its own run-to-run variance**
and no way to tell which of its differences were real. Same binary (HEAD `02fcc15a651`, rebuilt
2026-08-09 11:25, sha `bdd5d13f...` verified identical on the nodes), same fleet shape, same layout.
Bucket at start **2,805,375 objects / 122.43 TB**. Window 11:35:02-12:10:45 (+03:00).
Logs: stamp `121055` (48 files).

| phase | req/s | requests | files | MB/s | throttles | lost | exhaustions | elapsed |
|---|---|---|---|---|---|---|---|---|
| download | 6,603 | 7,934,502 | 374,995 | 17,580 | 0 | 0 | 0 | 1,202 s |
| upload p1 | 2,281 | 687,566 | 374,740 | 52,090 | 31,098 | 255 | 555 | 306 s |
| upload p2 | 2,512 | 979,696 | 374,942 | 40,368 | 110,441 | 53 | 69 | 391 s |

**Totals: 141,539 throttles, 308 lost, 624 exhaustions** (600 throttling, 19 GnuTLS push, 5 broken
pipe). CloudWatch: 1,665,375 PutRequests (harness 1,667,262, 0.11% apart), **357,554 5xxErrors**
= 2.53x the harness count -- the undercount now reproduced a **third** time (2.52 / 2.58 / 2.53).

### What repeats and what does not

| metric | NV4 | NV5 | repeat delta |
|---|---|---|---|
| **throttles** | 141,317 | 141,539 | **+0.16%** |
| **CloudWatch 5xxErrors** | 364,626 | 357,554 | **-1.9%** |
| lost | 212 | 308 | **+45%** |
| exhaustions | 395 | 624 | **+58%** |
| p1 throttles | 23,222 | 31,098 | +34% |
| p2 throttles | 118,095 | 110,441 | -6.5% |

**Total throttle count is the only precise instrument in this harness.** It repeats to a sixth of a
percent. Loss and exhaustion counts swing by half between identical runs, and even the p1/p2 *split*
moves 34% while the total barely moves -- as if the endpoint concedes a roughly fixed throttling
budget per run and only the distribution across passes wanders.

### Two conclusions this forces

1. **Every loss-based comparison in NV1-NV5 is inside the noise.** Losses ran 250 / 392 / 260 / 212 /
   308, and an identical pair produced 212 and 308. NV2's 392 is barely outside that. **Downgrade the
   "a cold bucket costs losses" claim** -- it was one run against noise this wide. Same for NV4's 212
   "lowest of the series".
2. **The 4% throttle spread is ~25x the repeat noise, so it is probably real -- and it runs against
   randomization.** All four random-root runs (137,985 / 137,141 / 141,317 / 141,539) sit *above*
   static-prefix NV1's 135,908, by 1.5% to 4.1%. Randomization never once helped; it may cost a
   couple of percent. Caveat unchanged: NV1 was a different bucket, so the static-prefix cell inside
   `ernest-object-storage` is still the one run that would settle it without a confound.

Caveat on the variance estimate itself: it is **two samples**. 0.16% agreement from one pair could be
luck, and no standard deviation should be quoted from n=2.

## NV6 -- 2026-09-09, reduced PR + squashed new throttler, first run of the new constants

Same fleet and flow as NV1: 16 x i4i.16xlarge **on-demand**, 64 shards, 20-min download then
2 upload passes into `sstables_ewz` (static prefix). Branch `perf-s3-nv-throttling` @ `ca49d8967f0`,
binary md5 `b524fdf23a9259d151df82a394ef9ba4`. Logs: stamp `164633` (48 files).

**Config difference from NV1, stated up front:** the target prefix was **cold**. The upload bucket
carries a bucket-wide `auto-deletion` lifecycle at 7 days and the last write was 2026-08-13, so
`sstables_ewz` was empty. NV1 wrote into a prefix warmed by the whole earlier series. NV1-NV4 found
warmth moves nothing beyond a 4.0% spread, so this is noted rather than treated as fatal.

| phase | req/s | requests | files/objects | MB/s | throttles | lost | freezes |
|---|---|---|---|---|---|---|---|
| download | 6,266 | 7,608,243 | 371,000 (35,658 sst) | 17,343 | 0 | 0 | 0 |
| upload p1 | 2,767 | 664,849 | 371,000 | 65,833 | 4,423 | 0 | 0 |
| upload p2 | 2,698 | 667,191 | 371,000 | 64,832 | 7,268 | 0 | 0 |

### Against the NV1 bare-master baseline

| | NV1 (no mitigations) | NV6 | |
|---|---|---|---|
| download amplification | 21.0 req/obj | 20.51 | matched |
| download MB/s | 18,052 | 17,343 | -4% |
| upload req/s (p1/p2) | 2,278 / 2,539 | 2,767 / 2,698 | **+21% / +6%** |
| **throttles total** | **135,908** | **11,691** | 11.6x fewer |
| throttle density | 80.1 /1k req | 8.8 /1k req | 9.1x lower |
| **objects lost** | **250** | **0** | |
| retry exhaustions | 384 | **0** | |

### The brake never fired -- so it did not produce this

`freezes = 0` on every node in every phase. The refused share, over **45,191 upload samples**
across 1024 shard-instances:

| p50 | p90 | p99 | p99.9 | peak | fraction >= 0.2 |
|---|---|---|---|---|---|
| 0.0000 | 0.0000 | 0.0400 | 0.0950 | **0.1400** | **0.000000** |

The estimate never came within 30% of `ratio_threshold` anywhere, and `freezes = 0` corroborates it
independently. **Whatever produced 0 lost, it was not the send brake.**

Two candidates, and this run cannot separate them:

1. **The retry backoff reshaping** (`throttling_base_sec` 0.025 -> 1.0, cap 20 -> 60 s). Slower
   retries put less self-inflicted load on a refusing prefix, which would lower the 503 count itself.
2. **S3 capacity drift** over the five weeks since NV1. This is not hypothetical -- the 2026-08-10
   run's claimed 22x was retracted for exactly this, see
   [[mitigated-run-22x-fewer-throttles-but-one-node-stalled]].

Do not attribute the improvement to code without a same-day bare-master control run.

### What this says about each constant

| constant | verdict |
|---|---|
| `ratio_threshold = 0.2` | **unreachable on this workload as measured.** Client-side p99 is 0.04, peak 0.14. It was placed from S3's own request metrics (p1 9-11%, p2 28-30%) but the controller compares against a client-side share, where p1 mean is 0.023 and p2 0.037 -- a 2.5-4x scale gap, consistent with the known harness/CloudWatch undercount. To engage where its derivation intended, the client-side threshold is about **0.08**, or the ratio has to be measured on the scale the threshold came from. |
| `outcomes_per_sample = 50` | **too coarse for a share this low.** At p=0.03 a closed sample can only read multiples of 0.02, and the EMA's sd is 0.012 -- comparable to the mean itself. At a 0.08 threshold, sd is 0.022, so 0.08 sits only ~2 sd above a 0.03 baseline. Raising n sharpens the discrimination. |
| `ratio_ema_factor = 0.5` | **no data** -- never exercised, the brake never armed. |
| `freeze_duration = 4000 ms` | **no data** -- same. |

### Download amplification: the brake does not pace an unrefused phase

20.51 req/object matches NV1's unpaced 21.0, not run 17's 12.05. Run 17 carried CUBIC, which paced
sends continuously; the reduced brake is event-driven and never armed here, so amplification returns
to the unpaced baseline. Working as designed, but worth stating: **the brake buys nothing on a phase
that is not being refused**, and the download phase is never refused (0 throttles in NV1 and NV6
alike).

Cost: 16 on-demand instances, ~46 min including setup, ~$67.

## NV8 -- 2026-09-09, ice-cold `ernest-object-storage`, corpus reused from NV7

Same code as NV6 (`ca49d8967f0`, binary md5 `b524fdf23a9259d151df82a394ef9ba4`), same 16 x
i4i.16xlarge on-demand fleet, 64 shards, upload concurrency **4** (the default -- verified on all
16 nodes before the run: 1 process each, no `--file_concurrency` flag, right bucket).

**No download phase.** NV7's corpus (1.04 TB/node, 373,077 objects) was reused, so this measures the
upload path only. Target: `ernest-object-storage`, verified empty (5-day `remove junk` lifecycle had
cleared NV4's 2.05M objects) -- the same bucket in the same ice-cold state NV2 used.
Logs: stamp `175902`.

| phase | req/s | requests | files | MB/s | throttles | lost | exhaustions | freezes | refused mean/max |
|---|---|---|---|---|---|---|---|---|---|
| upload p1 | 2,690 | 668,572 | 373,077 | 65,593 | 4,827 | 0 | 0 | 0 | 0.0256 / 0.1350 |
| upload p2 | 2,786 | 665,611 | 373,077 | 66,450 | 1,202 | 0 | 0 | 0 | 0.0063 / 0.0900 |

### Against NV2, which used this bucket ice cold

| | NV2 (stock retry) | NV8 (new code) |
|---|---|---|
| bucket at start | cold, 0 obj | cold, 0 obj |
| **throttles** | **137,985** | **6,029** (22.9x fewer) |
| **lost** | **392** | **0** |
| exhaustions | 753 | **0** |

The bucket variable is controlled here: NV2/NV3/NV4 span cold / 1-pass-warm / 2.05M-objects-warm and
throttling moved only 4.0% (137,985 / 137,141 / 141,317). So a low NV8 number cannot be dismissed as
prefix state.

### The pass-order inversion is the discriminator

| | pass 1 | pass 2 | direction |
|---|---|---|---|
| NV1 (stock retry) | 24,414 | **111,494** | 4.6x **worse** |
| NV8 (new code) | 4,827 | **1,202** | 4.0x **better** |

**S3 capacity drift cannot produce this.** More endpoint capacity lowers both passes uniformly; it
does not reverse their order. A retry storm compounding across passes explains the old escalation --
fast retries (25 ms base) add load to an already-refusing prefix -- and the reshaped backoff
(base 0.025 -> 1.0 s, cap 20 -> 60 s) explains its absence. This is the first within-run evidence in
the series that attributes an improvement to the code rather than to the endpoint.

Caveat kept: it does not prove the *magnitude*. Part of the 22.9x may still be capacity. A same-day
bare-master control run is what would settle the split.

### The brake still never fires

0 freezes in both passes; refused share peaked at 0.135 against `ratio_threshold = 0.2`. Third
consecutive run (NV6 p1, NV6 p2, NV8 p1, NV8 p2) in which the send brake contributes nothing. See
CANONICAL-RUN-CONFIG.md for why the threshold is unreachable on the client-side scale it is compared
against.

## CONTROL -- 2026-09-10, bare master on today's endpoint (the run that settles attribution)

Purpose: split "our code" from "the endpoint changed" for the 12-23x throttle drop seen in NV6/NV8.

Built on branch `nv-control` = today's `upstream/master c99402fba78` + the harness series with the
throttler commit and its three dependent harness commits **dropped**. Verified before deploy: zero
`throttling_controller` files or references, retry backoff back to stock `25 ms x 2^n` with no cap,
RESULT line carrying exactly NV1's field set. Binary md5 `6d5fa7b64932225b76527cec3bc1ac31`,
md5-matched on the instances. Canonical config, 16 x i4i.16xlarge on-demand, `sstables_ewz`.
Logs: stamp `134438`.

| phase | req/s | requests | files | req/file | refused/attempt | throttles | lost |
|---|---|---|---|---|---|---|---|
| download | 6,213 | 7,482,393 | 365,489 | 20.47 | 0 | 0 | 0 |
| upload p1 | 2,195 | 660,333 | 365,454 | 1.81 | 0.0210 | 13,857 | 35 |
| upload p2 | 2,329 | 949,705 | 365,166 | **2.60** | **0.1342** | **127,440** | 323 |

**Totals: 141,297 throttles, 358 lost, p2/p1 = 9.20x.**

### The endpoint has not changed

| run | code | throttles | lost |
|---|---|---|---|
| NV1, 2026-08-04 | bare master | 135,908 | 250 |
| **CONTROL, 2026-09-10** | **bare master** | **141,297** | **358** |
| NV6, 2026-09-09 | new code | 11,691 | 0 |
| NV8, 2026-09-09 | new code | 6,029 | 0 |

Bare master five weeks apart: 135,908 -> 141,297, **+4%** -- inside the 4.0% spread the NV2/NV3/NV4
series already established for this workload. **The endpoint is the same. The whole 12-23x reduction
and all of the loss elimination is the code.**

This retracts two earlier readings from 2026-09-10, both mine:
- "~5x of the gap is endpoint capacity" -- wrong. It is ~1.0x.
- The intermediate revision to "~1.3x endpoint drift" -- also wrong, for the same reason.

The error was inferring from an unchanged `req/file` (1.82 vs 1.79 in pass 1) that our retry
behaviour could not be responsible. Attempt *counts* were indeed unchanged; what the reshaped
backoff changes is attempt *timing*. At a 25 ms base a retry lands while the prefix is still
refusing and is refused again; at 1 s it lands after recovery and succeeds. Same count, different
refusal rate. Only a same-code-different-day or same-day-different-code control can separate these,
which is what this run is.

### The mechanism, in one column

`req/file` in pass 2: **2.60** with stock backoff (this run) against **2.60** in NV1 five weeks ago,
and **1.80** with the reshaped backoff (NV6/NV8). 0.8 extra attempts per file is the retry storm,
reproduced to two decimals and then removed. That is
`throttling_base_sec 0.025 -> 1.0` plus `cap_sec 20 -> 60` doing the work -- **not** the send brake,
which recorded 0 freezes in all four new-code passes.

### Background-load check (Ernest's objection, measured)

`manager-backup-tests-us-east-1` is shared and SCT was hammering it: **13,000-15,400 req/s from
12:00 to 13:15 local**. But it collapsed to 0 by 13:25, and our uploads ran 13:30-13:43 at
1,514-2,544 req/s with no other traffic. NV6's uploads yesterday (16:30-16:45) also ran against
0-84 req/s of background. **Both compared runs had an idle bucket at the same offered rate**, so
concurrent competition is ruled out.

Two residual uncertainties that measurement cannot close here:
1. **Prior load may shape partitions.** Today's uploads followed 75 min of 13-15k req/s; yesterday's
   followed idle. S3 splits partitions from request history.
2. **The metric is bucket-wide.** This bucket's metrics configuration has no prefix filter, so
   `sstables_ewz` cannot be isolated, and throttling is per-prefix.

**Use `ernest-object-storage` for future A/B runs** -- it is Ernest's alone and also has request
metrics enabled, so bucket-level CloudWatch is exactly our load, with no other tenant and no prefix
ambiguity.
