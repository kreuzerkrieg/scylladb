# perf_s3_downloader — what it measures, how to run it, what it has measured

This harness exists to reproduce SRE-1418 (a backup losing objects when S3 refuses requests) and
to measure whether a client-side send brake and a reshaped retry backoff prevent that loss. It is
not a correctness test and is not wired into CI: it needs a real bucket, real credentials, and a
fleet to reach the request rates at which S3 pushes back.

Read this before changing a knob or comparing a new run against an old number. Several of the
figures below cost a fleet run each, and two earlier conclusions were withdrawn because a
load-bearing parameter had silently changed between runs.

## Picking this up

### What this branch is

`f5ba984f70e s3: hold sending back when the endpoint refuses requests` is **not this branch's
work**. It is the client-side throttling implementation from the `s3-throttling` branch
(PR scylladb/scylladb#30775), squashed and cherry-picked here so the harness has something to
measure and something to compile against. If that PR is rebased or reworked, this copy goes stale --
re-cherry-pick it rather than editing it here, and never fold harness changes into it.

Everything above it is this branch's own: four `s3:` diagnostics the measurements depend on, the
harness, and this document.

### Before you can run anything

1. **AWS credentials.** Ask for a 6-digit TOTP code, then `refresh-aws-creds <code>`. They land in
   the `797456418907-DevOpsAccessRole` profile and last ~6 h. The `[default]` profile is stale and
   its errors read as expiry -- `ExpiredToken` means refresh, `InvalidAccessKeyId` means wrong
   profile. Credentials are baked into the generated per-instance run script, so a running job
   cannot pick up new ones: refresh immediately before launching, not before provisioning.
2. **SSH key.** The fleet driver needs `~/.ssh/ernest.pem` and the matching EC2 key pair. Not in the
   repo, for the obvious reason.
3. **A release build.** `cmake --build build/release --target perf_s3_downloader`. A dev build links
   seastar dynamically and dies on the instance.

### Open questions

- **`transient_base_sec = 0.025`** is the only constant implicated in the one object still lost at
  fleet scale: a broken pipe on a multipart `Data.db` whose two parts each burned all 10 transient
  retries inside an 8 s churn episode. Half that budget is spent in the first 1.55 s, against a
  303 s window for the throttling class. `0.1` would give 102 s while keeping a sub-second first
  retry. **Not changed**, because it rests on a single object and losses in this series swing ±45%
  run to run. Rerun before touching it.
- **The brake's own value is unproven.** It fires (1,346 freezes in one run) but no run has shown it
  cutting throttling or loss on its own -- the reshaped retry backoff accounts for the measured
  effect. Whether the brake earns its place is an open question for the PR.
- **Throttle counts are not comparable across runs.** See *What is reproducible and what is not*.

## Modes

| mode | what it does |
|---|---|
| `--mode download` | restore-shaped: lists a backup prefix, fetches whole sstables through `chunked_download_source`, optionally saves them under `--corpus_dir` |
| `--mode upload` | backup-shaped: walks a local corpus and PUTs each component under `<bucket>/<prefix>/<sstable_id>/<component>` |
| `--mode generate` | writes a synthetic corpus with controllable component size and count, so request rate can be decoupled from bytes |

`--delete_after_upload` adds a delete phase over the same keys, at the same concurrency.

## Knobs — what each one controls

Defaults are the production shapes wherever a production shape exists, so an unmodified run is the
one worth comparing against. Every default below is what all fleet results in this document were
measured at.

### Selecting and bounding the work

| knob | default | controls |
|---|---|---|
| `--bucket` / `--prefix` | `$S3_BUCKET_FOR_TEST` | the download source: bucket, and the key prefix the listing is filtered to |
| `--max_objects` | `0` (all) | caps the **listing**, so the corpus is bounded by object count |
| `--round_timeout` | `15` min | caps a round by **wall clock**; `0` runs until the slice is exhausted |
| `--max_rounds` | `0` (unlimited) | stops after N rounds. Walking the list once is one round by definition, so `--corpus_dir` runs force it to 1 |
| `--fleet_size` / `--fleet_index` | `1` / `0` | each instance takes the listing positions congruent to its index. Disjoint, balanced, no coordinator, because S3 lists lexicographically everywhere |

**`--max_objects` and `--round_timeout` are not interchangeable.** A timeout at higher concurrency
yields a *larger* corpus, not the same one sooner. Pin the corpus with `--max_objects` whenever two
runs must feed their upload phase the same input.

### Concurrency and connections

| knob | default | controls |
|---|---|---|
| `--sstable_concurrency` | `16` | sstables in flight per shard on download. Restore uses 16 |
| `--file_concurrency` | `4` | components in flight per shard on upload. Backup uses `initial_sstable_loading_concurrency` = 4 |
| `--initial_connections` | `128` | connections per shard; **doubles each round** in download mode |
| `--sample_interval` | `10` s | seconds between per-second rate samples. The fleet driver passes 60; drop it to 1 when the refused-share series is what you are after |
| `--max_retries` | `10` | attempts per request inside the retry strategy. Coupled to the 60 s cap derivation in `default_aws_retry_strategy` -- changing one means re-deriving the other |
| `--upload_batch` | `1` | items a fiber claims per work-queue round trip. A backup writes an sstable's components as a straight-line chain on one fiber, so >1 is the production shape; 1 pays a queue round trip per request |

### Corpus generation

| knob | default | controls |
|---|---|---|
| `--generate_sstables` | `4000` | sstable directories written |
| `--generate_components` | `1` | components per sstable, i.e. **operations per sstable**. The first ten get real component names; beyond that they are synthesised |
| `--generate_component_size` | `5500` | bytes per component -- **the requests-per-byte ratio**, and the strongest lever on offered request rate |

### Upload key layout and path

| knob | default | controls |
|---|---|---|
| `--upload_bucket` / `--upload_prefix` | `manager-backup-tests-us-east-1` / `sstables_ewz` | upload target |
| `--upload_random_prefix N` | `0` (off) | prepends N random base64url chars **at the key root**, ahead of `--upload_prefix`. N is a character count, not a number of prefixes -- a large N produces keys S3 rejects as too long |
| `--upload_no_prefix` | off | omits the prefix entirely (boost rejects an empty `--upload_prefix`) |
| `--upload_from_memory` | off | reads the component in and PUTs a buffer instead of streaming from a file, removing `upload_file`'s 128 KiB read-ahead stream per object |
| `--delete_after_upload` | off | deletes the same keys afterwards at the same concurrency, reported as its own RESULT line |

### Two knobs that change the request shape, not just its rate

- **`--corpus_dir`** turns the download into a different workload. Saving to disk makes the write
  the consumer that decides how an object is fetched, and the client fragments into 5 MiB ranged
  GETs -- about 19x more requests per object than a whole-object fetch. Amplification measured with
  a corpus is ~20.5 req/object; do not compare it against a run without one.
- **`--upload_random_prefix`** changes which S3 partition the keys land in. Measured across NV1-NV4
  it moves throttling by 4.0%, i.e. not at all for this workload, so it is not a lever for
  provoking or avoiding refusals.

## Recipes — what to set to get a given behaviour

### Reproduce object loss (needs a fleet)

    16 x i4i.16xlarge on-demand, 64 shards, defaults throughout:
      --mode download --prefix <backup>/ --fleet_size 16 --fleet_index <i> \
        --round_timeout 20 --max_rounds 1 --sstable_concurrency 16 --corpus_dir /mnt/data/corpus
      then twice:
      --mode upload --corpus_dir /mnt/data/corpus --upload_bucket <b> --upload_prefix <p>

Stock retry loses 212-392 objects this way; the braked build loses 0-1. **A single machine cannot
do this** -- see below.

### Provoke throttling on one machine

Shrink the objects. Request rate, not bytes, is what the endpoint refuses:

    --mode generate --generate_sstables 6000 --generate_components 10 \
      --generate_component_size 100 --corpus_dir <dir>
    --mode upload --corpus_dir <dir> --file_concurrency 512 --initial_connections 512 --smp 1

Measured: 7,745 PUT/s peak, 1,297 throttling responses, 0 lost. At the default 5,500 B the same run
reaches 1,853 PUT/s and sees **zero** refusals -- below the endpoint's threshold entirely.

Add `--delete_after_upload` for the same effect via a bodyless verb: 2,406/s sustained, 8,109/s
peak, 1,721 refusals. PUT/COPY/POST/DELETE share one per-prefix write budget and a request with no
body is cheap to issue.

**This provokes throttling, not loss.** Local refusals come in bursts -- the longest streak of
consecutive seconds containing one was 2 s against a ~303 s retry window -- so every refusal is
absorbed on the first retry. Use it for brake mechanics only.

### Measure throughput without provoking refusals

Leave `--generate_component_size` at 5,500 or use a real corpus. 1,853 PUT/s on one machine, 0
refusals; ~2,200-2,800 req/s per fleet run at 16 nodes.

### Make two runs comparable

Pin the corpus by count, not by clock, and change nothing else:

    --max_objects 371000 --round_timeout 0 --sstable_concurrency 16

Raising `--sstable_concurrency` then shortens the download without changing what the upload phase is
fed -- the download runs at ~35% of NIC, so there is headroom. Do **not** shorten
`--round_timeout` instead: at higher concurrency that yields a bigger corpus.

### Exercise the send brake

The brake arms when the refused share crosses 0.2. That needs real endpoint pressure: at fleet scale
under refusal it reached the threshold on 1.05% of samples and fired 1,346 freezes, while a milder
fleet run peaked at 0.140 and never armed. **A run reporting 0 freezes means the endpoint was not
pushing back**, not that the threshold is misplaced. There is no knob that arms it directly.

### Raise the offered request rate

In order of effect:

1. **`--generate_component_size` down** -- 5,500 B to 100 B took one machine from 1,853 to 7,745 PUT/s.
2. **`--generate_components` up** -- more operations per sstable at the same object size.
3. **More nodes.**

**Not `--file_concurrency`.** The upload phase already sits at the wire limit: measured `tx` 5,025
MB/s against a 4,688 MB/s nominal NIC on `i4i.16xlarge`. Raising it cannot add requests of a given
size, and it was never once raised on any fleet run that produced a number in this document.

## Reading the RESULT line

One JSON line per phase, per process, aggregated across that node's shards.

- `slowdown` — throttling responses seen by the retry strategy. **Not a stable metric**; see below.
- `failed` / `failed_throttled` / `failed_masked` — objects lost, and the attributed cause.
  `failed_masked` counts losses that would otherwise have surfaced as "Failed to parse ETag list".
- `throttle_exhaustions` — requests that ran out of retries *because of throttling*.
- `freezes` — times the send brake held sending back.
- `refused_ratio_mean` / `refused_ratio_max` — the brake's own input. A share does not sum across
  shards, so it travels as a sum with its own divisor plus a max. The max is a **snapshot at
  collect time**, not a running peak; the per-second sample lines carry the real series.

## Where the full record lives

This document is the summary. The run-by-run record sits next to the driver:

| file | what it holds |
|---|---|
| `test/perf/s3-fleet/measurements-fleet.md` | every fleet run NV1..NV9 with its parameters, per-phase numbers, what each was testing, and the conclusions that were later withdrawn and why |
| `test/perf/s3-fleet/measurements-local.md` | the single-machine investigation: what was eliminated by measurement, and five retracted readings |
| `test/perf/s3-fleet/run-config.md` | the canonical parameter set with the transcript-swept evidence for each knob, plus the deviation log |

The retractions are kept deliberately. Several of them are conclusions that looked solid for hours --
"the endpoint got faster", "the brake cuts throttling", "prefix concentration explains it" -- and were
overturned by a control run. Reading which arguments failed is cheaper than repeating them.

## Fleet results

All rows: 16 x `i4i.16xlarge` on-demand, 64 shards, 20-minute download then 2 upload passes,
~371-375k objects per pass, upload concurrency 4.

| run | date | client code | throttles | exhaustions | **lost** |
|---|---|---|---|---|---|
| NV1 | 2026-08-04 | stock retry, no brake | 135,908 | 384 | **250** |
| NV2 | 2026-08-04 | stock retry, random key root, cold bucket | 137,985 | 753 | **392** |
| NV3 | 2026-08-04 | stock retry, random root, warmed 1 pass | 137,141 | 565 | **260** |
| NV4 | 2026-08-06 | stock retry, random root, warmed a day (2.05M obj) | 141,317 | 395 | **212** |
| NV5 | 2026-08-09 | stock retry, exact repeat of NV4 | 141,539 | 624 | **308** |
| run 17 | 2026-08 | full PR: CUBIC + jitter + cap 60 | 76,219 | 0 | **0** |
| — | 2026-08-13 | reduced PR, cap 30 | 131,899 | 1 | **1** |
| — | 2026-08-13 | reduced PR, cap 60 | 98,619 | 0 | **0** |
| NV6 | 2026-09-09 | squashed brake + reshaped backoff | 11,691 | 0 | **0** |
| NV8 | 2026-09-09 | same, ice-cold `ernest-object-storage`, corpus reused | 6,029 | 0 | **0** |
| CONTROL | 2026-09-10 | **stock retry, no brake** (this series minus the throttler) | 141,297 | 513 | **358** |
| NV9 | 2026-09-10 | brake + reshaped backoff, 49 min after CONTROL | 112,019 | 2 | **1** |

### What is reproducible and what is not

**`slowdown` is not comparable across runs.** The same binary against the same bucket a day apart
gave 11,691 (NV6) and 112,019 (NV9) -- a 10x swing driven by prefix and endpoint state we do not
control. Do not quote a throttle-reduction ratio.

**Loss is reproducible.** Stock retry lost 212-392 across five runs; the braked build lost 0, 0
and 1 across three. The CONTROL/NV9 pair is the cleanest evidence because it is the same day, same
bucket, same upstream base, 49 minutes apart, with the client code as the only variable:

| | stock retry | brake + backoff |
|---|---|---|
| retry exhaustions | 513 | **2** |
| — of them throttling | 351 | **0** |
| — GnuTLS push function | 92 | **0** |
| — broken pipe | 70 | 2 |
| objects lost | 358 | **1** |

The surviving loss was **not** throttling: a broken pipe on a multipart `Data.db`, two parts each
exhausting their 10 transient retries within ~8 s. `failed_throttled` and `throttle_exhaustions`
were both 0 for that pass. Transport churn tracks offered load rather than refusal rate -- CONTROL
saw 70,638 broken pipes against NV9's 75,457, i.e. more churn with fewer refusals.

### The brake's input, measured (NV9, 57,063 per-second samples)

| p50 | p90 | p99 | p99.9 | peak |
|---|---|---|---|---|
| 0.000 | 0.134 | **0.200** | 0.280 | 0.371 |

1.05% of samples reach the 0.2 threshold; 1,346 freezes fired. The threshold sits at the knee of
the distribution. Under milder conditions (NV6) the share peaked at 0.140 and the brake never
armed at all -- so a run with 0 freezes says the endpoint was not pushing back, not that the
threshold is misplaced.

## Single-machine results (2026-08-24, one desktop, `il-central-1`)

Object size, not concurrency, sets the achievable PUT rate. 60,000 objects, 1 shard, 512 fibers:

| component size | peak PUT/s | throttles | lost |
|---|---|---|---|
| 5,500 B | 1,853 | **0** | 0 |
| 100 B | **7,745** | **1,297** | 0 |

At 5,500 B the run sits below the endpoint's refusal threshold and never sees a 503; at 100 B it
goes straight through. A bodyless DELETE behaves the same way for the same reason -- 2,406/s
sustained with 8,109/s peaks, 1,721 throttles -- because PUT/COPY/POST/DELETE share one per-prefix
write budget and a request with no body is cheap to issue.

**A single machine cannot reproduce loss.** Local throttling is bursty: the longest streak of
consecutive seconds containing a refusal was 2 s, against a ~303 s retry window, so every refusal
is absorbed on the first retry. Use the local path to exercise brake mechanics, never to measure
loss.

## Fleet driver and the traps that cost runs

The driver is `test/perf/s3-fleet/s3-fleet.sh`, with `remote-setup.sh` beside it as the
per-instance provisioning step it copies over. Both are committed so a session on another
machine can drive a fleet without reconstructing them.

`BIN` defaults to `build/release/test/perf/perf_s3_downloader` relative to the script's own
location and `OUT` to a repo-local `fleet-runs/`; both are overridable by environment. The
binary **must** be a release build -- a dev build links seastar dynamically and dies on the
instance with `libseastar_perf_testing.so: cannot open shared object file`.

`setup` is the slow step (~4 min) and is worth keeping warm across runs. It RAID-0s the
instance-store NVMe with the geometry `dist/common/scripts/scylla_raid_setup` uses -- 1024 KB chunks
and `mkfs.xfs -K -m rmapbt=0 -m reflink=0`, not ext4 on mdadm's default -- because the disk write is
the consumer that decides whether a large object arrives as one ranged GET or as many 5 MiB chunks,
so filesystem throughput moves the request rate being measured. It then installs the ~14 sonames the
Fedora Cloud Base image lacks, upgrades `libstdc++`/`libgcc` to cover point-release drift against the
build host, and prints `SETUP_OK` only once `ldd` is clean and the binary runs `--help`. A node that
does not print it must not be counted as ready. The reasoning for each step is in the script.

Canonical invocation, and **anything extra is a deviation that must be stated**:

    FLEET=<name> TYPES=i4i.16xlarge MARKET=ondemand ./s3-fleet.sh launch 16
    FLEET=<name> ./s3-fleet.sh setup
    FLEET=<name> PHASES=both ROUND_MIN=20 UPLOAD_PASSES=2 ./s3-fleet.sh run
    FLEET=<name> ./s3-fleet.sh collect && FLEET=<name> ./s3-fleet.sh kill

- **`TYPES` must be passed.** The driver defaults to 8xlarge, which saturates its NIC at ~1.3
  requests per object and throttles about once in 3 million requests. One run measured nothing
  because of this.
- **`--file_concurrency` was never passed on any fleet run.** The default 4 is what every number
  above was measured at. Raising it also cannot raise the request rate: the upload phase is already
  at the wire limit -- measured `tx` 5,025 MB/s against a 4,688 MB/s nominal NIC. The download phase
  runs at ~35% of NIC, so the "we are only at half the NIC" recollection applies to download only.
- **Spot is unusable at this size.** Placement score 1 for 16 units, and a spot instance was
  reclaimed two minutes into a run.
- **The retry logger must be raised** for any probe that counts retries from log text:
  `--logger-log-level default_http_retry_strategy=debug`. The line is emitted at `debug`, and a run
  at `warn` reports zero retries whatever happens.
- **Prefer `ernest-object-storage` for A/B runs.** `manager-backup-tests-us-east-1` is shared with
  SCT, which was measured at 13,000-15,400 req/s on it, and its metrics configuration has no prefix
  filter so our prefix cannot be isolated.
