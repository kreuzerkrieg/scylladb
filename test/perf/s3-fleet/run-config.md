# Canonical fleet run configuration — read before launching anything

Evidence: swept from all session transcripts under
`~/.claude/projects/-home-ernest-zaslavsky-Development-scylladb/*.jsonl` on 2026-09-09
(occurrence counts in parentheses) plus `git log -L` on the harness defaults.

**Any deviation from this table makes the run non-comparable to NV1-NV6. State the diff up
front or do not launch.**

## The NV-series config (runs 1-17 and NV1-NV6 all used this)

| knob | value | how it is set | evidence |
|---|---|---|---|
| instance type | `i4i.16xlarge` | `TYPES=i4i.16xlarge` | 51 hits. **The driver defaults to 8xlarge — always pass this.** 8xlarge saturates its NIC at ~1.3 req/object and throttles ~never (1 in 3.05 M) |
| node count | 16 | `./s3-fleet.sh launch 16` | every NV run |
| market | on-demand | `MARKET=ondemand` | 50 hits. Spot placement score is 1 for 16 units; a spot instance was reclaimed 2 min into a run on 2026-09-09 |
| shards | 64 | implied by 16xlarge | |
| phases | download then upload | `PHASES=both` | |
| download round | 20 min | `ROUND_MIN=20` | 76 hits |
| upload passes | 2 | `UPLOAD_PASSES=2` | 79 hits |
| download concurrency | 16 | driver passes `--sstable_concurrency 16` | 63 hits |
| **upload concurrency** | **4 (the default)** | **never passed** | see below |
| connections/shard | default | never overridden on a fleet run | |
| max_retries | default 10 | never overridden | |
| source | `manager-backup-tests-permanent-snapshots-us-east-1` `ernest-sct-tests/6TB-tablets-RF3-6node` | `SRC_BUCKET` / `SRC_PREFIX` defaults | 29 hits |
| upload target | `manager-backup-tests-us-east-1` `sstables_ewz` | `UPLOAD_PREFIX` default | 33 hits |
| key layout | static prefix | `RANDOM_PREFIX` unset (0) | `RANDOM_PREFIX=16` only for NV2/NV3/NV4/NV5 |
| retry logger | debug | driver passes `--logger-log-level default_http_retry_strategy=debug` | required or the probe reports zero |

### Canonical invocation

```bash
FLEET=ernest-s3fleet-<name> TYPES=i4i.16xlarge MARKET=ondemand ./s3-fleet.sh launch 16
FLEET=ernest-s3fleet-<name> ./s3-fleet.sh setup
FLEET=ernest-s3fleet-<name> PHASES=both ROUND_MIN=20 UPLOAD_PASSES=2 ./s3-fleet.sh run
FLEET=ernest-s3fleet-<name> ./s3-fleet.sh collect
FLEET=ernest-s3fleet-<name> ./s3-fleet.sh kill
```

Pass **nothing else**. Every extra argument is a deviation.

## `--file_concurrency`: never set on a fleet run, ever

- `default_file_concurrency = 4` was introduced by `ee60188922b test: perf_s3_downloader: add
  an upload mode mirroring backup` and **never changed** (`git log -L` over the whole series
  returns exactly one hit, the introduction). 4 is what backup uses
  (`initial_sstable_loading_concurrency`).
- All 54 command-line occurrences of `--file_concurrency` in session history belong to the
  **local single-machine** work against the Israel bucket
  (`ernest-il-797456418907-il-central-1-an`, `--upload_no_prefix`, `deltest`, `$SCR/huge`,
  `$SCR/c2`, `$SCR/short`) -- the 2026-08-24 Petr investigation, values 8/16/64/128/256/512/1024.
  **None is a fleet invocation.**
- Confirmed from a run log: NV6 printed
  `Uploading 23919 components to s3://.../sstables_ewz/ (4 concurrent per shard x 64 shards)`.

**2026-09-09 mistake to not repeat:** NV7 was launched with
`UPLOAD_EXTRA="--file_concurrency 16"` as a way to "add strain". That is 4x the series default
and makes the upload phase incomparable to every prior run. Caught by Ernest after launch; the
chain parent was killed on all 16 nodes so the download survived, and the upload passes were
re-run at the default. The wrong pass is kept as `ul_conc16.txt`.

## NIC utilisation, so it is not re-derived wrongly

Per-node ceiling on `i4i.16xlarge` is 37.5 Gbit/s = **4,688 MB/s**.

| phase | measured | % NIC | source |
|---|---|---|---|
| download | 1,155 / 1,457 / 1,479 MB/s | 24-31% | sizing sweep, prior sessions |
| download, NV7 direct `rx` sample | 1,643 MB/s | 35% | `/sys/class/net/ens5/statistics/rx_bytes`, 2026-09-09 |
| **upload, NV7 direct `tx` sample** | **5,025 MB/s** | **107%** | `/sys/class/net/ens5/statistics/tx_bytes`, 2026-09-09, three consecutive 8 s samples (5025/5040/5014) |

`describe-instance-types` gives baseline == peak == 37.5 Gbit/s for this type, so there is no burst
allowance to explain 107%; the excess is wire framing (TLS+TCP+IP+eth) over payload plus the round
37.5 figure. Either way **the upload phase runs at the wire limit.**

Consequence, and it is the important one: **raising upload concurrency cannot raise the request
rate.** At 4 concurrent per shard the NIC is already full. The only lever that raises requests per
second at a fixed wire rate is *smaller objects* -- measured 2026-08-24 on one machine as
1,853 PUT/s at 5,500 B against 7,745 PUT/s at 100 B. Any future "add more strain" plan has to go
through object size, not concurrency.

The "we were barely scratching 50% of NIC" recollection is the **download** phase and it is
correct. Do not apply it to the upload phase -- upload moves the same bytes in about a quarter
of the time, so its byte rate is roughly 4x the download's.

## Deviation log

| run | date | deviation from canonical |
|---|---|---|
| NV2, NV3, NV4, NV5 | 2026-08-04..09 | `RANDOM_PREFIX=16` (deliberate: key-layout test) |
| run 13 | 2026-07 | 8xlarge instead of 16xlarge -- measured nothing, $27 wasted |
| NV6 | 2026-09-09 | target prefix cold (7-day lifecycle had emptied `sstables_ewz`) |
| NV7 | 2026-09-09 | upload pass 1 ran at `--file_concurrency 16` by mistake; corrected passes re-run at 4 |
