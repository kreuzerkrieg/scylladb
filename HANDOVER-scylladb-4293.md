# Handover — SCYLLADB-4293, object-storage short reads

**NOT FOR MERGE.** This file is the last commit on the branch so it can be dropped with
`git rebase --onto HEAD~1 HEAD~1` (or `git reset --hard HEAD~1`) before opening the PR.

Written 2026-09-09. Branch `EWZ/object-storage-short-read`, pushed to `origin`
(`git@github.com:kreuzerkrieg/scylladb.git`).

---

## 1. Base, and the fmt submodule

The series is rebased onto `c99402fba78` (`upstream/master` of 2026-09-08). Nothing needs working
around here any more, but the trap is worth knowing if you clone fresh.

Master carries an **`fmt` submodule with a relative URL** (`../fmt` in `.gitmodules`). A relative
URL resolves against the remote you are working from, so on this fork it becomes
`git@github.com:kreuzerkrieg/fmt.git`, which does not exist, and
`git checkout -b <branch> upstream/master` aborts part-way through with

```
fatal: not a git repository: ../.git/modules/fmt
fatal: could not reset submodule index
```

leaving the tree half-converted. Nothing gets staged and HEAD does not move, so recovery is
`git restore --source=HEAD --worktree .`, but it reads like data loss.

This worktree already carries the fix in local config (`submodule.fmt.url =
git@github.com:scylladb/fmt`). For a new clone, either of:

```bash
git -c submodule.fmt.url=https://github.com/scylladb/fmt.git submodule update --init fmt
git -c submodule.recurse=false checkout -b <branch> upstream/master --no-recurse-submodules
```

## 2. What the series does

Eleven commits, in this order. Fix before test, so no commit is red. The last commit is this file
and is dropped before the PR. Subjects rather than SHAs: every rebase rewrites them.

| # | Commit subject | What |
|---|---|---|
| 1 | `ent/encryption: clamp transform() to the bytes it actually decoded` | stops reporting the distance to end of data for a buffer that stopped short |
| 2 | `test/boost: cover a short read that is not at end of file` | `test_short_read_mid_file_is_not_eof` |
| 3 | `utils/gcp: start the read count over on every attempt` | the accumulator survived a retry and placed the range at the wrong offset |
| 4 | `utils/gcp: fail a positional read that comes back short` | `readable_file::read_dma` |
| 5 | `utils/s3: fail a positional read that comes back short` | the same check on all three `readable_file` paths |
| 6 | `ent/encryption: use the delivered length in the iovec read path` | independent, pre-existing; only test code reaches it today |
| 7 | `test/boost: cover a short iovec read that is not at end of file` | the iovec counterpart of 2 |
| 8 | `test/boost, utils/s3: cover the short-read check on a readable file` | both directions, via an injection point |
| 9 | `utils: retry a reply body that ends before its declared length` | moves the detection inside the reply handler so the retry strategy sees it |
| 10 | `utils/gcp: fail a ranged read of an object that comes back short` | `object_data_source`, the path Data/Index reads actually take on `gs` |
| 11 | `test/boost, utils/gcp: cover a truncated range on the download source` | two ranged GETs, so a retried range has to land in the right place |

Commits 6-8 came out of a review pass on 2026-09-09, 9-11 out of the upstream comparison on
2026-09-10. Two groups could be split off if a reviewer prefers: the iovec pair (6, 7) is an
independent pre-existing defect in a path only tests reach, and the retry work (9-11) is a
refinement of 4 and 5 rather than a fix for them.

### Why the retry work exists

Commits 4 and 5 made a short positional read fail. That is right, but they put the check *after*
the retry loop had already exited, because a truncated body is not an error anywhere in the http
stack - `content_length_source_impl` answers a connection closed mid-body with an empty buffer, and
`do_make_request()` then drains the remainder or drops the connection and returns a ready future.
So a byte range that was still there to be fetched became a hard failure, and a user read has other
replicas to fall back on where compaction and restore have nobody to ask.

Commit 9 moves the detection into the reply handler, where an exception still reaches the retry
strategy, and raises it as `protocol_error`, which `from_system_error()` classifies as retryable.
The checks outside the loop stay and now answer a different question - whether the reply described
the range that was asked for - which no retry can fix.

**Both native clients do it this way**, which is the strongest argument in the series:

- **aws-sdk-cpp** makes exactly this comparison in exactly this place. `CurlHttpClient`, on the
  success path once the body has been read, rejects `contentLength != numBytesResponseReceived`
  with `CoreErrors::NETWORK_CONNECTION`, and `AWSErrorMarshaller` turns that class into a retryable
  error. The same check sits in `WinSyncHttpClient`, so it is policy, not a curl quirk.
- **google-cloud-cpp** reaches the same place from the other side. It carries no length check
  because libcurl reports a short transfer as `CURLE_PARTIAL_FILE`; `RetryObjectReadSource::Read()`
  then advances `current_offset_` by `bytes_received` and, on a failed read, rebuilds the connection
  with `ReadFromOffset(current_offset_)` and the generation pinned. Commit 10 is that design.
- Worth knowing it validates crc32c and md5 on download but **gives up for ranged reads**
  (`CreateHashValidator(ReadObjectRangeRequest)` returns a null validator when
  `RequiresRangeHeader()`), so a ranged read gets no checksum there either and the length is all
  there is.

seastar is the outlier: it tolerates `left_content_length > 0` by design, which is why the check has
to live in our clients. Permalinks are in the commit messages.

### The defect, in one paragraph

A truncated HTTP response body reaches the object-storage clients as a *clean end of stream*, not an
error — seastar's `content_length_source_impl::get()` answers a connection closed mid-body with an
empty buffer, and `client::do_make_request()` then skips the unread remainder or drops the
connection rather than raising. Both `readable_file` implementations return whatever arrived without
comparing it to the range they asked for. Layers above are entitled to assume otherwise, because on
a local file `dma_read()` only comes back short at end of file. `encrypted_file_impl::transform()`
acted on exactly that assumption: any length not a multiple of 4096 was taken for the file's last
block, and it returned `max(_file_length, pos) - pos` while having decoded only
`align_down(rem, 16)` bytes — so the caller was handed a full-length buffer whose tail this read
never wrote.

## 3. Verification already done (all on this machine, dev mode)

| What | Result |
|---|---|
| `encrypted_file_test` full suite | 26/26, 0 skipped — the #22236 padding cases still pass |
| new test, fix applied | reports **992** (`align_down(1000,16)`) instead of 16384 |
| new test, `6619e97c38` reverted | fails both assertions, as intended |
| `gcp_object_storage_test` | 19/19, 0 skipped; the new check never fired spuriously |
| `s3_test` via `test.py` | 45/45, includes `test_client_readable_file_{minio,proxy}` and `_stream_` |
| `s3_test` via `test.py`, 2026-09-09 | 46/46 with the new short-read case |
| new iovec test, fix applied / reverted | passes / fails with `reported 8192 bytes from a read that delivered only 4096` |
| new s3 short-read test, check reverted | fails with `exception storage_io_error expected but not raised` |
| all three suites, refined series, 2026-09-10 | `encrypted_file_test` 28/28, `gcp_object_storage_test` 21/21, `s3_test` 47/47 |
| `object_data_source` guards off, one range's body dropped | a 33,623,263 byte object read as **0 bytes, no error** |
| same experiment, guards on | `storage_io_error: ... answered 0 bytes for the 8388608 bytes asked for at offset 0 of 33623263` |

Reproducing those:

```bash
ninja -C build/dev test_boost_encrypted_file_test
./build/dev/test/boost/encrypted_file_test --log_level=test_suite

ninja -C build/dev test_boost_gcp_object_storage_test
ENABLE_GCP_STORAGE_TEST=1 ./build/dev/test/boost/gcp_object_storage_test --log_level=test_suite

# S3 needs the real /usr/local/bin/minio, which test.py starts itself.
mkdir -p testlog/pytest_log
~/Development/scylladb/venv/bin/python ./test.py --mode dev test/boost/s3_test.cc
```

Two local gotchas worth carrying over: a single boost case needs both the suite prefix *and* its
gate variable (`--run_test='gcs_tests/<name>'` plus `ENABLE_GCP_STORAGE_TEST=1`), or you get the
misleading `no test cases matching filter or all test cases were disabled`; and `./test.py` must run
through the venv python, since the pyenv shim is 3.12 and `runner.py` needs ≥ 3.13.

## 4. Deliberately not done

- **No `encrypted_data_source` change.** That is the wrapper GCS data reads actually go through
  (`object_storage_base::make_source` ignores the file it is passed and always builds a fresh
  download source). It *trims* an unaligned tail rather than over-reporting, so a truncated body
  there ends the stream early and fails loudly. It also already has short-buffer coverage:
  `strict_memory_source` built on `limiting_data_source_impl`, plus `test_encrypted_data_source_fuzzy`
  (1000 iterations, random chunk sizes 1–14700). Nothing to fix, and a new test would be redundant.
- **No change to `get_object_contiguous()`.** The body-vs-`Content-Length` check belongs there and
  would collide with open PR #31503, which guards the *oversized* direction in the same function.
  The two checks are complementary — one bounds the allocation, this series bounds what may be
  reported as read.
- **No PR opened, no Jira comment posted.**

## 5. What the run's logs settle, and what they do not

Read from `db-cluster-3e1296ae.tar.zst` and `sct-runner-events-3e1296ae.tar.zst` on 2026-09-09.

### Established

| Fact | Evidence |
|---|---|
| The client-visible corruption is one partition, deterministic | 10 `ReadValidationError` events, all partition `O057OO2920`, all byte-identical: a 1024 byte cell correct for 563 bytes then wrong to the end (457 of 1024 bytes differ, no matching suffix) |
| The server-side corruption is broad, not one bad file | 12 read failures across **10 distinct sstables**, in 3 bursts (09:32:42, 09:32:48, 09:35:16), all in the `sl:d` group, reported by coordinators against replicas `f1d249a9` and `9354141c` — both nodes that joined during the run |
| Eight distinct signatures, one of them a premature end of stream | `end of input, but not end of partition` x3, `consumer not at partition boundary` x3, `SSTables with Cassandra-style shadowable deletion cannot be read by Scylla` x2, `static row should be a first unfiltered in a partition`, `Corrupted range tombstone: invalid boundary type 96`, `... type 253`, `Closing range tombstone that wasn't opened` (with `deletion_time=-9223372036853554350`) |
| Nothing was reported as a storage error | all 27,678 GCS non-retryable errors are `404 Not Found` at `Retry# 0`; no 5xx and no connection error reached that branch; `system.corrupt_data` has no entries |
| The window is topology churn | tablet migration and tablet cleanup from 09:32:41, compactions stopped mid-flight on five nodes, new compactions starting inside the same second |
| It is not a slow-burning fault | the stress ran 29m2s with no parse failure before 09:32:42 |

### The series is mostly not on this run's read path

The run configures `object_storage_endpoints: [{name: https://storage.googleapis.com, type: gs}]`.
`gcs_storage` does not override `make_source`, so `object_storage_base::make_source` always builds
`_client->make_download_source(...)` and **ignores the `file` it is handed** — for Data and for Index
alike, since `make_data_or_index_source` just forwards. So on this run, Data and Index reads reach
neither `gcp::readable_file` nor `encrypted_file_impl`; they go through `encrypted_data_source`.

`s3_storage::make_source` is different: for `offset != 0` it calls `make_file_data_source(f, ...)`, so
on S3 a positional read does go `encrypted_file_impl` -> `s3::client::readable_file`. That is the pair
this series fixes, and it is a real path — just not the one that produced these log lines.

What the series does cover on a `gs` cluster is the components opened as files by
`object_storage_base::open_component` -> `make_readable_file`: TOC, Statistics, Summary, Filter and
the Scylla metadata.

### Hypothesis, code path verified, occurrence not

A silently short read of **Summary** or the Scylla/Statistics metadata through `gcp::readable_file`
yields wrong index positions, and the Data reader then starts mid-record. That is exactly the
`consumer not at partition boundary` / `static row should be a first unfiltered` / `Corrupted range
tombstone: invalid boundary type` family seen above. The commit `utils/gcp: fail a positional read
that comes back short` closes that path. Nothing in the logs confirms it happened.

### Closed: the IV-desync hypothesis does not hold

The earlier next-step guess was that `encrypted_data_source::get()` advances `_current_position +=
block_size` over a trimmed partial block and desyncs the per-block IV. It does not: the partial-block
branch is only reached when `read_exactly()` comes back short, which for a seastar input stream means
EOF, and the stream terminates there — so there is no later block to decrypt against a stale
position. `assert(is_aligned(_current_position, block_size))` also holds trivially, the counter only
ever moving by a whole block. Consistent with the loud premature-EOF signature actually logged.

Two notes on reading the 563 byte boundary:

- The cipher is `AES/CBC/PKCS5Padding` (the default; the run sets no `cipher_algorithm`), applied
  unpadded per 4096 byte block with an ESSIV IV. In CBC a **wrong IV corrupts only the first 16
  bytes** of a block. Garbage running to the end of the cell therefore needs the ciphertext itself to
  be wrong or misaligned from that point, which is what a buffer tail this read never wrote looks
  like — and is not what an IV-only fault looks like.
- 563 is not a multiple of 16, but that proves nothing: the cell's offset inside the decrypted page
  is unknown, so the boundary can still be 16-byte aligned in file coordinates.

### Still open

1. **That a truncated body occurred at all.** `object_storage_retry_strategy::should_retry` logs
   retryable failures at `debug`, `debug` was off, and seastar tolerates `left_content_length > 0`
   silently. The probe could not have fired, so its silence is not evidence either way.
2. **Why ten reads returned identical garbage.** Either the row/page cache served one bad row ten
   times, or the bytes were persisted by a write path that consumed them. The logs do not separate
   these.
3. **Why it correlates with tablet cleanup.** Cleanup drives LIST/DELETE traffic (the 404s track it)
   plus aborted compactions, which churns pooled HTTP connections. Still no direct evidence.

## 6. Facts about the run, corrected

From `db-cluster-3e1296ae.tar.zst` (fetch it with `argus run logs download db-cluster-3e1296ae.tar.zst
--run-id 3e1296ae-2326-495c-bafd-e2bb23fbc234` — Argus proxies the S3 fetch, so expired AWS
credentials do not matter):

- **`user_info_encryption: enabled: true`** (GCP KMS) on every node. Absent from the ticket, and it
  is what turns a short read into silent garbage rather than a clean failure.
- **27,678 GCS 404s** cluster-wide (4,226–4,952 per node). The ticket says none appear anywhere.
  They are benign — `Retry# 0` deletion and existence probes that `delete_components()` and
  `exists()` swallow as `ENOENT` — and there are zero `Could not read object` and zero
  `storage_io_error` escapes, so the reads did all return 2xx. Right conclusion, wrong premise.
- The sstable names in the log are **display names**: `component_name::format()` always renders
  `prefix/component_basename` regardless of layout, so no layout or provenance can be read off them.
- 12 parse failures, 3 bursts, replicas node3 (`f1d249a9`) and node4 (`9354141c`), all inside a
  tablet-cleanup window on the shard that had just handed a tablet away.

## 7. Prior art

This is the fourth premature-EOF bug in this layer; the first three were each fixed in whichever
wrapper surfaced them, none at the client where a short body is first observed.

- SCYLLADB-1523 / 1706 — `create_file_for_seekable_source` `read_dma(iovec)` never advanced position. Done.
- SCYLLADB-2812 — concurrent positional reads interleaved `seek()`+`get()`; `_position` ran past EOF. Done, semaphore.
- SCYLLADB-2213 → **2962** — a nested wrapper's empty buffer from `skip()` read as EOF. Done, PR #30478, backported 2026.2.
  2962 states the rule this series applies: *"Need to check whether we in fact are at (indicated) EOF
  before returning this empty buffer."*
- #22236 (`a51888694e`, Calle Wilund) — introduced the `max(l, pos) - pos` return being fixed here.
  It was correct for its case; keep that case working.

Worth copying **Calle Wilund** on 4293 — he owns the rule and the reproduction harness.

## 8. Next steps

1. **Get the baseline retry count before spending a rerun.** `object_storage_total_get_retries`
   (utils/object_storage_metrics.cc:67, from seastar's `http.get_stats()[method].retries`) is a
   counter, so no log level can hide it - unlike `should_retry`, which logs retryable failures at
   `debug` and is why §5.1 is still open. It is in the original run's
   `monitor-set-3e1296ae.tar.zst`, which has not been downloaded. In the unfixed build a truncated
   body was never an error, so it was never retried: a baseline near zero is what the theory
   predicts, and a rerun that shows GET retries climbing is the probe finally firing.
2. Drop this file, rebase onto current master (see §1), push.
3. Open as draft PR against `master`, `maintainer_can_modify: true`, assignee `kreuzerkrieg`.
   Labels: `ai-assisted`, `area/object_storage`, `area/test`, and `backport/2026.1` + `.2` + `.3` —
   all three branches carry both defects. The encryption fix and the client checks are independent
   and could go as separate PRs if a reviewer prefers.
4. Analysis write-up (full reasoning, diagrams, citations):
   https://claude.ai/code/artifact/10af99b2-39d3-4d37-a211-94760294524b
