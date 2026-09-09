# Handover — SCYLLADB-4293, object-storage short reads

**NOT FOR MERGE.** This file is the last commit on the branch so it can be dropped with
`git rebase --onto HEAD~1 HEAD~1` (or `git reset --hard HEAD~1`) before opening the PR.

Written 2026-09-09. Branch `EWZ/object-storage-short-read`, pushed to `origin`
(`git@github.com:kreuzerkrieg/scylladb.git`).

---

## 1. Read this first if you check out current master

Today's `upstream/master` added an **`fmt` submodule with a relative URL** (`../fmt` in
`.gitmodules`). A relative submodule URL resolves against the remote you are working from, so on
this fork it becomes `git@github.com:kreuzerkrieg/fmt.git`, which does not exist.

Consequence: `git checkout -b <branch> upstream/master` **aborts part-way through** with

```
fatal: not a git repository: ../.git/modules/fmt
fatal: could not reset submodule index
```

and leaves the working tree half-converted to master's contents (44 modified files here, plus
`test/pylib/gcs_upload_validator.py` deleted). Nothing gets staged and HEAD does not move, so
recovery is `git restore --source=HEAD --worktree .` — but it looks alarming and it is easy to
misread as data loss.

Workarounds, either of:

```bash
git -c submodule.fmt.url=https://github.com/scylladb/fmt.git submodule update --init fmt
# or avoid touching submodules during the switch:
git -c submodule.recurse=false checkout -b <branch> upstream/master --no-recurse-submodules
```

**This series therefore bases on `aa6f18a0b6`** ("Merge 'test.py: fix coverage mode' from Andrei
Chekun", 2026-09-07), which is a real upstream commit and an ancestor of current master, so the
series rebases forward normally. It was also what the local `build/dev` was already warm for.

## 2. What the series does

Five commits, 149 insertions. Fix before test, so no commit in the series is red.

| Commit | What |
|---|---|
| `6619e97c38` | `ent/encryption`: `transform()` returns the bytes it decoded, clamped, instead of the distance to end of data |
| `03db78d10c` | `test/boost`: new `test_short_read_mid_file_is_not_eof` |
| `2f66bafd7a` | `utils/gcp`: zero the read accumulator at handler entry (retry misplacement) |
| `60f38fd32b` | `utils/gcp`: fail `read_dma` when the delivered length ≠ the requested range |
| `2ed2416d21` | `utils/s3`: same check on all three `readable_file` read paths |

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

## 5. Open question — do not overstate the analysis

The mechanism above is code-verified and now unit-reproduced. What is **not** established:

1. **That a truncated body actually occurred in the 4293 run.** Retryable failures are logged at
   `debug` by `object_storage_retry_strategy::should_retry`, `debug` was off, and seastar tolerates
   `left_content_length > 0` silently. The probe *could not have fired*, so its absence from the
   logs is not evidence either way.
2. **The silent wrong value on GCS.** The 12 loud parse failures fit the `encrypted_data_source`
   trimming path cleanly. The single client-visible corrupt value (1024-byte cell correct for 563
   bytes, garbage after) does **not** — `encrypted_file_impl`'s over-report explains that shape, but
   it is not on the GCS data path. Next thing to test: in `encrypted_data_source::get()`,
   `_current_position += block_size` advances a full block even for the trimmed partial one, which
   would desync the per-block IV and decrypt everything after it to garbage. Plausible, unverified.
3. **Why it correlates with tablet cleanup.** Cleanup drives a burst of LIST/DELETE traffic
   (the 404 spikes track it) plus aborted compactions, which churns pooled HTTP connections. No
   direct evidence.

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

1. Read `SCYLLADB-4293-comment.md` (working tree, uncommitted) and post it if it reads right.
2. Drop this file, rebase onto current master (see §1), push.
3. Open as draft PR against `master`, `maintainer_can_modify: true`, assignee `kreuzerkrieg`.
   Labels: `ai-assisted`, `area/object_storage`, `area/test`, and `backport/2026.1` + `.2` + `.3` —
   all three branches carry both defects. The encryption fix and the client checks are independent
   and could go as separate PRs if a reviewer prefers.
4. Analysis write-up (full reasoning, diagrams, citations):
   https://claude.ai/code/artifact/10af99b2-39d3-4d37-a211-94760294524b
