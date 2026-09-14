#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Read-integrity workload for a keyspace on object storage.

A reduced local copy of the SCT longevity run that fails with SCYLLADB-4293,
longevity-150GB-12h-autorization-LimitedMonkey-cql-stress.yaml. That run reads
back 1024-byte cells whose leading bytes are correct and whose tail is garbage,
and nothing in test/cluster would notice: no test there checks the content of a
value read from an object-storage keyspace.

What the SCT run does, and what is mirrored here
------------------------------------------------
    prepare_write_cmd   write cl=ALL n=50050075 -col 'size=FIXED(1024) n=FIXED(1)'
                        -schema compaction(strategy=LeveledCompactionStrategy)
    stress_cmd          write cl=QUORUM duration=11h -pop seq=400200300..600200300
    stress_read_cmd     read  cl=QUORUM duration=11h -pop seq=1..50050075
    run_fullscan        mode table_and_aggregate, every 5 minutes

So: a bulk prepare, then a long phase where reads of the prepared range run
*concurrently* with writes into a disjoint range and with periodic full scans,
while compaction churns underneath. The corruption appears in that second
phase, not in the prepare.

cql-stress writes cassandra-stress' standard1 schema, `key blob PRIMARY KEY,
"C0" blob`, so every partition holds exactly one row of one 1024-byte cell.
There is no clustering key and no promoted index - worth knowing, because it
rules out a promoted-index offset as the explanation and leaves the partition
index itself.

Four things have to line up for the corruption to be visible at all:

  * a keyspace on object storage - parametrized local / s3 / gs by the `storage`
    fixture, where the local arm is the control;
  * encryption with AES/CBC, as `user_info_encryption` configures cluster-wide.
    Without it a read at a wrong offset throws a parse error; with it the result
    is garbage that parses, which is the shape the failing runs show;
  * reads that run while writes and compactions are in flight;
  * a check on the bytes. Every row is written from a deterministic generator,
    so the expected value of any key is recomputable, and a mismatch reports how
    much of the cell was correct before it diverged - the number that separates
    a read at a wrong offset from a bad decryption.

Not mirrored: 50M rows and 11 hours. The nemesis is approximated by adding and
decommissioning a node during phase 2, which drives the tablet streaming and
cleanup that both instrumented runs were inside when they failed.
"""

import asyncio
import logging
import os
import random
import struct
import time

import pytest

from cassandra import ReadFailure, ReadTimeout, Unavailable, WriteTimeout
from cassandra.cluster import NoHostAvailable
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.cluster.util import new_test_keyspace, wait_for_no_pending_topology_transition
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.object_storage import Storage, format_tuples

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Workload shape. The SCT numbers are in the comments; these are scaled so the
# run finishes in minutes while still giving compaction real work.
# ---------------------------------------------------------------------------
# Both scale knobs can be overridden from the environment, so the same test can
# be a quick smoke run or a long soak without editing it.
NUM_NODES = int(os.environ.get("OBJSTORE_NODES", 6))             # SCT: n_db_nodes 6
PREPARE_ROWS = int(os.environ.get("OBJSTORE_PREPARE_ROWS", 1_000_000))   # SCT: 50_050_075
VALUE_SIZE = 1024             # SCT: -col 'size=FIXED(1024) n=FIXED(1)'
WRITE_POP_OFFSET = 4_000_000  # Second-phase writes go to a disjoint key range.
PHASE2_SECONDS = int(os.environ.get("OBJSTORE_PHASE2_SECONDS", 120))     # SCT: 11h
READ_CONCURRENCY = 64         # SCT: threads=250
WRITE_CONCURRENCY = 64        # SCT: threads=250
PREPARE_CONCURRENCY = 1024    # SCT: threads=1000
PREPARE_BATCH = 20_000        # Coroutines in flight at once during prepare.
FULLSCAN_LIMIT = 5_000        # A full scan has to fit the driver; SCT has no limit.
FULLSCAN_INTERVAL = 15        # SCT: every 5 minutes.
CHURN_INTERVAL = int(os.environ.get("OBJSTORE_CHURN_INTERVAL", 30))  # SCT nemesis: 30 min

# The default query_page_size_in_bytes would split the scans below into pages.
QUERY_PAGE_SIZE = 64 * 1024 * 1024

# A 128-bit AES/CBC system key, the shape test_topology_ops_encrypted uses. CBC
# is what user_info_encryption defaults to, and it is the mode in which wrong
# ciphertext corrupts everything after it rather than one block.
SYSTEM_KEY = 'AES/CBC/PKCS5Padding:128:ApvJEoFpQmogvam18bb54g=='


def blob_value(seed: int, size: int = VALUE_SIZE) -> bytes:
    """A deterministic value, so the expected bytes of any key are recomputable."""
    unit = struct.pack('<Q', seed & 0xFFFF_FFFF_FFFF_FFFF)
    return (unit * ((size + 7) // 8))[:size]


# Errors a node going away legitimately produces. The nemesis makes these normal;
# cassandra-stress reports and keeps going, and so must we. A content mismatch is
# never in this set, which is the whole point: availability is noise here, wrong
# bytes are the signal.
TRANSIENT = (ReadFailure, ReadTimeout, WriteTimeout, Unavailable, NoHostAvailable)


def check_value(key: int, got: bytes | None, label: str) -> None:
    """
    The cell must hold exactly the bytes it was written with.

    This is what SCYLLADB-4293 breaks, and it breaks it without changing the row
    count or the cell length, so only a content check can see it. On a mismatch
    report how far the value was correct: in the failing runs the prefix was 563,
    677, 806 and 332 bytes of 1024.
    """
    want = blob_value(key)
    if got == want:
        return
    if got is None or len(got) != len(want):
        raise AssertionError(
            f"{label}: key={key} value has length {None if got is None else len(got)}, "
            f"expected {len(want)}")
    first = next(i for i in range(len(want)) if got[i] != want[i])
    tail_ok = 0
    while tail_ok < len(want) and got[-1 - tail_ok] == want[-1 - tail_ok]:
        tail_ok += 1
    differ = sum(1 for i in range(len(want)) if got[i] != want[i])
    raise AssertionError(
        f"{label}: key={key} is corrupt - first {first} of {len(want)} bytes are "
        f"correct, last {tail_ok} are, {differ} bytes differ")


async def test_range_read_integrity(manager: ScyllaClusterManager, storage: Storage | None,
                                    tmp_path) -> None:
    """Prepare a dataset, then read it back under write, scan and topology churn."""
    mode = 'local' if storage is None else storage.type

    cfg = {'query_page_size_in_bytes': QUERY_PAGE_SIZE}
    if storage is not None:
        cfg['object_storage_endpoints'] = storage.create_endpoint_conf()
    # Encryption is always on: it is on in the failing run, and without it a read
    # at a wrong offset throws instead of returning garbage that parses.
    key_dir = tmp_path / "system_keys"
    key_dir.mkdir()
    (key_dir / "system_key").write_text(SYSTEM_KEY)
    cfg['system_key_directory'] = str(key_dir)
    cfg['user_info_encryption'] = {
        'enabled': True,
        'key_provider': 'LocalFileSystemKeyProviderFactory',
    }

    logger.info("Bootstrapping %d-node cluster [storage=%s]", NUM_NODES, mode)
    servers = await manager.servers_add(NUM_NODES, config=cfg, auto_rack_dc='dc1')
    cql = manager.get_cql()

    ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    if storage is not None:
        ks_opts += " AND STORAGE = " + format_tuples(type=storage.type,
                                                     endpoint=storage.address,
                                                     bucket=storage.bucket_name)

    async with new_test_keyspace(manager, ks_opts) as ks:
        table = f"{ks}.standard1"
        # cassandra-stress' standard1: one row per partition, one blob cell.
        await cql.run_async(
            f"CREATE TABLE {table} (key bigint PRIMARY KEY, c0 blob) "
            f"WITH compaction = {{'class': 'LeveledCompactionStrategy'}} "
            # The SCT case runs cql-stress's standard1, which is uncompressed.
            # That matters: an uncompressed sstable takes a different read path
            # and is only checksummed under integrity_check::yes.
            f"AND compression = {{}}")

        insert = cql.prepare(f"INSERT INTO {table} (key, c0) VALUES (?, ?)")
        insert.consistency_level = ConsistencyLevel.ALL
        select = cql.prepare(f"SELECT key, c0 FROM {table} WHERE key = ?")
        select.consistency_level = ConsistencyLevel.QUORUM

        # -- prepare: bulk write, CL=ALL, no other activity -------------------
        logger.info("[%s] prepare: writing %d rows", mode, PREPARE_ROWS)
        sem = asyncio.Semaphore(PREPARE_CONCURRENCY)

        async def write_key(key: int) -> None:
            async with sem:
                await cql.run_async(insert, [key, blob_value(key)])

        for start in range(0, PREPARE_ROWS, PREPARE_BATCH):
            end = min(start + PREPARE_BATCH, PREPARE_ROWS)
            await asyncio.gather(*[write_key(k) for k in range(start, end)])
            logger.info("[%s] prepare: %d/%d rows", mode, end, PREPARE_ROWS)

        # Flush only. keyspace_compaction() blocks until the major finishes and on
        # gs that outruns the REST client's timeout, failing the test for reasons
        # that have nothing to do with read integrity. The SCT case this mirrors
        # triggers no explicit majors either.
        await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))
        logger.info("[%s] prepare complete", mode)

        # -- phase 2: read the prepared range while writing a disjoint one ----
        stop = asyncio.Event()
        failures: list[BaseException] = []
        counts: list[tuple[int, int]] = []

        def record(exc: BaseException) -> None:
            logger.error("worker failed: %s", exc)
            failures.append(exc)
            stop.set()

        async def reader(worker: int) -> None:
            rng = random.Random(worker)
            reads = transient = 0
            try:
                while not stop.is_set():
                    key = rng.randrange(PREPARE_ROWS)
                    try:
                        rows = list(await cql.run_async(select, [key]))
                    except TRANSIENT:
                        transient += 1
                        continue
                    if len(rows) != 1:
                        # A missing row under churn is an availability problem, not
                        # the corruption this test is looking for.
                        transient += 1
                        continue
                    check_value(key, rows[0].c0, f"[{mode}] point read")
                    reads += 1
            except BaseException as exc:      # noqa: BLE001 - recorded, raised at the end
                record(exc)
            finally:
                counts.append((reads, transient))
                logger.info("[%s] reader %d: %d verified reads, %d transient errors",
                            mode, worker, reads, transient)

        async def writer(worker: int) -> None:
            key = WRITE_POP_OFFSET + worker
            try:
                while not stop.is_set():
                    try:
                        await cql.run_async(insert, [key, blob_value(key)])
                    except TRANSIENT:
                        pass
                    key += WRITE_CONCURRENCY
            except BaseException as exc:      # noqa: BLE001
                record(exc)

        async def full_scanner() -> None:
            scan = SimpleStatement(
                f"SELECT key, c0 FROM {table} LIMIT {FULLSCAN_LIMIT} BYPASS CACHE",
                consistency_level=ConsistencyLevel.QUORUM)
            aggregate = SimpleStatement(f"SELECT count(*) FROM {table} BYPASS CACHE",
                                        consistency_level=ConsistencyLevel.QUORUM)
            rng = random.Random(0)
            try:
                while not stop.is_set():
                    await asyncio.sleep(FULLSCAN_INTERVAL)
                    if stop.is_set():
                        break
                    # SCT's mode is table_and_aggregate: one or the other.
                    try:
                        if rng.random() < 0.5:
                            rows = list(await cql.run_async(scan))
                        else:
                            await cql.run_async(aggregate)
                            continue
                    except TRANSIENT:
                        continue
                    logger.info("[%s] full scan returned %d rows", mode, len(rows))
                    for row in rows:
                        check_value(row.key, row.c0, f"[{mode}] full scan")
            except BaseException as exc:      # noqa: BLE001
                record(exc)

        # No explicit major compaction here: SCT does not run one either. The churn
        # comes from LeveledCompactionStrategy reacting to the writes below, and a
        # blocking keyspace_compaction call under this load outruns the REST
        # client's timeout.
        async def topology_churn() -> None:
            """
            Stand-in for SCT's nemesis. Both instrumented runs put their parse
            failures and their corrupt reads inside tablet cleanup windows, so
            moving tablets while the readers verify is the ingredient the earlier
            green runs of this test were missing. Adding then decommissioning a
            node drives streaming in and cleanup out, which is what the nemesis
            does to reach the same state.
            """
            try:
                while not stop.is_set():
                    await asyncio.sleep(CHURN_INTERVAL)
                    if stop.is_set():
                        break
                    logger.info("[%s] churn: adding a node", mode)
                    extra = await manager.server_add(config=cfg, property_file={
                        "dc": "dc1", "rack": f"r{len(servers) % 3}"})
                    await wait_for_no_pending_topology_transition(manager, time.time() + 600)
                    if stop.is_set():
                        break
                    logger.info("[%s] churn: decommissioning it", mode)
                    await manager.decommission_node(extra.server_id)
                    await wait_for_no_pending_topology_transition(manager, time.time() + 600)
                    logger.info("[%s] churn: cycle complete", mode)
            except BaseException as exc:      # noqa: BLE001
                record(exc)

        logger.info("[%s] phase 2: %ds of concurrent read, write, scan and topology churn",
                    mode, PHASE2_SECONDS)
        workers = [asyncio.create_task(reader(i)) for i in range(READ_CONCURRENCY)]
        workers += [asyncio.create_task(writer(i)) for i in range(WRITE_CONCURRENCY)]
        workers += [asyncio.create_task(full_scanner()), asyncio.create_task(topology_churn())]

        deadline = time.time() + PHASE2_SECONDS
        while time.time() < deadline and not stop.is_set():
            await asyncio.sleep(1)
        stop.set()
        await asyncio.gather(*workers, return_exceptions=True)

        verified = sum(c[0] for c in counts)
        transient = sum(c[1] for c in counts)
        logger.info("[%s] phase 2 done: %d verified reads, %d transient errors",
                    mode, verified, transient)
        if failures:
            raise failures[0]
        # A run that verified nothing proves nothing.
        assert verified > 0, "no read was verified; the workload never got going"
