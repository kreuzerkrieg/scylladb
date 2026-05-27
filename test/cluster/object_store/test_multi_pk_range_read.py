#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Functional test for S3-backed keyspace read patterns.

Mirrors all SELECT query patterns from:
    data_dir/latte/s3_range_query.rn

Schema
------
    CREATE TABLE range_test (
        pk    bigint,
        ck    bigint,
        value1 .. value10  blob,
        PRIMARY KEY (pk, ck)
    ) WITH compaction = {'class': 'IncrementalCompactionStrategy'}

The *keyspace* is stored on S3 (via STORAGE = {'type': 'S3', ...}).

Query patterns covered (from latte workload)
---------------------------------------------
    multi_pk_read       — SELECT * WHERE pk IN (?, ...) BYPASS CACHE
    range_read          — SELECT * WHERE ck >= ? AND ck < ? ALLOW FILTERING BYPASS CACHE
    full_scan           — SELECT * LIMIT <N> BYPASS CACHE
    partition_scan      — SELECT * WHERE pk = ? ORDER BY ck DESC BYPASS CACHE
    partition_scan_lt   — SELECT * WHERE pk = ? AND ck < ? ORDER BY ck DESC BYPASS CACHE
    partition_scan_gt   — SELECT * WHERE pk = ? AND ck > ? ORDER BY ck DESC BYPASS CACHE
    partition_scan_range— SELECT * WHERE pk = ? AND ck > ? AND ck < ? ORDER BY ck DESC BYPASS CACHE
    partition_group_by_ck — SELECT pk, ck, count(*) WHERE pk = ? GROUP BY pk, ck
    partition_max_ck    — SELECT count(*), max(ck) WHERE pk = ?
    count_read          — SELECT count(*) BYPASS CACHE

Latency injection
-----------------
A toxiproxy instance sits between Scylla and Minio.  During the write
phase no toxic is active (full speed); before reads, a latency toxic is
added to simulate real S3 round-trip latency (~25 ms).
"""

import asyncio
import logging
import socket
import struct
import subprocess
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse

import pytest
import requests

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient
from test.pylib.minio_server import MinioServer
from test.pylib.object_storage import format_tuples

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data-volume configuration
# ---------------------------------------------------------------------------
ROW_COUNT = 3_000_000        # Total rows to write.   Latte default: 30_000_000
ROWS_PER_PARTITION = 30_000  # Rows per partition.    Latte default:     30_000
VALUE_SIZE = 512             # Bytes per blob column. Latte default:        512
NUM_VALUE_COLS = 10          # Number of blob columns (value1 .. value10)
NUM_PKS = 10                 # Partitions per IN query. Latte default:      10
WRITE_CONCURRENCY = 100      # Max simultaneous inserts in flight
RANGE_LIMIT = 2_000_000        # Limit for full_scan / range queries. Latte default: 2_000_000
                             # Reduced from 2M: 100K rows × ~5KB = ~500MB is sufficient to
                             # exercise multi-SSTable S3 reads without running for 40+ minutes.

# Simulated S3 latency (ms) injected via toxiproxy during reads.
# Set to 0 to disable latency injection.
S3_READ_LATENCY_MS = 25


# ---------------------------------------------------------------------------
# Toxiproxy helper
# ---------------------------------------------------------------------------

class ToxiproxyS3:
    """Manages a toxiproxy instance between Scylla and Minio for latency injection."""

    PROXY_NAME = "minio_s3"

    def __init__(self, minio_host: str, minio_port: int, listen_host: str = "127.0.0.1"):
        self._minio_host = minio_host
        self._minio_port = minio_port
        self._listen_host = listen_host
        self._listen_port = self._find_free_port()
        self._api_port = self._find_free_port()
        self._process = None

    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]

    @property
    def listen_address(self) -> str:
        return f"http://{self._listen_host}:{self._listen_port}"

    @property
    def listen_port(self) -> int:
        return self._listen_port

    @property
    def api_url(self) -> str:
        return f"http://{self._listen_host}:{self._api_port}"

    def start(self) -> None:
        """Start toxiproxy-server and create the Minio proxy."""
        self._process = subprocess.Popen(
            ['toxiproxy-server', '-host', self._listen_host, '-port', str(self._api_port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for API to be ready
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                requests.get(f"{self.api_url}/version", timeout=0.5)
                break
            except (requests.ConnectionError, requests.Timeout):
                time.sleep(0.1)
        else:
            raise RuntimeError("toxiproxy-server failed to start")

        # Create proxy: listen_port → minio_port
        requests.post(f"{self.api_url}/proxies", json={
            "name": self.PROXY_NAME,
            "listen": f"{self._listen_host}:{self._listen_port}",
            "upstream": f"{self._minio_host}:{self._minio_port}",
            "enabled": True,
        }).raise_for_status()
        logger.info("Toxiproxy started: %s:%d → %s:%d (API on :%d)",
                    self._listen_host, self._listen_port,
                    self._minio_host, self._minio_port, self._api_port)

    def add_latency(self, latency_ms: int, jitter_ms: int = 0) -> None:
        """Add a latency toxic to the S3 proxy (upstream direction)."""
        requests.post(f"{self.api_url}/proxies/{self.PROXY_NAME}/toxics", json={
            "name": "s3_latency",
            "type": "latency",
            "stream": "upstream",
            "attributes": {"latency": latency_ms, "jitter": jitter_ms},
        }).raise_for_status()
        logger.info("Toxiproxy: added %d ms latency (jitter=%d ms) to S3 reads",
                    latency_ms, jitter_ms)

    def remove_latency(self) -> None:
        """Remove the latency toxic."""
        resp = requests.delete(f"{self.api_url}/proxies/{self.PROXY_NAME}/toxics/s3_latency")
        if resp.status_code == 204 or resp.status_code == 200:
            logger.info("Toxiproxy: removed latency toxic")

    def stop(self) -> None:
        """Terminate toxiproxy-server."""
        if self._process:
            self._process.terminate()
            self._process.wait(timeout=5)
            self._process = None

    def create_endpoint_conf(self, region: str):
        """Create a Scylla object_storage_endpoints config pointing at the proxy."""
        return MinioServer.create_conf(self.listen_address, region)


# ---------------------------------------------------------------------------
# Latte-compatible data helpers
# ---------------------------------------------------------------------------

def _partition_pk(partition_idx: int) -> int:
    """
    Map a partition index to a signed bigint pk, mirroring latte's hash().

    Uses a Knuth multiplicative hash for determinism across Python runs.
    The result fits in a CQL ``bigint`` (signed int64).
    """
    h = (partition_idx * 2654435761) & 0xFFFF_FFFF_FFFF_FFFF
    return h - (1 << 64) if h >= (1 << 63) else h


def _blob_value(seed: int, size: int) -> bytes:
    """
    Generate a deterministic blob of ``size`` bytes, mirroring latte's
    blob(seed, size): repeats the 8-byte little-endian encoding of ``seed``
    until the buffer is filled.
    """
    unit = struct.pack('<Q', seed & 0xFFFF_FFFF_FFFF_FFFF)
    repetitions = (size + 7) // 8
    return (unit * repetitions)[:size]


def _row_params(cycle_i: int, row_count: int, num_partitions: int, value_size: int) -> list:
    """
    Build the INSERT parameter list for a single row, following the latte
    insert() pattern.
    """
    idx = cycle_i % row_count
    partition_idx = idx % num_partitions
    pk = _partition_pk(partition_idx)
    ck = cycle_i
    values = [_blob_value(idx + n, value_size) for n in range(NUM_VALUE_COLS)]
    return [pk, ck, *values]


def _multi_pk_keys(cycle_i: int, num_pks: int, row_count: int, num_partitions: int) -> list[int]:
    """
    Return ``num_pks`` pk values for cycle ``cycle_i``, matching latte's
    multi_pk_read() logic.
    """
    pks = []
    for j in range(num_pks):
        idx = (cycle_i + j) % row_count
        partition_idx = idx % num_partitions
        pks.append(_partition_pk(partition_idx))
    return pks


# ---------------------------------------------------------------------------
# Query pattern definitions
# ---------------------------------------------------------------------------

@dataclass
class QueryPattern:
    """Defines a single read query pattern from the latte workload."""
    name: str
    description: str
    build_query: Callable  # (table: str) -> str
    build_params: Callable  # (cycle_i: int, num_partitions: int) -> list
    validate: Callable      # (rows: list, cycle_i: int, num_partitions: int) -> None


def _build_multi_pk_read_query(table: str) -> str:
    pk_placeholders = ', '.join(['?'] * NUM_PKS)
    return f"SELECT * FROM {table} WHERE pk IN ({pk_placeholders}) BYPASS CACHE"


def _build_multi_pk_read_params(cycle_i: int, num_partitions: int) -> list:
    return _multi_pk_keys(cycle_i, NUM_PKS, ROW_COUNT, num_partitions)


def _validate_multi_pk_read(rows: list, cycle_i: int, num_partitions: int) -> None:
    pks = _multi_pk_keys(cycle_i, NUM_PKS, ROW_COUNT, num_partitions)
    num_distinct = len(set(pks))
    expected_rows = num_distinct * ROWS_PER_PARTITION
    assert len(rows) == expected_rows, (
        f"multi_pk_read cycle {cycle_i}: expected {expected_rows} rows "
        f"({num_distinct} distinct pks × {ROWS_PER_PARTITION} rows/partition), "
        f"got {len(rows)}"
    )


def _build_range_read_query(table: str) -> str:
    return f"SELECT * FROM {table} WHERE ck >= ? AND ck < ? ALLOW FILTERING BYPASS CACHE"


def _build_range_read_params(cycle_i: int, num_partitions: int) -> list:
    ck_start = cycle_i
    ck_end = ck_start + RANGE_LIMIT
    return [ck_start, ck_end]


def _validate_range_read(rows: list, cycle_i: int, num_partitions: int) -> None:
    # Range read returns all rows with ck in [ck_start, ck_end).
    # With ck = cycle counter (1..ROW_COUNT), we expect rows in that range.
    assert len(rows) > 0, f"range_read cycle {cycle_i}: expected rows, got 0"
    assert len(rows) <= RANGE_LIMIT, (
        f"range_read cycle {cycle_i}: got {len(rows)} rows, exceeds RANGE_LIMIT {RANGE_LIMIT}"
    )


def _build_full_scan_query(table: str) -> str:
    return f"SELECT * FROM {table} LIMIT {RANGE_LIMIT} BYPASS CACHE"


def _build_full_scan_params(cycle_i: int, num_partitions: int) -> list:
    return []


def _validate_full_scan(rows: list, cycle_i: int, num_partitions: int) -> None:
    expected = min(RANGE_LIMIT, ROW_COUNT)
    assert len(rows) == expected, (
        f"full_scan: expected {expected} rows (LIMIT {RANGE_LIMIT}, total {ROW_COUNT}), got {len(rows)}"
    )


def _build_partition_scan_query(table: str) -> str:
    return f"SELECT * FROM {table} WHERE pk = ? ORDER BY ck DESC BYPASS CACHE"


def _build_partition_scan_params(cycle_i: int, num_partitions: int) -> list:
    idx = cycle_i % ROW_COUNT
    partition_idx = idx % num_partitions
    return [_partition_pk(partition_idx)]


def _validate_partition_scan(rows: list, cycle_i: int, num_partitions: int) -> None:
    assert len(rows) == ROWS_PER_PARTITION, (
        f"partition_scan cycle {cycle_i}: expected {ROWS_PER_PARTITION} rows, got {len(rows)}"
    )


def _build_partition_scan_lt_query(table: str) -> str:
    return f"SELECT * FROM {table} WHERE pk = ? AND ck < ? ORDER BY ck DESC BYPASS CACHE"


def _build_partition_scan_lt_params(cycle_i: int, num_partitions: int) -> list:
    idx = cycle_i % ROW_COUNT
    partition_idx = idx % num_partitions
    pk = _partition_pk(partition_idx)
    # Use a ck_upper that includes roughly half the partition's rows.
    ck_upper = ROW_COUNT // 2
    return [pk, ck_upper]


def _validate_partition_scan_lt(rows: list, cycle_i: int, num_partitions: int) -> None:
    assert len(rows) > 0, f"partition_scan_lt cycle {cycle_i}: got 0 rows"
    assert len(rows) <= ROWS_PER_PARTITION, (
        f"partition_scan_lt cycle {cycle_i}: got {len(rows)} rows, "
        f"exceeds partition size {ROWS_PER_PARTITION}"
    )


def _build_partition_scan_gt_query(table: str) -> str:
    return f"SELECT * FROM {table} WHERE pk = ? AND ck > ? ORDER BY ck DESC BYPASS CACHE"


def _build_partition_scan_gt_params(cycle_i: int, num_partitions: int) -> list:
    idx = cycle_i % ROW_COUNT
    partition_idx = idx % num_partitions
    pk = _partition_pk(partition_idx)
    ck_lower = ROW_COUNT // 2
    return [pk, ck_lower]


def _validate_partition_scan_gt(rows: list, cycle_i: int, num_partitions: int) -> None:
    assert len(rows) > 0, f"partition_scan_gt cycle {cycle_i}: got 0 rows"
    assert len(rows) <= ROWS_PER_PARTITION, (
        f"partition_scan_gt cycle {cycle_i}: got {len(rows)} rows, "
        f"exceeds partition size {ROWS_PER_PARTITION}"
    )


def _build_partition_scan_range_query(table: str) -> str:
    return f"SELECT * FROM {table} WHERE pk = ? AND ck > ? AND ck < ? ORDER BY ck DESC BYPASS CACHE"


def _build_partition_scan_range_params(cycle_i: int, num_partitions: int) -> list:
    idx = cycle_i % ROW_COUNT
    partition_idx = idx % num_partitions
    pk = _partition_pk(partition_idx)
    ck_lower = ROW_COUNT // 3
    ck_upper = 2 * ROW_COUNT // 3
    return [pk, ck_lower, ck_upper]


def _validate_partition_scan_range(rows: list, cycle_i: int, num_partitions: int) -> None:
    assert len(rows) > 0, f"partition_scan_range cycle {cycle_i}: got 0 rows"
    assert len(rows) <= ROWS_PER_PARTITION, (
        f"partition_scan_range cycle {cycle_i}: got {len(rows)} rows, "
        f"exceeds partition size {ROWS_PER_PARTITION}"
    )


def _build_partition_group_by_ck_query(table: str) -> str:
    return f"SELECT pk, ck, count(*) FROM {table} WHERE pk = ? GROUP BY pk, ck"


def _build_partition_group_by_ck_params(cycle_i: int, num_partitions: int) -> list:
    idx = cycle_i % ROW_COUNT
    partition_idx = idx % num_partitions
    return [_partition_pk(partition_idx)]


def _validate_partition_group_by_ck(rows: list, cycle_i: int, num_partitions: int) -> None:
    # GROUP BY pk, ck returns one row per unique (pk, ck) — same as ROWS_PER_PARTITION.
    assert len(rows) == ROWS_PER_PARTITION, (
        f"partition_group_by_ck cycle {cycle_i}: expected {ROWS_PER_PARTITION} rows, got {len(rows)}"
    )


def _build_partition_max_ck_query(table: str) -> str:
    return f"SELECT count(*), max(ck) FROM {table} WHERE pk = ?"


def _build_partition_max_ck_params(cycle_i: int, num_partitions: int) -> list:
    idx = cycle_i % ROW_COUNT
    partition_idx = idx % num_partitions
    return [_partition_pk(partition_idx)]


def _validate_partition_max_ck(rows: list, cycle_i: int, num_partitions: int) -> None:
    assert len(rows) == 1, f"partition_max_ck cycle {cycle_i}: expected 1 row, got {len(rows)}"
    count_val = rows[0][0]
    assert count_val == ROWS_PER_PARTITION, (
        f"partition_max_ck cycle {cycle_i}: expected count={ROWS_PER_PARTITION}, got {count_val}"
    )


def _build_count_read_query(table: str) -> str:
    return f"SELECT count(*) FROM {table} BYPASS CACHE"


def _build_count_read_params(cycle_i: int, num_partitions: int) -> list:
    return []


def _validate_count_read(rows: list, cycle_i: int, num_partitions: int) -> None:
    assert len(rows) == 1, f"count_read: expected 1 row, got {len(rows)}"
    count_val = rows[0][0]
    assert count_val == ROW_COUNT, (
        f"count_read: expected count={ROW_COUNT}, got {count_val}"
    )


# All query patterns from the latte s3_range_query.rn workload.
QUERY_PATTERNS = [
    QueryPattern(
        name="multi_pk_read",
        description="SELECT * WHERE pk IN (?, ...) BYPASS CACHE",
        build_query=_build_multi_pk_read_query,
        build_params=_build_multi_pk_read_params,
        validate=_validate_multi_pk_read,
    ),
    QueryPattern(
        name="range_read",
        description="SELECT * WHERE ck >= ? AND ck < ? ALLOW FILTERING BYPASS CACHE",
        build_query=_build_range_read_query,
        build_params=_build_range_read_params,
        validate=_validate_range_read,
    ),
    QueryPattern(
        name="full_scan",
        description="SELECT * LIMIT <N> BYPASS CACHE",
        build_query=_build_full_scan_query,
        build_params=_build_full_scan_params,
        validate=_validate_full_scan,
    ),
    QueryPattern(
        name="partition_scan",
        description="SELECT * WHERE pk = ? ORDER BY ck DESC BYPASS CACHE",
        build_query=_build_partition_scan_query,
        build_params=_build_partition_scan_params,
        validate=_validate_partition_scan,
    ),
    QueryPattern(
        name="partition_scan_lt",
        description="SELECT * WHERE pk = ? AND ck < ? ORDER BY ck DESC BYPASS CACHE",
        build_query=_build_partition_scan_lt_query,
        build_params=_build_partition_scan_lt_params,
        validate=_validate_partition_scan_lt,
    ),
    QueryPattern(
        name="partition_scan_gt",
        description="SELECT * WHERE pk = ? AND ck > ? ORDER BY ck DESC BYPASS CACHE",
        build_query=_build_partition_scan_gt_query,
        build_params=_build_partition_scan_gt_params,
        validate=_validate_partition_scan_gt,
    ),
    QueryPattern(
        name="partition_scan_range",
        description="SELECT * WHERE pk = ? AND ck > ? AND ck < ? ORDER BY ck DESC BYPASS CACHE",
        build_query=_build_partition_scan_range_query,
        build_params=_build_partition_scan_range_params,
        validate=_validate_partition_scan_range,
    ),
    QueryPattern(
        name="partition_group_by_ck",
        description="SELECT pk, ck, count(*) WHERE pk = ? GROUP BY pk, ck",
        build_query=_build_partition_group_by_ck_query,
        build_params=_build_partition_group_by_ck_params,
        validate=_validate_partition_group_by_ck,
    ),
    QueryPattern(
        name="partition_max_ck",
        description="SELECT count(*), max(ck) WHERE pk = ?",
        build_query=_build_partition_max_ck_query,
        build_params=_build_partition_max_ck_params,
        validate=_validate_partition_max_ck,
    ),
    QueryPattern(
        name="count_read",
        description="SELECT count(*) BYPASS CACHE",
        build_query=_build_count_read_query,
        build_params=_build_count_read_params,
        validate=_validate_count_read,
    ),
]


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("query_pattern", QUERY_PATTERNS, ids=[qp.name for qp in QUERY_PATTERNS])
@pytest.mark.parametrize("storage_mode", ["local", "s3"], ids=["local", "s3"])
async def test_read_pattern(manager: ManagerClient, s3_storage, tmp_path, query_pattern: QueryPattern, storage_mode: str) -> None:
    """
    Verify that each latte s3_range_query.rn SELECT pattern works correctly
    against both local and S3-backed keyspaces.

    Parametrized over:
      - storage_mode: "local" (filesystem) or "s3" (object storage via Minio)
      - query_pattern: each SELECT from data_dir/latte/s3_range_query.rn

    The local variant serves as a baseline for comparison: same schema, data,
    and queries but SSTables are on the local filesystem. Comparing make_source
    logs between the two reveals differences in read patterns (offset, len,
    frequency of re-reads).
    """
    toxiproxy = None
    try:
        # -----------------------------------------------------------------
        # 1. Storage-specific setup
        # -----------------------------------------------------------------
        system_keys_dir = tmp_path / "system_keys"
        system_keys_dir.mkdir()

        cfg = {
            'system_key_directory': str(system_keys_dir),
            # QPROBE EXPERIMENT: default query_page_size_in_bytes is 1 MiB, which caps each
            # per-tablet read at ~199 wide (5 KiB) rows and forces the range-scan coordinator
            # to read ~25 tablets ahead per page (discarded + re-read every page). Raise to
            # 64 MiB so the 5000-row page limit binds first and each read fills a full page.
            'query_page_size_in_bytes': 64 * 1024 * 1024,
        }
        cmdline = ['--smp', '8', '-m', '16G']

        if storage_mode == "s3":
            parsed = urlparse(s3_storage.address)
            minio_host = parsed.hostname
            minio_port = parsed.port

            toxiproxy = ToxiproxyS3(minio_host, minio_port)
            toxiproxy.start()

            cfg['object_storage_endpoints'] = toxiproxy.create_endpoint_conf(s3_storage.region)
            cfg['experimental_features'] = ['keyspace-storage-options']

        logger.info("Bootstrapping 3-node cluster [%s] for query pattern: %s (%s)",
                    storage_mode, query_pattern.name, query_pattern.description)
        servers = await manager.servers_add(3, config=cfg, cmdline=cmdline, auto_rack_dc='dc1')
        cql = manager.get_cql()

        # -----------------------------------------------------------------
        # 2. Schema
        # -----------------------------------------------------------------
        num_partitions = ROW_COUNT // ROWS_PER_PARTITION

        value_col_defs = ', '.join(f'value{n + 1} blob' for n in range(NUM_VALUE_COLS))
        value_col_names = ', '.join(f'value{n + 1}' for n in range(NUM_VALUE_COLS))
        value_placeholders = ', '.join(['?'] * NUM_VALUE_COLS)

        table_options = "compaction = {'class': 'IncrementalCompactionStrategy'}"

        if storage_mode == "s3":
            storage_opts = format_tuples(type='S3', endpoint=toxiproxy.listen_address, bucket=s3_storage.bucket_name)
            ks_opts = (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} "
                       f"AND STORAGE = {storage_opts}")
        else:
            ks_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"

        async with new_test_keyspace(manager, ks_opts) as ks:
            table = f"{ks}.range_test"
            await cql.run_async(
                f"CREATE TABLE {table} ("
                f"  pk bigint,"
                f"  ck bigint,"
                f"  {value_col_defs},"
                f"  PRIMARY KEY (pk, ck)"
                f") WITH {table_options}"
            )

            # ---------------------------------------------------------------
            # 3. Write data
            # ---------------------------------------------------------------
            logger.info("[%s] Writing %d rows across %d partitions",
                        storage_mode, ROW_COUNT, num_partitions)

            insert_stmt = cql.prepare(
                f"INSERT INTO {table} (pk, ck, {value_col_names}) "
                f"VALUES (?, ?, {value_placeholders})"
            )

            sem = asyncio.Semaphore(WRITE_CONCURRENCY)

            async def _insert_row(cycle_i: int) -> None:
                params = _row_params(cycle_i, ROW_COUNT, num_partitions, VALUE_SIZE)
                async with sem:
                    await cql.run_async(insert_stmt, params)

            await asyncio.gather(*[_insert_row(i) for i in range(1, ROW_COUNT + 1)])
            logger.info("[%s] Wrote %d rows", storage_mode, ROW_COUNT)

            # ---------------------------------------------------------------
            # 4. Flush
            # ---------------------------------------------------------------
            logger.info("[%s] Flushing all nodes", storage_mode)
            await asyncio.gather(*(manager.api.flush_keyspace(s.ip_addr, ks) for s in servers))
            logger.info("[%s] Flush complete", storage_mode)

            # ---------------------------------------------------------------
            # 4b. Major compaction
            # ---------------------------------------------------------------
            logger.info("[%s] Running major compaction on all nodes", storage_mode)
            await asyncio.gather(*(
                manager.api.keyspace_compaction(s.ip_addr, ks, "range_test")
                for s in servers
            ))
            logger.info("[%s] Major compaction complete", storage_mode)

            # ---------------------------------------------------------------
            # 5. Inject S3 latency (S3 mode only)
            # ---------------------------------------------------------------
            # if storage_mode == "s3" and S3_READ_LATENCY_MS > 0:
            #     toxiproxy.add_latency(S3_READ_LATENCY_MS)

            # ---------------------------------------------------------------
            # 6. Execute the query pattern
            # ---------------------------------------------------------------
            query_cql = query_pattern.build_query(table)
            logger.info("[%s] Preparing query [%s]: %s", storage_mode, query_pattern.name, query_cql)
            stmt = cql.prepare(query_cql)

            cycle_i = 1
            params = query_pattern.build_params(cycle_i, num_partitions)
            logger.info("[%s] Executing query [%s] with %d params",
                        storage_mode, query_pattern.name, len(params))
            rows = await cql.run_async(stmt, params)
            logger.info("[%s] Query [%s] returned %d rows",
                        storage_mode, query_pattern.name, len(rows))

            query_pattern.validate(rows, cycle_i, num_partitions)
            logger.info("[%s] Query [%s] validation passed", storage_mode, query_pattern.name)
    finally:
        if toxiproxy:
            toxiproxy.stop()
