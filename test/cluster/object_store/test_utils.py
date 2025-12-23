#!/usr/bin/env python3
import asyncio
import os

from cassandra.query import SimpleStatement  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.object_store.conftest import format_tuples
from test.pylib.util import unique_name

BASE_CONFIG = {
    "enable_user_defined_functions": False,
    "experimental_features": ["keyspace-storage-options"],
    "task_ttl_in_seconds": 300,
}

# Base logger settings; tests can extend/override via cmd_overrides
BASE_LOG_LEVELS = (
    "snapshots=trace:"
    "task_manager=trace:"
    "api=info:"
    "sstables_loader=debug:"
    "sstable_directory=trace:"
    "s3=trace:"
    "sstable=debug:"
    "http=debug"
)

BASE_CMDLINE = [
    "--logger-log-level",
    BASE_LOG_LEVELS,
]


async def create_cluster(manager, object_storage=None, nodes: int = 1, config_overrides: dict | None = None,
                         cmd_overrides: list[str] | None = None, auto_rack_dc: str | None = "dc1", ):
    """
    Create a cluster with given number of nodes.
    - object_storage may be None for tests that don't need it.
    - config_overrides and cmd_overrides are merged with defaults.
    """

    cfg = dict(BASE_CONFIG)
    if object_storage is not None:
        cfg["object_storage_endpoints"] = object_storage.create_endpoint_conf()

    if config_overrides:
        cfg.update(config_overrides)

    cmd = list(BASE_CMDLINE)
    if cmd_overrides:
        cmd += cmd_overrides

    return await manager.servers_add(servers_num=nodes, config=cfg, cmdline=cmd, auto_rack_dc=auto_rack_dc, )


def create_keyspace_and_table(cql, replication: dict | None = None, tablets: int | None = None,
                              extra_properties: dict | None = None, ):
    """
    Create a keyspace and table in one shot.

    Parameters:
        cql: CQL session
        ks: keyspace name
        cf: table name
        replication: dict for replication options, e.g.
            {"class": "NetworkTopologyStrategy", "replication_factor": "3"}
        tablets: optional min_tablet_count (int)
        extra_properties: dict of additional table properties, e.g.
            {"compaction": "{'class': 'SizeTieredCompactionStrategy'}"}

    Behavior:
        - If replication is None → RF=1 single‑DC
        - If tablets is provided → adds tablets = {'min_tablet_count': '<value>'}
        - If extra_properties is provided → merges into WITH clause
    """

    # Default replication
    if replication is None:
        replication = {
            "class": "NetworkTopologyStrategy",
            "replication_factor": "1",
        }

    rep_str = format_tuples(replication)
    ks = unique_name("test_ks_")
    cf = unique_name("test_cf_")
    cql.execute(f"CREATE KEYSPACE {ks} WITH REPLICATION = {rep_str};")

    # Build table properties
    props = {}

    if tablets is not None:
        props["tablets"] = f"{{'min_tablet_count': '{tablets}'}}"

    if extra_properties:
        props.update(extra_properties)

    if props:
        props_str = " WITH " + " AND ".join(f"{k} = {v}" for k, v in props.items())
    else:
        props_str = ""

    cql.execute(
        f"CREATE TABLE {ks}.{cf} (name text PRIMARY KEY, value text) {props_str};"
    )
    return ks, cf


async def backup_cluster(manager, servers, ks: str, cf: str, snapshot_name: str, storage, prefix: str, **kwargs, ):
    """
    Run backup on all servers, wait for completion, and return a dict
    server_id -> status.
    """
    tids = {}
    for s in servers:
        tid = await manager.api.backup(s.ip_addr, ks, cf, snapshot_name, storage.address, storage.bucket_name,
                                       f"{prefix}/{s.server_id}", **kwargs, )
        tids[s.server_id] = tid

    async def wait_one(server, tid):
        return server.server_id, await manager.api.wait_task(server.ip_addr, tid)

    results = await asyncio.gather(*[
        wait_one(s, tids[s.server_id]) for s in servers
    ])

    return {sid: status for sid, status in results}


async def restore_cluster(manager, servers, ks: str, cf: str, storage, prefix: str, toc_files_by_server: dict,
                          scope: str | None = None, primary_replica_only: bool | None = None,
                          return_tids: bool = False, ):
    """
    Run restore on all servers, optionally with scope/primary_replica_only.
    toc_files_by_server: {server_id: [toc1, toc2, ...]}
    If return_tids=True, return dict server_id -> tid.
    Otherwise return dict server_id -> status.
    """
    tids = {}
    for s in servers:
        kwargs = {}
        if scope is not None:
            kwargs["scope"] = scope
        if primary_replica_only is not None:
            kwargs["primary_replica_only"] = primary_replica_only

        tid = await manager.api.restore(
            s.ip_addr,
            ks,
            cf,
            storage.address,
            storage.bucket_name,
            f"{prefix}/{s.server_id}",
            toc_files_by_server[s.server_id],
            **kwargs,
        )
        tids[s.server_id] = tid

    if return_tids:
        return tids

    async def wait_one(server, tid):
        return server.server_id, await manager.api.wait_task(server.ip_addr, tid)

    results = await asyncio.gather(*[
        wait_one(s, tids[s.server_id]) for s in servers
    ])

    return {sid: status for sid, status in results}


async def abort_tasks(manager, servers, tids: dict):
    """
    Abort a set of tasks on each server and wait for final status.
    tids: {server_id: tid}
    """
    for s in servers:
        tid = tids[s.server_id]
        await manager.api.abort_task(s.ip_addr, tid)

    async def wait_one(server, tid):
        return server.server_id, await manager.api.wait_task(server.ip_addr, tid)

    results = await asyncio.gather(*[
        wait_one(s, tids[s.server_id]) for s in servers
    ])

    return {sid: status for sid, status in results}


async def prepare_snapshot_for_backup(manager, servers, ks: str, cf: str, snap_name: str):
    """
    Prepare a snapshot for backup:
        - Flush keyspace on all nodes
        - Take snapshot on all nodes
    """

    # Flush on all nodes concurrently
    await asyncio.gather(*[
        manager.api.flush_keyspace(s.ip_addr, ks)
        for s in servers
    ])

    # Take snapshot on all nodes concurrently
    await asyncio.gather(*[
        manager.api.take_snapshot(s.ip_addr, ks, snap_name)
        for s in servers
    ])


async def get_cf_dir(manager, server, keyspace: str) -> str:
    """
    Return the first column-family directory name under workdir/data/<ks>.
    """
    workdir = await manager.server_get_workdir(server.server_id)
    cf_dirs = os.listdir(f"{workdir}/data/{keyspace}")
    if not cf_dirs:
        raise RuntimeError(f"No CF dirs under {workdir}/data/{keyspace}")
    return cf_dirs[0]


async def insert_row(cql, ks: str, cf: str):
    loop = asyncio.get_running_loop()
    key = os.urandom(64).hex()
    value = os.urandom(1024).hex()
    query = f"INSERT INTO {ks}.{cf} (name, value) VALUES ('{key}', '{value}')"
    await loop.run_in_executor(None, cql.execute, query)


async def insert_rows_parallel(cql, ks: str, cf: str, count: int):
    tasks = [insert_row(cql, ks, cf) for _ in range(count)]
    await asyncio.gather(*tasks)
