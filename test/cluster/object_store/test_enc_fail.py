#!/usr/bin/env python3
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra.query import SimpleStatement  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.object_store.conftest import format_tuples
from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name, wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def create_keyspace_and_table(cql, keyspace, table, rf: int = 3):
    replication_opts = format_tuples({
        'class': 'NetworkTopologyStrategy',
        'replication_factor': rf
    })
    create_ks_query = f"CREATE KEYSPACE {keyspace} WITH REPLICATION = {replication_opts};"
    create_table_query = (
        f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text)"
    )
    cql.execute(create_ks_query)
    cql.execute(create_table_query)


def insert_rows(cql, keyspace, table, count):
    query = f"INSERT INTO {keyspace}.{table} (name, value) VALUES (?, ?)"
    prepared = cql.prepare(query)
    futures = []
    for _ in range(count):
        key = os.urandom(64).hex()
        value = os.urandom(1024).hex()
        future = cql.execute_async(prepared, (key, value))
        futures.append(future)
    for f in futures:
        f.result()


def insert_rows_mt(cql, keyspace, table, total_rows, thread_count=256):
    rows_per_thread = total_rows // thread_count
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = [
            executor.submit(insert_rows, cql, keyspace, table, rows_per_thread)
            for _ in range(thread_count)
        ]
        for future in futures:
            future.result()


async def get_snapshot_files(manager, server, keyspace, snapshot_name):
    workdir = await manager.server_get_workdir(server.server_id)
    data_path = os.path.join(workdir, 'data', keyspace)
    cf_dirs = os.listdir(data_path)
    if not cf_dirs:
        raise RuntimeError(f"No column family directories found in {data_path}")
    cf_dir = cf_dirs[0]
    snapshot_path = os.path.join(data_path, cf_dir, 'snapshots', snapshot_name)
    return [
        f.name for f in os.scandir(snapshot_path)
        if f.is_file() and f.name.endswith('TOC.txt')
    ]


async def do_real_restore(manager: ManagerClient, tmp_path, restart):
    config = {
        'enable_user_defined_functions': False,
        'experimental_features': ['keyspace-storage-options'],
    }
    d = tmp_path / "system_keys"
    d.mkdir()
    # to get the real master key, run:
    # aws kms create-key --description "qa-kms-key-for-rotation" --tags TagKey=Environment,TagValue=QA TagKey=Owner,TagValue=SecurityTeam
    # of course, you need to have awscli configured with proper credentials
    config = config | {
        'system_key_directory': str(d),
        'user_info_encryption':
            {'enabled': True,
             'key_provider': 'KmsKeyProviderFactory', 'kms_host': 'auto'},
        'object_storage_endpoints': [{'name': 's3.us-east-1.amazonaws.com', 'port': 443, 'https': True,
                                      'aws_region': 'us-east-1'}],
        'kms_hosts': {
            'auto': {
                'aws_region': 'us-east-1',
                'aws_use_ec2_credentials': 'false',
                'region': 'us-east-1',
                'master_key': '712e704a-4f63-4bc8-8663-0a9e321ee0f0',
            }}}

    cmd = ['--smp', '8', '-m', '32G', '--logger-log-level',
           'sstables_loader=debug:sstable=info:encryption=info']
    servers = await manager.servers_add(servers_num=3, config=config, cmdline=cmd, auto_rack_dc="dc1")

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    # 1) create table with N tablets.
    keyspace = 'test_ks'
    table = 'test_cf'
    await create_keyspace_and_table(cql, keyspace, table, 3)
    insert_rows_mt(cql, keyspace, table, 100_000)

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, keyspace)

    row_count = 0
    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")
    row_count += res[0].count
    print(f"Initial row count: {row_count}")

    # Take snapshot for keyspace
    snapshot_name = unique_name('backup_')
    print(f"Taking snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
    for server in servers:
        await manager.api.take_snapshot(server.ip_addr, keyspace, snapshot_name)

    # Collect snapshot files from each server
    sstables = {
        server.server_id: await get_snapshot_files(manager, server, keyspace, snapshot_name)
        for server in servers
    }
    for server_id, toc_files in sstables.items():
        print(f"Server ID: {server_id}, TOC files: {len(toc_files)}")

    # Backup the keyspace on each server to S3
    prefix = f"{table}/{snapshot_name}"
    print(f"Backing up keyspace using prefix '{prefix}' on all servers...")
    backup_tasks = {}
    for server in servers:
        backup_tasks[server.server_id] = await manager.api.backup(
            server.ip_addr, keyspace, table, snapshot_name,
            "s3.us-east-1.amazonaws.com", "manager-backup-tests-us-east-1", f'{prefix}/{server.server_id}'
        )
    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, backup_tasks[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Backup task failed on server {server.server_id}. Status: {status}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")

    if restart:
        print("Restarting cluster...")
        for server in servers:
            await manager.server_restart(server.server_id)
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    print("Restoring table data...")
    restore_task_ids = {}
    for server in servers:
        restore_task_ids[server.server_id] = await manager.api.restore(
            server.ip_addr, keyspace, table,
            "s3.us-east-1.amazonaws.com", "manager-backup-tests-us-east-1",
            f'{prefix}/{server.server_id}', sstables[server.server_id], "node"
        )

    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Restore task failed on server {server.server_id}. Status: {status}"

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

    assert res[0].count == row_count, f"number of rows after restore is incorrect: {res[0].count}"


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_real_restore(manager: ManagerClient, restart, tmp_path):
    await do_real_restore(manager, tmp_path, restart)
