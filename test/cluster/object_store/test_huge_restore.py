#!/usr/bin/env python3
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from cassandra.query import SimpleStatement  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.object_store.conftest import format_tuples
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name

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


async def get_base_table(manager, table_id):
    rows = await manager.get_cql().run_async(f"SELECT base_table FROM system.tablets WHERE table_id = {table_id}")
    return rows[0].base_table if rows and rows[0].base_table else table_id


async def get_tablet_count(manager, server, keyspace, table):
    host = manager.cql.cluster.metadata.get_host(server.ip_addr)
    await read_barrier(manager.api, server.ip_addr)
    table_id = await manager.get_table_or_view_id(keyspace, table)
    table_id = await get_base_table(manager, table_id)
    rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets WHERE table_id = {table_id}",
                                       host=host)
    return rows[0].tablet_count


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


async def do_direct_restore(manager: ManagerClient, s3_storage, tmp_path, key_provider):
    objconf = s3_storage.create_endpoint_conf()
    config = {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
    }
    # d = tmp_path / "system_keys"
    # d.mkdir()
    # config = config | {
    #     'system_key_directory': str(d),
    #     'user_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}
    # }
    # ciphers={"AES/CBC/PKCS5Padding": [128]}
    # async with make_key_provider_factory(KeyProvider.kms, tmp_path) as key_provider:
    config = config | key_provider.configuration_parameters()
    cmd = ['--smp', '4', '-m', '4G', '--logger-log-level', 'sstables_loader=info:sstable=info']
    servers = await manager.servers_add(servers_num=3, config=config, cmdline=cmd, auto_rack_dc="dc1")

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    # 1) create table with N tablets.
    keyspace = 'test_ks'
    table = 'test_cf'
    await create_keyspace_and_table(cql, keyspace, table)
    insert_rows_mt(cql, keyspace, table, 100_000)

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, keyspace)
        # await manager.api.repair(server.ip_addr, keyspace, table)
        # await manager.api.major_compaction(server.ip_addr)

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
            s3_storage.address, s3_storage.bucket_name, f'{prefix}/{server.server_id}'
            # s3_storage.address, s3_storage.bucket_name, f'{prefix}/foo'
        )
    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, backup_tasks[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Backup task failed on server {server.server_id}. Status: {status}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")

    restore_task_ids = {}
    for server in servers:
        restore_task_ids[server.server_id] = await manager.api.restore(
            server.ip_addr, keyspace, table,
            s3_storage.address, s3_storage.bucket_name,
            f'{prefix}/{server.server_id}', sstables[server.server_id], "node"
            # f'{prefix}/foo', sstables[server.server_id], "node"
        )

    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Restore task failed on server {server.server_id}. Status: {status}"

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

    assert res[0].count == row_count, f"number of rows after restore is incorrect: {res[0].count}"


@pytest.mark.asyncio
async def test_large_restore(manager: ManagerClient, s3_storage, tmp_path, key_provider):
    await do_direct_restore(manager, s3_storage, tmp_path, key_provider)


async def do_real_restore(manager: ManagerClient, tmp_path):
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
                # 'master_key': '712e704a-4f63-4bc8-8663-0a9e321ee0f0'
                'master_key': 'alias/testid-498f2c8d-4dfe-46c7-851f-063173ae8004',
            }}}

    cmd = ['--smp', '1', '-m', '4G', '--logger-log-level',
           'sstables_loader=info:sstable=info:encryption=trace:kms=trace']
    servers = await manager.servers_add(servers_num=1, config=config, cmdline=cmd, auto_rack_dc="dc1")

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    # 1) create table with N tablets.
    keyspace = 'test_ks'
    table = 'test_cf'
    await create_keyspace_and_table(cql, keyspace, table, 1)
    insert_rows_mt(cql, keyspace, table, 100_000)

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, keyspace)
        await manager.api.repair(server.ip_addr, keyspace, table)
        await manager.api.major_compaction(server.ip_addr)

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
            # s3_storage.address, s3_storage.bucket_name, f'{prefix}/foo'
        )
    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, backup_tasks[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Backup task failed on server {server.server_id}. Status: {status}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")

    for server in servers:
        await manager.server_restart(server.server_id)
    restore_task_ids = {}
    for server in servers:
        restore_task_ids[server.server_id] = await manager.api.restore(
            server.ip_addr, keyspace, table,
            "s3.us-east-1.amazonaws.com", "manager-backup-tests-us-east-1",
            f'{prefix}/{server.server_id}', sstables[server.server_id], "node"
            # f'{prefix}/foo', sstables[server.server_id], "node"
        )

    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Restore task failed on server {server.server_id}. Status: {status}"

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

    assert res[0].count == row_count, f"number of rows after restore is incorrect: {res[0].count}"


@pytest.mark.asyncio
async def test_real_restore(manager: ManagerClient, tmp_path):
    await do_real_restore(manager, tmp_path)


async def do_restore_from_files(manager: ManagerClient, s3_storage, tmp_path):
    objconf = s3_storage.create_endpoint_conf()
    config = {'enable_user_defined_functions': False,
              'object_storage_endpoints': objconf,
              'experimental_features': ['keyspace-storage-options']}
    d = tmp_path / "system_keys"
    d.mkdir()
    config = config | {
        'system_key_directory': str(d),
        'user_info_encryption': {'enabled': False, 'key_provider': 'LocalFileSystemKeyProviderFactory'}
    }
    cmd = ['--smp', '4', '-m', '32G', '--logger-log-level', 'sstables_loader=debug:sstable=debug']
    server = await manager.server_add(config=config, cmdline=cmd)

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    # 1) create table with N tablets.
    keyspace = 'test_ks'
    table = 'test_cf'
    await create_keyspace_and_table(cql, keyspace, table)

    tmp_dir = "/home/ernest.zaslavsky/Downloads/s3-sstables/"
    prefix = unique_name('/test/streaming_')
    s3_resource = s3_storage.get_resource()
    bucket = s3_resource.Bucket(s3_storage.bucket_name)
    sstables = []

    print(f"Uploading files from '{tmp_dir}' to prefix '{prefix}':")

    for root, _, files in os.walk(tmp_dir):
        for file in files:
            if file.endswith("-TOC.txt"):
                sstables.append(file)
            local_path = os.path.join(root, file)
            s3_key = f"{prefix}/{file}"

            print(f" - Uploading {local_path} to {s3_key}")
            bucket.upload_file(local_path, s3_key)

    restore_task_id = await manager.api.restore(
        server.ip_addr, keyspace, table,
        s3_storage.address, s3_storage.bucket_name,
        prefix, sstables, "node"
    )

    status = await manager.api.wait_task(server.ip_addr, restore_task_id)
    assert status and status.get(
        'state') == 'done', f"Restore task failed on server {server.server_id}. Reason {status}"

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

    assert res[0].count == 10000000, f"number of rows after restore is incorrect: {res[0].count}"


@pytest.mark.asyncio
async def test_large_restore_from_directory(manager: ManagerClient, s3_storage, tmp_path):
    await do_restore_from_files(manager, s3_storage, tmp_path)


#    16751552-da32-450c-9231-21b0bae919a4


async def do_restore_from_s3(manager: ManagerClient, tmp_path):
    config = {
        'enable_user_defined_functions': False,
        'experimental_features': ['keyspace-storage-options'],
    }
    d = tmp_path / "system_keys"
    d.mkdir()
    # to get the real master key get scylla.yaml from one of the scylla nodes and get the value of "master_key"
    # it will be in form of "alias/testid-some-uuid-here"
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
                # 'master_key': '16751552-da32-450c-9231-21b0bae919a4',
                'master_key': 'alias/testid-498f2c8d-4dfe-46c7-851f-063173ae8004',
            }}}

    # cmd = ['--smp', '1', '-m', '4G', '--default-log-level', 'trace']
    cmd = ['--smp', '1', '-m', '4G', '--logger-log-level',
           'sstables_loader=trace:sstable=trace:encryption=trace:kms=trace']
    servers = await manager.servers_add(servers_num=1, config=config, cmdline=cmd, auto_rack_dc="dc1")

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")
    # CREATE TABLE keyspace1.standard1 (
    #     key blob,
    #     "C0" blob,
    #     PRIMARY KEY (key)
    # ) WITH bloom_filter_fp_chance = 0.01
    #     AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    #     AND comment = ''
    #     AND compaction = {'class': 'IncrementalCompactionStrategy'}
    #     AND compression = {}
    #     AND crc_check_chance = 1
    #     AND default_time_to_live = 0
    #     AND gc_grace_seconds = 864000
    #     AND max_index_interval = 2048
    #     AND memtable_flush_period_in_ms = 0
    #     AND min_index_interval = 128
    #     AND speculative_retry = '99.0PERCENTILE'
    #     AND tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '3600'};

    # 1) create table with N tablets.
    keyspace = 'test_ks'
    table = 'test_cf'
    await create_keyspace_and_table(cql, keyspace, table, 1)

    prefix = "standard1/keyspace1/dc2e8658-8168-44ae-85c0-ef5d57d795dd/37edb310-d59a-11f0-9d5a-0257e719cc4b"
    restore_task_ids = {}
    for server in servers:
        restore_task_ids[server.server_id] = await manager.api.restore(
            server.ip_addr, keyspace, table,
            "s3.us-east-1.amazonaws.com", "manager-backup-tests-us-east-1",
            prefix, ["ms-3gw3_0lpo_4iw742l6fvgn40qq2j-big-TOC.txt"], "node"
            # f'{prefix}/foo', sstables[server.server_id], "node"
        )

    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Restore task failed on server {server.server_id}. Status: {status}"

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

    assert res[0].count == 100000000, f"number of rows after restore is incorrect: {res[0].count}"


@pytest.mark.asyncio
async def test_restore_from_s3(manager: ManagerClient, tmp_path):
    await do_restore_from_s3(manager, tmp_path)
