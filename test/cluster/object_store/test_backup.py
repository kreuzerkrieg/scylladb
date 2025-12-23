#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import subprocess
import tempfile
from collections import defaultdict

import pytest
from cassandra.query import SimpleStatement  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.conftest import skip_mode
from test.cluster.object_store.test_helpers import do_test_backup_abort, do_test_simple_backup_and_restore, \
    do_direct_restore, do_abort_restore, topo, check_mutation_replicas
from test.cluster.object_store.test_utils import create_cluster, insert_rows_parallel, \
    prepare_snapshot_for_backup, get_cf_dir, backup_cluster, create_keyspace_and_table
from test.cluster.util import get_replication, new_test_keyspace
from test.cqlpy.util import local_process_id
from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_simple_backup(manager: ManagerClient, object_storage):
    '''check that backing up a snapshot for a keyspace works'''

    servers = await create_cluster(manager, object_storage, nodes=1)
    cql = manager.get_cql()
    ks, cf = create_keyspace_and_table(cql)
    prefix = f'{cf}/backup'

    await insert_rows_parallel(cql, ks, cf, 10)
    await prepare_snapshot_for_backup(manager, servers, ks, cf, 'backup')

    files = {}
    for server in servers:
        workdir = await manager.server_get_workdir(server.server_id)
        cf_dir = await get_cf_dir(manager, server, ks)
        files[server.server_id] = set(os.listdir(f"{workdir}/data/{ks}/{cf_dir}/snapshots/backup"))
        assert files[server.server_id]

    statuses = await backup_cluster(manager, servers, ks, cf, snapshot_name="backup", storage=object_storage,
                                    prefix=prefix)

    for status in statuses.values():
        assert status is not None
        assert status["state"] == "done"
        assert status["progress_total"] > 0
        assert status["progress_completed"] == status["progress_total"]

    objects = set(
        o.key
        for o in object_storage
        .get_resource()
        .Bucket(object_storage.bucket_name)
        .objects.all()
    )

    for server in servers:
        for f in files[server.server_id]:
            assert f"{prefix}/{server.server_id}/{f}" in objects
        # Check that task runs in the streaming sched group
        log = await manager.server_open_log(server.server_id)
        res = await log.grep(r'INFO.*\[shard [0-9]:([a-z]+)\] .* Backup sstables from .* to')
        assert len(res) == 1 and res[0][1].group(1) == 'strm'


@pytest.mark.asyncio
@pytest.mark.parametrize("move_files", [False, True])
async def test_backup_move(manager: ManagerClient, object_storage, move_files):
    '''check that backing up a snapshot by _moving_ sstable to object storage'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    print('Backup snapshot')
    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, object_storage.bucket_name,
                                   prefix,
                                   move_files=move_files)
    print(f'Started task {tid}')
    status = await manager.api.get_task_status(server.ip_addr, tid)
    print(f'Status: {status}, waiting to finish')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')
    assert (status['progress_total'] > 0) and (status['progress_completed'] == status['progress_total'])

    # all components in the "backup" snapshot should have been moved into bucket if move_files
    assert len(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup')) == 0 if move_files else len(files)


@pytest.mark.asyncio
async def test_backup_to_non_existent_bucket(manager: ManagerClient, object_storage):
    '''backup should fail if the destination bucket does not exist'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', object_storage.address, "non-existant-bucket",
                                   prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'failed'
    # assert 'S3 request failed. Code: 15. Reason: Access Denied.' in status['error']


@pytest.mark.asyncio
async def test_backup_to_non_existent_endpoint(manager: ManagerClient, object_storage):
    '''backup should fail if the endpoint is invalid/inaccessible'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 0

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'backup', "does_not_exist", object_storage.bucket_name,
                                   prefix)
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert status is not None
    assert status['state'] == 'failed'
    assert status['error'] == 'std::invalid_argument (endpoint does_not_exist not found)'


@pytest.mark.asyncio
async def test_backup_to_non_existent_snapshot(manager: ManagerClient, object_storage):
    '''backup should fail if the snapshot does not exist'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    ks, cf = await prepare_snapshot_for_backup(manager, server)

    prefix = f'{cf}/backup'
    tid = await manager.api.backup(server.ip_addr, ks, cf, 'nonexistent-snapshot',
                                   object_storage.address, object_storage.bucket_name, prefix)
    # The task is expected to fail immediately due to invalid snapshot name.
    # However, since internal implementation details may change, we'll wait for
    # task completion if immediate failure doesn't occur.
    actual_state = None
    for status_api in [manager.api.get_task_status,
                       manager.api.wait_task]:
        status = await status_api(server.ip_addr, tid)
        assert status is not None
        actual_state = status['state']
        if actual_state == 'failed':
            break
    else:
        assert actual_state == 'failed'


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_backup_is_abortable(manager: ManagerClient, object_storage):
    '''check that backing up a snapshot for a keyspace works'''
    await do_test_backup_abort(manager, object_storage, breakpoint_name="backup_task_pause", min_files=0)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_backup_is_abortable_in_s3_client(manager: ManagerClient, object_storage):
    '''check that backing up a snapshot for a keyspace works'''
    await do_test_backup_abort(manager, object_storage, breakpoint_name="backup_task_pre_upload", min_files=0,
                               max_files=1)


@pytest.mark.asyncio
async def test_simple_backup_and_restore(manager: ManagerClient, object_storage, tmp_path):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, object_storage, tmp_path, False, False)


@pytest.mark.asyncio
async def test_abort_simple_backup_and_restore(manager: ManagerClient, object_storage, tmp_path):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, object_storage, tmp_path, False, True)


# def insert_rows(cql, keyspace, table, count):
#     for _ in range(count):
#         key = os.urandom(64).hex()
#         value = os.urandom(1024).hex()
#         query = f"INSERT INTO {keyspace}.{table} (name, value) VALUES ('{key}', '{value}');"
#         cql.execute(query)
#
#
# async def get_snapshot_files(manager, server, keyspace, snapshot_name):
#     workdir = await manager.server_get_workdir(server.server_id)
#     data_path = os.path.join(workdir, 'data', keyspace)
#     cf_dirs = os.listdir(data_path)
#     if not cf_dirs:
#         raise RuntimeError(f"No column family directories found in {data_path}")
#     cf_dir = cf_dirs[0]
#     snapshot_path = os.path.join(data_path, cf_dir, 'snapshots', snapshot_name)
#     return [
#         f.name for f in os.scandir(snapshot_path)
#         if f.is_file() and f.name.endswith('TOC.txt')
#     ]


@pytest.mark.asyncio
@skip_mode('dev', 'Too slow for dev mode')
@skip_mode('debug', 'Too slow for debug mode')
async def test_direct_restore(manager: ManagerClient, s3_storage, tmp_path):
    await do_direct_restore(manager, s3_storage, tmp_path)


@pytest.mark.asyncio
@pytest.mark.skip(reason="a very slow test (20+ seconds), skipping it")
async def test_abort_restore_with_rpc_error(manager: ManagerClient, object_storage):
    await do_abort_restore(manager, object_storage)


@pytest.mark.asyncio
async def test_simple_backup_and_restore_with_encryption(manager: ManagerClient, object_storage, tmp_path):
    '''check that restoring from backed up snapshot for a keyspace:table works'''
    await do_test_simple_backup_and_restore(manager, object_storage, tmp_path, True, False)


# async def create_cluster(topology, rf_rack_valid_keyspaces, manager, logger, object_storage=None):
#     logger.info(
#         f'Start cluster with {topology.nodes} nodes in {topology.dcs} DCs, {topology.racks} racks, rf_rack_valid_keyspaces: {rf_rack_valid_keyspaces}')
#
#     cfg = {'task_ttl_in_seconds': 300, 'rf_rack_valid_keyspaces': rf_rack_valid_keyspaces}
#     if object_storage:
#         objconf = object_storage.create_endpoint_conf()
#         cfg['object_storage_endpoints'] = objconf
#
#     cmd = ['--logger-log-level',
#            'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug:api=info']
#     servers = []
#     host_ids = {}
#
#     for s in range(topology.nodes):
#         dc = f'dc{s % topology.dcs}'
#         rack = f'rack{s % topology.racks}'
#         s = await manager.server_add(config=cfg, cmdline=cmd, property_file={'dc': dc, 'rack': rack})
#         logger.info(f'Created node {s.ip_addr} in {dc}.{rack}')
#         servers.append(s)
#         host_ids[s.server_id] = await manager.get_host_id(s.server_id)
#
#     return servers, host_ids


# def create_dataset(manager, ks, cf, topology, logger, dcs_num=None):
#     cql = manager.get_cql()
#     logger.info(f'Create keyspace, rf={topology.rf}')
#     keys = range(256)
#     replication_opts = {'class': 'NetworkTopologyStrategy'}
#     if dcs_num is not None:
#         for dc in range(dcs_num):
#             replication_opts[f'dc{dc}'] = int(topology.rf / dcs_num)
#     else:
#         replication_opts['replication_factor'] = f'{topology.rf}'
#     replication_opts = format_tuples(replication_opts)
#
#     print(replication_opts)
#
#     cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
#
#     schema = f"CREATE TABLE {ks}.{cf} ( pk int primary key, value text );"
#     cql.execute(schema)
#     for k in keys:
#         cql.execute(f"INSERT INTO {ks}.{cf} ( pk, value ) VALUES ({k}, '{k}');")
#
#     return schema, keys, replication_opts
#
#
# async def take_snapshot(ks, servers, manager, logger):
#     logger.info(f'Take snapshot and collect sstables lists')
#     snap_name = unique_name('backup_')
#     sstables = []
#     for s in servers:
#         await manager.api.flush_keyspace(s.ip_addr, ks)
#         await manager.api.take_snapshot(s.ip_addr, ks, snap_name)
#         workdir = await manager.server_get_workdir(s.server_id)
#         cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
#         tocs = [f.name for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/{snap_name}') if
#                 f.is_file() and f.name.endswith('TOC.txt')]
#         logger.info(f'Collected sstables from {s.ip_addr}:{cf_dir}/snapshots/{snap_name}: {tocs}')
#         sstables += tocs
#
#     return snap_name, sstables


def compute_scope(topology, servers):
    if topology.dcs > 1:
        scope = 'dc'
        r_servers = servers[:topology.dcs]
    elif topology.racks > 1:
        scope = 'rack'
        r_servers = servers[:topology.racks]
    else:
        scope = 'node'
        r_servers = servers

    return scope, r_servers


async def check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, r_servers, host_ids, scope):
    logger.info(f'Check the data is back')

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(
            r'INFO.*sstables_loader - load_and_stream:.*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        streamed_to = set([str(host_ids[s.server_id])] + [r[1].group(1) for r in res])
        scope_nodes = set([str(host_ids[s.server_id])])
        # See comment near merge_tocs() above for explanation of servers list filtering below
        if scope == 'rack':
            rf = get_replication(cql, ks)[s.datacenter]
            if type(rf) is list:
                scope_nodes.update([str(host_ids[s.server_id]) for s in servers[i::topology.racks] if s.rack in rf])
            else:
                scope_nodes.update([str(host_ids[s.server_id]) for s in servers[i::topology.racks]])
        elif scope == 'dc':
            scope_nodes.update([str(host_ids[s.server_id]) for s in servers[i::topology.dcs]])
        logger.info(f'{s.ip_addr} streamed to {streamed_to}, expected {scope_nodes}')
        assert streamed_to == scope_nodes


# async def do_restore(ks, cf, s, toc_names, scope, prefix, object_storage, manager, logger, primary_replica_only=False):
#     logger.info(f'Restore {s.ip_addr} with {toc_names}, scope={scope}')
#     tid = await manager.api.restore(s.ip_addr, ks, cf, object_storage.address, object_storage.bucket_name, prefix,
#                                     toc_names, scope, primary_replica_only=primary_replica_only)
#     status = await manager.api.wait_task(s.ip_addr, tid)
#     assert (status is not None) and (status['state'] == 'done')
#
#
# async def do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger):
#     logger.info(f'Backup to {snap_name}')
#     tid = await manager.api.backup(s.ip_addr, ks, cf, snap_name, object_storage.address, object_storage.bucket_name,
#                                    prefix)
#     status = await manager.api.wait_task(s.ip_addr, tid)
#     assert (status is not None) and (status['state'] == 'done')


@pytest.mark.asyncio
@pytest.mark.parametrize("topology_rf_validity", [
    (topo(rf=1, nodes=3, racks=1, dcs=1), True),
    (topo(rf=3, nodes=5, racks=1, dcs=1), False),
    (topo(rf=1, nodes=4, racks=2, dcs=1), True),
    (topo(rf=3, nodes=6, racks=2, dcs=1), False),
    (topo(rf=3, nodes=6, racks=3, dcs=1), True),
    (topo(rf=2, nodes=8, racks=4, dcs=2), True)
])
async def test_restore_with_streaming_scopes(manager: ManagerClient, object_storage, topology_rf_validity):
    '''Check that restoring of a cluster with stream scopes works'''

    topology, rf_rack_valid_keyspaces = topology_rf_validity

    servers, host_ids = await create_cluster(topology, rf_rack_valid_keyspaces, manager, logger, object_storage)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()

    ks = 'ks'
    cf = 'cf'

    schema, keys, replication_opts = create_dataset(manager, ks, cf, topology, logger)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    scope, r_servers = compute_scope(topology, servers)

    await asyncio.gather(
        *(do_restore(ks, cf, s, sstables, scope, prefix, object_storage, manager, logger) for s in r_servers))

    await check_data_is_back(manager, logger, cql, ks, cf, keys, servers, topology, r_servers, host_ids, scope)


@pytest.mark.asyncio
async def test_restore_with_non_existing_sstable(manager: ManagerClient, object_storage):
    '''Check that restore task fails well when given a non-existing sstable'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    cmd = ['--logger-log-level', 'snapshots=trace:task_manager=trace:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)
    cql = manager.get_cql()
    print('Create keyspace')
    ks, cf = create_ks_and_cf(cql)

    # The name must be parseable by sstable layer, yet such file shouldn't exist
    sstable_name = 'me-3gou_0fvw_4r94g2h8nw60b8ly4c-big-TOC.txt'
    tid = await manager.api.restore(server.ip_addr, ks, cf, object_storage.address, object_storage.bucket_name,
                                    'no_such_prefix', [sstable_name])
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert 'state' in status and status['state'] == 'failed'
    assert 'error' in status and 'Not Found' in status['error']


@pytest.mark.asyncio
async def test_backup_broken_streaming(manager: ManagerClient, s3_storage):
    # Define configuration for the servers.
    objconf = s3_storage.create_endpoint_conf()
    config = {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
        'task_ttl_in_seconds': 300,
    }
    cmd = ['--smp', '1', '--logger-log-level', 'sstables_loader=debug:sstable=debug']
    server = await manager.server_add(config=config, cmdline=cmd)

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    pid = local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local Scylla process")
    # Now that we know the process id, use /proc to find the executable.
    try:
        scylla_path = os.readlink(f'/proc/{pid}/exe')
    except:
        pytest.skip("Can't find local Scylla executable")
    # Confirm that this executable is a real tool-providing Scylla by trying
    # to run it with the "--list-tools" option
    try:
        subprocess.check_output([scylla_path, '--list-tools'])
    except:
        pytest.skip("Local server isn't Scylla")

    async with new_test_keyspace(manager,
                                 "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        table = 'test_cf'
        create_table_query = (
            f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text) "
            f"WITH tablets = {{'min_tablet_count': '16'}};"
        )
        cql.execute(create_table_query)

        expected_rows = 0
        with tempfile.TemporaryDirectory() as tmp_dir:
            resource_dir = "test/resource/sstables/fully_partially_contained_ssts"
            schema_file = os.path.join(tmp_dir, "schema.cql")
            with open(schema_file, "w") as f:
                f.write(f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text)")
                f.flush()
            for root, _, files in os.walk(resource_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    print("Processing file:", local_path)
                    sst = subprocess.check_output(
                        [scylla_path, "sstable", "write", "--schema-file", schema_file, "--input-format", "json",
                         "--output-dir", tmp_dir, "--input-file", local_path])
                    expected_rows += json.loads(subprocess.check_output(
                        [scylla_path, "sstable", "query", "-q", f"SELECT COUNT(*) FROM scylla_sstable.{table}",
                         "--output-format", "json", "--sstables",
                         os.path.join(tmp_dir, f"me-{sst.decode().strip()}-big-TOC.txt")]).decode())[0]['count']

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

        assert res[0].count == expected_rows, f"number of rows after restore is incorrect: {res[0].count}"


@pytest.mark.asyncio
async def test_restore_primary_replica_same_rack_scope_rack(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope rack streams only to primary replica in the same rack.
    The test checks that each mutation exists exactly 2 times within the cluster, once in each rack
    (each restoring node streams to one primary replica in its rack. Without primary_replica_only we'd see 4 replicas, 2 in each rack).
    The test also checks that the logs of each restoring node shows streaming to a single node, which is the primary replica within the same rack.'''

    topology = topo(rf=4, nodes=8, racks=2, dcs=1)
    scope = "rack"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, False, manager, logger, object_storage)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()

    schema, keys, replication_opts = create_dataset(manager, ks, cf, topology, logger)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    _, r_servers = compute_scope(topology, servers)

    await asyncio.gather(
        *(do_restore(ks, cf, s, sstables, scope, prefix, object_storage, manager, logger, primary_replica_only=True) for
          s in r_servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, expected_replicas=2)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(
            r'INFO.*sstables_loader - load_and_stream: ops_uuid=([0-9a-z-]+).*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        nodes_by_operation = defaultdict(list)
        for r in res:
            nodes_by_operation[r[1].group(1)].append(r[1].group(2))

        scope_nodes = set([str(host_ids[s.server_id]) for s in servers[i::topology.racks]])
        for op, nodes in nodes_by_operation.items():
            logger.info(f'Operation {op} streamed to nodes {nodes}')
            assert len(nodes) == 1, "Each streaming operation should stream to exactly one primary replica"
            assert nodes[0] in scope_nodes, f"Primary replica should be within the scope {scope}"


@pytest.mark.asyncio
async def test_restore_primary_replica_different_rack_scope_dc(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope dc permits cross-rack streaming.
    The test checks that each mutation exists exactly 1 time within the cluster, in one of the racks.
    (each restoring node would pick the same primary replica, one would pick it within its own rack(itself), one would pick it from the other rack.
     Without primary_replica_only we'd see 2 replicas, 1 in each rack).
    The test also checks that the logs of each restoring node shows streaming to two nodes because cross-rack streaming is allowed
    and eventually one node, depending on tablet_id of mutations, will end up choosing either of the two nodes as primary replica.'''

    topology = topo(rf=2, nodes=2, racks=2, dcs=1)
    scope = "dc"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, True, manager, logger, object_storage)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()

    schema, keys, replication_opts = create_dataset(manager, ks, cf, topology, logger)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    _, r_servers = compute_scope(topology, servers)

    await asyncio.gather(
        *(do_restore(ks, cf, s, sstables, scope, prefix, object_storage, manager, logger, primary_replica_only=True) for
          s in r_servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, expected_replicas=1)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(
            r'INFO.*sstables_loader - load_and_stream:.*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        streamed_to = set([r[1].group(1) for r in res])
        logger.info(f'{s.ip_addr} {host_ids[s.server_id]} streamed to {streamed_to}')
        assert len(streamed_to) == 2


@pytest.mark.asyncio
async def test_restore_primary_replica_same_dc_scope_dc(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope dc streams only to primary replica in the local dc.
    The test checks that each mutation exists exactly 2 times within the cluster, once in each dc
    (each restoring node streams to one primary replica in its dc. Without primary_replica_only we'd see 4 replicas, 2 in each dc).
    The test also checks that the logs of each restoring node shows streaming to a single node, which is the primary replica within the same dc.'''

    topology = topo(rf=4, nodes=8, racks=2, dcs=2)
    scope = "dc"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, False, manager, logger, object_storage)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()

    schema, keys, replication_opts = create_dataset(manager, ks, cf, topology, logger)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    _, r_servers = compute_scope(topology, servers)

    await asyncio.gather(
        *(do_restore(ks, cf, s, sstables, scope, prefix, object_storage, manager, logger, primary_replica_only=True) for
          s in r_servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, expected_replicas=2)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(
            r'INFO.*sstables_loader - load_and_stream: ops_uuid=([0-9a-z-]+).*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        nodes_by_operation = defaultdict(list)
        for r in res:
            nodes_by_operation[r[1].group(1)].append(r[1].group(2))

        scope_nodes = set([str(host_ids[s.server_id]) for s in servers[i::topology.dcs]])
        for op, nodes in nodes_by_operation.items():
            logger.info(f'Operation {op} streamed to nodes {nodes}')
            assert len(nodes) == 1, "Each streaming operation should stream to exactly one primary replica"
            assert nodes[0] in scope_nodes, f"Primary replica should be within the scope {scope}"


@pytest.mark.asyncio
async def test_restore_primary_replica_different_dc_scope_all(manager: ManagerClient, object_storage):
    '''Check that restoring with primary_replica_only and scope all permits cross-dc streaming.
    The test checks that each mutation exists exactly 1 time within the cluster, in only one of the dcs.
    (each restoring node would pick the same primary replica, one would pick it within its own dc(itself), one would pick it from the other dc.
     Without primary_replica_only, we'd see 2 replicas, 1 in each dc).
    The test also checks that the logs of each restoring node shows streaming to two nodes because cross-dc streaming is allowed
    and eventually one node, depending on tablet_id of mutations, will end up choosing either of the two nodes as primary replica.'''

    topology = topo(rf=2, nodes=2, racks=2, dcs=2)
    scope = "all"
    ks = 'ks'
    cf = 'cf'

    servers, host_ids = await create_cluster(topology, False, manager, logger, object_storage)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)
    cql = manager.get_cql()

    schema, keys, replication_opts = create_dataset(manager, ks, cf, topology, logger, dcs_num=2)

    snap_name, sstables = await take_snapshot(ks, servers, manager, logger)
    prefix = f'{cf}/{snap_name}'

    await asyncio.gather(*(do_backup(s, snap_name, prefix, ks, cf, object_storage, manager, logger) for s in servers))

    logger.info(f'Re-initialize keyspace')
    cql.execute(f'DROP KEYSPACE {ks}')
    cql.execute((f"CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts};"))
    cql.execute(schema)

    r_servers = servers

    await asyncio.gather(
        *(do_restore(ks, cf, s, sstables, scope, prefix, object_storage, manager, logger, primary_replica_only=True) for
          s in r_servers))

    await check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, expected_replicas=1)

    logger.info(f'Validate streaming directions')
    for i, s in enumerate(r_servers):
        log = await manager.server_open_log(s.server_id)
        res = await log.grep(
            r'INFO.*sstables_loader - load_and_stream:.*target_node=([0-9a-z-]+),.*num_bytes_sent=([0-9]+)')
        streamed_to = set([r[1].group(1) for r in res])
        logger.info(f'{s.ip_addr} {host_ids[s.server_id]} streamed to {streamed_to}, expected {r_servers}')
        assert len(streamed_to) == 2
