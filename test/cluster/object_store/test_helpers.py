#!/usr/bin/env python3
import asyncio
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

from cassandra.query import SimpleStatement  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.object_store.conftest import format_tuples
from test.cluster.object_store.test_utils import create_cluster, insert_rows_parallel, \
    prepare_snapshot_for_backup, create_keyspace_and_table, backup_cluster
from test.cluster.util import wait_for_cql_and_get_hosts, get_replication
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.tablets import get_tablet_count
from test.pylib.util import unique_name, wait_for_first_completed

logger = logging.getLogger(__name__)


async def do_test_backup_abort(manager: ManagerClient, object_storage,
                               breakpoint_name, min_files, max_files=None):
    '''helper for backup abort testing'''

    servers = await create_cluster(manager, object_storage, nodes=1)
    cql = manager.get_cql()
    ks, cf = create_keyspace_and_table(cql)
    prefix = f'{cf}/backup'

    await insert_rows_parallel(cql, ks, cf, 10)
    await prepare_snapshot_for_backup(manager, servers, ks, cf, 'backup')

    workdir = await manager.server_get_workdir(server.server_id)
    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]
    files = set(os.listdir(f'{workdir}/data/{ks}/{cf_dir}/snapshots/backup'))
    assert len(files) > 1

    await manager.api.enable_injection(server.ip_addr, breakpoint_name, one_shot=True)
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    print('Backup snapshot')
    statuses = await backup_cluster(manager, servers, ks, cf, snapshot_name="backup", storage=object_storage,
                                    prefix=prefix)

    print(f'Started task {tid}, aborting it early')
    await log.wait_for(breakpoint_name + ': waiting', from_mark=mark)
    await manager.api.abort_task(server.ip_addr, tid)
    await manager.api.message_injection(server.ip_addr, breakpoint_name)
    status = await manager.api.wait_task(server.ip_addr, tid)
    print(f'Status: {status}')
    assert (status is not None) and (status['state'] == 'failed')
    assert "seastar::abort_requested_exception (abort requested)" in status['error']

    objects = set(o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.all())
    uploaded_count = 0
    for f in files:
        in_backup = f'{prefix}/{f}' in objects
        print(f'Check {f} is in backup: {in_backup}')
        if in_backup:
            uploaded_count += 1
    # Note: since s3 client is abortable and run async, we might fail even the first file
    # regardless of if we set the abort status before or after the upload is initiated.
    # Parallelism is a pain.
    assert min_files <= uploaded_count < len(files)
    assert max_files is None or uploaded_count < max_files


async def do_test_simple_backup_and_restore(manager: ManagerClient, object_storage, tmpdir, do_encrypt=False,
                                            do_abort=False):
    '''check that restoring from backed up snapshot for a keyspace:table works'''

    objconf = object_storage.create_endpoint_conf()
    cfg = {'enable_user_defined_functions': False,
           'object_storage_endpoints': objconf,
           'experimental_features': ['keyspace-storage-options'],
           'task_ttl_in_seconds': 300
           }
    if do_encrypt:
        d = tmpdir / "system_keys"
        d.mkdir()
        cfg = cfg | {
            'system_key_directory': str(d),
            'user_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}
        }
    cmd = ['--logger-log-level',
           'sstables_loader=debug:sstable_directory=trace:snapshots=trace:s3=trace:sstable=debug:http=debug:encryption=debug:api=info']
    server = await manager.server_add(config=cfg, cmdline=cmd)

    cql = manager.get_cql()
    workdir = await manager.server_get_workdir(server.server_id)

    # This test is sensitive not to share the bucket with any other test
    # that can run in parallel, so generate some unique name for the snapshot
    snap_name = unique_name('backup_')
    print(f'Create and backup keyspace (snapshot name is {snap_name})')
    ks, cf = await prepare_snapshot_for_backup(manager, server, snap_name)

    cf_dir = os.listdir(f'{workdir}/data/{ks}')[0]

    def list_sstables():
        return [f for f in os.scandir(f'{workdir}/data/{ks}/{cf_dir}') if f.is_file()]

    orig_res = cql.execute(f"SELECT * FROM {ks}.{cf}")
    orig_rows = {x.name: x.value for x in orig_res}

    # include a "suffix" in the key to mimic the use case where scylla-manager
    # 1. backups sstables of multiple snapshots, and deduplicate the backup'ed
    #    sstables by only upload the new sstables
    # 2. restore a given snapshot by collecting all sstables of this snapshot from
    #    multiple places
    #
    # in this test, we:
    # 1. upload:
    #    prefix: {some}/{objects}/{path}
    #    sstables:
    #    - 1-TOC.txt
    #    - 2-TOC.txt
    #    - ...
    # 2. download:
    #    prefix = {some}/{objects}/{path}
    #    sstables:
    #    - 1-TOC.txt
    #    - 2-TOC.txt
    #    - ...
    old_files = list_sstables();
    toc_names = [f'{entry.name}' for entry in old_files if entry.name.endswith('TOC.txt')]

    prefix = f'{cf}/{snap_name}'
    tid = await manager.api.backup(server.ip_addr, ks, cf, snap_name, object_storage.address,
                                   object_storage.bucket_name, f'{prefix}')
    status = await manager.api.wait_task(server.ip_addr, tid)
    assert (status is not None) and (status['state'] == 'done')

    print('Drop the table data and validate it\'s gone')
    cql.execute(f"TRUNCATE TABLE {ks}.{cf};")
    files = list_sstables()
    assert len(files) == 0
    res = cql.execute(f"SELECT * FROM {ks}.{cf};")
    assert not res
    objects = set(
        o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.filter(Prefix=prefix))
    assert len(objects) > 0

    print('Try to restore')
    tid = await manager.api.restore(server.ip_addr, ks, cf, object_storage.address, object_storage.bucket_name, prefix,
                                    toc_names)

    if do_abort:
        await manager.api.abort_task(server.ip_addr, tid)

    status = await manager.api.wait_task(server.ip_addr, tid)
    if not do_abort:
        assert status is not None
        assert status['state'] == 'done'
        assert status['progress_units'] == 'batches'
        assert status['progress_completed'] == status['progress_total']
        assert status['progress_completed'] > 0

    print('Check that sstables came back')
    files = list_sstables()

    sstable_names = [f'{entry.name}' for entry in files if entry.name.endswith('.db')]
    db_objects = [object for object in objects if object.endswith('.db')]

    if do_abort:
        assert len(files) >= 0
        # These checks can be viewed as dubious. We restore (atm) on a mutation basis mostly.
        # There is no guarantee we'll generate the same amount of sstables as was in the original
        # backup (?). But, since we are not stressing the server here (not provoking memtable flushes),
        # we should in principle never generate _more_ sstables than originated the backup.
        assert len(old_files) >= len(files)
        assert len(sstable_names) <= len(db_objects)
    else:
        assert len(files) > 0
        assert (status is not None) and (status['state'] == 'done')
        print(f'Check that data came back too')
        res = cql.execute(f"SELECT * FROM {ks}.{cf};")
        rows = {x.name: x.value for x in res}
        assert rows == orig_rows, "Unexpected table contents after restore"

    print('Check that backup files are still there')  # regression test for #20938
    post_objects = set(
        o.key for o in object_storage.get_resource().Bucket(object_storage.bucket_name).objects.filter(Prefix=prefix))
    assert objects == post_objects


async def do_direct_restore(manager: ManagerClient, s3_storage, tmp_path):
    # This test creates multiple SSTables and resizes the number of tablets in the table, triggering
    # a split. As a result, some SSTables will be partially contained while others will be fully
    # contained within a tablet. When restoring at the node scope, this setup enables direct SSTable
    # downloads for fully contained SSTables, bypassing the standard load and stream mechanisms.
    # To achieve the above a recipe by Raphael S. Carvalho was proposed.
    # 1) create table with N tablets.
    # 2) disable balancing.
    # 3) write hundreds of keys, so data is spread across N tablets.
    # 4) after flushing, copy sstables somewhere
    # 5) alter table to have min_tablet_count hint set to N*2 (causes split)
    # 6) enable balancing (enables split too)
    # 7) wait for tablet count to be greater than N (see test_tablets2.py for guidance)
    # 8) write hundreds of keys, so data is spread across N*2 tablets.
    # 9) after flushing, copy sstables somewhere
    # The "copy sstables somewhere" steps are replaced by taking snapshots and backing them up to S3.

    objconf = s3_storage.create_endpoint_conf()
    config = {
        'enable_user_defined_functions': False,
        'object_storage_endpoints': objconf,
        'experimental_features': ['keyspace-storage-options'],
        'task_ttl_in_seconds': 300,
    }
    d = tmp_path / "system_keys"
    d.mkdir()
    config = config | {
        'system_key_directory': str(d),
        'user_info_encryption': {'enabled': True, 'key_provider': 'LocalFileSystemKeyProviderFactory'}
    }
    cmd = ['--smp', '16', '-m', '32G', '--logger-log-level', 'sstables_loader=debug:sstable=info']
    servers = await manager.servers_add(servers_num=3, config=config, cmdline=cmd, auto_rack_dc="dc1")

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    initial_tablets = 8
    target_tablets = initial_tablets * 2

    # 1) create table with N tablets.
    keyspace = 'test_ks'
    table = 'test_cf'
    await create_keyspace_and_table(manager, cql, servers, keyspace, table, initial_tablets)

    # 2) disable balancing.
    for server in servers:
        # Disable auto compaction just to keep more sstables around for restore
        await manager.api.disable_autocompaction(server.ip_addr, keyspace)
        await manager.api.disable_tablet_balancing(server.ip_addr)

    # 3) write hundreds of keys, so data is spread across N tablets.
    insert_rows_parallel(cql, keyspace, table, 100_000)

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
            s3_storage.address, s3_storage.bucket_name, f'{prefix}/{server.server_id}'
        )
    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, backup_tasks[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Backup task failed on server {server.server_id}. Status: {status}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")

    # 5) alter table to have min_tablet_count hint set to N*2 (causes split)
    alter_query = f"ALTER TABLE {keyspace}.{table} WITH tablets = {{'min_tablet_count': '{target_tablets}'}};"
    cql.execute(alter_query)

    # 6) enable balancing (enables split too)
    for server in servers:
        await manager.api.enable_tablet_balancing(server.ip_addr)

    # 7) wait for tablet count to be greater than N (see test_tablets2.py for guidance)
    start_time = time.time()
    while True:
        actual_tablet_count = await get_tablet_count(manager, servers[0], keyspace, table)
        logger.debug(f'actual/expected tablet count: {actual_tablet_count}/{target_tablets}')
        if actual_tablet_count == target_tablets:
            break
        assert time.time() - start_time < 120, 'Timeout while waiting for tablet merge'
        await asyncio.sleep(1)

    logger.info(f'Merged test table; new number of tablets: {actual_tablet_count}')

    # 8) write hundreds of keys, so data is spread across N*2 tablets.
    insert_rows_parallel(cql, keyspace, table, 100_000)

    # Flush keyspace on all servers
    print("Flushing keyspace on all servers...")
    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, keyspace)

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table}")
    row_count += res[0].count
    print(f"Row count after second insert: {row_count}")

    snapshot_name = unique_name('backup_')
    # Take snapshot for keyspace
    print(f"Taking snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
    for server in servers:
        await manager.api.take_snapshot(server.ip_addr, keyspace, snapshot_name)

    # Collect snapshot files from each server
    for server in servers:
        additional_files = await get_snapshot_files(manager, server, keyspace, snapshot_name)
        if server.server_id in sstables:
            sstables[server.server_id].extend(additional_files)
        else:
            sstables[server.server_id] = additional_files

    for server_id, toc_files in sstables.items():
        print(f"Server ID: {server_id}, TOC files: {len(toc_files)}")

    # Backup the keyspace on each server to S3
    print(f"Backing up keyspace using prefix '{prefix}' on all servers...")
    backup_tasks = {}
    for server in servers:
        backup_tasks[server.server_id] = await manager.api.backup(
            server.ip_addr, keyspace, table, snapshot_name,
            s3_storage.address, s3_storage.bucket_name, f'{prefix}/{server.server_id}'
        )
    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, backup_tasks[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Backup task failed on server {server.server_id}. Status: {status}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")
    print("Initiating restore operations...")

    restore_task_ids = {}
    for server in servers:
        restore_task_ids[server.server_id] = await manager.api.restore(
            server.ip_addr, keyspace, table,
            s3_storage.address, s3_storage.bucket_name,
            f'{prefix}/{server.server_id}', sstables[server.server_id], "node"
        )

    for server in servers:
        status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        assert status and status.get(
            'state') == 'done', f"Restore task failed on server {server.server_id}. Status: {status}"

    res = cql.execute(f"SELECT COUNT(*) FROM {keyspace}.{table} BYPASS CACHE USING TIMEOUT 600s;")

    assert res[0].count == row_count, f"number of rows after restore is incorrect: {res[0].count}"

    logs = [await manager.server_open_log(server.server_id) for server in servers]
    await wait_for_first_completed(
        [l.wait_for("fully contained SSTables to local node from object storage", timeout=10) for l in logs])


async def do_abort_restore(manager: ManagerClient, object_storage):
    # Define configuration for the servers.
    objconf = object_storage.create_endpoint_conf()
    config = {'enable_user_defined_functions': False,
              'object_storage_endpoints': objconf,
              'experimental_features': ['keyspace-storage-options'],
              'task_ttl_in_seconds': 300,
              }

    servers = await manager.servers_add(servers_num=3, config=config, auto_rack_dc='dc1')

    # Obtain the CQL interface from the manager.
    cql = manager.get_cql()

    # Create keyspace, table, and fill data
    print("Creating keyspace and table, then inserting data...")

    def create_keyspace_and_table(cql):
        keyspace = 'test_ks'
        table = 'test_cf'
        replication_opts = format_tuples({
            'class': 'NetworkTopologyStrategy',
            'replication_factor': '3'
        })
        create_ks_query = f"CREATE KEYSPACE {keyspace} WITH REPLICATION = {replication_opts};"
        create_table_query = f"CREATE TABLE {keyspace}.{table} (name text PRIMARY KEY, value text);"
        cql.execute(create_ks_query)
        cql.execute(create_table_query)

        def insert_rows(cql, keyspace, table, inserts):
            for _ in range(inserts):
                key = os.urandom(64).hex()
                value = os.urandom(1024).hex()
                insert_query = f"INSERT INTO {keyspace}.{table} (name, value) VALUES ('{key}', '{value}');"
                cql.execute(insert_query)

        thread_count = 128
        rows_per_thread = 100000 // thread_count
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            # Submit tasks for each thread
            futures = [
                executor.submit(
                    insert_rows,
                    cql, keyspace, table,
                    rows_per_thread
                )
                for _ in range(thread_count)
            ]

        # Ensure all tasks are completed
        for future in futures:
            future.result()
        return keyspace, table

    keyspace, table = create_keyspace_and_table(cql)

    # Flush keyspace on all servers
    print("Flushing keyspace on all servers...")
    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, keyspace)

    # Take snapshot for keyspace
    snapshot_name = unique_name('backup_')
    print(f"Taking snapshot '{snapshot_name}' for keyspace '{keyspace}'...")
    for server in servers:
        await manager.api.take_snapshot(server.ip_addr, keyspace, snapshot_name)

    # Collect snapshot files from each server
    async def get_snapshot_files(server, snapshot_name):
        workdir = await manager.server_get_workdir(server.server_id)
        data_path = os.path.join(workdir, 'data', keyspace)
        cf_dirs = os.listdir(data_path)
        if not cf_dirs:
            raise RuntimeError(f"No column family directories found in {data_path}")
        # Assumes that there is only one column family directory under the keyspace.
        cf_dir = cf_dirs[0]
        snapshot_path = os.path.join(data_path, cf_dir, 'snapshots', snapshot_name)
        return [
            f.name for f in os.scandir(snapshot_path)
            if f.is_file() and f.name.endswith('TOC.txt')
        ]

    sstables = {}
    for server in servers:
        snapshot_files = await get_snapshot_files(server, snapshot_name)
        sstables[server.server_id] = snapshot_files

    # Backup the keyspace on each server to S3
    prefix = f"{table}/{snapshot_name}"
    print(f"Backing up keyspace using prefix '{prefix}' on all servers...")
    for server in servers:
        backup_tid = await manager.api.backup(
            server.ip_addr,
            keyspace,
            table,
            snapshot_name,
            object_storage.address,
            object_storage.bucket_name,
            prefix
        )
        backup_status = await manager.api.wait_task(server.ip_addr, backup_tid)
        assert backup_status is not None and backup_status.get('state') == 'done', \
            f"Backup task failed on server {server.server_id}"

    # Truncate data and start restore
    print("Dropping table data...")
    cql.execute(f"TRUNCATE TABLE {keyspace}.{table};")
    print("Initiating restore operations...")

    restore_task_ids = {}
    for server in servers:
        restore_tid = await manager.api.restore(
            server.ip_addr,
            keyspace,
            table,
            object_storage.address,
            object_storage.bucket_name,
            prefix,
            sstables[server.server_id]
        )
        restore_task_ids[server.server_id] = restore_tid

    await asyncio.sleep(0.1)

    print("Aborting restore tasks...")
    for server in servers:
        await manager.api.abort_task(server.ip_addr, restore_task_ids[server.server_id])

    # Check final status of restore tasks
    for server in servers:
        final_status = await manager.api.wait_task(server.ip_addr, restore_task_ids[server.server_id])
        print(f"Restore task status on server {server.server_id}: {final_status}")
        assert (final_status is not None) and (final_status['state'] == 'failed')
    logs = [await manager.server_open_log(server.server_id) for server in servers]
    await wait_for_first_completed([l.wait_for(
        "Failed to handle STREAM_MUTATION_FRAGMENTS \(receive and distribute phase\) for .+: Streaming aborted",
        timeout=10) for l in logs])


# Helper class to parametrize the test below
class topo:
    def __init__(self, rf, nodes, racks, dcs):
        self.rf = rf
        self.nodes = nodes
        self.racks = racks
        self.dcs = dcs


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


async def collect_mutations(cql, server, manager, ks, cf):
    host = await wait_for_cql_and_get_hosts(cql, [server], time.time() + 30)
    await read_barrier(manager.api, server.ip_addr)  # scylladb/scylladb#18199
    ret = {}
    for frag in await cql.run_async(f"SELECT * FROM MUTATION_FRAGMENTS({ks}.{cf})", host=host[0]):
        if not frag.pk in ret:
            ret[frag.pk] = []
        ret[frag.pk].append({'mutation_source': frag.mutation_source, 'partition_region': frag.partition_region,
                             'node': server.ip_addr})
    return ret


async def check_mutation_replicas(cql, manager, servers, keys, topology, logger, ks, cf, expected_replicas=None):
    '''Check that each mutation is replicated to the expected number of replicas'''
    if expected_replicas is None:
        expected_replicas = topology.rf * topology.dcs

    by_node = await asyncio.gather(*(collect_mutations(cql, s, manager, ks, cf) for s in servers))
    mutations = {}
    for node_frags in by_node:
        for pk in node_frags:
            if not pk in mutations:
                mutations[pk] = []
            mutations[pk].append(node_frags[pk])

    for k in random.sample(keys, 17):
        if not k in mutations:
            logger.info(f'{k} not found in mutations')
            logger.info(f'Mutations: {mutations}')
            assert False, "Key not found in mutations"

        if len(mutations[k]) != expected_replicas:
            logger.info(f'{k} is replicated {len(mutations[k])} times only, expect {expected_replicas}')
            logger.info(f'Mutations: {mutations}')
            assert False, "Key not replicated enough"
