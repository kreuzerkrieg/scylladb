#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import json
import logging
import time

import pytest
from cassandra.protocol import ConfigurationException

from test.pylib.object_storage import format_tuples
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)


async def wait_for_endpoints(manager: ScyllaClusterManager, expected: dict) -> None:
    """Wait until each server reports exactly the expected endpoint names with the expected uri."""
    async def endpoints_match():
        for server, endpoints in expected.items():
            actual = await manager.api.get_config(server.ip_addr, 'object_storage_endpoints')
            actual = {name: json.loads(conf).get('uri') for name, conf in actual.items()}
            if actual != endpoints:
                logger.info('Endpoints of %s: %s, expected %s', server.ip_addr, actual, endpoints)
                return None
        return True

    await wait_for(endpoints_match, deadline=time.time() + 120)


def create_keyspace_statement(ks, object_storage, endpoint, bucket=None):
    '''With bucket=False the keyspace names no bucket and takes the one of its endpoint.'''
    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy', 'replication_factor': '1'})
    storage = {'type': f'{object_storage.type}', 'endpoint': endpoint}
    if bucket is not False:
        storage['bucket'] = bucket or object_storage.bucket_name
    return f'CREATE KEYSPACE {ks} WITH REPLICATION = {replication_opts} AND STORAGE = {format_tuples(storage)};'


async def test_keyspace_on_endpoint_from_cluster_config(manager: ScyllaClusterManager, object_storage):
    '''A keyspace is created on an endpoint that only cluster config defines, and its sstables
    are readable after a restart, when the node has to know the endpoint before it loads them.'''
    server = await manager.server_add(config={'enable_user_defined_functions': False})
    cql = manager.get_cql()
    await wait_for_endpoints(manager, {server: {}})

    create_keyspace = create_keyspace_statement('ks', object_storage, 'store')
    with pytest.raises(ConfigurationException, match='not configured'):
        cql.execute(create_keyspace)

    base = object_storage.create_endpoint_conf()[0]
    endpoints = json.dumps([{**base, 'name': 'store', 'uri': base['name']}])
    await cql.run_async(f"ALTER CLUSTER WITH object_storage_endpoints = '{endpoints}'")
    await wait_for_endpoints(manager, {server: {'store': base['name']}})

    cql.execute(create_keyspace)
    cql.execute('CREATE TABLE ks.test (name text PRIMARY KEY, value int);')
    await cql.run_async("INSERT INTO ks.test (name, value) VALUES ('key', 1);")
    await manager.api.flush_keyspace(server.ip_addr, 'ks')
    read = "SELECT value FROM ks.test WHERE name = 'key' BYPASS CACHE;"
    assert cql.execute(read).one().value == 1, 'Unexpected value before the restart'

    await manager.server_restart(server.server_id)
    cql, _ = await manager.get_ready_cql([server])
    await wait_for_endpoints(manager, {server: {'store': base['name']}})
    assert cql.execute(read).one().value == 1, 'Unexpected value after the restart'


async def test_endpoints_per_datacenter(manager: ScyllaClusterManager):
    '''A datacenter resolves the endpoint list of its own scope, and an override replaces the
    list of a broader scope and of scylla.yaml as a whole.'''
    def endpoint(name, uri):
        return {'name': name, 'type': 'gs', 'uri': uri, 'credentials_file': 'none'}

    dc1 = await manager.server_add(property_file={'dc': 'dc1', 'rack': 'rack1'},
                                   config={'object_storage_endpoints': [endpoint('local', 'http://127.0.0.1:1')]})
    dc2 = await manager.server_add(property_file={'dc': 'dc2', 'rack': 'rack1'})
    cql, _ = await manager.get_ready_cql([dc1, dc2])
    await wait_for_endpoints(manager, {dc1: {'local': 'http://127.0.0.1:1'}, dc2: {}})

    in_dc1 = json.dumps([endpoint('store', 'http://127.0.0.1:2')])
    await cql.run_async(f"ALTER DATACENTER dc1 WITH object_storage_endpoints = '{in_dc1}'")
    await wait_for_endpoints(manager, {dc1: {'store': 'http://127.0.0.1:2'}, dc2: {}})

    everywhere = json.dumps([endpoint('store', 'http://127.0.0.1:3'), endpoint('archive', 'http://127.0.0.1:4')])
    await cql.run_async(f"ALTER CLUSTER WITH object_storage_endpoints = '{everywhere}'")
    await wait_for_endpoints(manager, {dc1: {'store': 'http://127.0.0.1:2'},
                                       dc2: {'store': 'http://127.0.0.1:3', 'archive': 'http://127.0.0.1:4'}})

    await cql.run_async('ALTER DATACENTER dc1 WITH object_storage_endpoints = null')
    await wait_for_endpoints(manager, {dc1: {'store': 'http://127.0.0.1:3', 'archive': 'http://127.0.0.1:4'},
                                       dc2: {'store': 'http://127.0.0.1:3', 'archive': 'http://127.0.0.1:4'}})

    await cql.run_async('ALTER CLUSTER WITH object_storage_endpoints = null')
    await wait_for_endpoints(manager, {dc1: {'local': 'http://127.0.0.1:1'}, dc2: {}})


async def test_keyspace_takes_the_bucket_of_its_endpoint(manager: ScyllaClusterManager, object_storage):
    '''A keyspace that names no bucket keeps its data in the bucket of its endpoint, and a
    keyspace that names one keeps it there whatever the endpoint says.'''
    base = object_storage.create_endpoint_conf()[0]
    def endpoint(name, bucket=None):
        entry = {**base, 'name': name, 'uri': base['name']}
        return {**entry, 'bucket': bucket} if bucket else entry

    endpoints = json.dumps([endpoint('store', object_storage.bucket_name),
                            endpoint('other_bucket', 'no-such-bucket-in-this-store'),
                            endpoint('no_bucket')])
    server = await manager.server_add(config={'enable_user_defined_functions': False})
    cql = manager.get_cql()
    await cql.run_async(f"ALTER CLUSTER WITH object_storage_endpoints = '{endpoints}'")
    await wait_for_endpoints(manager, {server: {name: base['name'] for name in ('store', 'other_bucket', 'no_bucket')}})

    with pytest.raises(ConfigurationException, match='has no bucket'):
        cql.execute(create_keyspace_statement('ks', object_storage, 'no_bucket', bucket=False))

    cql.execute(create_keyspace_statement('from_endpoint', object_storage, 'store', bucket=False))
    cql.execute(create_keyspace_statement('from_keyspace', object_storage, 'other_bucket'))
    stored = {row.keyspace_name: dict(row.storage_options) for row in
              cql.execute('SELECT keyspace_name, storage_options FROM system_schema.scylla_keyspaces')}
    assert 'bucket' not in stored['from_endpoint'], f'Unexpected stored options: {stored["from_endpoint"]}'
    assert stored['from_keyspace']['bucket'] == object_storage.bucket_name

    for ks in ('from_endpoint', 'from_keyspace'):
        cql.execute(f'CREATE TABLE {ks}.test (name text PRIMARY KEY, value int);')
        await cql.run_async(f"INSERT INTO {ks}.test (name, value) VALUES ('key', 1);")
        await manager.api.flush_keyspace(server.ip_addr, ks)
        assert cql.execute(f"SELECT value FROM {ks}.test WHERE name = 'key' BYPASS CACHE;").one().value == 1

    await manager.server_restart(server.server_id)
    cql, _ = await manager.get_ready_cql([server])
    for ks in ('from_endpoint', 'from_keyspace'):
        value = cql.execute(f"SELECT value FROM {ks}.test WHERE name = 'key' BYPASS CACHE;").one().value
        assert value == 1, f'Unexpected value in {ks} after the restart'
