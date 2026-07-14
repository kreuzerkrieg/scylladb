#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


import logging
from contextlib import asynccontextmanager

import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.connect_options import add_s3_options
from test.pylib.object_storage import (
    format_tuples,
    keyspace_options,
    create_s3_server,
    create_gs_server,
    GSFront,
    GSServer,
    S3Server,
    S3_Server,
    MinioWrapper,
    s3_server,
)


logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    add_s3_options(parser)


@asynccontextmanager
async def make_object_storage(kind, pytestconfig, tmpdir, test_name):
    if kind == 'gs':
        server = create_gs_server(tmpdir)
    else:
        server = create_s3_server(pytestconfig, tmpdir)

    bucket_created = False
    try:
        await server.start()
        server.create_test_bucket(test_name)
        bucket_created = True
        yield server
    finally:
        if bucket_created:
            server.destroy_test_bucket()
        await server.stop()


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir):
    async with make_object_storage(request.param, pytestconfig, tmpdir, request.node.name) as server:
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir):
    async with make_object_storage('s3', pytestconfig, tmpdir, request.node.name) as server:
        yield server


@asynccontextmanager
async def make_cluster_with_object_storage(manager: ManagerClient,
                                           kind: str,
                                           num_nodes: int,
                                           pytestconfig,
                                           tmpdir,
                                           test_name: str,
                                           extra_cfg: dict | None = None,
                                           cmdline: list[str] | None = None,
                                           property_file: list[dict] | dict | None = None,
                                           configure_endpoints: bool = True):
    """Start an object-storage backend and a Scylla cluster wired to it.

    The base config always enables the ``keyspace-storage-options`` experimental
    feature.  ``object_storage_endpoints`` is injected too unless
    ``configure_endpoints`` is False -- tests that exercise live config update
    of the endpoint want to start the server without it configured.
    ``extra_cfg`` is merged on top (test-supplied keys win).

    On exit, every currently running Scylla server is stopped before the
    bucket is destroyed -- so in-flight operations (compaction, tablet
    migration) cannot race with bucket teardown (see SCYLLADB-2471).
    Stopping *all* running servers (not just the ones this wrapper started)
    also covers tests that add more servers within the scope.

    Parameters:
        manager (ManagerClient): test-framework cluster manager.
        kind (str): object-storage backend, either ``'s3'`` or ``'gs'``.
        num_nodes (int): number of Scylla nodes to start.
        pytestconfig: the pytest ``pytestconfig`` fixture (used to locate the
            local MinIO / GCS emulator).
        tmpdir: per-test temporary directory (forwarded to the emulator).
        test_name (str): unique test name; used to derive the bucket name.
        extra_cfg (dict | None): extra Scylla config merged on top of the
            base config; test-supplied keys win.
        cmdline (list[str] | None): extra command-line arguments forwarded
            to every started Scylla server.
        property_file (list[dict] | dict | None): forwarded to
            ``manager.servers_add`` -- a per-node list assigns DC/rack per
            node, a single dict applies to every node.
        configure_endpoints (bool): when False, omit
            ``object_storage_endpoints`` from the initial config so the test
            can exercise live endpoint (re)configuration.

    Yields:
        tuple[S3Server | GSServer, list[ServerInfo]]: the running storage
            server and the list of Scylla servers that were started.
    """
    async with make_object_storage(kind, pytestconfig, tmpdir, test_name) as storage_server:
        cfg = {
            'experimental_features': ['keyspace-storage-options'],
        }
        if configure_endpoints:
            cfg['object_storage_endpoints'] = storage_server.create_endpoint_conf()
        if extra_cfg:
            cfg.update(extra_cfg)
        servers = await manager.servers_add(num_nodes, config=cfg,
                                            cmdline=cmdline,
                                            property_file=property_file)
        try:
            yield storage_server, servers
        finally:
            # Stop every running Scylla server before the bucket is destroyed.
            # Iterating running_servers() (rather than the initial `servers`
            # list) also covers tests that add more servers within the scope.
            for srv in await manager.running_servers():
                try:
                    await manager.server_stop(srv.server_id, convict=False)
                except Exception as e:
                    # Best effort — server may already be stopped.  Log at
                    # DEBUG so real failures remain observable during incident
                    # triage.
                    logger.debug("server_stop during cluster teardown failed for %s: %s",
                                 srv.server_id, e)


def _cluster_with_storage_factory(kind: str, request, pytestconfig, tmpdir, manager: ManagerClient):
    """Return a partial application of :func:`make_cluster_with_object_storage`
    bound to the fixture-provided ``pytestconfig`` / ``tmpdir`` /
    ``request.node.name`` / ``manager`` / ``kind``.  The returned callable
    only needs the per-call parameters.
    """
    def factory(num_nodes: int,
                extra_cfg: dict | None = None,
                cmdline: list[str] | None = None,
                property_file: list[dict] | dict | None = None,
                configure_endpoints: bool = True):
        """Build a cluster + object-storage async context manager.

        Parameters:
            num_nodes (int): number of Scylla nodes to start.
            extra_cfg (dict | None): extra Scylla config merged on top of the
                base config; test-supplied keys win.
            cmdline (list[str] | None): extra command-line arguments
                forwarded to every started Scylla server.
            property_file (list[dict] | dict | None): forwarded to
                ``manager.servers_add`` -- a per-node list assigns DC/rack
                per node, a single dict applies to every node.
            configure_endpoints (bool): when False, omit
                ``object_storage_endpoints`` from the initial config so the
                test can exercise live endpoint (re)configuration.

        Returns:
            AsyncContextManager[tuple[S3Server | GSServer, list[ServerInfo]]]:
                an async context manager yielding the storage server and the
                list of started Scylla servers.  On exit, every running
                Scylla server is stopped before the bucket is destroyed.
        """
        return make_cluster_with_object_storage(
            manager, kind, num_nodes, pytestconfig, tmpdir, request.node.name,
            extra_cfg, cmdline, property_file, configure_endpoints,
        )
    return factory


@pytest.fixture(scope="function")
def cluster_with_s3_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """Factory fixture: yields a callable that returns an async context manager
    managing both an S3 bucket and a Scylla cluster wired to it.

    Usage::

        async def test_foo(cluster_with_s3_storage):
            async with cluster_with_s3_storage(num_nodes=2, extra_cfg={'a': 1}) as (s3, servers):
                ...
    """
    return _cluster_with_storage_factory('s3', request, pytestconfig, tmpdir, manager)


@pytest.fixture(scope="function")
def cluster_with_gs_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """Factory fixture: same as :func:`cluster_with_s3_storage` but for GCS."""
    return _cluster_with_storage_factory('gs', request, pytestconfig, tmpdir, manager)


@pytest.fixture(scope="function", params=['s3', 'gs'])
def cluster_with_object_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """Factory fixture parametrized over S3 and GCS.

    Same shape as :func:`cluster_with_s3_storage`; tests get one call per flavor.

    Usage::

        async def test_foo(cluster_with_object_storage):
            async with cluster_with_object_storage(num_nodes=1) as (storage, servers):
                ...
    """
    return _cluster_with_storage_factory(request.param, request, pytestconfig, tmpdir, manager)

