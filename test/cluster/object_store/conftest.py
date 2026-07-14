#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


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


def pytest_addoption(parser):
    add_s3_options(parser)


@asynccontextmanager
async def make_object_storage(kind, pytestconfig, tmpdir, test_name, manager: ManagerClient | None = None):
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
        # Stop all running Scylla servers before destroying the bucket.
        # Without this, in-flight operations (compaction, tablet migration) may
        # still reference objects in the bucket, causing S3 404s that abort the
        # node.  See SCYLLADB-2471.
        if manager is not None:
            try:
                for srv in await manager.running_servers():
                    await manager.server_stop(srv.server_id, convict=False)
            except Exception:
                pass  # Best effort — servers may already be stopped
        if bucket_created:
            server.destroy_test_bucket()
        await server.stop()


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """Object storage fixture. Depends on manager to stop servers before
    bucket teardown (see SCYLLADB-2471)."""
    async with make_object_storage(request.param, pytestconfig, tmpdir, request.node.name, manager) as server:
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """S3 storage fixture. Depends on manager to stop servers before
    bucket teardown (see SCYLLADB-2471)."""
    async with make_object_storage('s3', pytestconfig, tmpdir, request.node.name, manager) as server:
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

    Yields a ``(storage_server, servers)`` tuple.  On exit the servers are
    stopped and the bucket is destroyed, in that order (see SCYLLADB-2471).

    The base config always enables the ``keyspace-storage-options`` experimental
    feature.  ``object_storage_endpoints`` is injected too unless
    ``configure_endpoints`` is False -- tests that exercise live config update
    of the endpoint want to start the server without it configured.
    ``extra_cfg`` is merged on top (test-supplied keys win).  ``cmdline`` and
    ``property_file`` are forwarded to ``manager.servers_add``.
    """
    async with make_object_storage(kind, pytestconfig, tmpdir, test_name, manager) as storage_server:
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
            # Stop the servers this helper started before make_object_storage
            # destroys the bucket.  make_object_storage also stops any leftover
            # running servers as a safety net, but doing it here keeps the
            # ownership explicit: this helper started them, so it stops them.
            for srv in servers:
                try:
                    await manager.server_stop(srv.server_id, convict=False)
                except Exception:
                    pass  # Best effort — server may already be stopped


def _cluster_with_storage_factory(kind: str, request, pytestconfig, tmpdir, manager: ManagerClient):
    """Return a partial application of make_cluster_with_object_storage that
    only needs the per-call parameters (``num_nodes``, ``extra_cfg`` and the
    optional ``cmdline`` / ``property_file`` forwarded to ``servers_add``)."""
    def factory(num_nodes: int,
                extra_cfg: dict | None = None,
                cmdline: list[str] | None = None,
                property_file: list[dict] | dict | None = None,
                configure_endpoints: bool = True):
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
