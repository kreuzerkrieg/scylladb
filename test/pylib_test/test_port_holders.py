#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests that an EADDRINUSE failure names whoever holds the address (SCYLLADB-5288)."""
import asyncio
import errno
import os
import socket
import subprocess
import sys
from contextlib import closing

import psutil
import pytest

from test.pylib.host_registry import HostRegistry
from test.pylib.ldap_server import try_something_backoff
from test.pylib.port_holders import describe_address_in_use, describe_port_holders, describe_scylla_bind_failure

pytest_plugins = ["pytester"]


@pytest.fixture
async def host():
    registry = HostRegistry()
    leased = await registry.lease_host()
    yield leased
    await registry.release_host(leased)


@pytest.fixture
def listener(host):
    with closing(socket.socket()) as s:
        s.bind((host, 0))
        s.listen()
        yield s


def time_wait_port(host) -> int:
    """Leaves (host, port) in TIME_WAIT with no live socket on it, the way a server that closes first does."""
    with closing(socket.socket()) as server:
        server.bind((host, 0))
        server.listen()
        port = server.getsockname()[1]
        with closing(socket.create_connection((host, port))):
            accepted, _ = server.accept()
            accepted.close()

    def in_time_wait():
        return any(c.status == psutil.CONN_TIME_WAIT and c.laddr == (host, port) for c in psutil.net_connections(kind='tcp'))
    assert try_something_backoff(in_time_wait), f'{host}:{port} never reached TIME_WAIT'
    return port


def test_names_a_draining_port(host):
    port = time_wait_port(host)
    with closing(socket.socket()) as s, pytest.raises(OSError) as bind_error:
        s.bind((host, port))
    assert bind_error.value.errno == errno.EADDRINUSE
    assert f'TIME_WAIT {host}:{port} -> ' in describe_port_holders(host, port)


def test_names_the_process_of_a_listener(host, listener):
    port = listener.getsockname()[1]
    holders = describe_port_holders(host, port)
    assert f'LISTEN {host}:{port} -> - pid {os.getpid()} (this process)' in holders


def test_names_a_child_process_and_its_age(host):
    child = subprocess.Popen([sys.executable, '-c', f"""
import socket, sys
s = socket.socket(); s.bind(('{host}', 0)); s.listen()
print(s.getsockname()[1], flush=True)
sys.stdin.read()
"""], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        port = int(child.stdout.readline())
        holders = describe_port_holders(host, port)
        assert f'LISTEN {host}:{port} -> - pid {child.pid} (started ' in holders
        assert 's ago, child of this process) ' in holders
    finally:
        child.kill()
        child.wait()


def test_lists_the_other_listeners_on_the_subnet_with_their_roles(host):
    with closing(socket.socket()) as cql:
        cql.bind((host, 9042))
        cql.listen()
        holders = describe_port_holders(host, 7000)
    assert f'no TCP socket holds {host}:7000 now' in holders
    assert f'Other listeners on {HostRegistry.subnet_in_use()}.*:' in holders
    assert f'LISTEN {host}:9042 [Scylla CQL] -> - pid {os.getpid()} (this process)' in holders


def test_names_who_leased_the_address(host):
    holders = describe_port_holders(host, 7000)
    assert f'{host} leased ' in holders
    assert '::test_names_who_leased_the_address' in holders
    assert ' (setup), still leased' in holders


def test_flags_an_address_bound_without_a_lease():
    unleased = f'{HostRegistry.subnet_in_use()}.254'
    with closing(socket.socket()) as s:
        s.bind((unleased, 0))
        s.listen()
        holders = describe_port_holders(unleased, s.getsockname()[1])
    assert f'{unleased} was never leased by this process' in holders


def test_a_bare_bind_error_lists_the_subnet(host, listener):
    port = listener.getsockname()[1]
    with closing(socket.socket()) as s, pytest.raises(OSError) as bind_error:
        s.bind((host, port))
    assert str(bind_error.value) == '[Errno 98] Address already in use'  # names no address
    holders = describe_address_in_use(bind_error.value)
    assert f'TCP sockets holding {HostRegistry.subnet_in_use()}.*' in holders
    assert f'LISTEN {host}:{port} ' in holders


async def test_an_asyncio_bind_error_names_its_port(host, listener):
    port = listener.getsockname()[1]
    with pytest.raises(OSError) as bind_error:
        await asyncio.get_running_loop().create_server(asyncio.Protocol, host, port)
    try:
        raise RuntimeError('wrapped') from bind_error.value
    except RuntimeError as wrapped:
        holders = describe_address_in_use(wrapped)
    assert f'TCP sockets holding {host}:{port} ' in holders
    assert f'LISTEN {host}:{port} ' in holders


def test_other_errors_are_not_described():
    assert describe_address_in_use(OSError(errno.ECONNREFUSED, 'Connection refused')) is None


def test_a_scylla_bind_failure_names_its_port(host, listener, tmp_path):
    port = listener.getsockname()[1]
    log = tmp_path / 'scylla.log'
    old_failure = f'posix_listen failed for address {host}:1: Address already in use\n'
    log.write_text(old_failure + f'ERROR 2026-10-12 18:00:00,000 [shard 0:main] init - Startup failed: std::system_error '
                                 f'(error system:98, posix_listen failed for address {host}:{port}: Address already in use)\n')
    holders = describe_scylla_bind_failure(log, offset=len(old_failure))
    assert f'LISTEN {host}:{port} ' in holders
    assert describe_scylla_bind_failure(log, offset=log.stat().st_size) is None


def test_the_failure_report_carries_the_holders(pytester, host, listener):
    port = listener.getsockname()[1]
    pytester.makeconftest('from test.conftest import pytest_runtest_makereport')
    pytester.makepyfile(f"""
        import asyncio

        async def test_bind():
            await asyncio.get_running_loop().create_server(asyncio.Protocol, '{host}', {port})
    """)
    result = pytester.runpytest('-o', 'asyncio_mode=auto', '-o', 'asyncio_default_fixture_loop_scope=function')
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(['*Address already in use*', f'LISTEN {host}:{port} -> - pid *'])
