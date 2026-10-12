#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Names whoever holds an address when a bind fails with EADDRINUSE (SCYLLADB-5288).

By the time someone reads a CI log, the holder is gone, so the sockets have to be listed at the
moment of the failure.
"""
from __future__ import annotations

import errno
import os
import re
import time
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Optional

import psutil

from test.pylib.host_registry import Host, HostRegistry

MAX_LINES = 50
WILDCARD_ADDRESSES = ('0.0.0.0', '::')

# An owner in another PID namespace shows as 'pid -', and these are then the only hint of what it is.
PORT_ROLES = {
    7000: 'Scylla internode',
    7001: 'Scylla internode TLS',
    8000: 'Scylla Alternator',
    8474: 'toxiproxy',
    9042: 'Scylla CQL',
    9142: 'Scylla CQL TLS',
    9180: 'Scylla Prometheus',
    10000: 'Scylla REST API',
    19042: 'Scylla shard-aware CQL',
    19142: 'Scylla shard-aware CQL TLS',
}

# asyncio: "[Errno 98] error while attempting to bind on address ('127.1.62.5', 7000): address already in use"
ASYNCIO_BIND_ADDRESS = re.compile(r"address \('([^']+)', (\d+)")
# seastar: "posix_listen failed for address 127.1.62.5:7000: Address already in use", IPv6 as [::1]:7000
SEASTAR_BIND_FAILURE = re.compile(r'posix_listen failed for address \[?([^\]\s]+?)\]?:(\d+): Address already in use')


def _clock(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')


def _owner(pid: Optional[int]) -> str:
    if pid is None:
        return 'pid -'
    if pid == os.getpid():
        return f'pid {pid} (this process)'
    try:
        proc = psutil.Process(pid)
        with proc.oneshot():
            parent = 'child of this process' if proc.ppid() == os.getpid() else f'parent {proc.ppid()}'
            return f"pid {pid} (started {time.time() - proc.create_time():.0f}s ago, {parent}) {' '.join(proc.cmdline())}"
    except psutil.Error as exc:
        return f'pid {pid} ({type(exc).__name__})'


def _describe(conn) -> str:
    role = f' [{PORT_ROLES[conn.laddr.port]}]' if conn.laddr.port in PORT_ROLES else ''
    raddr = f'{conn.raddr.ip}:{conn.raddr.port}' if conn.raddr else '-'
    return f'{conn.status} {conn.laddr.ip}:{conn.laddr.port}{role} -> {raddr} {_owner(conn.pid)}'


def _section(title: str, lines: list[str]) -> str:
    lines = sorted(lines)
    more = [f'... {len(lines) - MAX_LINES} more'] if len(lines) > MAX_LINES else []
    return '\n'.join([title, *lines[:MAX_LINES], *more])


def _lease_lines(hosts: Iterable[str]) -> list[str]:
    """Only this process's leases are known: an address bound without a lease, or leased by another run, has none."""
    subnet = HostRegistry.subnet_in_use()
    if subnet is None:
        return []
    registry = HostRegistry()
    lines = []
    for host in sorted(h for h in hosts if h.startswith(f'{subnet}.')):
        leases = registry.leases(Host(host))
        if not leases:
            lines.append(f'{host} was never leased by this process')
        for lease in leases:
            released = f'released {_clock(lease.released_at)}' if lease.released_at is not None else 'still leased'
            lines.append(f'{host} leased {_clock(lease.leased_at)} by {lease.user}, {released}')
    return lines


def describe_port_holders(host: str, port: Optional[int] = None) -> str:
    """Describes who holds host:port: its sockets, the other listeners on its subnet, and its recent leases.

    A host ending in '.' is a prefix and covers a subnet; then every socket on it but ESTABLISHED
    ones is listed, because a bind never conflicts with a client connection's ephemeral port.
    """
    conns = [conn for conn in psutil.net_connections(kind='tcp') if conn.laddr]
    if port is not None:
        target = f'{host}:{port}'
        subnet = host.rsplit('.', 1)[0] + '.'
        holders = [conn for conn in conns if conn.laddr.port == port and conn.laddr.ip in (host, *WILDCARD_ADDRESSES)]
        neighbors = [conn for conn in conns if conn.status == psutil.CONN_LISTEN and conn.laddr.ip.startswith(subnet)
                     and conn not in holders]
        lease_hosts = {host}
    else:
        target = f'{host}*'
        subnet = host
        holders = [conn for conn in conns if conn.laddr.ip.startswith(host) and conn.status != psutil.CONN_ESTABLISHED]
        neighbors = []
        lease_hosts = {conn.laddr.ip for conn in holders}

    if holders:
        parts = [_section(f"TCP sockets holding {target} (pid '-' means no live owner, as in TIME_WAIT, "
                          f"or an owner outside this PID namespace):", [_describe(conn) for conn in holders])]
    else:
        parts = [f'no TCP socket holds {target} now']
    if neighbors:
        parts.append(_section(f'Other listeners on {subnet}*:', [_describe(conn) for conn in neighbors]))
    if leases := _lease_lines(lease_hosts):
        parts.append('Recent HostRegistry leases, oldest first:\n' + '\n'.join(leases))
    return '\n'.join(parts)


def _address_in_use_error(exc: Optional[BaseException]) -> Optional[OSError]:
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, OSError) and exc.errno == errno.EADDRINUSE:
            return exc
        exc = exc.__cause__ or exc.__context__
    return None


def describe_address_in_use(exc: BaseException) -> Optional[str]:
    """Describes the holders of the address when exc, or an exception it was raised from, is EADDRINUSE.

    asyncio puts the address in the message; a plain socket.bind() does not, and then every
    socket on this process's HostRegistry subnet is listed.
    """
    error = _address_in_use_error(exc)
    if error is None:
        return None
    if match := ASYNCIO_BIND_ADDRESS.search(str(error)):
        return describe_port_holders(match.group(1), int(match.group(2)))
    if subnet := HostRegistry.subnet_in_use():
        return describe_port_holders(f'{subnet}.')
    return 'the error does not name the address, and this process leases no HostRegistry subnet'


def describe_scylla_bind_failure(log_path: Path, offset: int = 0) -> Optional[str]:
    """Describes the holders of the address a Scylla node failed to listen on, read from its log past offset."""
    with log_path.open('r', encoding='utf-8', errors='replace') as log:
        log.seek(offset)
        for line in log:
            if match := SEASTAR_BIND_FAILURE.search(line):
                return describe_port_holders(match.group(1), int(match.group(2)))
    return None
