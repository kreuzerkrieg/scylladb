#!/usr/bin/python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

"""Fake GCS server for testing.
   Provides helpers to set up and manage fake GCS for testing.
"""
import argparse
import asyncio
import logging
import os
import pathlib
import random
import shutil
import socket
import tempfile
import time
from asyncio.subprocess import Process
from io import BufferedWriter
from typing import Generator, Optional


class GCSServer:
    ENV_ADDRESS = 'GCS_SERVER_ADDRESS'
    ENV_PORT = 'GCS_SERVER_PORT'

    log_file: BufferedWriter

    def __init__(self, tempdir_base, address, logger):
        self.srv_exe = shutil.which('fake-gcs-server')
        self.address = address
        self.port = None
        tempdir = tempfile.mkdtemp(dir=tempdir_base, prefix="gcs-")
        self.tempdir = pathlib.Path(tempdir)
        # -backend string
        # storage backend (memory or filesystem) (default "filesystem")
        # -cert-location string
        # location for server certificate
        # -cors-headers string
        # comma separated list of headers to add to the CORS allowlist
        # -data string
        # where to load data from (provided that the directory exists)
        # -event.bucket string
        # if not empty, only objects in this bucket will generate trigger events
        # -event.list string
        # comma separated list of events to publish on cloud function URl. Options are: finalize, delete, and metadataUpdate (default "finalize")
        # -event.object-prefix string
        # if not empty, only objects having this prefix will generate trigger events
        # -event.pubsub-project-id string
        # project ID containing the pubsub topic
        # -event.pubsub-topic string
        # pubsub topic name to publish events on
        # -external-url string
        # optional external URL, returned in the Location header for uploads. Defaults to the address where the server is running
        # -filesystem-root string
        # filesystem root (required for the filesystem backend). folder will be created if it doesn't exist (default "/storage")
        # -host string
        # host to bind to (default "0.0.0.0")
        # -location string
        # location for buckets (default "US-CENTRAL1")
        # -log-level string
        # level for logging. Options same as for logrus: trace, debug, info, warn, error, fatal, and panic (default "info")
        # -port uint
        # port to bind to (default 4443)
        # -port-http uint
        # used only when scheme is 'both' as the port to bind http to (default 8000)
        # -private-key-location string
        # location for private key
        # -public-host string
        # Optional URL for public host (default "storage.googleapis.com")
        # -scheme string
        # using 'http' or 'https' or 'both' (default "https")


        self.logger = logger
        self.cmd: Optional[Process] = None
        self.log_filename = (self.tempdir / 'minio').with_suffix(".log")
        self.old_env = dict()
        self.default_config = None


def check_server(self, port):
    s = socket.socket()
    try:
        s.connect((self.address, port))
        return True
    except socket.error:
        return False
    finally:
        s.close()


def log_to_file(self, str):
    self.log_file.write(str.encode())
    self.log_file.write('\n'.encode())
    self.log_file.flush()


def _get_local_ports(self, num_ports: int) -> Generator[int, None, None]:
    with open('/proc/sys/net/ipv4/ip_local_port_range', encoding='ascii') as port_range:
        min_port, max_port = map(int, port_range.read().split())
    for _ in range(num_ports):
        yield random.randint(min_port, max_port)


@staticmethod


def create_conf(address: str, port: int, region: str):
    endpoint = {'name': address,
                'port': port,
                # don't put credentials here. We're exporing env vars, which should
                # be picked up properly by scylla.
                # https://github.com/scylladb/scylla-pkg/issues/3845
                # 'aws_access_key_id': acc_key,
                # 'aws_secret_access_key': secret_key,
                'aws_region': region,
                'iam_role_arn': '',
                'use_https': False
                }
    return [endpoint]


async def _run_server(self, port):
    self.logger.info(f'Starting minio server at {self.address}:{port}')
    cmd = await asyncio.create_subprocess_exec(
        self.srv_exe,
        *['server', '--address', f'{self.address}:{port}', self.rootdir],
        preexec_fn=os.setsid,
        stderr=self.log_file,
        stdout=self.log_file,
    )
    timeout = time.time() + 30
    while time.time() < timeout:
        if cmd.returncode is not None:
            # the minio server exits before it starts to server. maybe the
            # port is used by another server?
            self.logger.info('minio exited with %s', cmd.returncode)
            raise RuntimeError("Failed to start minio server")
        if self.check_server(port):
            self.logger.info('minio is up and running')
            break

        await asyncio.sleep(0.1)

    return cmd


def _set_environ(self):
    self.old_env = dict(os.environ)
    os.environ[self.ENV_ADDRESS] = f'{self.address}'
    os.environ[self.ENV_PORT] = f'{self.port}'


def _get_environs(self):
    return [self.ENV_ADDRESS,
            self.ENV_PORT,
            ]


def get_envs_settings(self):
    return {key: os.environ[key] for key in self._get_environs()}


def _unset_environ(self):
    for env in self._get_environs():
        if value := self.old_env.get(env):
            os.environ[env] = value
        else:
            del os.environ[env]


def print_environ(self):
    msgs = []
    for key in self._get_environs():
        value = os.environ[key]
        msgs.append(f'export {key}={value}')
    print('\n'.join(msgs))


async def start(self):
    if self.srv_exe is None:
        self.logger.error(
            "Minio not installed, get it from https://dl.minio.io/server/minio/release/linux-amd64/minio and put into PATH")
        return

    self.log_file = self.log_filename.open("wb")
    os.mkdir(self.rootdir)

    retries = 42  # just retry a fixed number of times
    for port in self._get_local_ports(retries):
        try:
            self.cmd = await self._run_server(port)
            self.port = port
        except RuntimeError:
            pass
        else:
            break
    else:
        self.logger.error("Failed to start Minio server")
        return

    self._set_environ()


async def stop(self):
    self.logger.info('Killing minio server')
    if not self.cmd:
        return

    # so the test's process environment is not polluted by a test case
    # which launches the MinioServer by itself.
    self._unset_environ()
    try:
        self.cmd.kill()
    except ProcessLookupError:
        pass
    else:
        await self.cmd.wait()
    finally:
        self.logger.info('Killed minio server')
        self.cmd = None
        shutil.rmtree(self.tempdir)


async def main():
    parser = argparse.ArgumentParser(description="Start a GCS server")
    parser.add_argument('--tempdir')
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(suffix='-fake-gcs', dir=args.tempdir) as tempdir:
        if args.tempdir is None:
            print(f'{tempdir=}')
        server = GCSServer(tempdir, args.host, logging.getLogger('fake-gcs'))
        await server.start()
        server.print_environ()
        try:
            _ = input('server started. press any key to stop: ')
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()


if __name__ == '__main__':
    asyncio.run(main())
