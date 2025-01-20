#!/usr/bin/python3
#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# S3 proxy server to inject retryable errors for fuzzy testing.

import logging
import os
import random
import sys
import socket
import asyncio
import struct

import requests
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import uuid
from functools import partial
from collections import OrderedDict
from requests import Response
from typing_extensions import Optional

sys.path.insert(0, os.path.dirname(__file__))


def checksum(msg):
    s = 0
    for i in range(0, len(msg), 2):
        w = (msg[i] << 8) + (msg[i + 1])
        s = s + w

    s = (s >> 16) + (s & 0xffff)
    s = ~s & 0xffff
    return s


def create_rst_packet(src_ip, dst_ip, src_port, dst_port, seq):
    # IP header fields
    ip_ihl = 5
    ip_ver = 4
    ip_tos = 0
    ip_tot_len = 20 + 20  # IP header + TCP header
    ip_id = 54321
    ip_frag_off = 0
    ip_ttl = 255
    ip_proto = socket.IPPROTO_TCP
    ip_check = 0
    ip_saddr = socket.inet_aton(src_ip)
    ip_daddr = socket.inet_aton(dst_ip)

    ip_ihl_ver = (ip_ver << 4) + ip_ihl

    ip_header = struct.pack('!BBHHHBBH4s4s',
                            ip_ihl_ver, ip_tos, ip_tot_len, ip_id, ip_frag_off, ip_ttl, ip_proto, ip_check, ip_saddr,
                            ip_daddr)

    # TCP header fields
    tcp_seq = seq
    tcp_ack_seq = 0
    tcp_doff = 5
    tcp_flags = 0x04  # RST flag
    tcp_window = socket.htons(5840)
    tcp_check = 0
    tcp_urg_ptr = 0

    tcp_offset_res = (tcp_doff << 4) + 0
    tcp_header = struct.pack('!HHLLBBHHH', src_port, dst_port, tcp_seq, tcp_ack_seq, tcp_offset_res, tcp_flags,
                             tcp_window, tcp_check, tcp_urg_ptr)

    # Pseudo header fields for checksum calculation
    src_addr = socket.inet_aton(src_ip)
    dest_addr = socket.inet_aton(dst_ip)
    placeholder = 0
    protocol = socket.IPPROTO_TCP
    tcp_length = len(tcp_header)

    psh = struct.pack('!4s4sBBH', src_addr, dest_addr, placeholder, protocol, tcp_length)
    psh = psh + tcp_header

    tcp_check = checksum(psh)
    tcp_header = struct.pack('!HHLLBBH',
                             src_port, dst_port, tcp_seq, tcp_ack_seq, tcp_offset_res, tcp_flags,
                             tcp_window) + struct.pack('H', tcp_check) + struct.pack('!H', tcp_urg_ptr)

    packet = ip_header + tcp_header
    return packet


class Policy:
    def __init__(self, max_retries: int):
        self.should_forward: bool = True
        self.should_fail: bool = False
        self.server_should_fail: bool = False
        self.error_count: int = 0
        self.max_errors: int = random.choice(list(range(1, max_retries)))


class LRUCache:
    lock = threading.Lock()

    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str) -> Optional[Policy]:
        with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: Policy) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)


# Simple proxy between s3 client and minio to randomly inject errors and simulate cases when the request succeeds but the wire got "broken"
def true_or_false():
    return random.choice([True, False])


class InjectingHandler(BaseHTTPRequestHandler):
    retryable_codes = list((408, 419, 429, 440)) + list(range(500, 599))
    error_names = list(("InternalFailureException",
                        "InternalFailure",
                        "InternalServerError",
                        "InternalError",
                        "RequestExpiredException",
                        "RequestExpired",
                        "ServiceUnavailableException",
                        "ServiceUnavailableError",
                        "ServiceUnavailable",
                        "RequestThrottledException",
                        "RequestThrottled",
                        "ThrottlingException",
                        "ThrottledException",
                        "Throttling",
                        "SlowDownException",
                        "SlowDown",
                        "RequestTimeTooSkewedException",
                        "RequestTimeTooSkewed",
                        "RequestTimeoutException",
                        "RequestTimeout"))

    def __init__(self, policies, logger, minio_uri, max_retries, *args, **kwargs):
        self.minio_uri = minio_uri
        self.policies = policies
        self.logger = logger
        self.max_retries = max_retries
        super().__init__(*args, **kwargs)
        self.close_connection = False

    def log_message(self, format, *args):
        if not self.logger.isEnabledFor(logging.INFO):
            return
        self.logger.info("%s - - [%s] %s",
                         self.client_address[0],
                         self.log_date_time_string(),
                         format % args)

    def parsed_qs(self):
        parsed_url = urlparse(self.path)
        query_components = parse_qs(parsed_url.query)
        for key in parsed_url.query.split('&'):
            if '=' not in key:
                query_components[key] = ['']
        return query_components

    def get_policy(self):
        policy = self.policies.get(f"{self.command}_{self.path}")
        if policy is None:
            policy = Policy(self.max_retries)
            policy.should_forward = true_or_false()
            if policy.should_forward:
                policy.should_fail = true_or_false()
            else:
                policy.should_fail = True

            if policy.should_fail:
                policy.server_should_fail = true_or_false()
            self.policies.put(f"{self.command}_{self.path}", policy)

        # Unfortunately MPU completion retry on already completed upload would introduce flakiness to unit tests, for example `s3_test`
        if self.command == "POST" and "uploadId" in self.parsed_qs():
            policy.should_forward = not policy.should_fail

        return policy

    def get_retryable_http_codes(self):
        return random.choice(self.retryable_codes), random.choice(self.error_names)

    def reset_server_connection(self):
        # Parameters for the RST packet
        src_ip, src_port = self.server.socket.getsockname()
        dst_ip, dst_port = self.wfile._sock.getpeername()
        print (f"src ip: {src_ip}, src port: {src_port}, dst ip: {dst_ip}, dst port: {dst_port}")
        seq = 1000

        # Create the packet
        rst_packet = create_rst_packet(src_ip, dst_ip, src_port, dst_port, seq)
        self.wfile._sock.sendto(rst_packet, (dst_ip, dst_port))

    def respond_with_error(self, reset_connection: bool):
        # if reset_connection:
        #     try:
        #         # Forcefully close the connection to simulate a connection reset
        #         self.request.shutdown_request()
        #     except OSError:
        #         pass
        #     finally:
        #         return
        code, error_name = self.get_retryable_http_codes()
        self.send_response(code)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        req_uuid = str(uuid.uuid4())
        response = f"""<?xml version="1.0" encoding="UTF-8"?>

                                <Error>
                                    <Code>{error_name}</Code>
                                    <Message>Minio proxy injected error. Client should retry.</Message>
                                    <RequestId>{req_uuid}</RequestId>
                                    <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                </Error>""".encode('utf-8')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)
        if reset_connection:
            self.reset_server_connection()
            # self.server.shutdown_request(self.request)
            # packet, _socket = self.request.get_request()
            # self.wfile._sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            # self.wfile._sock.close()
            # self.server.socket.send()
            # self.server.socket = socket.socket(self.server.address_family,
            #                                    self.server.socket_type)
            # self.server.server_bind()

            # self.server.socket.shutdown(2)
            # self.server.shutdown_request(self.request)
            # self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Partial write.")

    def process_request(self):
        try:
            policy = self.get_policy()

            if policy.error_count >= policy.max_errors:
                policy.should_fail = False
                policy.server_should_fail = False
                policy.should_forward = True

            response = Response()
            body = None

            content_length = self.headers['Content-Length']
            if content_length:
                body = self.rfile.read(int(content_length))

            if policy.should_forward:
                target_url = self.minio_uri + self.path
                headers = {key: value for key, value in self.headers.items()}
                response = requests.request(self.command, target_url, headers=headers, data=body)

            if policy.should_fail:
                policy.error_count += 1
                self.respond_with_error(reset_connection=policy.server_should_fail)
            else:
                self.send_response(response.status_code)
                for key, value in response.headers.items():
                    if key.upper() != 'CONTENT-LENGTH':
                        self.send_header(key, value)

                if self.command == 'HEAD':
                    self.send_header("Content-Length", response.headers['Content-Length'])
                else:
                    self.send_header("Content-Length", str(len(response.content)))
                self.end_headers()
                self.wfile.write(response.content)
        except Exception as e:
            self.logger.error("%s", e)

    def do_GET(self):
        self.process_request()

    def do_POST(self):
        self.process_request()

    def do_PUT(self):
        self.process_request()

    def do_DELETE(self):
        self.process_request()

    def do_HEAD(self):
        self.process_request()


# Proxy server to setup `ThreadingHTTPServer` instance with custom request handler (see above), managing requests state
# in the `self.req_states`, adding custom logger, etc. This server will be started automatically from `test.py`. In
# addition, it is possible just to start this server using another script - `start_s3_proxy.py` to run it locally to
# provide proxy between tests and minio
class S3ProxyServer:
    def __init__(self, host: str, port: int, minio_uri: str, max_retries: int, seed: int, logger):
        self.logger = logger
        self.logger.info('Setting minio proxy random seed to %s', seed)
        random.seed(seed)
        self.req_states = LRUCache(10000)
        handler = partial(InjectingHandler, self.req_states, logger, minio_uri, max_retries)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.server_thread = None
        self.server.request_queue_size = 1000
        self.server.timeout = 10000
        self.server.socket.settimeout(10000)
        self.server.socket.listen(1000)
        self.is_running = False
        os.environ['PROXY_S3_SERVER_PORT'] = f'{port}'
        os.environ['PROXY_S3_SERVER_HOST'] = host

    async def start(self):
        if not self.is_running:
            self.logger.info('Starting S3 proxy server on %s', self.server.server_address)
            loop = asyncio.get_running_loop()
            self.server_thread = loop.run_in_executor(None, self.server.serve_forever)
            self.is_running = True

    async def stop(self):
        if self.is_running:
            self.logger.info('Stopping S3 proxy server')
            self.server.shutdown()
            await self.server_thread
            self.is_running = False

    async def run(self):
        try:
            await self.start()
            while self.is_running:
                await asyncio.sleep(1)
        except Exception as e:
            self.logger.error("Server error: %s", e)
            await self.stop()
