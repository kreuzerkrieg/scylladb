#!/usr/bin/python3
#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# S3 proxy server to inject retryable errors for fuzzy testing.

import os
import random
import sys
import asyncio
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


class Policy:
    def __init__(self):
        self.lock = threading.Lock()
        self.should_forward: bool = True
        self.should_fail: bool = False
        self.error_count: int = 0
        self.max_errors: int = random.choice(list(range(1, 5)))


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


# Simple HTTP handler for testing (at this moment) and injecting errors for S3 multipart uploads. It can be easily
# extended to support whatever S3 call is needed by adding any logic to the `do_*` handlers. Also one can extend this
# handler to process additional HTTP methods, for example HEAD.
# In case one have to keep some sort of tracking or state there is shared LRUCache maintained by the MockS3Server class
# (see below). At this moment this LRU is used to track failure policies for the test case by tracking it using the S3
# object name. One can extend it to whatever key to accommodate new needs.
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

    def __init__(self, policies, logger, *args, **kwargs):
        self.minio_uri = "http://127.0.0.1:9000"
        self.policies = policies
        self.logger = logger
        super().__init__(*args, **kwargs)
        self.close_connection = False

    def log_error(self, format, *args):
        if self.logger:
            self.logger.info("%s - - [%s] %s\n" %
                             (self.client_address[0],
                              self.log_date_time_string(),
                              format % args))
        else:
            sys.stderr.write("%s - - [%s] %s\n" %
                             (self.address_string(),
                              self.log_date_time_string(),
                              format % args))

    def log_message(self, format, *args):
        # Just don't be too verbose
        if not self.logger:
            sys.stderr.write("%s - - [%s] %s\n" %
                             (self.address_string(),
                              self.log_date_time_string(),
                              format % args))

    def parsed_qs(self):
        parsed_url = urlparse(self.path)
        query_components = parse_qs(parsed_url.query)
        for key in parsed_url.query.split('&'):
            if '=' not in key:
                query_components[key] = ['']
        return query_components

    def get_policy(self):
        policy = self.policies.get(self.path)
        if policy is None:
            policy = Policy()
            policy.should_forward = true_or_false()
            if policy.should_forward:
                policy.should_fail = true_or_false()
            else:
                policy.should_fail = True
            self.policies.put(self.path, policy)

        return policy

    def get_retryable_http_codes(self):
        return random.choice(self.retryable_codes), random.choice(self.error_names)

    def respond_with_error(self):
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

    def process_request(self):
        try:
            policy = self.get_policy()

            if policy.error_count >= policy.max_errors:
                with policy.lock:
                    policy.should_fail = False
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
                with policy.lock:
                    policy.error_count += 1

                self.respond_with_error()
            else:
                self.send_response(response.status_code)
                for key, value in response.headers.items():
                    # if key.upper() == 'CONTENT-LENGTH' and len(response.content) == 0:
                    if key.upper() != 'CONTENT-LENGTH':
                        self.send_header(key, value)

                if self.command == 'HEAD':
                    self.send_header("Content-Length", response.headers['Content-Length'])
                else:
                    self.send_header("Content-Length", str(len(response.content)))
                self.end_headers()
                if self.command == 'GET':
                    print("Content length from header: " + response.headers['Content-Length'])
                    print("Real content length: " + str(len(response.content)))
                self.wfile.write(response.content)
        except Exception as e:
            print(e)

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


# Mock server to setup `ThreadingHTTPServer` instance with custom request handler (see above), managing requests state
# in the `self.req_states`, adding custom logger, etc. This server will be started automatically from `test.py` once the
# pytest kicks in. In addition, it is possible just to start this server using another script - `start_mock.py` to
# run it locally to provide mocking functionality for tests executed during development process
class S3ProxyServer:
    def __init__(self, host, port, logger=None):
        self.req_states = LRUCache(10000)
        handler = partial(InjectingHandler, self.req_states, logger)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.server_thread = None
        self.server.request_queue_size = 1000
        self.server.timeout = 10000
        self.server.socket.settimeout(10000)
        self.server.socket.listen(1000)
        self.is_running = False
        os.environ['MOCK_S3_SERVER_PORT'] = f'{port}'
        os.environ['MOCK_S3_SERVER_HOST'] = host

    async def start(self):
        if not self.is_running:
            print(f'Starting S3 proxy server on {self.server.server_address}')
            loop = asyncio.get_running_loop()
            self.server_thread = loop.run_in_executor(None, self.server.serve_forever)
            self.is_running = True

    async def stop(self):
        if self.is_running:
            print('Stopping S3 proxy server')
            self.server.shutdown()
            await self.server_thread
            self.is_running = False

    async def run(self):
        try:
            await self.start()
            while self.is_running:
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Server error: {e}")
            await self.stop()
