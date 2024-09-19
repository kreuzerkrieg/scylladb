#!/usr/bin/python3
#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

# Mock S3 server to inject errors for testing.

import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import uuid
from enum import Enum
from functools import partial
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(__file__))


class Policy(Enum):
    SUCCESS = 0
    RETRYABLE_FAILURE = 1
    NONRETRYABLE_FAILURE = 2


class LRUCache:
    lock = threading.Lock()

    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str) -> Policy:
        with self.lock:
            if key not in self.cache:
                return Policy.SUCCESS
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: Policy) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)


class InjectingHandler(BaseHTTPRequestHandler):

    def __init__(self, policies, logger, *args, **kwargs):
        self.policies = policies
        self.logger = logger
        super().__init__(*args, **kwargs)

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

    def do_POST(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        response = self.build_POST_reponse(self.parsed_qs(), urlparse(self.path).path).encode('utf-8')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def do_PUT(self):
        content_length = int(self.headers['Content-Length'])
        put_data = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', '0')
        query_components = self.parsed_qs()
        if 'Key' in query_components and 'Policy' in query_components:
            self.policies.put(query_components['Key'][0], Policy(int(query_components['Policy'][0])))
        elif 'uploadId' in query_components and 'partNumber' in query_components:
            self.send_header('ETag', "SomeTag_" + query_components.get("partNumber")[0])
        else:
            self.send_header('ETag', "SomeTag")
        self.end_headers()

    def do_DELETE(self):
        query_components = self.parsed_qs()
        if 'uploadId' in query_components and self.policies.get(
                query_components.get("uploadId")[0]) == Policy.NONRETRYABLE_FAILURE:
            self.send_response(404)
        else:
            self.send_response(204)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', '0')
        self.end_headers()

    def build_POST_reponse(self, query, path):
        if 'uploads' in query:
            req_uuid = str(uuid.uuid4())
            return f"""<InitiateMultipartUploadResult>
                            <Bucket>bucket</Bucket>
                            <Key>key</Key>
                            <UploadId>{req_uuid}</UploadId>
                        </InitiateMultipartUploadResult>"""
        if 'uploadId' in query:
            match self.policies.get(path):
                case Policy.SUCCESS:
                    return """<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                    <Location>http://Example-Bucket.s3.Region.amazonaws.com/Example-Object</Location>
                                    <Bucket>Example-Bucket</Bucket>
                                    <Key>Example-Object</Key>
                                    <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
                                </CompleteMultipartUploadResult>"""
                case Policy.RETRYABLE_FAILURE:
                    # should succeed on retry
                    self.policies.put(path, Policy.SUCCESS)
                    return """<?xml version="1.0" encoding="UTF-8"?>
                    
                                <Error>
                                    <Code>InternalError</Code>
                                    <Message>We encountered an internal error. Please try again.</Message>
                                    <RequestId>656c76696e6727732072657175657374</RequestId>
                                    <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                </Error>"""
                case Policy.NONRETRYABLE_FAILURE:
                    return """<?xml version="1.0" encoding="UTF-8"?>
                                
                                <Error>
                                    <Code>InvalidAction</Code>
                                    <Message>Something went terribly wrong</Message>
                                    <RequestId>656c76696e6727732072657175657374</RequestId>
                                    <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                </Error>"""
                case _:
                    raise ValueError("Unknown policy")


class MockS3Server:
    def __init__(self, host, port, logger=None):
        self.req_states = LRUCache(10000)
        handler = partial(InjectingHandler, self.req_states, logger)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.thread = None
        os.environ['MOCK_S3_SERVER_PORT'] = f'{port}'
        os.environ['MOCK_S3_SERVER_HOST'] = host

    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self):
        self.server.shutdown()

    def run(self):
        try:
            self.server.serve_forever()
        except RuntimeError as e:
            self.server.shutdown()
            print(e)
