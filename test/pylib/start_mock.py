#!/usr/bin/python3
import sys
import signal
import argparse
from s3_server_mock import MockS3Server


def run():
    parser = argparse.ArgumentParser(description="Start a S3 mock server")
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', default='2012')
    args = parser.parse_args()
    server = MockS3Server(args.host, int(args.port))

    print('Starting S3 mock server')
    server.start()
    signal.sigwait({signal.SIGINT,signal.SIGTERM})
    print('Stopping S3 mock server')
    server.stop()
    sys.exit(0)


if __name__ == '__main__':
    run()
