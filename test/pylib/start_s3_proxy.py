#!/usr/bin/python3
import asyncio
import signal
import argparse
from s3_proxy import S3ProxyServer


async def run():
    parser = argparse.ArgumentParser(description="Start S3 proxy server")
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9002)
    args = parser.parse_args()
    server = S3ProxyServer(args.host, args.port)

    print('Starting S3 proxy server')
    await server.start()
    signal.sigwait({signal.SIGINT, signal.SIGTERM})
    print('Stopping S3 proxy server')
    await server.stop()


if __name__ == '__main__':
    asyncio.run(run())
