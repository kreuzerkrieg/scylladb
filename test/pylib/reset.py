import asyncio
import socket
import struct
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
import http.client

class MyHandler(BaseHTTPRequestHandler):
    def handle_one_request(self):
        self.raw_requestline = self.rfile.readline(65537)
        if len(self.raw_requestline) > 65536:
            self.requestline = ''
            self.request_version = ''
            self.command = ''
            self.send_error(414)
            return
        if not self.parse_request():
            return

        self.do_GET()  # or call the appropriate handler based on the command

        # Flush the output stream to ensure the body is sent
        # self.wfile.flush()
        #
        # # Add a brief delay to ensure the response body is sent
        # time.sleep(0.1)
        #
        # # Forcefully close the socket before fully sending
        # self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        # self.connection.close()

    def do_GET(self):
        print("GET request received")
        response_headers = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/html\r\n"
            "Connection: close\r\n"
            "\r\n"
        )
        response_body = "Hello, world!"
        response = response_headers + response_body

        # Send the headers and part of the body
        self.connection.send(response_headers.encode('utf-8'))
        partial_body = response_body[:5]  # Send only part of the body
        self.connection.send(partial_body.encode('utf-8'))

        # Forcefully close the socket
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        self.connection.close()


def run_server():
    server_address = ('', 8080)
    httpd = HTTPServer(server_address, MyHandler)
    print('Starting server on port 8080...')
    httpd.serve_forever()

async def client_request(host, port):
    await asyncio.sleep(1)  # Wait for the server to start
    try:
        # Create an HTTP connection
        conn = http.client.HTTPConnection(host, port)

        # Send an HTTP GET request
        conn.request("GET", "/")

        # Get the response
        response = conn.getresponse()
        print("Server response received:")
        print("Status:", response.status)
        print("Headers:", response.getheaders())
        print("Body:", response.read().decode('utf-8', errors='replace'))

    except socket.error as e:
        print(f"Socket error: {e}")

    finally:
        conn.close()

async def main():
    server_thread = Thread(target=run_server)
    server_thread.start()

    # Run the client request after the server starts
    await client_request('localhost', 8080)

if __name__ == '__main__':
    asyncio.run(main())
