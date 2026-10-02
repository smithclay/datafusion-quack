#!/usr/bin/env python3
"""Recording proxy for the Quack protocol.

Listens on --listen, forwards every request to --upstream, and writes each
exchange to --out as a pair of files: NNNNN.req (the POST body) and NNNNN.resp
(the response body). The captures feed the golden wire fixtures (gate G2) and
the per-client SQL replay lists (gate G3).

    record_proxy.py --listen 127.0.0.1:9600 --upstream 127.0.0.1:9494 --out capture/
"""

import argparse
import http.client
import itertools
import os
import socketserver
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

counter = itertools.count(1)
counter_lock = threading.Lock()


def make_handler(upstream_host, upstream_port, out_dir):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def _forward(self, method):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            conn = http.client.HTTPConnection(upstream_host, upstream_port, timeout=600)
            headers = {
                k: v
                for k, v in self.headers.items()
                if k.lower() not in ("host", "content-length", "connection")
            }
            conn.request(method, self.path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            if method == "POST":
                with counter_lock:
                    n = next(counter)
                with open(os.path.join(out_dir, f"{n:05d}.req"), "wb") as f:
                    f.write(body)
                with open(os.path.join(out_dir, f"{n:05d}.resp"), "wb") as f:
                    f.write(data)
            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() not in ("content-length", "transfer-encoding", "connection"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            conn.close()

        def do_POST(self):
            self._forward("POST")

        def do_GET(self):
            self._forward("GET")

        def do_OPTIONS(self):
            self._forward("OPTIONS")

    return Handler


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256

    def server_bind(self):
        # HTTPServer.server_bind resolves the host's FQDN, which can hang on DNS.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)
    host, port = args.listen.rsplit(":", 1)
    up_host, up_port = args.upstream.rsplit(":", 1)
    server = Server((host, int(port)), make_handler(up_host, int(up_port), args.out))
    server.serve_forever()


if __name__ == "__main__":
    main()
