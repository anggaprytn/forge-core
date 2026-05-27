import os
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


PORT = int(os.environ.get("PORT", "3000"))
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")


def redis_endpoint():
    parsed = urlparse(REDIS_URL)
    host = parsed.hostname or "redis"
    port = parsed.port or 6379
    return host, port


def redis_ping():
    host, port = redis_endpoint()
    payload = b"*1\r\n$4\r\nPING\r\n"
    with socket.create_connection((host, port), timeout=2) as conn:
        conn.sendall(payload)
        response = conn.recv(16)
    return response.startswith(b"+PONG")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            try:
                healthy = redis_ping()
            except Exception as exc:
                self.respond(503, f"redis unavailable: {exc}\n")
                return
            self.respond(200 if healthy else 503, "ok\n" if healthy else "redis ping failed\n")
            return
        self.respond(200, "Forge compose app+redis fixture\n")

    def log_message(self, fmt, *args):
        return

    def respond(self, status, body):
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
