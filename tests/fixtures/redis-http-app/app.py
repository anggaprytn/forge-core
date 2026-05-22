import os
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer


REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
COUNTER_KEY = os.environ.get("COUNTER_KEY", "counter")
PORT = int(os.environ.get("PORT", "3000"))


def redis_command(*parts):
    payload = f"*{len(parts)}\r\n".encode()
    for part in parts:
        value = str(part).encode()
        payload += f"${len(value)}\r\n".encode() + value + b"\r\n"

    with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=5) as conn:
        conn.sendall(payload)
        return read_resp(conn)


def read_line(conn):
    data = bytearray()
    while not data.endswith(b"\r\n"):
        chunk = conn.recv(1)
        if not chunk:
            raise ConnectionError("unexpected EOF from redis")
        data.extend(chunk)
    return bytes(data[:-2])


def read_resp(conn):
    prefix = conn.recv(1)
    if not prefix:
        raise ConnectionError("empty redis response")
    if prefix == b"+":
        return read_line(conn).decode()
    if prefix == b":":
        return int(read_line(conn))
    if prefix == b"$":
        size = int(read_line(conn))
        if size == -1:
            return None
        data = bytearray()
        while len(data) < size:
            data.extend(conn.recv(size - len(data)))
        if conn.recv(2) != b"\r\n":
            raise ConnectionError("invalid bulk string terminator")
        return bytes(data).decode()
    if prefix == b"-":
        raise RuntimeError(read_line(conn).decode())
    raise RuntimeError(f"unsupported redis response prefix: {prefix!r}")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            try:
                result = redis_command("PING")
            except Exception as exc:
                self.respond(503, f"{exc}\n")
                return
            if result != "PONG":
                self.respond(503, "redis ping failed\n")
                return
            self.respond(200, "ok\n")
            return
        if self.path == "/counter":
            try:
                value = redis_command("GET", COUNTER_KEY)
            except Exception as exc:
                self.respond(500, f"{exc}\n")
                return
            self.respond(200, f"{value or '0'}\n")
            return
        if self.path == "/incr":
            try:
                value = redis_command("INCR", COUNTER_KEY)
            except Exception as exc:
                self.respond(500, f"{exc}\n")
                return
            self.respond(200, f"{value}\n")
            return
        self.respond(404, "not found\n")

    def log_message(self, fmt, *args):
        return

    def respond(self, status, body):
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
