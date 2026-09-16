"""Minimal JSON HTTP framework on top of http.server (stdlib only)."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


class ApiError(Exception):
    def __init__(self, status, code, message=""):
        super().__init__(message or code)
        self.status = status
        self.code = code
        self.message = message or code


class App:
    def __init__(self, name):
        self.name = name
        self.routes = []  # list of (method, [pattern segments], handler)

    def route(self, method, pattern):
        def deco(fn):
            segs = [s for s in pattern.strip("/").split("/") if s]
            self.routes.append((method, segs, fn))
            return fn
        return deco

    def _match(self, method, path):
        segs = [s for s in path.strip("/").split("/") if s]
        for m, pat, fn in self.routes:
            if m != method or len(pat) != len(segs):
                continue
            params, ok = {}, True
            for p, s in zip(pat, segs):
                if p.startswith("{") and p.endswith("}"):
                    params[p[1:-1]] = s
                elif p != s:
                    ok = False
                    break
            if ok:
                return fn, params
        return None, None

    def serve(self, host, port):
        app = self

        class Handler(BaseHTTPRequestHandler):
            def _handle(self, method):
                parsed = urlparse(self.path)
                fn, params = app._match(method, parsed.path)
                if fn is None:
                    return self._send(404, {"error": "not_found"})
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                    raw = self.rfile.read(length) if length else b""
                    body = json.loads(raw) if raw else {}
                except Exception:
                    return self._send(400, {"error": "bad_json"})
                try:
                    query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                    status, payload = fn(body=body, params=params, query=query)
                except ApiError as e:
                    return self._send(e.status, {"error": e.code, "message": e.message})
                except Exception as e:  # noqa: BLE001
                    return self._send(500, {"error": "internal", "message": str(e)})
                self._send(status, payload)

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def do_PUT(self):
                self._handle("PUT")

            def _send(self, status, payload):
                data = json.dumps(payload, default=str).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer((host, port), Handler)
        print(f"[{self.name}] listening on {host}:{port}", flush=True)
        server.serve_forever()
