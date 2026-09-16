"""Tiny inter-service HTTP client (stdlib only)."""
import json
import urllib.error
import urllib.request


class ServiceUnavailable(Exception):
    pass


def call(method, url, payload=None, timeout=10):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else {})
        except Exception:
            return e.code, {"error": "bad_response"}
    except urllib.error.URLError as e:
        raise ServiceUnavailable(f"{url}: {e.reason}")
