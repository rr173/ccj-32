"""Registry service: dataset registration and budget policy source of truth.

Independently deployable. Owns dataset metadata: sensitivity, per-period
epsilon budget, period length, per-request cap, and composition rule.
"""
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from common.http import App, ApiError
from common.db import DB

SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  sensitivity TEXT NOT NULL CHECK(sensitivity IN ('low','medium','high')),
  epsilon_per_period REAL NOT NULL CHECK(epsilon_per_period > 0),
  period_seconds INTEGER NOT NULL CHECK(period_seconds > 0),
  max_epsilon_per_request REAL NOT NULL CHECK(max_epsilon_per_request > 0),
  composition TEXT NOT NULL DEFAULT 'pure' CHECK(composition IN ('pure')),
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
"""

db = DB(os.environ.get("DB_PATH", "/data/registry.db"), SCHEMA)
app = App("registry")


def row_to_dict(r):
    return {k: r[k] for k in r.keys()}


@app.route("POST", "/datasets")
def create_dataset(body, params, query):
    required = ["name", "sensitivity", "epsilon_per_period",
                "period_seconds", "max_epsilon_per_request"]
    missing = [k for k in required if k not in body]
    if missing:
        raise ApiError(400, "missing_fields", f"missing: {', '.join(missing)}")
    if body["sensitivity"] not in ("low", "medium", "high"):
        raise ApiError(400, "bad_sensitivity", "sensitivity must be low|medium|high")
    now = time.time()
    ds_id = "ds-" + uuid.uuid4().hex[:12]
    conn = db.conn()
    try:
        conn.execute(
            "INSERT INTO datasets(id,name,sensitivity,epsilon_per_period,"
            "period_seconds,max_epsilon_per_request,composition,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (ds_id, body["name"], body["sensitivity"],
             float(body["epsilon_per_period"]), int(body["period_seconds"]),
             float(body["max_epsilon_per_request"]),
             body.get("composition", "pure"), now, now))
    except Exception as e:
        if "UNIQUE" in str(e):
            raise ApiError(409, "name_taken", body["name"])
        raise
    return 201, row_to_dict(conn.execute(
        "SELECT * FROM datasets WHERE id=?", (ds_id,)).fetchone())


@app.route("GET", "/datasets")
def list_datasets(body, params, query):
    rows = db.conn().execute("SELECT * FROM datasets ORDER BY created_at").fetchall()
    return 200, {"datasets": [row_to_dict(r) for r in rows]}


@app.route("GET", "/datasets/{ds_id}")
def get_dataset(body, params, query):
    r = db.conn().execute(
        "SELECT * FROM datasets WHERE id=?", (params["ds_id"],)).fetchone()
    if not r:
        raise ApiError(404, "dataset_not_found", params["ds_id"])
    return 200, row_to_dict(r)


@app.route("PUT", "/datasets/{ds_id}")
def update_dataset(body, params, query):
    conn = db.conn()
    r = conn.execute("SELECT * FROM datasets WHERE id=?", (params["ds_id"],)).fetchone()
    if not r:
        raise ApiError(404, "dataset_not_found", params["ds_id"])
    allowed = ("sensitivity", "epsilon_per_period", "period_seconds",
               "max_epsilon_per_request", "composition")
    sets, vals = [], []
    for k in allowed:
        if k in body:
            sets.append(f"{k}=?")
            vals.append(body[k])
    if not sets:
        raise ApiError(400, "nothing_to_update")
    sets.append("updated_at=?")
    vals.append(time.time())
    vals.append(params["ds_id"])
    conn.execute(f"UPDATE datasets SET {', '.join(sets)} WHERE id=?", vals)
    return 200, row_to_dict(conn.execute(
        "SELECT * FROM datasets WHERE id=?", (params["ds_id"],)).fetchone())


@app.route("GET", "/health")
def health(body, params, query):
    return 200, {"status": "ok", "service": "registry"}


if __name__ == "__main__":
    app.serve("0.0.0.0", int(os.environ.get("PORT", "8001")))
