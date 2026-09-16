"""Authorization service: query applications, approvals, revocation,
result binding and provenance verification. Independently deployable.

Application lifecycle:
    PENDING -> RESERVED -> APPROVED -> EXECUTED
                  |          |
                  v          v
        CANCELLED/REVOKED/EXPIRED/FAILED (budget refunded, exactly once)

Rules implemented here (enforced with budget-service state machine):
  - cancel / execution failure / revocation of a non-executed application
    -> refund (idempotent, never twice).
  - committed (executed) consumption is retained; revoke only flips the
    authorization flag, previously issued results stay verifiable because
    verification is pure HMAC over the immutable result record.
  - retry creates a NEW application with version+1 and a NEW reservation;
    the original entry keeps its own terminal state, so no double refund
    and no double spending of the same reservation.
"""
import hashlib
import hmac
import json
import math
import os
import random
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from common.http import App, ApiError
from common.db import DB
from common import client

REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8001")
BUDGET_URL = os.environ.get("BUDGET_URL", "http://localhost:8002")
AUTHZ_SECRET = os.environ.get("AUTHZ_SECRET", "dev-secret-change-me")
RESERVATION_TTL = float(os.environ.get("RESERVATION_TTL_SECONDS", "300"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS applications(
  id TEXT PRIMARY KEY,
  idempotency_key TEXT UNIQUE,
  parent_id TEXT,
  version INTEGER NOT NULL,
  dataset_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  epsilon REAL NOT NULL,
  delta REAL NOT NULL DEFAULT 0,
  mechanism TEXT NOT NULL DEFAULT 'laplace',
  sensitivity REAL NOT NULL,
  query TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('PENDING','RESERVED','APPROVED','EXECUTED','CANCELLED','FAILED',
     'REVOKED','EXPIRED','REJECTED')),
  authorization_revoked INTEGER NOT NULL DEFAULT 0,
  reservation_request_id TEXT,
  status_reason TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS results(
  id TEXT PRIMARY KEY,
  application_id TEXT NOT NULL,
  app_version INTEGER NOT NULL,
  dataset_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  mechanism TEXT NOT NULL,
  epsilon REAL NOT NULL,
  delta REAL NOT NULL,
  sensitivity REAL NOT NULL,
  payload TEXT NOT NULL,
  signature TEXT NOT NULL,
  created_at REAL NOT NULL
);
"""

db = DB(os.environ.get("DB_PATH", "/data/authz.db"), SCHEMA)
app = App("authz")


# ---------------------------------------------------------------- helpers

def now():
    return time.time()


def get_app(app_id):
    r = db.conn().execute(
        "SELECT * FROM applications WHERE id=?", (app_id,)).fetchone()
    if not r:
        raise ApiError(404, "application_not_found", app_id)
    return r


def app_view(r):
    d = {k: r[k] for k in r.keys()}
    d["query"] = json.loads(d["query"])
    d["authorization_revoked"] = bool(d["authorization_revoked"])
    return d


def set_status(app_id, status, reason=None):
    db.conn().execute(
        "UPDATE applications SET status=?, status_reason=?, updated_at=?"
        " WHERE id=?", (status, reason, now(), app_id))


def budget_reserve(app_row):
    status, body = client.call("POST", f"{BUDGET_URL}/reserve", {
        "request_id": app_row["reservation_request_id"],
        "application_id": app_row["id"],
        "dataset_id": app_row["dataset_id"],
        "subject_id": app_row["subject_id"],
        "amount": app_row["epsilon"],
        "ttl_seconds": RESERVATION_TTL,
    })
    return status, body


def budget_refund(request_id, reason):
    return client.call("POST", f"{BUDGET_URL}/refund",
                       {"request_id": request_id, "reason": reason})


def budget_commit(request_id):
    return client.call("POST", f"{BUDGET_URL}/commit",
                       {"request_id": request_id})


def laplace_noise(scale):
    u = random.random() - 0.5
    return -scale * math.copysign(math.log1p(-2.0 * abs(u)), u)


def sign_result(rec):
    canonical = json.dumps(rec, sort_keys=True, separators=(",", ":"))
    return hmac.new(AUTHZ_SECRET.encode(), canonical.encode(),
                    hashlib.sha256).hexdigest()


def result_record(r):
    """Immutable signed content of a result row."""
    return {
        "result_id": r["id"],
        "application_id": r["application_id"],
        "app_version": r["app_version"],
        "dataset_id": r["dataset_id"],
        "subject_id": r["subject_id"],
        "mechanism": r["mechanism"],
        "epsilon": r["epsilon"],
        "delta": r["delta"],
        "sensitivity": r["sensitivity"],
        "payload": json.loads(r["payload"]),
    }


def result_view(r):
    d = result_record(r)
    d["signature"] = r["signature"]
    d["created_at"] = r["created_at"]
    return d


def refund_and_close(app_row, final_status, reason):
    """Idempotent refund path shared by cancel / revoke / failure."""
    status, body = budget_refund(app_row["reservation_request_id"], reason)
    if status not in (200,):
        # 409 already_committed cannot happen for non-executed apps;
        # anything else means the budget service is misbehaving.
        raise ApiError(502, "refund_failed", json.dumps(body))
    set_status(app_row["id"], final_status, reason)


# ---------------------------------------------------------------- endpoints

@app.route("POST", "/applications")
def create_application(body, params, query):
    for k in ("dataset_id", "subject_id", "epsilon", "query"):
        if k not in body:
            raise ApiError(400, "missing_fields", f"missing: {k}")
    idem = body.get("idempotency_key")
    conn = db.conn()
    if idem:
        dup = conn.execute(
            "SELECT * FROM applications WHERE idempotency_key=?",
            (idem,)).fetchone()
        if dup:
            return 200, {"application": app_view(dup),
                         "idempotent_replay": True}

    st, ds = client.call("GET", f"{REGISTRY_URL}/datasets/{body['dataset_id']}")
    if st != 200:
        raise ApiError(400, "unknown_dataset", body["dataset_id"])
    epsilon = float(body["epsilon"])
    if epsilon <= 0 or epsilon > float(ds["max_epsilon_per_request"]):
        raise ApiError(400, "invalid_epsilon",
                       f"epsilon must be in (0, "
                       f"{ds['max_epsilon_per_request']}]")

    app_id = "app-" + uuid.uuid4().hex[:12]
    t = now()
    conn.execute(
        "INSERT INTO applications(id,idempotency_key,parent_id,version,"
        "dataset_id,subject_id,epsilon,delta,mechanism,sensitivity,query,"
        "status,reservation_request_id,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?,?)",
        (app_id, idem, body.get("parent_id"), int(body.get("version", 1)),
         body["dataset_id"], body["subject_id"], epsilon,
         float(body.get("delta", 0)), body.get("mechanism", "laplace"),
         float(body.get("sensitivity", 1.0)), json.dumps(body["query"]),
         f"req-{app_id}", t, t))
    row = get_app(app_id)

    st, resp = budget_reserve(row)
    if st not in (200, 201):
        set_status(app_id, "REJECTED",
                   resp.get("error", "budget_reservation_failed"))
        return 409, {"application": app_view(get_app(app_id)),
                     "budget": resp}
    set_status(app_id, "RESERVED")
    return 201, {"application": app_view(get_app(app_id)),
                 "reservation": resp.get("reservation")}


@app.route("GET", "/applications/{app_id}")
def read_application(body, params, query):
    row = get_app(params["app_id"])
    # Lazy sync with budget-side expiry so a lapsed reservation is visible.
    if row["status"] in ("RESERVED", "APPROVED"):
        st, resp = client.call(
            "GET", f"{BUDGET_URL}/reservations/"
                   f"{row['reservation_request_id']}")
        if st == 200 and resp["reservation"]["state"] == "EXPIRED":
            set_status(row["id"], "EXPIRED", "reservation_ttl_expired")
            row = get_app(params["app_id"])
    return 200, {"application": app_view(row)}


@app.route("GET", "/applications")
def list_applications(body, params, query):
    clauses, vals = [], []
    for k in ("status", "dataset_id", "subject_id"):
        if query.get(k):
            clauses.append(f"{k}=?")
            vals.append(query[k])
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = db.conn().execute(
        f"SELECT * FROM applications{where} ORDER BY created_at DESC"
        " LIMIT 200", vals).fetchall()
    return 200, {"applications": [app_view(r) for r in rows]}


@app.route("POST", "/applications/{app_id}/approve")
def approve(body, params, query):
    row = get_app(params["app_id"])
    if row["status"] != "RESERVED":
        raise ApiError(409, "invalid_state",
                       f"cannot approve from {row['status']}")
    set_status(row["id"], "APPROVED")
    return 200, {"application": app_view(get_app(row["id"]))}


@app.route("POST", "/applications/{app_id}/execute")
def execute(body, params, query):
    row = get_app(params["app_id"])
    if row["status"] != "APPROVED":
        raise ApiError(409, "invalid_state",
                       f"cannot execute from {row['status']}")
    if row["authorization_revoked"]:
        raise ApiError(409, "revoked", "authorization was revoked")
    try:
        if body.get("fail_execution"):  # test hook for the failure path
            raise RuntimeError("simulated executor failure")
        if row["mechanism"] != "laplace":
            raise RuntimeError(f"unsupported mechanism {row['mechanism']}")
        true_value = float(body.get("true_value", 0.0))
        noisy = true_value + laplace_noise(row["sensitivity"] / row["epsilon"])
        payload = {"noisy_value": noisy,
                   "mechanism": "laplace",
                   "query": json.loads(row["query"])}
    except Exception as e:  # execution failed -> refund, exactly once
        refund_and_close(row, "FAILED", f"execution_failed: {e}")
        return 500, {"application": app_view(get_app(row["id"])),
                     "error": "execution_failed", "message": str(e)}

    st, resp = budget_commit(row["reservation_request_id"])
    if st != 200:
        # Budget refused the commit; do not release the result.
        refund_and_close(row, "FAILED", f"commit_failed: {st}")
        return 502, {"application": app_view(get_app(row["id"])),
                     "error": "commit_failed", "budget": resp}

    result_id = "res-" + uuid.uuid4().hex[:12]
    rec = {
        "id": result_id, "application_id": row["id"],
        "app_version": row["version"], "dataset_id": row["dataset_id"],
        "subject_id": row["subject_id"], "mechanism": row["mechanism"],
        "epsilon": row["epsilon"], "delta": row["delta"],
        "sensitivity": row["sensitivity"], "payload": json.dumps(payload),
        "created_at": now(),
    }
    rec["signature"] = sign_result(result_record(rec))
    db.conn().execute(
        "INSERT INTO results(id,application_id,app_version,dataset_id,"
        "subject_id,mechanism,epsilon,delta,sensitivity,payload,signature,"
        "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (rec["id"], rec["application_id"], rec["app_version"],
         rec["dataset_id"], rec["subject_id"], rec["mechanism"],
         rec["epsilon"], rec["delta"], rec["sensitivity"], rec["payload"],
         rec["signature"], rec["created_at"]))
    set_status(row["id"], "EXECUTED")
    return 201, {"application": app_view(get_app(row["id"])),
                 "result": result_view(db.conn().execute(
                     "SELECT * FROM results WHERE id=?",
                     (result_id,)).fetchone())}


@app.route("POST", "/applications/{app_id}/cancel")
def cancel(body, params, query):
    row = get_app(params["app_id"])
    if row["status"] in ("CANCELLED",):
        return 200, {"application": app_view(row), "idempotent_replay": True}
    if row["status"] not in ("PENDING", "RESERVED", "APPROVED"):
        raise ApiError(409, "invalid_state",
                       f"cannot cancel from {row['status']}")
    refund_and_close(row, "CANCELLED", body.get("reason", "cancelled"))
    return 200, {"application": app_view(get_app(row["id"]))}


@app.route("POST", "/applications/{app_id}/revoke")
def revoke(body, params, query):
    row = get_app(params["app_id"])
    if row["status"] in ("PENDING", "RESERVED", "APPROVED"):
        # Not yet executed: stop it and release the budget (idempotent).
        refund_and_close(row, "REVOKED", body.get("reason", "revoked"))
    elif row["status"] == "EXECUTED":
        # Already executed: consumption is retained; only the authorization
        # is flagged. Issued results remain verifiable (HMAC is offline).
        db.conn().execute(
            "UPDATE applications SET authorization_revoked=1, updated_at=?"
            " WHERE id=?", (now(), row["id"]))
    else:
        raise ApiError(409, "invalid_state",
                       f"cannot revoke from {row['status']}")
    return 200, {"application": app_view(get_app(row["id"]))}


@app.route("POST", "/applications/{app_id}/retry")
def retry(body, params, query):
    row = get_app(params["app_id"])
    if row["status"] not in ("FAILED", "CANCELLED", "EXPIRED", "REJECTED",
                             "EXECUTED"):
        raise ApiError(409, "invalid_state",
                       f"cannot retry from {row['status']}")
    # New application, version+1, fresh reservation. The original keeps its
    # terminal state: failed/cancelled were already refunded (once),
    # executed consumption is retained.
    st, resp = create_application({
        "dataset_id": row["dataset_id"], "subject_id": row["subject_id"],
        "epsilon": row["epsilon"], "delta": row["delta"],
        "mechanism": row["mechanism"], "sensitivity": row["sensitivity"],
        "query": json.loads(row["query"]),
        "parent_id": row["id"], "version": row["version"] + 1,
    }, params, query)
    return (201 if st == 201 else st), resp


@app.route("GET", "/results/{result_id}")
def get_result(body, params, query):
    r = db.conn().execute("SELECT * FROM results WHERE id=?",
                          (params["result_id"],)).fetchone()
    if not r:
        raise ApiError(404, "result_not_found", params["result_id"])
    return 200, {"result": result_view(r)}


@app.route("POST", "/results/verify")
def verify_result(body, params, query):
    """Verify provenance of a result. Works regardless of the application's
    current authorization state: the signature covers the immutable record
    (application id, version, noise parameters, payload)."""
    r = db.conn().execute("SELECT * FROM results WHERE id=?",
                          (body.get("result_id"),)).fetchone()
    if not r:
        raise ApiError(404, "result_not_found", body.get("result_id"))
    expected = sign_result(result_record(r))
    valid = hmac.compare_digest(expected, r["signature"])
    if "signature" in body:
        valid = valid and hmac.compare_digest(expected,
                                              str(body["signature"]))
    if "payload" in body:
        valid = valid and (json.dumps(body["payload"], sort_keys=True)
                           == json.dumps(json.loads(r["payload"]),
                                         sort_keys=True))
    return 200, {"valid": valid, "result_id": r["id"],
                 "application_id": r["application_id"],
                 "app_version": r["app_version"]}


@app.route("GET", "/health")
def health(body, params, query):
    return 200, {"status": "ok", "service": "authz"}


if __name__ == "__main__":
    app.serve("0.0.0.0", int(os.environ.get("PORT", "8003")))
