"""Budget service: privacy budget accounting. Independently deployable.

Responsibilities:
  - Atomic reservation of budget (BEGIN IMMEDIATE + conditional UPDATE).
  - Composition: pure-DP accounting, epsilons of a subject sum within a period.
  - Idempotent commit / refund / expire with a single state machine:
        RESERVED -> COMMITTED | REFUNDED | EXPIRED
    Every transition is guarded by `UPDATE ... WHERE state='RESERVED'` and
    rowcount checks, so no refund/release can happen twice.
  - Period rollover: unconsumed reservations of the closed period are EXPIRED
    (released, not carried over); consumption never migrates across periods.
  - Full audit ledger: RESERVE / COMMIT / REFUND / EXPIRE / REJECT /
    CONFLICT / OPEN / ROLLOVER.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from common.http import App, ApiError
from common.db import DB
from common import client

REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8001")
POLICY_CACHE_TTL = float(os.environ.get("POLICY_CACHE_TTL", "30"))
EPS = 1e-9

SCHEMA = """
CREATE TABLE IF NOT EXISTS policies(
  dataset_id TEXT PRIMARY KEY,
  epsilon_per_period REAL NOT NULL,
  period_seconds INTEGER NOT NULL,
  max_epsilon_per_request REAL NOT NULL,
  composition TEXT NOT NULL DEFAULT 'pure',
  fetched_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS accounts(
  dataset_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  period_index INTEGER NOT NULL,
  period_start REAL NOT NULL,
  period_end REAL NOT NULL,
  budget_total REAL NOT NULL,
  consumed REAL NOT NULL DEFAULT 0,
  reserved REAL NOT NULL DEFAULT 0,
  closed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(dataset_id, subject_id, period_index)
);
CREATE TABLE IF NOT EXISTS reservations(
  request_id TEXT PRIMARY KEY,
  application_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  period_index INTEGER NOT NULL,
  amount REAL NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('RESERVED','COMMITTED','REFUNDED','EXPIRED')),
  expires_at REAL NOT NULL,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_res_acct
  ON reservations(dataset_id, subject_id, period_index, state);
CREATE TABLE IF NOT EXISTS ledger(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts REAL NOT NULL,
  op TEXT NOT NULL,
  dataset_id TEXT, subject_id TEXT, period_index INTEGER,
  application_id TEXT, request_id TEXT,
  amount REAL, reason TEXT, balance_after TEXT
);
"""

db = DB(os.environ.get("DB_PATH", "/data/budget.db"), SCHEMA)
app = App("budget")


# ---------------------------------------------------------------- helpers

def get_policy(dataset_id):
    conn = db.conn()
    row = conn.execute(
        "SELECT * FROM policies WHERE dataset_id=?", (dataset_id,)).fetchone()
    if row and time.time() - row["fetched_at"] < POLICY_CACHE_TTL:
        return dict(row)
    status, body = client.call("GET", f"{REGISTRY_URL}/datasets/{dataset_id}")
    if status != 200:
        raise ApiError(400, "unknown_dataset",
                       f"registry has no dataset {dataset_id}")
    conn.execute(
        "INSERT INTO policies(dataset_id,epsilon_per_period,period_seconds,"
        "max_epsilon_per_request,composition,fetched_at) VALUES(?,?,?,?,?,?)"
        " ON CONFLICT(dataset_id) DO UPDATE SET"
        " epsilon_per_period=excluded.epsilon_per_period,"
        " period_seconds=excluded.period_seconds,"
        " max_epsilon_per_request=excluded.max_epsilon_per_request,"
        " composition=excluded.composition, fetched_at=excluded.fetched_at",
        (dataset_id, float(body["epsilon_per_period"]),
         int(body["period_seconds"]), float(body["max_epsilon_per_request"]),
         body.get("composition", "pure"), time.time()))
    return dict(conn.execute(
        "SELECT * FROM policies WHERE dataset_id=?", (dataset_id,)).fetchone())


def log(conn, op, dataset_id=None, subject_id=None, period_index=None,
        application_id=None, request_id=None, amount=None, reason=None,
        balance=None):
    conn.execute(
        "INSERT INTO ledger(ts,op,dataset_id,subject_id,period_index,"
        "application_id,request_id,amount,reason,balance_after)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (time.time(), op, dataset_id, subject_id, period_index,
         application_id, request_id, amount, reason,
         json.dumps(balance) if balance is not None else None))


def balance_of(acct):
    return {
        "consumed": round(acct["consumed"], 9),
        "reserved": round(acct["reserved"], 9),
        "remaining": round(acct["budget_total"] - acct["consumed"]
                           - acct["reserved"], 9),
    }


def expire_reservation_locked(conn, res, reason):
    """Expire one reservation. Caller holds the write lock. Returns True if
    this call performed the transition (exactly-once semantics)."""
    cur = conn.execute(
        "UPDATE reservations SET state='EXPIRED'"
        " WHERE request_id=? AND state='RESERVED'", (res["request_id"],))
    if cur.rowcount != 1:
        return False
    conn.execute(
        "UPDATE accounts SET reserved = MAX(0, reserved - ?)"
        " WHERE dataset_id=? AND subject_id=? AND period_index=?",
        (res["amount"], res["dataset_id"], res["subject_id"],
         res["period_index"]))
    acct = conn.execute(
        "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
        " AND period_index=?",
        (res["dataset_id"], res["subject_id"], res["period_index"])).fetchone()
    log(conn, "EXPIRE", res["dataset_id"], res["subject_id"],
        res["period_index"], res["application_id"], res["request_id"],
        res["amount"], reason, balance_of(acct) if acct else None)
    return True


def expire_due_locked(conn, now):
    rows = conn.execute(
        "SELECT * FROM reservations WHERE state='RESERVED' AND expires_at<=?",
        (now,)).fetchall()
    for r in rows:
        expire_reservation_locked(conn, r, "ttl_expired")


def get_or_create_account_locked(conn, policy, dataset_id, subject_id, now):
    """Return the open account row, rolling the period forward if needed.
    Caller holds the write lock."""
    row = conn.execute(
        "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
        " ORDER BY period_index DESC LIMIT 1",
        (dataset_id, subject_id)).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO accounts(dataset_id,subject_id,period_index,"
            "period_start,period_end,budget_total) VALUES(?,?,?,?,?,?)",
            (dataset_id, subject_id, 0, now,
             now + policy["period_seconds"], policy["epsilon_per_period"]))
        log(conn, "OPEN", dataset_id, subject_id, 0,
            amount=policy["epsilon_per_period"], reason="first_period",
            balance={"consumed": 0, "reserved": 0,
                     "remaining": policy["epsilon_per_period"]})
        return conn.execute(
            "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
            " AND period_index=0", (dataset_id, subject_id)).fetchone()
    if now < row["period_end"] and not row["closed"]:
        return row
    # ---- period rollover ------------------------------------------------
    conn.execute(
        "UPDATE accounts SET closed=1 WHERE dataset_id=? AND subject_id=?"
        " AND period_index=?",
        (dataset_id, subject_id, row["period_index"]))
    # Unconsumed reservations of the closing period expire: released here,
    # never carried over, and later refunds for them become idempotent no-ops.
    pending = conn.execute(
        "SELECT * FROM reservations WHERE dataset_id=? AND subject_id=?"
        " AND period_index=? AND state='RESERVED'",
        (dataset_id, subject_id, row["period_index"])).fetchall()
    for r in pending:
        expire_reservation_locked(conn, r, "period_closed")
    # Advance whole periods until the new window covers `now`.
    index = row["period_index"]
    start, end = row["period_start"], row["period_end"]
    while end <= now:
        index += 1
        start, end = end, end + policy["period_seconds"]
    conn.execute(
        "INSERT INTO accounts(dataset_id,subject_id,period_index,"
        "period_start,period_end,budget_total) VALUES(?,?,?,?,?,?)",
        (dataset_id, subject_id, index, start, end,
         policy["epsilon_per_period"]))
    log(conn, "ROLLOVER", dataset_id, subject_id, index,
        amount=policy["epsilon_per_period"],
        reason=f"period {row['period_index']} closed; "
               f"{len(pending)} reservation(s) expired",
        balance={"consumed": 0, "reserved": 0,
                 "remaining": policy["epsilon_per_period"]})
    return conn.execute(
        "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
        " AND period_index=?", (dataset_id, subject_id, index)).fetchone()


def reservation_view(r):
    return {k: r[k] for k in
            ("request_id", "application_id", "dataset_id", "subject_id",
             "period_index", "amount", "state", "expires_at", "created_at")}


# ---------------------------------------------------------------- endpoints

@app.route("POST", "/reserve")
def reserve(body, params, query):
    for k in ("request_id", "application_id", "dataset_id", "subject_id",
              "amount"):
        if k not in body:
            raise ApiError(400, "missing_fields", f"missing: {k}")
    request_id = body["request_id"]
    amount = float(body["amount"])
    ttl = float(body.get("ttl_seconds", 300))
    policy = get_policy(body["dataset_id"])  # network call, outside the txn

    conn = db.conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = time.time()
        existing = conn.execute(
            "SELECT * FROM reservations WHERE request_id=?",
            (request_id,)).fetchone()
        if existing:  # idempotent replay of the same request
            conn.execute("COMMIT")
            code = 200 if existing["state"] == "RESERVED" else 409
            return code, {"reservation": reservation_view(existing),
                          "idempotent_replay": True}

        if amount <= 0 or amount > policy["max_epsilon_per_request"] + EPS:
            log(conn, "REJECT", body["dataset_id"], body["subject_id"],
                None, body["application_id"], request_id, amount,
                f"amount {amount} outside (0, "
                f"{policy['max_epsilon_per_request']}]")
            conn.execute("COMMIT")
            raise ApiError(400, "invalid_amount",
                           f"amount must be in (0, "
                           f"{policy['max_epsilon_per_request']}]")

        expire_due_locked(conn, now)
        acct = get_or_create_account_locked(
            conn, policy, body["dataset_id"], body["subject_id"], now)

        if acct["consumed"] + acct["reserved"] + amount \
                > acct["budget_total"] + EPS:
            log(conn, "CONFLICT", acct["dataset_id"], acct["subject_id"],
                acct["period_index"], body["application_id"], request_id,
                amount, "insufficient_budget", balance_of(acct))
            conn.execute("COMMIT")
            return 409, {"error": "insufficient_budget",
                         "balance": balance_of(acct)}

        # Conditional update: even if the lock were somehow bypassed, the
        # overspend cannot commit.
        cur = conn.execute(
            "UPDATE accounts SET reserved = reserved + ?"
            " WHERE dataset_id=? AND subject_id=? AND period_index=?"
            " AND consumed + reserved + ? <= budget_total",
            (amount, acct["dataset_id"], acct["subject_id"],
             acct["period_index"], amount))
        if cur.rowcount != 1:
            log(conn, "CONFLICT", acct["dataset_id"], acct["subject_id"],
                acct["period_index"], body["application_id"], request_id,
                amount, "concurrent_conflict", balance_of(acct))
            conn.execute("COMMIT")
            return 409, {"error": "insufficient_budget",
                         "balance": balance_of(acct)}

        expires_at = min(now + ttl, acct["period_end"])
        conn.execute(
            "INSERT INTO reservations(request_id,application_id,dataset_id,"
            "subject_id,period_index,amount,state,expires_at,created_at)"
            " VALUES(?,?,?,?,?,?,'RESERVED',?,?)",
            (request_id, body["application_id"], acct["dataset_id"],
             acct["subject_id"], acct["period_index"], amount,
             expires_at, now))
        acct = conn.execute(
            "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
            " AND period_index=?",
            (acct["dataset_id"], acct["subject_id"],
             acct["period_index"])).fetchone()
        log(conn, "RESERVE", acct["dataset_id"], acct["subject_id"],
            acct["period_index"], body["application_id"], request_id,
            amount, None, balance_of(acct))
        conn.execute("COMMIT")
        res = conn.execute(
            "SELECT * FROM reservations WHERE request_id=?",
            (request_id,)).fetchone()
        return 201, {"reservation": reservation_view(res),
                     "balance": balance_of(acct)}
    except ApiError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise


@app.route("POST", "/commit")
def commit(body, params, query):
    request_id = body.get("request_id")
    if not request_id:
        raise ApiError(400, "missing_fields", "missing: request_id")
    conn = db.conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        res = conn.execute(
            "SELECT * FROM reservations WHERE request_id=?",
            (request_id,)).fetchone()
        if not res:
            conn.execute("COMMIT")
            raise ApiError(404, "reservation_not_found", request_id)
        if res["state"] == "COMMITTED":  # idempotent
            conn.execute("COMMIT")
            return 200, {"reservation": reservation_view(res),
                         "idempotent_replay": True}
        if res["state"] != "RESERVED":
            conn.execute("COMMIT")
            raise ApiError(409, "invalid_state",
                           f"reservation is {res['state']}")
        cur = conn.execute(
            "UPDATE accounts SET reserved = reserved - ?,"
            " consumed = consumed + ?"
            " WHERE dataset_id=? AND subject_id=? AND period_index=?"
            " AND reserved >= ?",
            (res["amount"], res["amount"], res["dataset_id"],
             res["subject_id"], res["period_index"], res["amount"]))
        if cur.rowcount != 1:
            conn.execute("ROLLBACK")
            raise ApiError(500, "account_inconsistent",
                           "reserved balance went negative")
        conn.execute(
            "UPDATE reservations SET state='COMMITTED' WHERE request_id=?"
            " AND state='RESERVED'", (request_id,))
        acct = conn.execute(
            "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
            " AND period_index=?",
            (res["dataset_id"], res["subject_id"],
             res["period_index"])).fetchone()
        log(conn, "COMMIT", res["dataset_id"], res["subject_id"],
            res["period_index"], res["application_id"], request_id,
            res["amount"], None, balance_of(acct))
        conn.execute("COMMIT")
        res = conn.execute(
            "SELECT * FROM reservations WHERE request_id=?",
            (request_id,)).fetchone()
        return 200, {"reservation": reservation_view(res),
                     "balance": balance_of(acct)}
    except ApiError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise


@app.route("POST", "/refund")
def refund(body, params, query):
    request_id = body.get("request_id")
    reason = body.get("reason", "unspecified")
    if not request_id:
        raise ApiError(400, "missing_fields", "missing: request_id")
    conn = db.conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        res = conn.execute(
            "SELECT * FROM reservations WHERE request_id=?",
            (request_id,)).fetchone()
        if not res:
            conn.execute("COMMIT")
            raise ApiError(404, "reservation_not_found", request_id)
        # Idempotent: already released (refunded or expired) -> success no-op.
        if res["state"] in ("REFUNDED", "EXPIRED"):
            conn.execute("COMMIT")
            return 200, {"reservation": reservation_view(res),
                         "idempotent_replay": True}
        # Committed consumption is retained, never refunded.
        if res["state"] == "COMMITTED":
            conn.execute("COMMIT")
            raise ApiError(409, "already_committed",
                           "committed consumption is retained")
        cur = conn.execute(
            "UPDATE accounts SET reserved = reserved - ?"
            " WHERE dataset_id=? AND subject_id=? AND period_index=?"
            " AND reserved >= ?",
            (res["amount"], res["dataset_id"], res["subject_id"],
             res["period_index"], res["amount"]))
        if cur.rowcount != 1:
            conn.execute("ROLLBACK")
            raise ApiError(500, "account_inconsistent",
                           "reserved balance went negative")
        conn.execute(
            "UPDATE reservations SET state='REFUNDED' WHERE request_id=?"
            " AND state='RESERVED'", (request_id,))
        acct = conn.execute(
            "SELECT * FROM accounts WHERE dataset_id=? AND subject_id=?"
            " AND period_index=?",
            (res["dataset_id"], res["subject_id"],
             res["period_index"])).fetchone()
        log(conn, "REFUND", res["dataset_id"], res["subject_id"],
            res["period_index"], res["application_id"], request_id,
            res["amount"], reason, balance_of(acct))
        conn.execute("COMMIT")
        res = conn.execute(
            "SELECT * FROM reservations WHERE request_id=?",
            (request_id,)).fetchone()
        return 200, {"reservation": reservation_view(res),
                     "balance": balance_of(acct)}
    except ApiError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise


@app.route("POST", "/sweep")
def sweep(body, params, query):
    """Expire all TTL-overdue reservations (lazy expiry also runs inline)."""
    conn = db.conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        before = conn.execute(
            "SELECT COUNT(*) c FROM reservations WHERE state='RESERVED'"
            " AND expires_at<=?", (time.time(),)).fetchone()["c"]
        expire_due_locked(conn, time.time())
        conn.execute("COMMIT")
        return 200, {"expired": before}
    except Exception:
        conn.execute("ROLLBACK")
        raise


@app.route("GET", "/reservations/{request_id}")
def get_reservation(body, params, query):
    r = db.conn().execute(
        "SELECT * FROM reservations WHERE request_id=?",
        (params["request_id"],)).fetchone()
    if not r:
        raise ApiError(404, "reservation_not_found", params["request_id"])
    return 200, {"reservation": reservation_view(r)}


@app.route("GET", "/accounts/{dataset_id}/{subject_id}")
def get_account(body, params, query):
    dataset_id, subject_id = params["dataset_id"], params["subject_id"]
    policy = get_policy(dataset_id)
    conn = db.conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = time.time()
        expire_due_locked(conn, now)
        acct = get_or_create_account_locked(
            conn, policy, dataset_id, subject_id, now)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    # Composition total (pure DP): sum of committed epsilons this period.
    return 200, {
        "dataset_id": dataset_id, "subject_id": subject_id,
        "period_index": acct["period_index"],
        "period_start": acct["period_start"], "period_end": acct["period_end"],
        "budget_total": acct["budget_total"],
        "composition": policy["composition"],
        **balance_of(acct),
    }


@app.route("GET", "/admin/ledger")
def admin_ledger(body, params, query):
    clauses, vals = [], []
    for k in ("dataset_id", "subject_id", "op", "application_id"):
        if query.get(k):
            clauses.append(f"{k}=?")
            vals.append(query[k])
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    limit = min(int(query.get("limit", 200)), 1000)
    rows = db.conn().execute(
        f"SELECT * FROM ledger{where} ORDER BY id DESC LIMIT ?",
        (*vals, limit)).fetchall()
    out = []
    for r in rows:
        d = {k: r[k] for k in r.keys()}
        if d["balance_after"]:
            d["balance_after"] = json.loads(d["balance_after"])
        out.append(d)
    return 200, {"ledger": out, "count": len(out)}


@app.route("GET", "/health")
def health(body, params, query):
    return 200, {"status": "ok", "service": "budget"}


if __name__ == "__main__":
    app.serve("0.0.0.0", int(os.environ.get("PORT", "8002")))
