"""End-to-end tests: spins up all three services as subprocesses and checks
the full rule set, including concurrent atomic reservation, idempotent
refunds, period rollover, revocation and result provenance.

Run:  python3 tests/test_flow.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REG, BUD, AUT = 18101, 18102, 18103
REG_URL, BUD_URL, AUT_URL = (f"http://127.0.0.1:{p}" for p in (REG, BUD, AUT))

FAILED = []


def call(method, url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def wait_up(url, tries=50):
    for _ in range(tries):
        try:
            s, _ = call("GET", url + "/health")
            if s == 200:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def start_services(tmp):
    envs = []
    procs = []
    specs = [
        ("registry-service", REG, {}),
        ("budget-service", BUD, {"REGISTRY_URL": REG_URL}),
        ("authz-service", AUT, {"REGISTRY_URL": REG_URL,
                                "BUDGET_URL": BUD_URL,
                                "AUTHZ_SECRET": "test-secret",
                                "RESERVATION_TTL_SECONDS": "2"}),
    ]
    for svc, port, extra in specs:
        env = dict(os.environ, PORT=str(port),
                   DB_PATH=os.path.join(tmp, f"{svc}.db"),
                   POLICY_CACHE_TTL="0.1", **extra)
        p = subprocess.Popen(
            [sys.executable, os.path.join(ROOT, svc, "app.py")],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)
        envs.append(env)
    assert all(wait_up(u) for u in (REG_URL, BUD_URL, AUT_URL)), \
        "services did not start"
    return procs


def make_dataset(name, eps=10.0, period=3600, max_req=5.0):
    s, ds = call("POST", REG_URL + "/datasets", {
        "name": name, "sensitivity": "high", "epsilon_per_period": eps,
        "period_seconds": period, "max_epsilon_per_request": max_req})
    assert s == 201, ds
    return ds["id"]


def ledger(ds_id, subject, op=None):
    q = f"dataset_id={ds_id}&subject_id={subject}&limit=500"
    s, b = call("GET", BUD_URL + "/admin/ledger?" + q)
    assert s == 200
    entries = b["ledger"]
    return [e for e in entries if op is None or e["op"] == op]


def main():
    tmp = tempfile.mkdtemp(prefix="pbtest-")
    procs = start_services(tmp)
    try:
        print("== 1. happy path: register -> apply -> approve -> execute -> verify")
        ds = make_dataset("census")
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds, "subject_id": "alice", "epsilon": 2.0,
            "query": {"sql": "SELECT count(*) FROM census"},
            "idempotency_key": "k-1"})
        check("application reserved", s == 201 and
              b["application"]["status"] == "RESERVED", json.dumps(b))
        app_id = b["application"]["id"]
        s, b2 = call("POST", AUT_URL + "/applications",
                     {"dataset_id": ds, "subject_id": "alice", "epsilon": 2.0,
                      "query": {}, "idempotency_key": "k-1"})
        check("idempotent create replays", s == 200 and
              b2["application"]["id"] == app_id)
        s, b = call("POST", f"{AUT_URL}/applications/{app_id}/approve")
        check("approved", s == 200 and b["application"]["status"] == "APPROVED")
        s, b = call("POST", f"{AUT_URL}/applications/{app_id}/execute",
                    {"true_value": 100.0})
        check("executed", s == 201 and b["application"]["status"] == "EXECUTED",
              json.dumps(b))
        result = b["result"]
        check("result binds version+noise params",
              result["app_version"] == 1 and result["mechanism"] == "laplace"
              and result["epsilon"] == 2.0)
        s, b = call("POST", AUT_URL + "/results/verify",
                    {"result_id": result["result_id"]})
        check("result verifies", s == 200 and b["valid"] is True)
        s, b = call("POST", AUT_URL + "/results/verify",
                    {"result_id": result["result_id"],
                     "payload": {"noisy_value": 999.0}})
        check("tampered payload rejected", s == 200 and b["valid"] is False)
        s, b = call("GET", f"{BUD_URL}/accounts/{ds}/alice")
        check("composition: consumed=2.0", b["consumed"] == 2.0 and
              b["reserved"] == 0.0, json.dumps(b))

        print("== 2. concurrency: 20 x 1.0 reservations against budget 10")
        ds2 = make_dataset("concurrent")
        def one(i):
            return call("POST", BUD_URL + "/reserve", {
                "request_id": f"c-{i}", "application_id": f"a-{i}",
                "dataset_id": ds2, "subject_id": "bob",
                "amount": 1.0, "ttl_seconds": 60})
        with ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(one, range(20)))
        ok = sum(1 for s, _ in results if s == 201)
        conflict = sum(1 for s, _ in results if s == 409)
        check("exactly 10 win, 10 conflict", ok == 10 and conflict == 10,
              f"ok={ok} conflict={conflict}")
        s, b = call("GET", f"{BUD_URL}/accounts/{ds2}/bob")
        check("no overspend: reserved=10, remaining=0",
              abs(b["reserved"] - 10.0) < 1e-9 and
              abs(b["remaining"]) < 1e-9, json.dumps(b))
        check("conflicts audited", len(ledger(ds2, "bob", "CONFLICT")) == 10)

        print("== 3. cancel -> refund, idempotent, no double refund")
        ds3 = make_dataset("refunds")
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds3, "subject_id": "carol", "epsilon": 3.0,
            "query": {}})
        app3 = b["application"]["id"]
        call("POST", f"{AUT_URL}/applications/{app3}/approve")
        s, b = call("POST", f"{AUT_URL}/applications/{app3}/cancel")
        check("cancelled", b["application"]["status"] == "CANCELLED")
        s, b = call("POST", f"{AUT_URL}/applications/{app3}/cancel")
        check("double cancel idempotent", s == 200)
        s, b = call("GET", f"{BUD_URL}/accounts/{ds3}/carol")
        check("budget fully restored", b["remaining"] == 10.0, json.dumps(b))
        check("exactly one REFUND entry",
              len(ledger(ds3, "carol", "REFUND")) == 1)

        print("== 4. execution failure -> refund; retry -> version+1")
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds3, "subject_id": "carol", "epsilon": 4.0,
            "query": {}})
        app4 = b["application"]["id"]
        call("POST", f"{AUT_URL}/applications/{app4}/approve")
        s, b = call("POST", f"{AUT_URL}/applications/{app4}/execute",
                    {"fail_execution": True})
        check("failed on executor error", s == 500 and
              b["application"]["status"] == "FAILED")
        s, b = call("GET", f"{BUD_URL}/accounts/{ds3}/carol")
        check("failure refunded", b["remaining"] == 10.0, json.dumps(b))
        s, b = call("POST", f"{AUT_URL}/applications/{app4}/retry")
        check("retry creates version 2", s == 201 and
              b["application"]["version"] == 2 and
              b["application"]["parent_id"] == app4, json.dumps(b))
        s, b = call("GET", f"{BUD_URL}/accounts/{ds3}/carol")
        check("retry reserved anew (no double refund)",
              b["reserved"] == 4.0 and b["remaining"] == 6.0, json.dumps(b))
        check("still exactly 2 REFUND entries total",
              len(ledger(ds3, "carol", "REFUND")) == 2)

        print("== 5. revocation: pending stopped, executed stays verifiable")
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds3, "subject_id": "dave", "epsilon": 1.0,
            "query": {}})
        app5 = b["application"]["id"]
        s, b = call("POST", f"{AUT_URL}/applications/{app5}/revoke")
        check("pending revoked", b["application"]["status"] == "REVOKED")
        s, b = call("POST", f"{AUT_URL}/applications/{app5}/approve")
        check("revoked app cannot proceed", s == 409)
        # executed app: revoke keeps result verifiable
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds3, "subject_id": "dave", "epsilon": 1.0,
            "query": {}})
        app6 = b["application"]["id"]
        call("POST", f"{AUT_URL}/applications/{app6}/approve")
        s, b = call("POST", f"{AUT_URL}/applications/{app6}/execute",
                    {"true_value": 7.0})
        rid = b["result"]["result_id"]
        s, b = call("POST", f"{AUT_URL}/applications/{app6}/revoke")
        check("executed revoke flags authorization",
              b["application"]["authorization_revoked"] is True and
              b["application"]["status"] == "EXECUTED")
        s, b = call("POST", AUT_URL + "/results/verify", {"result_id": rid})
        check("result still verifies after revoke", b["valid"] is True)
        s, b = call("GET", f"{BUD_URL}/accounts/{ds3}/dave")
        check("executed consumption retained", b["consumed"] == 1.0)

        print("== 6. committed budget cannot be refunded")
        s, b = call("GET", f"{AUT_URL}/applications/{app6}")
        req_id = b["application"]["reservation_request_id"]
        s, b = call("POST", BUD_URL + "/refund", {"request_id": req_id})
        check("refund of committed -> 409", s == 409, json.dumps(b))

        print("== 7. period rollover: expire, no carry-over, no double release")
        ds7 = make_dataset("rolling", eps=5.0, period=2)
        s, b = call("POST", BUD_URL + "/reserve", {
            "request_id": "r-old", "application_id": "a-old",
            "dataset_id": ds7, "subject_id": "erin",
            "amount": 2.0, "ttl_seconds": 300})
        check("reserved in period 0", s == 201)
        time.sleep(2.3)
        s, b = call("POST", BUD_URL + "/reserve", {
            "request_id": "r-new", "application_id": "a-new",
            "dataset_id": ds7, "subject_id": "erin",
            "amount": 5.0, "ttl_seconds": 300})
        check("new period grants full budget", s == 201, json.dumps(b))
        s, b = call("GET", f"{BUD_URL}/accounts/{ds7}/erin")
        check("rolled to period 1, no carry-over",
              b["period_index"] == 1 and b["reserved"] == 5.0 and
              b["remaining"] == 0.0, json.dumps(b))
        check("old reservation expired",
              len(ledger(ds7, "erin", "EXPIRE")) == 1)
        check("rollover audited",
              len(ledger(ds7, "erin", "ROLLOVER")) == 1)
        s, b = call("POST", BUD_URL + "/refund",
                    {"request_id": "r-old", "reason": "late_cancel"})
        check("late refund of expired is idempotent no-op", s == 200 and
              b["reservation"]["state"] == "EXPIRED")
        check("no REFUND ledger entry for expired reservation",
              len(ledger(ds7, "erin", "REFUND")) == 0)

        print("== 8. admin ledger completeness")
        ops = {e["op"] for e in ledger(ds2, "bob")}
        check("reserve+conflict visible", {"RESERVE", "CONFLICT"} <= ops,
              str(ops))
        ops3 = {e["op"] for e in ledger(ds3, "carol")}
        check("commit/refund visible", {"RESERVE", "REFUND"} <= ops3, str(ops3))
        ops7 = {e["op"] for e in ledger(ds7, "erin")}
        check("expire/rollover visible", {"EXPIRE", "ROLLOVER"} <= ops7,
              str(ops7))

        print("== 9. sensitivity comes from dataset policy, never the caller")
        ds9 = make_dataset("sensitivity")  # registered as "high" -> df 10.0
        s, b = call("GET", f"{REG_URL}/datasets/{ds9}")
        check("registry exposes policy sensitivity",
              s == 200 and b["query_sensitivity"] == 10.0, json.dumps(b))
        # forged tiny sensitivity must not stick
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds9, "subject_id": "mallory", "epsilon": 1.0,
            "sensitivity": 0.01, "query": {}})
        check("forged sensitivity ignored, policy value stored",
              s == 201 and b["application"]["sensitivity"] == 10.0,
              json.dumps(b))
        app9 = b["application"]["id"]
        call("POST", f"{AUT_URL}/applications/{app9}/approve")
        s, b = call("POST", f"{AUT_URL}/applications/{app9}/execute",
                    {"true_value": 50.0})
        check("signed result carries policy sensitivity",
              s == 201 and b["result"]["sensitivity"] == 10.0, json.dumps(b))
        s, b2 = call("POST", AUT_URL + "/results/verify",
                     {"result_id": b["result"]["result_id"]})
        check("policy-bound result verifies", s == 200 and b2["valid"] is True)
        # missing sensitivity must not default to anything caller-friendly
        s, b = call("POST", AUT_URL + "/applications", {
            "dataset_id": ds9, "subject_id": "mallory", "epsilon": 1.0,
            "query": {}})
        check("missing sensitivity also yields policy value",
              s == 201 and b["application"]["sensitivity"] == 10.0,
              json.dumps(b))
    finally:
        for p in procs:
            p.terminate()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if FAILED:
        print(f"{len(FAILED)} FAILED: {FAILED}")
        sys.exit(1)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
