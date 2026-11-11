import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from runtime.api import create_app
from runtime.db import Approval, Event, Job, Run, make_database
from runtime.service import Conflict, LeaseLost, Runtime
from runtime.worker import process

URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="PostgreSQL TEST_DATABASE_URL is required for durable-runtime tests")


@pytest.fixture
def context():
    schema = "test_" + uuid.uuid4().hex
    engine, sessions = make_database(URL, schema=schema)
    yield Runtime(sessions), sessions
    with engine.begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def create(runtime):
    return runtime.create_run(str(uuid.uuid4()), "maya", "analytics")


def start_approval(runtime, sessions):
    run_id = create(runtime)
    lease = runtime.claim("worker-a")
    assert runtime.prepare(lease) is None
    with sessions() as s:
        approval = s.scalar(select(Approval).where(Approval.run_id == run_id))
        assert s.get(Run, run_id).state == "WAITING_FOR_APPROVAL"
        return run_id, approval.id


def test_claim_is_exclusive_and_stale_worker_is_fenced(context):
    runtime, sessions = context
    run_id = create(runtime)
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(runtime.claim, ["a", "b"]))
    first = next(x for x in claims if x)
    assert sum(x is not None for x in claims) == 1
    with sessions.begin() as s:
        job = s.get(Job, first[0])
        job.lease_expires_at = job.lease_expires_at - timedelta(minutes=1)
    second = runtime.claim("replacement")
    assert second[2] == first[2] + 1
    with pytest.raises(LeaseLost):
        runtime.prepare(first)
    with pytest.raises(LeaseLost):
        runtime.renew(first)
    runtime.prepare(second)
    with sessions() as s:
        assert s.get(Run, run_id).state == "WAITING_FOR_APPROVAL"


def test_competing_decisions_create_one_resume(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    def decide(value):
        try:
            runtime.decide(approval_id, value, value)
            return value
        except Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(decide, ["APPROVED", "REJECTED"]))
    assert results.count("conflict") == 1
    winner = next(x for x in results if x != "conflict")
    with sessions() as s:
        assert s.get(Approval, approval_id).status == winner
        count = s.scalar(select(func.count()).select_from(Job).where(Job.approval_id == approval_id))
        assert count == (1 if winner == "APPROVED" else 0)
        assert s.get(Run, run_id).state == ("RUNNING" if winner == "APPROVED" else "REJECTED")


def test_decision_rollback_is_atomic(context, monkeypatch):
    import runtime.service as service
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    def fail(*args):
        raise RuntimeError("simulated crash before commit")
    monkeypatch.setattr(service, "event", fail)
    with pytest.raises(RuntimeError):
        runtime.decide(approval_id, "APPROVED", "operator")
    with sessions() as s:
        assert s.get(Approval, approval_id).status == "PENDING"
        assert s.get(Run, run_id).state == "WAITING_FOR_APPROVAL"
        assert s.scalar(select(func.count()).select_from(Job).where(Job.approval_id == approval_id)) == 0


def test_approval_change_or_expiry_blocks_execution(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        run.action = {**run.action, "workspace_id": "changed"}
    lease = runtime.claim("worker")
    assert runtime.prepare(lease) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
        assert s.get(Run, run_id).attempt_count == 0


def test_recovery_of_unknown_uses_same_key_and_fences_late_success(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    old = runtime.claim("old")
    intent = runtime.prepare(old)
    assert intent["mode"] == "send"
    with sessions.begin() as s:
        job = s.get(Job, old[0])
        job.lease_expires_at -= timedelta(minutes=1)
    new = runtime.claim("new")
    restored = runtime.prepare(new)
    assert restored["mode"] == "reconcile"
    assert (restored["key"], restored["action"]) == (intent["key"], intent["action"])
    outcome = {"status": "committed", "request_hash": intent["hash"],
               "action_id": intent["action"]["action_id"], "operation_id": "op-1"}
    with pytest.raises(LeaseLost):
        runtime.finish(old, outcome)
    runtime.finish(new, outcome)
    with sessions() as s:
        run = s.get(Run, run_id)
        assert run.state == "COMPLETED" and run.attempt_count == 1
        assert [e.sequence for e in s.scalars(select(Event).where(Event.run_id == run_id).order_by(Event.sequence))] == list(range(1, run.event_seq + 1))


def test_unknown_lookup_stops_for_review(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    lease = runtime.claim("worker")
    def handler(request):
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(404, request=request)
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://mock") as client:
        process(runtime, lease, client)
    with sessions() as s:
        run = s.get(Run, run_id)
        assert run.state == "NEEDS_REVIEW" and run.action_status == "UNKNOWN"


def test_api_auth_and_approval_expiry(context, monkeypatch):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    # Use the existing isolated test schema for API construction.
    import runtime.api as api
    monkeypatch.setattr(api, 'make_database', lambda url: (sessions.kw['bind'], sessions))
    app = create_app(URL, operator_token="secret")
    with TestClient(app) as client:
        assert client.get("/approvals").status_code == 401
        assert client.get("/approvals", headers={"Authorization": "Bearer wrong"}).status_code == 401
    app.state.engine.dispose()
    with sessions.begin() as s:
        approval = s.get(Approval, approval_id)
        approval.expires_at -= timedelta(hours=1)
    with pytest.raises(Conflict):
        runtime.decide(approval_id, "APPROVED", "operator")
    with sessions() as s:
        assert s.get(Run, run_id).state == "WAITING_FOR_APPROVAL"


def test_lease_lost_during_remote_call(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    old = runtime.claim("old")
    started, release = threading.Event(), threading.Event()
    seen = []
    def handler(request):
        if request.method == "POST":
            seen.append(request.headers["idempotency-key"])
            started.set()
            assert release.wait(5)
            with sessions() as s:
                run = s.get(Run, run_id)
                return httpx.Response(200, json={"status": "committed", "request_hash": run.action_hash,
                    "action_id": run.action["action_id"], "operation_id": "op-1"}, request=request)
        raise AssertionError("Unexpected request")
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://mock") as client:
        thread = threading.Thread(target=process, args=(runtime, old, client))
        thread.start()
        assert started.wait(5)
        with sessions.begin() as s:
            s.get(Job, old[0]).lease_expires_at -= timedelta(minutes=1)
        new = runtime.claim("new")
        restored = runtime.prepare(new)
        assert restored["mode"] == "reconcile" and restored["key"] == seen[0]
        release.set()
        thread.join(5)
        assert not thread.is_alive()
    with sessions() as s:
        assert s.get(Run, run_id).state == "RECONCILING"
    runtime.finish(new, None)


def test_claim_limit_escalates_unknown_action(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    lease = runtime.claim("worker")
    runtime.prepare(lease)
    with sessions.begin() as s:
        job = s.get(Job, lease[0])
        job.claims = 10
        job.lease_expires_at -= timedelta(minutes=1)
    assert runtime.claim("replacement") is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"


def test_ownership_loss_before_request_sends_nothing(context, monkeypatch):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    lease = runtime.claim("old")
    def lost(_):
        raise LeaseLost()
    monkeypatch.setattr(runtime, "check_lease", lost)
    def unexpected(request):
        raise AssertionError("A stale worker sent a request")
    with httpx.Client(transport=httpx.MockTransport(unexpected), base_url="http://mock") as client:
        process(runtime, lease, client)
    with sessions() as s:
        assert s.get(Run, run_id).action_status == "UNKNOWN"


def test_expired_approval_can_be_replaced_without_duplicate_jobs(context):
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    with sessions.begin() as s:
        s.get(Approval, approval_id).expires_at -= timedelta(hours=1)
    replacement = runtime.renew_approval(approval_id, "operator")
    with pytest.raises(Conflict):
        runtime.renew_approval(approval_id, "operator")
    runtime.decide(replacement, "APPROVED", "operator")
    with sessions() as s:
        assert s.get(Approval, approval_id).status == "SUPERSEDED"
        assert s.get(Run, run_id).state == "RUNNING"
        assert s.scalar(select(func.count()).select_from(Job).where(Job.approval_id == replacement)) == 1


@pytest.mark.parametrize("user,workspace,reason", [("missing", "analytics", "UNKNOWN_USER"),
                                               ("maya", "missing", "UNKNOWN_WORKSPACE"),
                                               ("maya", "admin", "DENIED")])
def test_directory_and_policy_reject_without_approval(context, user, workspace, reason):
    runtime, sessions = context
    run_id = runtime.create_run(str(uuid.uuid4()), user, workspace)
    assert runtime.prepare(runtime.claim("worker")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "REJECTED"
        assert s.scalar(select(Approval).where(Approval.run_id == run_id)) is None
        denied = s.scalar(select(Event).where(Event.run_id == run_id, Event.kind == "request_denied"))
        assert denied.payload["reason"] == reason


def test_policy_is_rechecked_before_remote_action(context, monkeypatch):
    from runtime.access_adapter import AccessAdapter
    from runtime.contracts import PolicyResult
    runtime, sessions = context
    run_id, approval_id = start_approval(runtime, sessions)
    runtime.decide(approval_id, "APPROVED", "operator")
    monkeypatch.setattr(AccessAdapter, "assess", lambda self, action: PolicyResult("DENIED"))
    assert runtime.prepare(runtime.claim("worker")) is None
    with sessions() as s:
        run = s.get(Run, run_id)
        assert run.state == "NEEDS_REVIEW" and run.attempt_count == 0


def test_run_list_is_authenticated_ordered_and_bounded(context, monkeypatch):
    import runtime.api as api
    runtime, sessions = context
    monkeypatch.setattr(api, "make_database", lambda url: (sessions.kw["bind"], sessions))
    from fastapi.testclient import TestClient
    first = runtime.create_run("tenant", "maya", "analytics")
    second = runtime.create_run("tenant", "maya", "admin")
    app = api.create_app(URL, operator_token="secret")
    with TestClient(app) as client:
        assert client.get("/runs").status_code == 401
        headers = {"Authorization": "Bearer secret"}
        assert client.get("/runs?limit=0", headers=headers).status_code == 422
        assert client.get("/runs?limit=101", headers=headers).status_code == 422
        result = client.get("/runs?limit=1", headers=headers)
        assert result.status_code == 200 and [r["id"] for r in result.json()] == [second]
        all_runs = client.get("/runs", headers=headers).json()
        assert [r["id"] for r in all_runs] == [second, first]
        assert all_runs[0]["created_at"] and all_runs[0]["action"]["workspace_id"] == "admin"
        detail = client.get("/runs/" + second, headers=headers).json()
        assert detail["contract"]["adapter"] == "synthetic-access" and not detail["legacy_contract"]
