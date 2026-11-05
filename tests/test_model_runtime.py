import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select, text
from fastapi.testclient import TestClient

from runtime.db import Approval, Job, ModelSession, Run, make_database
from runtime.model import ModelError, ModelResult
from runtime.service import Conflict, LeaseLost, Runtime
from runtime.worker import process

URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="PostgreSQL TEST_DATABASE_URL required")


@pytest.fixture
def ctx():
    schema = "test_" + uuid.uuid4().hex
    engine, sessions = make_database(URL, schema=schema)
    yield Runtime(sessions), sessions
    with engine.begin() as c:
        c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


class Fake:
    def __init__(self, result):
        self.result = result
    def call(self, model_id, messages):
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def process_one(runtime, result):
    lease = runtime.claim("worker")
    assert lease is not None
    with httpx.Client(transport=httpx.MockTransport(lambda req: (_ for _ in ()).throw(AssertionError("unexpected grant"))), base_url="http://mock") as client:
        process(runtime, lease, client, model_client=Fake(result))


def test_model_proposal_enters_existing_policy_and_approval(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Give Maya analytics access", "pinned-model")
    process_one(runtime, ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"}, 10, 3))
    with sessions() as s:
        run, model = s.get(Run, run_id), s.get(ModelSession, run_id)
        assert run.state == "PENDING" and run.action["user_id"] == "maya"
        assert model.model_calls == model.accounted_calls == 1
    process_one(runtime, None)
    with sessions() as s:
        assert s.get(Run, run_id).state == "WAITING_FOR_APPROVAL"
        assert s.scalar(select(Approval).where(Approval.run_id == run_id)).status == "PENDING"


def test_ungrounded_proposal_waits_for_input_and_resumes(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Maya needs access", "pinned-model")
    process_one(runtime, ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"}))
    with sessions() as s:
        model = s.get(ModelSession, run_id)
        assert s.get(Run, run_id).state == "WAITING_FOR_INPUT" and model.question
        assert model.model_calls == 1 and model.usage_missing
    runtime.provide_input(run_id, "The workspace is analytics", "operator")
    with pytest.raises(Conflict):
        runtime.provide_input(run_id, "analytics", "operator")
    process_one(runtime, ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"}, 4, 2))
    with sessions() as s:
        assert s.get(Run, run_id).state == "PENDING"
        assert len(s.get(ModelSession, run_id).messages) == 3


def test_model_cannot_bypass_denied_policy(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Give Maya admin access", "pinned-model")
    process_one(runtime, ModelResult("propose_access", {"user_id": "maya", "workspace_id": "admin"}))
    process_one(runtime, None)
    with sessions() as s:
        assert s.get(Run, run_id).state == "REJECTED"
        assert s.scalar(select(Approval).where(Approval.run_id == run_id)) is None


def test_provider_error_fails_without_executing_action(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Maya analytics", "pinned-model")
    process_one(runtime, ModelError("provider_or_output_error"))
    with sessions() as s:
        assert s.get(Run, run_id).state == "FAILED"
        assert s.get(ModelSession, run_id).last_error == "provider_or_output_error"
        assert s.get(Run, run_id).attempt_count == 0


def test_crash_consumes_call_budget_and_stale_output_is_fenced(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Maya analytics", "pinned-model")
    first = runtime.claim("old")
    assert runtime.prepare(first)["mode"] == "model"
    with sessions.begin() as s:
        s.get(Job, first[0]).lease_expires_at -= timedelta(minutes=1)
    second = runtime.claim("new")
    assert runtime.prepare(second)["mode"] == "model"
    result = ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"}, 3, 2)
    with pytest.raises(LeaseLost):
        runtime.finish_model(first, result)
    runtime.finish_model(second, result)
    with sessions() as s:
        m = s.get(ModelSession, run_id)
        assert m.model_calls == 2 and m.accounted_calls == 1


def test_calls_bounded_after_repeated_clarifications(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Need access", "pinned-model")
    for i in range(4):
        process_one(runtime, ModelResult("request_input", {"question": "Which workspace?"}))
        if i < 3:
            runtime.provide_input(run_id, "Not sure", "operator")
    with pytest.raises(Conflict):
        runtime.provide_input(run_id, "again", "operator")
    with sessions() as s:
        assert s.get(ModelSession, run_id).model_calls == 4


def test_model_to_verified_grant_requires_approval(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Give Maya analytics access", "pinned-model")
    process_one(runtime, ModelResult("propose_access", {"user_id": "maya", "workspace_id": "analytics"}, 9, 4))
    process_one(runtime, None)
    with sessions() as s:
        approval = s.scalar(select(Approval).where(Approval.run_id == run_id))
        approval_id = approval.id
        assert s.get(Run, run_id).attempt_count == 0
    runtime.decide(approval_id, "APPROVED", "operator")
    def grant(request):
        import json
        body = json.loads(request.content)
        with sessions() as s:
            run = s.get(Run, run_id)
            assert body == run.action and request.headers["Idempotency-Key"] == run.idempotency_key
            return httpx.Response(200, json={"status": "committed", "action_id": body["action_id"],
                                  "request_hash": run.action_hash, "operation_id": "verified-op"})
    with httpx.Client(transport=httpx.MockTransport(grant), base_url="http://mock") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        assert s.get(Run, run_id).state == "COMPLETED"
        assert s.get(Run, run_id).operation["operation_id"] == "verified-op"


def test_invalid_fake_tool_arguments_fail_closed(ctx):
    runtime, sessions = ctx
    run_id = runtime.create_model_run("tenant", "Maya analytics", "pinned-model")
    process_one(runtime, ModelResult("request_input", {"question": [], "extra": "ignored"}))
    with sessions() as s:
        assert s.get(Run, run_id).state == "FAILED"
        assert s.get(ModelSession, run_id).last_error == "invalid_model_output"


def test_model_api_requires_configuration_and_resumes_input(ctx, monkeypatch):
    import runtime.api as api
    runtime, sessions = ctx
    monkeypatch.setattr(api, "make_database", lambda url: (sessions.kw["bind"], sessions))
    headers = {"Authorization": "Bearer token"}
    with TestClient(api.create_app(URL, operator_token="token")) as client:
        response = client.post("/model-runs", headers=headers, json={"tenant_id": "tenant", "text": "Maya"})
        assert response.status_code == 503
    with TestClient(api.create_app(URL, operator_token="token", model_id="pinned-model")) as client:
        assert client.post("/model-runs", json={"tenant_id": "tenant", "text": "Maya"}).status_code == 401
        response = client.post("/model-runs", headers=headers, json={"tenant_id": "tenant", "text": "Maya"})
        assert response.status_code == 201
        run_id = response.json()["run_id"]
        process_one(runtime, ModelResult("request_input", {"question": "Which workspace?"}, 1, 2))
        state = client.get("/runs/" + run_id, headers=headers).json()
        assert state["state"] == "WAITING_FOR_INPUT" and state["model"]["question"]
        assert client.post("/runs/" + run_id + "/input", headers=headers, json={"text": "analytics"}).status_code == 200
        assert client.post("/runs/" + run_id + "/input", headers=headers, json={"text": "analytics"}).status_code == 409
