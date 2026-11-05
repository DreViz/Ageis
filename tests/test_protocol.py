import os
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from mock_identity.app import create_app
from mock_identity.db import Attempt, Grant, Operation
from mock_identity.reconcile import execute


@pytest.fixture(params=["sqlite"] + (["postgres"] if os.getenv("TEST_DATABASE_URL") else []))
def app(request, tmp_path):
    url = (f"sqlite:///{(tmp_path / 'test.db').as_posix()}" if request.param == "sqlite"
           else os.environ["TEST_DATABASE_URL"])
    application = create_app(url, enable_faults=True, fault_delay=0.4)
    yield application
    application.state.engine.dispose()


@pytest.fixture
def body():
    return {"tenant_id": str(uuid.uuid4()), "action_id": str(uuid.uuid4()),
            "user_id": "maya", "workspace_id": "analytics"}


def counts(app, tenant):
    with app.state.sessions() as session:
        return tuple(session.scalar(select(func.count()).select_from(table).where(table.tenant_id == tenant))
                     for table in (Attempt, Operation, Grant))


def test_idempotency_conflict_and_operation_lookup(app, body):
    with TestClient(app) as client:
        headers = {"Idempotency-Key": "key"}
        first = client.post("/grants", json=body, headers=headers)
        replay = client.post("/grants", json=body, headers=headers)
        conflict = client.post("/grants", json={**body, "workspace_id": "other"}, headers=headers)
        assert first.status_code == replay.status_code == 200
        assert first.json() == replay.json()
        assert conflict.status_code == 409
        lookup = client.get("/operations/" + first.json()["operation_id"], params={"tenant_id": body["tenant_id"]})
        assert lookup.json() == first.json()
        assert counts(app, body["tenant_id"]) == (3, 1, 1)


def test_concurrent_same_key_commits_once(app, body):
    with TestClient(app) as client:
        def send(_):
            return client.post("/grants", json=body, headers={"Idempotency-Key": "key"})
        with ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(send, range(6)))
        assert [r.status_code for r in responses] == [200] * 6
        assert len({r.json()["operation_id"] for r in responses}) == 1
        assert counts(app, body["tenant_id"]) == (6, 1, 1)


@pytest.fixture
def server(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(5)
    sock.close()


@pytest.mark.parametrize("fault,lookup_fault,expected,initial_counts", [
    ("before_commit", "none", "needs_review", (1, 0, 0)),
    ("after_commit", "none", "committed", (1, 1, 1)),
    ("after_commit", "unavailable", "needs_review", (1, 1, 1)),
])
def test_real_timeout_and_reconciliation(app, server, body, fault, lookup_fault, expected, initial_counts):
    with httpx.Client(base_url=server, timeout=0.15) as client:
        result = execute(client, body, "key", fault=fault, lookup_fault=lookup_fault)
        assert result["status"] == expected
        assert counts(app, body["tenant_id"]) == initial_counts
        # The pre-commit fault is a known abort in this fixture; the protected key
        # also makes a same-key retry safe even when absence alone proves nothing.
        response = client.post("/grants", json=body, headers={"Idempotency-Key": "key"})
        assert response.status_code == 200
        if expected == "committed":
            assert response.json()["operation_id"] == result["operation"]["operation_id"]
        assert counts(app, body["tenant_id"]) == (2, 1, 1)


def test_faults_disabled_and_validation(tmp_path, body):
    app = create_app(f"sqlite:///{(tmp_path / 'disabled.db').as_posix()}")
    with TestClient(app) as client:
        assert client.post("/grants", json=body, headers={"Idempotency-Key": "key", "X-Test-Fault": "after_commit"}).status_code == 403
        assert client.post("/grants", json={**body, "extra": 1}, headers={"Idempotency-Key": "key"}).status_code == 422
        assert client.get("/operations/by-key", params={"tenant_id": body["tenant_id"], "idempotency_key": "missing"}).status_code == 404
        assert counts(app, body["tenant_id"]) == (0, 0, 0)
    app.state.engine.dispose()


def test_expired_key_refuses_replay_but_status_survives(app, body):
    with TestClient(app) as client:
        headers = {"Idempotency-Key": "expired"}
        original = client.post("/grants", json=body, headers=headers)
        assert original.status_code == 200
        with app.state.sessions.begin() as session:
            op = session.get(Operation, original.json()["operation_id"])
            op.committed_at = op.committed_at - timedelta(hours=25)
        assert client.post("/grants", json=body, headers=headers).status_code == 409
        status = client.get("/operations/by-key", params={"tenant_id": body["tenant_id"], "idempotency_key": "expired"})
        assert status.json()["operation_id"] == original.json()["operation_id"]
        assert counts(app, body["tenant_id"]) == (2, 1, 1)
