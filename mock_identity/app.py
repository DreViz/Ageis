"""Synthetic identity API with transactional idempotency and opt-in fault injection."""
import hashlib
import json
import os
import time
import uuid
from datetime import timedelta, timezone
from typing import Literal

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .db import Attempt, Grant, Operation, make_database, now

RETENTION = timedelta(hours=24)


class GrantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tenant_id: str = Field(min_length=1, max_length=128)
    action_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    workspace_id: str = Field(min_length=1, max_length=128)


def request_hash(body: GrantRequest) -> str:
    # All fields are required; preserve exact Unicode code points and distinguish values.
    payload = {"operation_type": "grant_access", **body.model_dump()}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def as_utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def create_app(database_url: str, *, enable_faults: bool = False, fault_delay: float = 1.0):
    engine, sessions = make_database(database_url)
    app = FastAPI(title="Ageis synthetic identity service")
    app.state.engine = engine
    app.state.sessions = sessions

    def find_operation(session, tenant, key):
        return session.scalar(select(Operation).where(Operation.tenant_id == tenant,
            Operation.operation_type == "grant_access", Operation.key == key))

    def operation_result(op):
        return {"operation_id": op.id, "action_id": op.action_id, "status": "committed",
                "request_hash": op.request_hash, "committed_at": as_utc(op.committed_at).isoformat()}

    @app.post("/grants")
    def grant(body: GrantRequest, idempotency_key: str = Header(min_length=1, max_length=128),
              x_test_fault: Literal["none", "before_commit", "after_commit"] = Header(default="none")):
        if x_test_fault != "none" and not enable_faults:
            raise HTTPException(403, "Fault injection disabled")
        digest = request_hash(body)
        attempt_id = str(uuid.uuid4())
        with sessions.begin() as session:
            session.add(Attempt(id=attempt_id, tenant_id=body.tenant_id, key=idempotency_key,
                                request_hash=digest, action_id=body.action_id))
        if x_test_fault == "before_commit":
            with sessions.begin() as session:
                session.get(Attempt, attempt_id).outcome = "aborted_before_commit"
            time.sleep(fault_delay)
            raise HTTPException(503, "Injected abort before commit")
        try:
            with sessions.begin() as session:
                op = find_operation(session, body.tenant_id, idempotency_key)
                if op is None:
                    op = Operation(id=str(uuid.uuid4()), tenant_id=body.tenant_id,
                        operation_type="grant_access", key=idempotency_key, request_hash=digest,
                        action_id=body.action_id, user_id=body.user_id, workspace_id=body.workspace_id)
                    session.add(op)
                    session.flush()  # Reserve the key before touching the grant.
                    existing = session.scalar(select(Grant).where(Grant.tenant_id == body.tenant_id,
                        Grant.user_id == body.user_id, Grant.workspace_id == body.workspace_id))
                    if existing is None:
                        session.add(Grant(id=str(uuid.uuid4()), tenant_id=body.tenant_id,
                                          user_id=body.user_id, workspace_id=body.workspace_id))
                    session.flush()
                result = operation_result(op)
                stored_hash = op.request_hash
                committed_at = as_utc(op.committed_at)
        except IntegrityError:
            # A concurrent identical key waits on the unique constraint. A different key
            # racing for the same grant is retried by the caller, not misreported as success.
            with sessions() as session:
                op = find_operation(session, body.tenant_id, idempotency_key)
                if op is None:
                    with sessions.begin() as write:
                        write.get(Attempt, attempt_id).outcome = "concurrent_grant_conflict"
                    raise HTTPException(409, "Concurrent grant conflict; retry with same key")
                result, stored_hash, committed_at = operation_result(op), op.request_hash, as_utc(op.committed_at)
        if stored_hash != digest:
            outcome = "key_conflict"
        elif now() - committed_at >= RETENTION:
            outcome = "key_expired"
        else:
            outcome = "committed"
        with sessions.begin() as session:
            session.get(Attempt, attempt_id).outcome = outcome
        if outcome != "committed":
            raise HTTPException(409, outcome)
        if x_test_fault == "after_commit":
            time.sleep(fault_delay)
        return result

    @app.get("/operations/by-key")
    def status(tenant_id: str, idempotency_key: str, x_test_fault: Literal["none", "unavailable"] = Header(default="none")):
        if x_test_fault != "none":
            if not enable_faults:
                raise HTTPException(403, "Fault injection disabled")
            raise HTTPException(503, "Injected unavailable lookup")
        with sessions() as session:
            op = find_operation(session, tenant_id, idempotency_key)
            if op is None:
                # Absence is not a terminal result: another request may still be in flight.
                raise HTTPException(404, "No committed operation visible; outcome remains unknown")
            return operation_result(op)

    @app.get("/operations/{operation_id}")
    def operation(operation_id: str, tenant_id: str):
        with sessions() as session:
            op = session.get(Operation, operation_id)
            if op is None or op.tenant_id != tenant_id:
                raise HTTPException(404, "Operation not found")
            return operation_result(op)

    @app.get("/grants")
    def grants(tenant_id: str):
        with sessions() as session:
            return [{"user_id": g.user_id, "workspace_id": g.workspace_id} for g in
                    session.scalars(select(Grant).where(Grant.tenant_id == tenant_id)).all()]

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    return app


def create_env_app():
    return create_app(os.getenv("DATABASE_URL", "sqlite:///./ageis-mock.db"),
                      enable_faults=os.getenv("ENABLE_TEST_FAULTS", "false").lower() == "true")
