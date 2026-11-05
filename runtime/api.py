"""Authenticated local demo control API, for synthetic data only."""
import hmac
import os
from typing import Literal
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from .db import Approval, Event, Run, make_database
from .service import Conflict, Runtime


class CreateRun(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tenant_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    workspace_id: str = Field(min_length=1, max_length=128)


class Decision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: Literal["APPROVED", "REJECTED"]


def create_app(database_url, *, operator_token):
    if not operator_token:
        raise ValueError("An operator token is required")
    engine, sessions = make_database(database_url)
    runtime = Runtime(sessions)
    app = FastAPI(title="Ageis control API")
    app.state.engine, app.state.sessions, app.state.runtime = engine, sessions, runtime

    def authorize(authorization: str = Header(default="")):
        if not hmac.compare_digest(authorization, "Bearer " + operator_token):
            raise HTTPException(401, "Invalid operator credentials")
        return "demo-operator"

    @app.post("/runs", status_code=201)
    def create_run(body: CreateRun, actor=Depends(authorize)):
        return {"run_id": runtime.create_run(**body.model_dump())}

    @app.get("/runs/{run_id}")
    def get_run(run_id: str, actor=Depends(authorize)):
        with sessions() as s:
            run = s.get(Run, run_id)
            if run is None:
                raise HTTPException(404, "Run not found")
            return {"id": run.id, "state": run.state, "action": run.action, "action_status": run.action_status,
                    "operation": run.operation, "attempt_count": run.attempt_count, "deadline": run.deadline}

    @app.get("/runs/{run_id}/events")
    def get_events(run_id: str, actor=Depends(authorize)):
        with sessions() as s:
            if s.get(Run, run_id) is None:
                raise HTTPException(404, "Run not found")
            return [{"sequence": e.sequence, "kind": e.kind, "payload": e.payload, "created_at": e.created_at}
                    for e in s.scalars(select(Event).where(Event.run_id == run_id).order_by(Event.sequence))]

    @app.get("/approvals")
    def approvals(actor=Depends(authorize)):
        with sessions() as s:
            return [{"id": a.id, "run_id": a.run_id, "action": a.action, "action_hash": a.action_hash,
                     "expires_at": a.expires_at} for a in s.scalars(select(Approval).where(Approval.status == "PENDING"))]

    @app.post("/approvals/{approval_id}/decision")
    def decide(approval_id: str, body: Decision, actor=Depends(authorize)):
        try:
            runtime.decide(approval_id, body.decision, actor)
        except LookupError:
            raise HTTPException(404, "Approval not found")
        except Conflict as error:
            raise HTTPException(409, str(error))
        return {"status": body.decision}

    @app.post("/approvals/{approval_id}/renew", status_code=201)
    def renew(approval_id: str, actor=Depends(authorize)):
        try:
            return {"approval_id": runtime.renew_approval(approval_id, actor)}
        except LookupError:
            raise HTTPException(404, "Approval not found")
        except Conflict as error:
            raise HTTPException(409, str(error))

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    return app


def create_env_app():
    return create_app(os.environ["RUNTIME_DATABASE_URL"], operator_token=os.environ["OPERATOR_TOKEN"])
