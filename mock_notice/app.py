"""Synthetic notification API; no messages leave this service."""
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal
from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import DateTime, Integer, String, UniqueConstraint, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from .schema import NoticeRequest, request_hash


class Base(DeclarativeBase):
    pass


class Notice(Base):
    __tablename__ = "synthetic_notices"
    __table_args__ = (UniqueConstraint("org_id", "key"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    org_id: Mapped[str] = mapped_column(String, nullable=False)
    key: Mapped[str] = mapped_column(String, nullable=False)
    action_id: Mapped[str] = mapped_column(String, nullable=False)
    request_hash: Mapped[str] = mapped_column(String, nullable=False)
    recipient: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)
    committed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Attempt(Base):
    __tablename__ = "synthetic_notice_attempts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    org_id: Mapped[str] = mapped_column(String, nullable=False)
    key: Mapped[str] = mapped_column(String, nullable=False)
    action_id: Mapped[str] = mapped_column(String, nullable=False)
    request_hash: Mapped[str] = mapped_column(String, nullable=False)
    outcome: Mapped[str] = mapped_column(String, nullable=False)


class Fault(Base):
    __tablename__ = "synthetic_notice_faults"
    org_id: Mapped[str] = mapped_column(String, primary_key=True)
    scenario: Mapped[str] = mapped_column(String, nullable=False)
    calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


def utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def result(notice):
    return {"status": "committed", "operation_id": notice.id, "action_id": notice.action_id,
            "request_hash": notice.request_hash, "committed_at": utc(notice.committed_at).isoformat()}


def create_app(database_url, *, enable_faults=False, fault_delay=6.0):
    kwargs = {"check_same_thread": False, "timeout": 30} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, connect_args=kwargs)
    Base.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    app = FastAPI(title="Ageis synthetic notification service")
    app.state.engine, app.state.sessions = engine, sessions

    def find(session, org_id, key):
        return session.scalar(select(Notice).where(Notice.org_id == org_id, Notice.key == key))

    @app.post("/notices")
    def send(body: NoticeRequest, idempotency_key: str = Header(min_length=1, max_length=128)):
        digest, attempt_id = request_hash(body), str(uuid.uuid4())
        scenario, first = "normal", False
        with sessions.begin() as session:
            fault = session.scalar(select(Fault).where(Fault.org_id == body.org_id).with_for_update()) if enable_faults else None
            if fault:
                scenario, first = fault.scenario, fault.calls == 0
                fault.calls += 1
            session.add(Attempt(id=attempt_id, org_id=body.org_id, key=idempotency_key,
                                action_id=body.action_id, request_hash=digest, outcome="received"))
        if first and scenario == "before_commit":
            with sessions.begin() as session:
                session.get(Attempt, attempt_id).outcome = "aborted_before_commit"
            time.sleep(fault_delay)
            raise HTTPException(503, "Injected abort before commit")
        try:
            with sessions.begin() as session:
                notice = find(session, body.org_id, idempotency_key)
                if notice is None:
                    notice = Notice(id=str(uuid.uuid4()), org_id=body.org_id, key=idempotency_key,
                        action_id=body.action_id, request_hash=digest, recipient=body.recipient,
                        message=body.message, committed_at=datetime.now(timezone.utc))
                    session.add(notice)
                    session.flush()
                stored = result(notice)
        except IntegrityError:
            with sessions() as session:
                notice = find(session, body.org_id, idempotency_key)
                if notice is None:
                    raise
                stored = result(notice)
        outcome = "committed"
        if stored["request_hash"] != digest:
            outcome = "key_conflict"
        elif datetime.now(timezone.utc) - datetime.fromisoformat(stored["committed_at"]) >= timedelta(hours=1):
            outcome = "key_expired"
        with sessions.begin() as session:
            session.get(Attempt, attempt_id).outcome = outcome
        if outcome != "committed":
            raise HTTPException(409, outcome)
        if first and scenario in ("after_commit", "unresolved"):
            time.sleep(fault_delay)
        return stored

    @app.get("/notices/status")
    def status(org_id: str, idempotency_key: str):
        with sessions() as session:
            fault = session.get(Fault, org_id) if enable_faults else None
            if fault and fault.scenario == "unresolved":
                raise HTTPException(503, "Injected unavailable lookup")
            notice = find(session, org_id, idempotency_key)
            if notice is None:
                raise HTTPException(404, "No committed notice visible")
            return result(notice)

    @app.put("/test/scenario/{org_id}")
    def set_scenario(org_id: str, scenario: Literal["normal", "before_commit", "after_commit", "unresolved"]):
        if not enable_faults:
            raise HTTPException(404, "Not found")
        with sessions.begin() as session:
            if session.get(Fault, org_id) or session.scalar(select(Attempt).where(Attempt.org_id == org_id)):
                raise HTTPException(409, "Scenario already in use")
            session.add(Fault(org_id=org_id, scenario=scenario, calls=0))
        return {"scenario": scenario}

    @app.get("/test/evidence")
    def evidence(org_id: str):
        if not enable_faults:
            raise HTTPException(404, "Not found")
        with sessions() as session:
            attempts = session.scalars(select(Attempt).where(Attempt.org_id == org_id)).all()
            notices = session.scalars(select(Notice).where(Notice.org_id == org_id)).all()
            return {"attempts": [{"key": a.key, "action_id": a.action_id, "request_hash": a.request_hash, "outcome": a.outcome} for a in attempts],
                    "notices": [{**result(n), "recipient": n.recipient, "message": n.message} for n in notices]}

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    return app


def create_env_app():
    return create_app(os.environ["DATABASE_URL"], enable_faults=os.getenv("ENABLE_TEST_FAULTS", "false").lower() == "true")
