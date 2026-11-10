"""PostgreSQL persistence for the trusted, synthetic demo runtime."""
from datetime import datetime
import re
from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class Run(Base):
    __tablename__ = "runs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    state: Mapped[str] = mapped_column(String, nullable=False)
    request: Mapped[dict] = mapped_column(JSON, nullable=False)
    action: Mapped[dict] = mapped_column(JSON, nullable=False)
    action_hash: Mapped[str] = mapped_column(String, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    action_status: Mapped[str] = mapped_column(String, nullable=False, default="NOT_SENT")
    operation: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    event_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), nullable=False)
    approval_id: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)
    state: Mapped[str] = mapped_column(String, nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fence: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    claims: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class Approval(Base):
    __tablename__ = "approvals"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), nullable=False)
    action_hash: Mapped[str] = mapped_column(String, nullable=False)
    action: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    actor: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("run_id", "sequence"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ModelSession(Base):
    __tablename__ = "model_sessions"
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), primary_key=True)
    model_id: Mapped[str] = mapped_column(String, nullable=False)
    prompt_hash: Mapped[str] = mapped_column(String, nullable=False)
    tool_schema_hash: Mapped[str] = mapped_column(String, nullable=False)
    messages: Mapped[list] = mapped_column(JSON, nullable=False)
    question: Mapped[str | None] = mapped_column(String, nullable=True)
    model_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=4)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    usage_missing: Mapped[bool] = mapped_column(default=False, nullable=False)
    accounted_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)


class EvaluationRun(Base):
    __tablename__ = "evaluation_runs"
    __table_args__ = (CheckConstraint("status IN ('passed', 'failed', 'error', 'inconclusive')"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    suite_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    candidate: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    header: Mapped[dict] = mapped_column(JSON, nullable=False)


class EvaluationScenario(Base):
    __tablename__ = "evaluation_scenarios"
    __table_args__ = (UniqueConstraint("evaluation_id", "scenario"),
                      CheckConstraint("status IN ('passed', 'failed', 'error', 'inconclusive')"),
                      CheckConstraint("position >= 0"))
    evaluation_id: Mapped[str] = mapped_column(ForeignKey("evaluation_runs.id"), primary_key=True)
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    scenario: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    evidence_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result: Mapped[dict] = mapped_column(JSON, nullable=False)


class EvaluationComparison(Base):
    __tablename__ = "evaluation_comparisons"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    baseline_id: Mapped[str] = mapped_column(ForeignKey("evaluation_runs.id"), nullable=False)
    candidate_id: Mapped[str] = mapped_column(ForeignKey("evaluation_runs.id"), nullable=False)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact: Mapped[dict] = mapped_column(JSON, nullable=False)


class EvaluationDecision(Base):
    __tablename__ = "evaluation_decisions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    comparison_id: Mapped[str] = mapped_column(ForeignKey("evaluation_comparisons.id"), nullable=False, unique=True)
    evidence_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact: Mapped[dict] = mapped_column(JSON, nullable=False)


class EvaluationJob(Base):
    __tablename__ = "evaluation_jobs"
    __table_args__ = (CheckConstraint("state IN ('QUEUED', 'RUNNING', 'COMPLETED', 'INTERRUPTED', 'FAILED')"),)
    id: Mapped[str] = mapped_column(String, primary_key=True)
    request_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    candidate: Mapped[str] = mapped_column(String, nullable=False)
    manifest: Mapped[dict] = mapped_column(JSON, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String, nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claims: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    in_flight: Mapped[str | None] = mapped_column(String, nullable=True)
    fixture: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    results: Mapped[list] = mapped_column(JSON, nullable=False)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    result_id: Mapped[str | None] = mapped_column(ForeignKey("evaluation_runs.id"), nullable=True)


class EvaluationJobEvent(Base):
    __tablename__ = "evaluation_job_events"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("evaluation_jobs.id"), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String, nullable=False)
    actor: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)


def make_database(url, *, schema="ageis_runtime"):
    if not url.startswith("postgresql"):
        raise ValueError("The durable runtime requires PostgreSQL")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
        raise ValueError("Invalid schema name")
    engine = create_engine(url, pool_pre_ping=True, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        Base.metadata.create_all(connection)
    return engine, sessionmaker(engine, expire_on_commit=False)
