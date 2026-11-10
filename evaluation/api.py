"""Authenticated local-demo APIs for completed synthetic evaluations."""
from contextlib import contextmanager
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from runtime.db import EvaluationComparison, EvaluationDecision, EvaluationRun
from .cross import evaluate_cross
from .runner import compare, decide, evaluate, verify
from .store import EvaluationStore
from .jobs import JobConflict, JobService

# PostgreSQL advisory locks are scoped to this database and released with the connection.
EVALUATION_LOCK = 4189451101


class StartEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    suite: Literal["access-v1", "cross-v1"] = "cross-v1"
    candidate: Literal["safe-v1", "unsafe-retry-v1"]


class CreateComparison(BaseModel):
    model_config = ConfigDict(extra="forbid")
    baseline_id: str = Field(min_length=1, max_length=128)
    candidate_id: str = Field(min_length=1, max_length=128)


class CreateDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: Literal["approved", "rejected", "needs_review"]


class CreateEvaluationJob(BaseModel):
    model_config = ConfigDict(extra="forbid")
    suite: Literal["cross-v1"] = "cross-v1"
    candidate: Literal["safe-v1", "unsafe-retry-v1"]


@contextmanager
def evaluation_slot(engine):
    with engine.connect() as connection:
        acquired = connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": EVALUATION_LOCK})
        if not acquired:
            raise HTTPException(409, "An evaluation is already running")
        try:
            yield
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": EVALUATION_LOCK})


def verified_comparison(row):
    artifact = row.artifact
    try:
        verify(artifact, "comparison")
        if (row.id != artifact["id"] or row.evidence_hash != artifact["evidence_hash"] or
            row.baseline_id != artifact["baseline"]["id"] or row.candidate_id != artifact["candidate"]["id"] or
            artifact["deployment_performed"] is not False):
            raise ValueError("Comparison mismatch")
    except (ValueError, KeyError, TypeError):
        raise HTTPException(409, "Stored comparison evidence is invalid")
    return artifact


def comparison_artifact(session, comparison_id):
    row = session.get(EvaluationComparison, comparison_id)
    if row is None:
        raise HTTPException(404, "Comparison not found")
    return verified_comparison(row)


def verified_decision(row, comparison):
    try:
        result = row.artifact
        verify(result, "release_decision")
        if (row.id != result["id"] or row.evidence_hash != result["evidence_hash"] or
            row.comparison_id != comparison["id"] or result["comparison_id"] != comparison["id"] or
            result["comparison_hash"] != comparison["evidence_hash"] or
            result["deployment_performed"] is not False or
            result["decision"] not in ("approved", "rejected", "needs_review") or
            not isinstance(result["actor"], str) or not result["actor"] or
            not isinstance(result["created_at"], str) or not result["created_at"]):
            raise ValueError("Decision mismatch")
    except (ValueError, KeyError, TypeError):
        raise HTTPException(409, "Stored decision evidence is invalid")
    return result


def create_router(*, engine, sessions, database_url, identity_url, notice_url, authorize):
    router = APIRouter()
    store = EvaluationStore(sessions)

    def load_run(evaluation_id):
        try:
            artifact = store.load(evaluation_id)
        except (ValueError, KeyError, TypeError):
            raise HTTPException(409, "Stored evaluation evidence is invalid")
        if artifact is None:
            raise HTTPException(404, "Evaluation not found")
        return artifact

    def jobs():
        try:
            return JobService(sessions, {"access": identity_url, "notice": notice_url})
        except (ValueError, TypeError):
            raise HTTPException(503, "Synthetic evaluation services are not configured")

    @router.post("/evaluation-jobs", status_code=202)
    def create_job(body: CreateEvaluationJob, idempotency_key: str = Header(min_length=1, max_length=128), actor=Depends(authorize)):
        try:
            job, created = jobs().create(idempotency_key, body.candidate)
        except JobConflict:
            raise HTTPException(409, "Idempotency key belongs to another request")
        except ValueError:
            raise HTTPException(422, "Invalid evaluation job request")
        return {**job, "created": created}

    @router.get("/evaluation-jobs")
    def list_jobs(limit: int = Query(default=50, ge=1, le=100), actor=Depends(authorize)):
        try:
            return jobs().list(limit)
        except JobConflict:
            raise HTTPException(409, "Stored job checkpoints are invalid")

    @router.get("/evaluation-jobs/{job_id}")
    def get_job(job_id: str, actor=Depends(authorize)):
        try:
            job = jobs().get(job_id)
        except JobConflict:
            raise HTTPException(409, "Stored job checkpoints are invalid")
        if job is None:
            raise HTTPException(404, "Evaluation job not found")
        return job

    @router.post("/evaluation-jobs/{job_id}/cancel")
    def cancel_job(job_id: str, actor=Depends(authorize)):
        try:
            return jobs().cancel(job_id, actor)
        except LookupError:
            raise HTTPException(404, "Evaluation job not found")
        except JobConflict as error:
            raise HTTPException(409, str(error))

    @router.post("/evaluation-jobs/{job_id}/continue-inconclusive")
    def continue_inconclusive(job_id: str, actor=Depends(authorize)):
        try:
            return jobs().resolve_interrupted(job_id, actor)
        except LookupError:
            raise HTTPException(404, "Evaluation job not found")
        except JobConflict as error:
            raise HTTPException(409, str(error))

    @router.get("/evaluation-jobs/{job_id}/events")
    def job_events(job_id: str, actor=Depends(authorize)):
        try:
            return jobs().events(job_id)
        except LookupError:
            raise HTTPException(404, "Evaluation job not found")

    @router.post("/evaluations", status_code=201)
    def start(body: StartEvaluation, actor=Depends(authorize)):
        if not identity_url or (body.suite == "cross-v1" and not notice_url):
            raise HTTPException(503, "Synthetic evaluation services are not configured")
        with evaluation_slot(engine):
            try:
                if body.suite == "cross-v1":
                    result = evaluate_cross(database_url, {"access": identity_url, "notice": notice_url}, body.candidate)
                else:
                    result = evaluate(database_url, identity_url, body.candidate)
                store.save(result)
            except ValueError:
                raise HTTPException(503, "Synthetic evaluation configuration or evidence is invalid")
        return {"id": result["id"], "counts": result["counts"], "total": result["total"], "evidence_hash": result["evidence_hash"]}

    @router.get("/evaluations")
    def list_evaluations(limit: int = Query(default=50, ge=1, le=100), actor=Depends(authorize)):
        with sessions() as session:
            rows = session.scalars(select(EvaluationRun).order_by(EvaluationRun.created_at.desc(), EvaluationRun.id.desc()).limit(limit)).all()
            summaries = [{"id": row.id, "suite_id": row.suite_id, "candidate": row.candidate, "status": row.status,
                          "created_at": row.created_at, "evidence_hash": row.evidence_hash} for row in rows]
        for summary in summaries:
            artifact = load_run(summary["id"])
            if artifact["evidence_hash"] != summary["evidence_hash"]:
                raise HTTPException(409, "Stored evaluation evidence changed during listing")
            summary["counts"] = artifact["counts"]
        return summaries

    @router.get("/evaluations/{evaluation_id}")
    def get_evaluation(evaluation_id: str, actor=Depends(authorize)):
        return load_run(evaluation_id)

    @router.post("/evaluation-comparisons", status_code=201)
    def create_comparison(body: CreateComparison, actor=Depends(authorize)):
        baseline, candidate = load_run(body.baseline_id), load_run(body.candidate_id)
        try:
            result = compare(baseline, candidate)
        except (ValueError, KeyError, TypeError):
            raise HTTPException(409, "Evaluation evidence is incompatible or incomplete")
        with sessions.begin() as session:
            session.add(EvaluationComparison(id=result["id"], baseline_id=body.baseline_id, candidate_id=body.candidate_id,
                                             evidence_hash=result["evidence_hash"], artifact=result))
        return result

    @router.get("/evaluation-comparisons")
    def list_comparisons(limit: int = Query(default=50, ge=1, le=100), actor=Depends(authorize)):
        with sessions() as session:
            rows = session.execute(
                select(EvaluationComparison, EvaluationDecision)
                .outerjoin(EvaluationDecision, EvaluationDecision.comparison_id == EvaluationComparison.id)
                .order_by(EvaluationComparison.artifact["created_at"].as_string().desc().nullslast(),
                          EvaluationComparison.id.desc()).limit(limit)
            ).all()
            summaries = []
            for row, decision_row in rows:
                artifact = verified_comparison(row)
                try:
                    created_at = artifact["created_at"]
                    baseline, candidate = artifact["baseline"], artifact["candidate"]
                    suite_id = baseline["manifest"]["suite_id"]
                    if any(not isinstance(value, str) or not value for value in
                           (created_at, suite_id, baseline["manifest"]["candidate"], candidate["manifest"]["candidate"])):
                        raise ValueError("Invalid comparison summary")
                    decision = verified_decision(decision_row, artifact) if decision_row else None
                    summaries.append({"id": row.id, "created_at": created_at, "suite_id": suite_id,
                        "baseline_id": row.baseline_id, "candidate_id": row.candidate_id,
                        "baseline": baseline["manifest"]["candidate"], "candidate": candidate["manifest"]["candidate"],
                        "baseline_counts": baseline["counts"], "candidate_counts": candidate["counts"],
                        "decision": ({"decision": decision["decision"], "actor": decision["actor"],
                                      "created_at": decision["created_at"]} if decision else None)})
                except (KeyError, TypeError, ValueError):
                    raise HTTPException(409, "Stored comparison evidence is invalid")
            return summaries

    @router.get("/evaluation-comparisons/{comparison_id}")
    def get_comparison(comparison_id: str, actor=Depends(authorize)):
        with sessions() as session:
            return comparison_artifact(session, comparison_id)

    @router.post("/evaluation-comparisons/{comparison_id}/decision", status_code=201)
    def create_decision(comparison_id: str, body: CreateDecision, actor=Depends(authorize)):
        try:
            with sessions.begin() as session:
                comparison = comparison_artifact(session, comparison_id)
                result = decide(comparison, body.decision, actor)
                session.add(EvaluationDecision(id=result["id"], comparison_id=comparison_id,
                                               evidence_hash=result["evidence_hash"], artifact=result))
                session.flush()
        except IntegrityError:
            raise HTTPException(409, "A decision has already been recorded for this comparison")
        return result

    @router.get("/evaluation-comparisons/{comparison_id}/decision")
    def get_decision(comparison_id: str, actor=Depends(authorize)):
        with sessions() as session:
            comparison = comparison_artifact(session, comparison_id)
            row = session.scalar(select(EvaluationDecision).where(EvaluationDecision.comparison_id == comparison_id))
            if row is None:
                raise HTTPException(404, "Decision not found")
            return verified_decision(row, comparison)

    return router
