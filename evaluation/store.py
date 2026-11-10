"""Atomic storage for completed synthetic evaluation artifacts."""
from datetime import datetime
import json
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from runtime.db import EvaluationRun, EvaluationScenario
from .runner import digest, verify

STATUSES = ("passed", "failed", "error", "inconclusive")


class EvaluationConflict(ValueError):
    pass


def validate_artifact(artifact):
    verify(artifact, "evaluation")
    try:
        # PostgreSQL JSON must not receive NaN or Infinity, even in nested evidence.
        json.dumps(artifact, allow_nan=False)
        manifest, results, counts = artifact["manifest"], artifact["results"], artifact["counts"]
        created_at = datetime.fromisoformat(artifact["created_at"])
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError("Timestamp must include a timezone")
        if (not isinstance(artifact["id"], str) or not artifact["id"] or len(artifact["id"]) > 128 or
            not isinstance(manifest, dict) or not isinstance(results, list) or not results or
            not isinstance(manifest["suite_id"], str) or not manifest["suite_id"] or
            not isinstance(manifest["candidate"], str) or not manifest["candidate"]):
            raise ValueError("Invalid evaluation identity")
        names = [item["scenario"] for item in results]
        if (any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names) or
            names != manifest["scenario_ids"]):
            raise ValueError("Scenario identities are incomplete or inconsistent")
        for item in results:
            status = item["status"]
            if status not in STATUSES or not isinstance(item.get("failed_assertions"), list):
                raise ValueError("Invalid scenario status")
            if (any(not isinstance(value, str) or not value for value in item["failed_assertions"]) or
                (status == "passed" and item["failed_assertions"]) or
                (status == "failed" and not item["failed_assertions"]) or
                (status in ("error", "inconclusive") and
                 (item["failed_assertions"] or not isinstance(item.get("reason"), str) or not item["reason"]))):
                raise ValueError("Scenario outcome is inconsistent")
            if status in ("passed", "failed") and not isinstance(item.get("evidence"), dict):
                raise ValueError("Scenario evidence is missing")
            if "evidence" in item and not isinstance(item["evidence"], dict):
                raise ValueError("Invalid scenario evidence")
        actual = {status: sum(item["status"] == status for item in results) for status in STATUSES}
        if counts != actual or type(artifact["total"]) is not int or artifact["total"] != len(results):
            raise ValueError("Scenario counts are inconsistent")
        return created_at, next((status for status in ("error", "failed", "inconclusive") if actual[status]), "passed")
    except (KeyError, TypeError, OverflowError) as error:
        raise ValueError("Invalid evaluation artifact") from error


class EvaluationStore:
    def __init__(self, sessions):
        self.sessions = sessions

    def save(self, artifact):
        try:
            with self.sessions.begin() as session:
                self.save_in_transaction(session, artifact)
            return True
        except IntegrityError:
            # A duplicate id is an idempotent replay only if all stored evidence matches.
            existing = self.load(artifact["id"])
            if existing is None:
                raise
            if existing != artifact:
                raise EvaluationConflict("Evaluation ID already belongs to different evidence")
            return False

    @staticmethod
    def save_in_transaction(session, artifact):
        created_at, status = validate_artifact(artifact)
        header = {key: value for key, value in artifact.items() if key != "results"}
        session.add(EvaluationRun(id=artifact["id"], suite_id=artifact["manifest"]["suite_id"],
            candidate=artifact["manifest"]["candidate"], status=status, created_at=created_at,
            evidence_hash=artifact["evidence_hash"], header=header))
        session.flush()
        for position, item in enumerate(artifact["results"]):
            evidence = item.get("evidence")
            session.add(EvaluationScenario(evaluation_id=artifact["id"], position=position,
                scenario=item["scenario"], status=item["status"],
                evidence_hash=digest(evidence) if evidence is not None else None, result=item))

    def load(self, evaluation_id):
        with self.sessions() as session:
            run = session.get(EvaluationRun, evaluation_id)
            if run is None:
                return None
            rows = session.scalars(select(EvaluationScenario).where(EvaluationScenario.evaluation_id == evaluation_id)
                                   .order_by(EvaluationScenario.position)).all()
            if [row.position for row in rows] != list(range(len(rows))):
                raise ValueError("Stored evaluation scenarios are incomplete")
            for row in rows:
                evidence = row.result.get("evidence")
                if (row.scenario != row.result.get("scenario") or row.status != row.result.get("status") or
                    row.evidence_hash != (digest(evidence) if evidence is not None else None)):
                    raise ValueError("Stored scenario evidence is inconsistent")
            artifact = {**run.header, "results": [row.result for row in rows]}
            created_at, status = validate_artifact(artifact)
            if (run.id != artifact["id"] or run.evidence_hash != artifact["evidence_hash"] or
                run.suite_id != artifact["manifest"]["suite_id"] or run.candidate != artifact["manifest"]["candidate"] or
                run.status != status or run.created_at != created_at):
                raise ValueError("Stored evaluation metadata is inconsistent")
            return artifact
