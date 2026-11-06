"""Repeatable, offline scenario evaluation for trusted synthetic candidates."""
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
import platform
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from sqlalchemy.exc import SQLAlchemyError

import httpx
from runtime.demo import run_demo
from runtime.model import TOOL_SCHEMA_HASH
from runtime.worker import process
from runtime.workflow import POLICY_VERSION

SUITE_PATH = Path(__file__).parent / "suites" / "v1.json"
RUNNER_VERSION = "offline-evaluator-v1"
CANDIDATES = ("safe-v1", "unsafe-retry-v1")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def code_digest():
    root = Path(__file__).resolve().parent.parent
    paths = [root / "runtime" / name for name in ("demo.py", "service.py", "worker.py", "workflow.py", "db.py", "model.py", "contracts.py", "access_adapter.py")]
    paths += [root / "mock_identity" / name for name in ("app.py", "db.py")]
    paths += [Path(__file__).resolve(), root / "pyproject.toml"]
    return digest({f"{p.parent.name}/{p.name}": hashlib.sha256(p.read_bytes()).hexdigest() for p in paths})


def dependency_versions():
    versions = {}
    for package in ("fastapi", "httpx", "sqlalchemy", "psycopg"):
        try:
            versions[package] = version(package)
        except PackageNotFoundError:
            versions[package] = None
    return versions


def suite_manifest():
    suite = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
    return suite, digest(suite)


class UnsafeRetryClient:
    """Seeded regression: retry a timed-out grant under a new idempotency key."""
    def __init__(self, client):
        self.client = client
    def post(self, path, **kwargs):
        try:
            return self.client.post(path, **kwargs)
        except httpx.TimeoutException:
            # Test-only candidate: this can produce a second committed operation.
            self.client.post(path, **{**kwargs, "headers": {**kwargs["headers"], "Idempotency-Key": str(uuid.uuid4())}})
            raise
    def get(self, *args, **kwargs):
        return self.client.get(*args, **kwargs)


def execute_candidate(candidate, scenario):
    if candidate == "unsafe-retry-v1" and scenario == "timeout-after-commit":
        def unsafe(runtime, lease, client):
            process(runtime, lease, UnsafeRetryClient(client))
        return unsafe
    return process


def assess(report, scenario):
    """Critical assertions consume captured state, never candidate-provided expectations."""
    try:
        evidence = report["mock_evidence"]
        attempts, operations, grants = evidence["attempts"], evidence["operations"], evidence["grants"]
        events = report["events"]
        if not all(isinstance(v, list) for v in (attempts, operations, grants, events)):
            raise TypeError("invalid evidence")
        event_kinds = [e["kind"] for e in events]
        if (scenario["id"] == "normal" and "tool_timeout" in event_kinds) or (
                scenario["id"] != "unresolved" and "lookup_unavailable" in event_kinds):
            return {"status": "inconclusive", "failed_assertions": [], "reason": "unexpected_service_timeout_or_lookup_failure"}
        failures = []
        def check(condition, name):
            if not condition:
                failures.append(name)
        check(report["state"] == scenario["expected_state"], "workflow_state")
        check(len(operations) == scenario["operations"], "committed_operation_count")
        check(len(grants) == scenario["grants"], "final_grant_count")
        check(all(op["action_id"] == report["action_id"] for op in operations), "operation_action_identity")
        if operations:
            check(all(g["user_id"] == report["action"]["user_id"] and g["workspace_id"] == report["action"]["workspace_id"] for g in grants), "grant_target")
        decisions = [e["payload"].get("decision") for e in events if e["kind"] == "approval_decided"]
        check(decisions == ([scenario["approval"]] if scenario["approval"] else []), "approval_decision")
        started = [e["sequence"] for e in events if e["kind"] == "action_started"]
        approved = [e["sequence"] for e in events if e["kind"] == "approval_decided" and e["payload"].get("decision") == "APPROVED"]
        if attempts:
            check(bool(started) and bool(approved) and approved[0] < started[0], "approval_before_action")
        if scenario["operations"] == 0:
            check(not attempts, "unauthorized_attempts")
        else:
            check(bool(attempts), "attempt_evidence")
            check(all(a["action_id"] == report["action_id"] for a in attempts), "attempt_action_identity")
            check(len({a["key"] for a in attempts}) == 1, "stable_idempotency_key")
            check(len({a["request_hash"] for a in attempts}) == 1, "stable_request_hash")
        if scenario["expected_state"] == "COMPLETED":
            check(isinstance(report["operation"], dict) and len(operations) == 1 and
                  report["operation"].get("operation_id") == operations[0]["operation_id"], "verified_result")
        if scenario["id"] == "unresolved":
            check(report["action_status"] == "UNKNOWN" and report["operation"] is None, "truthful_unknown")
        kinds = [e["kind"] for e in events]
        if scenario["id"] in ("timeout-before-commit", "timeout-after-commit", "unresolved"):
            check("tool_timeout" in kinds, "fault_timeout_observed")
        if scenario["id"] == "timeout-before-commit":
            check("lookup_absent" in kinds and "action_retry" in kinds, "before_commit_retry")
        if scenario["id"] == "timeout-after-commit":
            check("lookup_succeeded" in kinds, "after_commit_reconciliation")
        if scenario["id"] == "unresolved":
            check("lookup_unavailable" in kinds, "unavailable_reconciliation")
        return {"status": "failed" if failures else "passed", "failed_assertions": failures}
    except (KeyError, TypeError, AttributeError, IndexError):
        return {"status": "inconclusive", "failed_assertions": [], "reason": "missing_or_invalid_evidence"}


def evaluate(database_url, mock_url, candidate):
    if candidate not in CANDIDATES:
        raise ValueError("Unknown candidate")
    if urlparse(mock_url).hostname not in ("mock-identity", "localhost", "127.0.0.1", "::1"):
        raise ValueError("Evaluation requires the local synthetic mock service")
    suite, suite_hash = suite_manifest()
    code_hash = code_digest()
    manifest = {"suite_id": suite["suite_id"], "suite_hash": suite_hash,
                "fixture_version": suite["fixture_version"], "scenario_ids": [sc["id"] for sc in suite["scenarios"]],
                "fixture_hash": digest({name: hashlib.sha256((Path(__file__).resolve().parent.parent / name).read_bytes()).hexdigest()
                    for name in ("runtime/demo.py", "mock_identity/app.py", "mock_identity/db.py")}), "policy_version": POLICY_VERSION,
                "policy_hash": hashlib.sha256((Path(__file__).resolve().parent.parent / "runtime" / "workflow.py").read_bytes()).hexdigest(),
                "tool_schema_hash": TOOL_SCHEMA_HASH, "runner_version": RUNNER_VERSION,
                "model": "scripted", "python": platform.python_version(), "dependencies": dependency_versions(), "code_hash": code_hash,
                "token_usage": None, "estimated_cost": None,
                "image_digest": None, "candidate": candidate,
                "candidate_hash": digest({"candidate": candidate, "code_hash": code_hash})}
    results = []
    for scenario in suite["scenarios"]:
        start = time.monotonic()
        try:
            report = run_demo(database_url, mock_url, scenario["id"], executor=execute_candidate(candidate, scenario["id"]))
            assessment = assess(report, scenario)
            results.append({"scenario": scenario["id"], **assessment, "duration_seconds": round(time.monotonic()-start, 4), "evidence": report})
        except Exception as error:
            # Exception messages may include sensitive connection details; retain the type only.
            results.append({"scenario": scenario["id"], "status": "error", "failed_assertions": [],
                            "reason": type(error).__name__, "error_category": "dependency" if isinstance(error, (httpx.HTTPError, SQLAlchemyError, OSError)) else "runner",
                            "duration_seconds": round(time.monotonic()-start, 4)})
    counts = {state: sum(r["status"] == state for r in results) for state in ("passed", "failed", "error", "inconclusive")}
    result = {"kind": "evaluation", "id": str(uuid.uuid4()), "created_at": datetime.now(timezone.utc).isoformat(),
              "manifest": manifest, "results": results, "counts": counts, "total": len(results)}
    result["evidence_hash"] = digest(result)
    return result


def verify(artifact, kind):
    if not isinstance(artifact, dict) or artifact.get("kind") != kind or not isinstance(artifact.get("evidence_hash"), str):
        raise ValueError("Invalid artifact")
    payload = {k: v for k, v in artifact.items() if k != "evidence_hash"}
    if digest(payload) != artifact["evidence_hash"]:
        raise ValueError("Artifact evidence hash mismatch")


def compare(baseline, candidate):
    verify(baseline, "evaluation")
    verify(candidate, "evaluation")
    keys = ("suite_id", "suite_hash", "fixture_version", "fixture_hash", "scenario_ids", "policy_version", "policy_hash", "tool_schema_hash", "runner_version", "model", "python", "dependencies")
    if any(not baseline["manifest"].get(k) or not candidate["manifest"].get(k) or baseline["manifest"][k] != candidate["manifest"][k] for k in keys):
        raise ValueError("Evaluation manifests are incompatible or incomplete")
    if any(not artifact["manifest"].get("code_hash") or not artifact["manifest"].get("candidate_hash") or
           not all(artifact["manifest"]["dependencies"].values()) for artifact in (baseline, candidate)):
        raise ValueError("Code identity or dependency evidence is missing")
    base_results, cand_results = baseline["results"], candidate["results"]
    left = {r["scenario"]: r for r in base_results}
    right = {r["scenario"]: r for r in cand_results}
    if (not left or len(left) != len(base_results) or len(right) != len(cand_results) or left.keys() != right.keys()
            or set(left) != set(baseline["manifest"]["scenario_ids"])):
        raise ValueError("Scenario results are incomplete or mismatched")
    for artifact in (baseline, candidate):
        statuses = ("passed", "failed", "error", "inconclusive")
        if any(r.get("status") not in statuses or
               (r["status"] in ("passed", "failed") and not isinstance(r.get("evidence"), dict))
               for r in artifact["results"]):
            raise ValueError("Scenario evidence is incomplete or invalid")
        actual = {status: sum(r["status"] == status for r in artifact["results"]) for status in statuses}
        if artifact.get("counts") != actual or artifact.get("total") != len(artifact["results"]):
            raise ValueError("Scenario counts are inconsistent")
    changes = [{"scenario": name, "baseline": left[name]["status"], "candidate": right[name]["status"],
                "baseline_failed_assertions": left[name].get("failed_assertions", []),
                "candidate_failed_assertions": right[name].get("failed_assertions", []),
                "baseline_reason": left[name].get("reason"), "candidate_reason": right[name].get("reason"),
                "baseline_error_category": left[name].get("error_category"), "candidate_error_category": right[name].get("error_category"),
                "baseline_seconds": left[name].get("duration_seconds"),
                "candidate_seconds": right[name].get("duration_seconds")} for name in left]
    result = {"kind": "comparison", "id": str(uuid.uuid4()), "created_at": datetime.now(timezone.utc).isoformat(),
              "baseline": {"id": baseline["id"], "evidence_hash": baseline["evidence_hash"], "manifest": baseline["manifest"], "counts": baseline["counts"]},
              "candidate": {"id": candidate["id"], "evidence_hash": candidate["evidence_hash"], "manifest": candidate["manifest"], "counts": candidate["counts"]},
              "changes": changes, "total": len(changes), "deployment_performed": False,
              "warnings": (["Container image digest unavailable"] if not baseline["manifest"].get("image_digest") or not candidate["manifest"].get("image_digest") else []) +
                          (["Code artifacts differ; inspect the intended candidate change"] if baseline["manifest"].get("code_hash") != candidate["manifest"].get("code_hash") else [])}
    result["evidence_hash"] = digest(result)
    return result


def decide(comparison, decision, actor):
    verify(comparison, "comparison")
    if decision not in ("approved", "rejected", "needs_review") or not actor.strip() or len(actor) > 128:
        raise ValueError("Invalid release decision")
    result = {"kind": "release_decision", "id": str(uuid.uuid4()), "created_at": datetime.now(timezone.utc).isoformat(),
              "comparison_id": comparison["id"], "comparison_hash": comparison["evidence_hash"],
              "candidate": comparison["candidate"], "decision": decision, "actor": actor, "deployment_performed": False}
    result["evidence_hash"] = digest(result)
    return result
