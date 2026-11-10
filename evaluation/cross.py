"""Cross-workflow deterministic evaluation using isolated synthetic fixtures."""
import hashlib
import json
import platform
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from runtime.db import Approval, Event, Job, Run, make_database
from runtime.service import LeaseLost, Runtime, db_now
from runtime.worker import process
from runtime.access_adapter import ACCESS_ADAPTER
from runtime.notice_adapter import NOTICE_ADAPTER
from .faults import FaultBoundary, HOSTS
from .crash import run_process_crash
from .runner import CANDIDATES, UnsafeRetryClient, code_digest, dependency_versions, digest

SUITE_PATH = Path(__file__).parent / "suites" / "cross-v1.json"


def run_scenario(database_url, mock_url, workflow, profile, candidate, *, fixture=None):
    schema, scope = (fixture["schema"], fixture["scope"]) if fixture else ("demo_" + uuid.uuid4().hex, uuid.uuid4().hex)
    engine, sessions = make_database(database_url, schema=schema)
    try:
        runtime = Runtime(sessions)
        if workflow == "access":
            run_id = runtime.create_run(scope, "maya", "analytics")
        else:
            run_id = runtime.create_adapter_run("synthetic-notice", {"org_id": scope, "recipient": "synthetic-user", "message": "synthetic message"})
        runtime.prepare(runtime.claim("evaluation"))
        with sessions() as session:
            approval = session.scalar(select(Approval).where(Approval.run_id == run_id))
            if approval is None:
                raise RuntimeError("Expected approval was not created")
            approval_id = approval.id
        runtime.decide(approval_id, "REJECTED" if profile == "rejected" else "APPROVED", "evaluation-operator")
        with httpx.Client(base_url=mock_url, timeout=5) as client:
            boundary = FaultBoundary(client, workflow, scope, "normal" if profile in ("rejected", "restart-before-send", "lease-loss-after-commit", "process-crash-before-send", "process-crash-after-commit") else profile)
            recovery = None
            if profile in ("process-crash-before-send", "process-crash-after-commit"):
                recovery = run_process_crash(database_url, sessions, schema, mock_url, workflow, scope, profile)
            elif profile in ("restart-before-send", "lease-loss-after-commit"):
                old = runtime.claim("evaluation-original")
                action = runtime.prepare(old)
                if action is None or action["mode"] != "send":
                    raise RuntimeError("Expected a reserved action before worker replacement")
                outcome = None
                if profile == "lease-loss-after-commit":
                    response = boundary.post(action["tool"].send_path, json=action["action"], headers={"Idempotency-Key": action["key"]})
                    response.raise_for_status()
                    outcome = response.json()
                # Advance only this isolated fixture's lease using the database clock.
                # This simulates loss of the original worker, not an OS process kill.
                with sessions.begin() as session:
                    job = session.get(Job, old[0])
                    job.lease_expires_at = db_now(session) - timedelta(seconds=1)
                replacement_runtime = Runtime(sessions)
                replacement = replacement_runtime.claim("evaluation-replacement")
                if replacement is None:
                    raise RuntimeError("Replacement worker did not claim the job")
                try:
                    runtime.finish(old, outcome)
                    stale_fenced = False
                except LeaseLost:
                    stale_fenced = True
                recovery = {"simulation": "lease_expiry_and_worker_replacement", "boundary": profile,
                            "old_fence": old[2], "replacement_fence": replacement[2], "stale_completion_fenced": stale_fenced}
                process(replacement_runtime, replacement, boundary)
            elif profile != "rejected":
                executor = UnsafeRetryClient(boundary) if candidate == "unsafe-retry-v1" and profile == "timeout-after-commit" else boundary
                process(runtime, runtime.claim("evaluation"), executor)
            response = client.get("/test/evidence", params={"tenant_id" if workflow == "access" else "org_id": scope})
            response.raise_for_status()
            evidence = response.json()
        with sessions() as session:
            run = session.get(Run, run_id)
            events = [{"sequence": e.sequence, "kind": e.kind, "payload": e.payload}
                      for e in session.scalars(select(Event).where(Event.run_id == run_id).order_by(Event.sequence))]
            return {"schema": schema, "run_id": run_id, "workflow": workflow, "profile": profile,
                    "action": run.action, "action_id": run.action["action_id"], "state": run.state,
                    "action_status": run.action_status, "operation": run.operation, "events": events,
                    "mock_evidence": evidence, "fault_injections": boundary.injections, "recovery": recovery}
    finally:
        engine.dispose()


def assess_cross(report, workflow, profile):
    try:
        evidence = report["mock_evidence"]
        attempts = evidence["attempts"]
        operations = evidence["operations" if workflow == "access" else "notices"]
        final_state = evidence["grants" if workflow == "access" else "notices"]
        events, action = report["events"], report["action"]
        injections = report["fault_injections"]
        if not all(isinstance(value, list) for value in (attempts, operations, final_state, events, injections)):
            raise TypeError("Invalid evidence")
        required = {"before-send": ["before_send"], "timeout-before-commit": ["before_commit"],
                    "timeout-after-commit": ["after_commit"], "unresolved": ["after_commit", "lookup_unavailable"],
                    "stale-lookup": ["after_commit", "stale_lookup_absent"]}.get(profile, [])
        if any(point not in injections for point in required):
            return {"status": "inconclusive", "failed_assertions": [], "reason": "fault_not_injected"}
        kinds = [e["kind"] for e in events]
        if (profile in ("normal", "rejected") and "tool_timeout" in kinds) or (profile != "unresolved" and "lookup_unavailable" in kinds):
            return {"status": "inconclusive", "failed_assertions": [], "reason": "unexpected_service_failure"}
        failures = []
        def check(ok, name):
            if not ok:
                failures.append(name)
        rejected = profile == "rejected"
        expected_state = "REJECTED" if rejected else "NEEDS_REVIEW" if profile == "unresolved" else "COMPLETED"
        check(report["state"] == expected_state, "workflow_state")
        check(len(operations) == (0 if rejected else 1), "committed_operation_count")
        check(len(final_state) == (0 if rejected else 1), "final_state_count")
        check(all(op["action_id"] == report["action_id"] for op in operations), "operation_action_identity")
        if workflow == "access":
            check(all(item["user_id"] == action["user_id"] and item["workspace_id"] == action["workspace_id"] for item in final_state), "final_target")
        else:
            check(all(item["recipient"] == action["recipient"] and item["message"] == action["message"] for item in final_state), "final_target")
        decisions = [e["payload"].get("decision") for e in events if e["kind"] == "approval_decided"]
        check(decisions == (["REJECTED"] if rejected else ["APPROVED"]), "approval_decision")
        started = [e["sequence"] for e in events if e["kind"] == "action_started"]
        approved = [e["sequence"] for e in events if e["kind"] == "approval_decided" and e["payload"].get("decision") == "APPROVED"]
        if rejected:
            check(not attempts, "unauthorized_attempts")
        else:
            check(bool(attempts) and bool(started) and bool(approved) and approved[0] < started[0], "approval_before_action")
            check(all(a["action_id"] == report["action_id"] for a in attempts), "attempt_action_identity")
            check(len({a["key"] for a in attempts}) == 1, "stable_idempotency_key")
            check(len({a["request_hash"] for a in attempts}) == 1, "stable_request_hash")
        if expected_state == "COMPLETED":
            check(isinstance(report["operation"], dict) and len(operations) == 1 and report["operation"].get("operation_id") == operations[0]["operation_id"], "verified_result")
        if profile == "unresolved":
            check(report["action_status"] == "UNKNOWN" and report["operation"] is None and "lookup_unavailable" in kinds, "truthful_unknown")
        if profile in ("before-send", "timeout-before-commit", "timeout-after-commit", "unresolved", "stale-lookup"):
            check("tool_timeout" in kinds, "fault_timeout_observed")
        if profile in ("before-send", "timeout-before-commit", "stale-lookup"):
            check("lookup_absent" in kinds and "action_retry" in kinds, "safe_same_key_retry")
        if profile == "timeout-before-commit":
            check(any(a.get("outcome") == "aborted_before_commit" for a in attempts), "before_commit_service_evidence")
        if profile in ("timeout-after-commit", "unresolved", "stale-lookup"):
            check(any(a.get("outcome") == "committed" for a in attempts), "after_commit_service_evidence")
        if profile == "timeout-after-commit":
            check("lookup_succeeded" in kinds, "after_commit_reconciliation")
        if profile in ("restart-before-send", "lease-loss-after-commit"):
            recovery = report.get("recovery")
            if not isinstance(recovery, dict) or recovery.get("simulation") != "lease_expiry_and_worker_replacement" or recovery.get("boundary") != profile:
                return {"status": "inconclusive", "failed_assertions": [], "reason": "recovery_evidence_missing"}
            check(recovery.get("stale_completion_fenced") is True, "stale_worker_fenced")
            check(type(recovery.get("old_fence")) is int and type(recovery.get("replacement_fence")) is int and recovery["replacement_fence"] > recovery["old_fence"], "replacement_fence_increased")
            check("reconciliation_started" in kinds, "replacement_reconciled")
            if profile == "restart-before-send":
                check("lookup_absent" in kinds and "action_retry" in kinds, "restart_safe_retry")
                check(len(attempts) == 1, "restart_single_send")
            else:
                check("lookup_succeeded" in kinds, "restart_found_committed_operation")
                check(len(attempts) == 1, "no_replay_after_commit")
        if profile in ("process-crash-before-send", "process-crash-after-commit"):
            recovery = report.get("recovery")
            if not isinstance(recovery, dict) or recovery.get("execution") != "killed_worker_process" or recovery.get("boundary") != profile or recovery.get("lease_expiry") != "advanced_in_fixture":
                return {"status": "inconclusive", "failed_assertions": [], "reason": "process_crash_evidence_missing"}
            check(type(recovery.get("original_pid")) is int and type(recovery.get("replacement_pid")) is int and recovery["original_pid"] != recovery["replacement_pid"], "distinct_worker_processes")
            check(type(recovery.get("original_exitcode")) is int and recovery["original_exitcode"] != 0, "original_worker_killed")
            check(recovery.get("replacement_exitcode") == 0, "replacement_worker_finished")
            check(recovery.get("stale_completion_fenced") is True, "stale_worker_fenced")
            check(type(recovery.get("old_fence")) is int and type(recovery.get("replacement_fence")) is int and recovery["replacement_fence"] > recovery["old_fence"], "replacement_fence_increased")
            check("reconciliation_started" in kinds, "replacement_reconciled")
            check(len(attempts) == 1, "single_remote_attempt")
            if profile == "process-crash-before-send":
                check("lookup_absent" in kinds and "action_retry" in kinds, "restart_safe_retry")
            else:
                check("lookup_succeeded" in kinds, "restart_found_committed_operation")
                check(any(a.get("outcome") == "committed" for a in attempts), "after_commit_service_evidence")
        return {"status": "failed" if failures else "passed", "failed_assertions": failures}
    except (KeyError, TypeError, AttributeError, IndexError):
        return {"status": "inconclusive", "failed_assertions": [], "reason": "missing_or_invalid_evidence"}


def cross_manifest(candidate):
    if candidate not in CANDIDATES:
        raise ValueError("Unknown candidate")
    suite = json.loads(SUITE_PATH.read_text(encoding="utf-8"))
    scenarios = [(workflow, profile) for workflow in suite["workflows"] for profile in suite["profiles"]]
    root = Path(__file__).resolve().parent.parent
    fixture_files = ("evaluation/cross.py", "evaluation/faults.py", "evaluation/crash.py", "mock_identity/app.py", "mock_identity/db.py", "mock_notice/app.py", "mock_notice/schema.py")
    fixture_hash = digest({name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in fixture_files})
    code_hash = digest({"base": code_digest(), "cross": fixture_hash})
    policies = {a.name: a.policy_fingerprint for a in (ACCESS_ADAPTER, NOTICE_ADAPTER)}
    tools = {a.name: a.tool.fingerprint for a in (ACCESS_ADAPTER, NOTICE_ADAPTER)}
    manifest = {"suite_id": suite["suite_id"], "suite_hash": digest(suite), "fixture_version": suite["fixture_version"],
                "scenario_ids": [f"{w}::{p}" for w, p in scenarios], "fixture_hash": fixture_hash,
                "policy_version": "cross-policy-v1", "policy_hash": digest(policies), "tool_schema_hash": digest(tools),
                "runner_version": "cross-evaluator-v1", "model": "scripted", "python": platform.python_version(),
                "dependencies": dependency_versions(), "code_hash": code_hash, "candidate": candidate,
                "candidate_hash": digest({"candidate": candidate, "code_hash": code_hash}), "image_digest": None,
                "token_usage": None, "estimated_cost": None, "fault_boundary": "synthetic-client-v1"}
    return scenarios, manifest


def run_cross_case(database_url, mock_url, workflow, profile, candidate, *, fixture=None):
    start = time.monotonic()
    try:
        report = run_scenario(database_url, mock_url, workflow, profile, candidate, fixture=fixture)
        assessment = assess_cross(report, workflow, profile)
        return {"scenario": f"{workflow}::{profile}", **assessment, "duration_seconds": round(time.monotonic()-start, 4), "evidence": report}
    except Exception as error:
        return {"scenario": f"{workflow}::{profile}", "status": "error", "failed_assertions": [],
            "reason": type(error).__name__, "error_category": "dependency" if isinstance(error, (httpx.HTTPError, SQLAlchemyError, OSError)) else "runner",
            "duration_seconds": round(time.monotonic()-start, 4)}


def evaluate_cross(database_url, mock_urls, candidate):
    if set(mock_urls) != {"access", "notice"} or any(urlparse(url).hostname not in HOSTS for url in mock_urls.values()):
        raise ValueError("Evaluation requires local synthetic mock services")
    scenarios, manifest = cross_manifest(candidate)
    results = [run_cross_case(database_url, mock_urls[workflow], workflow, profile, candidate)
               for workflow, profile in scenarios]
    counts = {state: sum(item["status"] == state for item in results) for state in ("passed", "failed", "error", "inconclusive")}
    result = {"kind": "evaluation", "id": str(uuid.uuid4()), "created_at": datetime.now(timezone.utc).isoformat(),
              "manifest": manifest, "results": results, "counts": counts, "total": len(results)}
    result["evidence_hash"] = digest(result)
    return result
