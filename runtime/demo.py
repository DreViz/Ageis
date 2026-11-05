"""Offline scripted workflow demonstration against the real mock HTTP service."""
import argparse
import json
import os
import uuid
import httpx
from sqlalchemy import select
from .db import Approval, Event, Run, make_database
from .service import Runtime
from .worker import process


def run_demo(database_url, mock_url, scenario):
    schema = "demo_" + uuid.uuid4().hex
    engine, sessions = make_database(database_url, schema=schema)
    runtime = Runtime(sessions)
    tenant = str(uuid.uuid4())
    run_id = runtime.create_run(tenant, "maya", "admin" if scenario == "denied" else "analytics")
    lease = runtime.claim("demo-worker")
    runtime.prepare(lease)
    with sessions() as s:
        approval = s.scalar(select(Approval).where(Approval.run_id == run_id))
        approval_id = approval.id if approval else None
    if approval_id:
        runtime.decide(approval_id, "REJECTED" if scenario == "rejected" else "APPROVED", "scripted-demo-operator")
    grant_fault = {"timeout-before-commit": "before_commit", "timeout-after-commit": "after_commit",
                   "unresolved": "after_commit"}.get(scenario)
    sent = False
    def inject(request):
        nonlocal sent
        if request.method == "POST" and request.url.path == "/grants" and grant_fault and not sent:
            request.headers["X-Test-Fault"] = grant_fault
            sent = True
        if request.url.path == "/operations/by-key" and scenario == "unresolved":
            request.headers["X-Test-Fault"] = "unavailable"
    with httpx.Client(base_url=mock_url, timeout=0.2, event_hooks={"request": [inject]}) as client:
        if approval_id and scenario != "rejected":
            lease = runtime.claim("demo-worker")
            process(runtime, lease, client)
        evidence_response = client.get("/test/evidence", params={"tenant_id": tenant}, timeout=5)
        evidence_response.raise_for_status()
        evidence = evidence_response.json()
    with sessions() as s:
        run = s.get(Run, run_id)
        events = [{"sequence": e.sequence, "kind": e.kind, "payload": e.payload}
                  for e in s.scalars(select(Event).where(Event.run_id == run_id).order_by(Event.sequence))]
        report = {"scenario": scenario, "schema": schema, "run_id": run_id, "state": run.state,
                  "action_status": run.action_status, "action_id": run.action["action_id"],
                  "approval_id": approval_id, "network_attempts": run.attempt_count,
                  "operation": run.operation, "events": events, "mock_evidence": evidence}
    engine.dispose()
    return report


def validate_report(report):
    scenario, evidence = report["scenario"], report["mock_evidence"]
    expected = "NEEDS_REVIEW" if scenario == "unresolved" else "REJECTED" if scenario in ("rejected", "denied") else "COMPLETED"
    assert report["state"] == expected, "Unexpected workflow state"
    operations, grants, attempts = evidence["operations"], evidence["grants"], evidence["attempts"]
    if scenario in ("rejected", "denied"):
        assert not operations and not grants and not attempts, "Denied action caused a side effect"
        return
    assert len(operations) == len(grants) == 1, "Expected exactly one committed operation and grant"
    assert operations[0]["action_id"] == report["action_id"]
    assert all(a["action_id"] == report["action_id"] for a in attempts)
    assert len({a["key"] for a in attempts}) == 1, "Attempts did not reuse one key"
    assert len({a["request_hash"] for a in attempts}) == 1, "Attempts changed the request"
    if scenario == "timeout-before-commit":
        assert len(attempts) == 2 and report["network_attempts"] == 2, "Expected one safe retry"
    else:
        assert len(attempts) == 1 and report["network_attempts"] == 1
    if expected == "COMPLETED":
        assert report["operation"]["operation_id"] == operations[0]["operation_id"]
    else:
        assert report["operation"] is None and report["action_status"] == "UNKNOWN"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=["normal", "timeout-before-commit", "timeout-after-commit",
                                               "unresolved", "rejected", "denied"], default="normal")
    args = parser.parse_args()
    report = run_demo(os.environ["RUNTIME_DATABASE_URL"], os.environ["MOCK_IDENTITY_URL"], args.scenario)
    print(json.dumps(report, indent=2))
    validate_report(report)


if __name__ == "__main__":
    main()
