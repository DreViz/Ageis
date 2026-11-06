import copy
import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import create_engine, text

from evaluation import runner
from test_workflow import mock_server

URL = os.getenv("TEST_DATABASE_URL")


def artifact(candidate="safe-v1"):
    suite, suite_hash = runner.suite_manifest()
    manifest = {"suite_id": suite["suite_id"], "suite_hash": suite_hash,
                "fixture_version": suite["fixture_version"], "fixture_hash": "fixture",
                "scenario_ids": [x["id"] for x in suite["scenarios"]], "policy_version": "v1", "policy_hash": "policy",
                "tool_schema_hash": "tools", "runner_version": "v1", "model": "scripted", "python": "3.12", "dependencies": {"httpx": "test"},
                "candidate": candidate, "code_hash": "code", "candidate_hash": candidate}
    results = [{"scenario": x["id"], "status": "passed", "failed_assertions": [], "evidence": {"synthetic": True}} for x in suite["scenarios"]]
    value = {"kind": "evaluation", "id": candidate, "manifest": manifest, "results": results,
             "counts": {"passed": 6, "failed": 0, "error": 0, "inconclusive": 0}, "total": 6}
    value["evidence_hash"] = runner.digest(value)
    return value


def test_comparison_and_recorded_release_decision():
    baseline, candidate = artifact(), artifact("candidate")
    result = runner.compare(baseline, candidate)
    decision = runner.decide(result, "needs_review", "operator")
    assert decision["comparison_hash"] == result["evidence_hash"]
    assert decision["candidate"]["id"] == candidate["id"]
    assert not decision["deployment_performed"]
    runner.verify(decision, "release_decision")


def test_comparison_rejects_tampering_mismatch_and_missing_cases():
    baseline = artifact()
    modified = copy.deepcopy(baseline)
    modified["results"][0]["status"] = "failed"
    with pytest.raises(ValueError, match="hash"):
        runner.compare(baseline, modified)
    modified["evidence_hash"] = runner.digest({k: v for k, v in modified.items() if k != "evidence_hash"})
    modified["manifest"]["fixture_hash"] = "other"
    modified["evidence_hash"] = runner.digest({k: v for k, v in modified.items() if k != "evidence_hash"})
    with pytest.raises(ValueError, match="incompatible"):
        runner.compare(baseline, modified)
    incomplete = artifact()
    incomplete["results"].pop()
    incomplete["evidence_hash"] = runner.digest({k: v for k, v in incomplete.items() if k != "evidence_hash"})
    with pytest.raises(ValueError, match="incomplete"):
        runner.compare(incomplete, incomplete)


def test_inconclusive_evidence_is_not_behavioral_failure():
    suite, _ = runner.suite_manifest()
    assert runner.assess({"mock_evidence": {}}, suite["scenarios"][0])["status"] == "inconclusive"


def test_infrastructure_errors_are_classified(monkeypatch):
    monkeypatch.setattr(runner, "run_demo", lambda *a, **kw: (_ for _ in ()).throw(ConnectionError("private detail")))
    result = runner.evaluate("unused", "http://localhost", "safe-v1")
    assert result["counts"]["error"] == result["total"] == 6
    assert all(r["reason"] == "ConnectionError" for r in result["results"])
    assert "private detail" not in str(result)


@pytest.mark.skipif(not URL, reason="PostgreSQL TEST_DATABASE_URL required")
def test_unsafe_candidate_detected_in_isolated_concurrent_evaluations(mock_server):
    reports = []
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(runner.evaluate, URL, mock_server, candidate) for candidate in ("safe-v1", "unsafe-retry-v1")]
            reports = [future.result() for future in futures]
        safe, unsafe = reports
        assert safe["counts"] == {"passed": 6, "failed": 0, "error": 0, "inconclusive": 0}
        assert unsafe["counts"] == {"passed": 5, "failed": 1, "error": 0, "inconclusive": 0}
        failure = next(r for r in unsafe["results"] if r["status"] == "failed")
        assert failure["scenario"] == "timeout-after-commit"
        assert "committed_operation_count" in failure["failed_assertions"]
        assert len(failure["evidence"]["mock_evidence"]["operations"]) == 2
        tenants = [r["evidence"]["action"]["tenant_id"] for report in reports for r in report["results"]]
        assert len(set(tenants)) == len(tenants)
        comparison = runner.compare(safe, unsafe)
        assert next(c for c in comparison["changes"] if c["scenario"] == "timeout-after-commit")["candidate"] == "failed"
    finally:
        engine = create_engine(URL)
        with engine.begin() as c:
            for report in reports:
                for result in report["results"]:
                    schema = result.get("evidence", {}).get("schema")
                    if schema:
                        assert schema.startswith("demo_") and len(schema) == 37
                        c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


def test_unexpected_normal_timeout_is_inconclusive():
    suite, _ = runner.suite_manifest()
    report = {"mock_evidence": {"attempts": [], "operations": [], "grants": []},
              "events": [{"kind": "tool_timeout"}]}
    assert runner.assess(report, suite["scenarios"][0])["status"] == "inconclusive"
