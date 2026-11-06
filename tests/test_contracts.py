from dataclasses import replace
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import select

from runtime.access_adapter import ACCESS_ADAPTER
from runtime.contracts import AdapterRegistry, ContractMismatch, PolicyResult, ToolContract, fingerprint
from runtime.db import Approval, Job, Run
from runtime.service import Conflict, Runtime
from runtime.worker import process
from test_runtime import context


class NoticeAdapter:
    """Trusted test-only workflow: different action shape and HTTP endpoints."""
    name, version, policy_version = "synthetic-notice", "1", "notice-policy-v1"
    implementation_fingerprint = fingerprint({"adapter": "notice-test-v1"})
    policy_fingerprint = fingerprint({"policy": "notice-policy-v1"})
    clarification_question = "Specify a recipient and message."
    tool = ToolContract(name="send_notice", version="1", send_path="/notices",
        lookup_path="/notices/status", lookup_scope_fields=("org_id",), hash_algorithm="notice-v1", side_effecting=True,
        idempotency_scope=("org_id", "key"), idempotency_atomic=True,
        same_key_same_request_safe=True, retention_seconds=3600,
        lookup_consistency="authoritative", lookup_positive_final=True)

    def build_action(self, request, action_id):
        return {"org_id": request["org_id"], "recipient": request["recipient"],
                "message": request["message"], "action_id": action_id}

    def validate_action(self, action):
        if set(action) != {"org_id", "recipient", "message", "action_id"} or not all(isinstance(v, str) and v for v in action.values()):
            raise ValueError("Invalid synthetic notice")
        return dict(action)

    def hash_action(self, action):
        return fingerprint({"operation": "notice", **self.validate_action(action)})

    def assess(self, action):
        self.validate_action(action)
        return PolicyResult("APPROVAL_REQUIRED", (("policy_checked", {"policy_version": self.policy_version, "result": "APPROVAL_REQUIRED"}),))

    def initial_model_action(self, tenant_id, action_id):
        raise NotImplementedError("Test adapter is scripted only")

    def validate_model_result(self, result):
        return False

    def model_proposal_evidence(self, action):
        return {}

    def model_action(self, result, messages, current_action):
        return None


def custom_runtime(context, adapter=None):
    _, sessions = context
    notice = adapter or NoticeAdapter()
    registry = AdapterRegistry([ACCESS_ADAPTER, notice], default=ACCESS_ADAPTER.name)
    return Runtime(sessions, registry=registry), sessions


def pending_notice(runtime, sessions):
    run_id = runtime.create_adapter_run("synthetic-notice", {"org_id": "test-org", "recipient": "dev", "message": "hello"})
    assert runtime.prepare(runtime.claim("initial")) is None
    with sessions() as s:
        approval = s.scalar(select(Approval).where(Approval.run_id == run_id))
        assert approval is not None and approval.action["recipient"] == "dev"
        approval_id = approval.id
    runtime.decide(approval_id, "APPROVED", "operator")
    return run_id


def test_second_adapter_recovers_timeout_without_access_specific_core(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    received = []
    committed = {}
    def handler(request):
        if request.method == "POST":
            import json
            assert request.url.path == "/notices"
            action = json.loads(request.content)
            key = request.headers["idempotency-key"]
            received.append(key)
            committed[key] = {"status": "committed", "action_id": action["action_id"],
                              "request_hash": NoticeAdapter().hash_action(action), "operation_id": "notice-op"}
            raise httpx.ReadTimeout("after commit", request=request)
        assert request.url.path == "/notices/status"
        assert request.url.params["org_id"] == "test-org"
        return httpx.Response(200, json=committed[request.url.params["idempotency_key"]])
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://synthetic") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        run = s.get(Run, run_id)
        assert run.state == "COMPLETED" and run.operation["operation_id"] == "notice-op"
        assert run.request["_ageis_contract"]["adapter"] == "synthetic-notice"
        assert "user_id" not in run.action and len(received) == len(committed) == 1


def test_unknown_idempotency_never_retries_an_absent_lookup(context):
    adapter = NoticeAdapter()
    adapter.tool = replace(adapter.tool, idempotency_atomic=False)
    runtime, sessions = custom_runtime(context, adapter)
    run_id = pending_notice(runtime, sessions)
    posts = []
    def handler(request):
        if request.method == "POST":
            posts.append(request.headers["idempotency-key"])
            raise httpx.ReadTimeout("unknown outcome", request=request)
        return httpx.Response(404)
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://synthetic") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
    assert len(posts) == 1


def test_untrusted_positive_lookup_does_not_confirm_success(context):
    adapter = NoticeAdapter()
    adapter.tool = replace(adapter.tool, lookup_consistency="unknown", lookup_positive_final=False)
    runtime, sessions = custom_runtime(context, adapter)
    run_id = pending_notice(runtime, sessions)
    def handler(request):
        if request.method == "POST":
            raise httpx.ReadTimeout("unknown outcome", request=request)
        with sessions() as s:
            run = s.get(Run, run_id)
            return httpx.Response(200, json={"status": "committed", "action_id": run.action["action_id"],
                                          "request_hash": run.action_hash, "operation_id": "untrusted"})
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://synthetic") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW" and s.get(Run, run_id).operation is None


def test_contract_change_is_blocked_before_remote_action(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    changed = NoticeAdapter()
    changed.tool = replace(changed.tool, version="2")
    updated, _ = custom_runtime(context, changed)
    assert updated.prepare(updated.claim("replacement")) is None
    with sessions() as s:
        run = s.get(Run, run_id)
        assert run.state == "NEEDS_REVIEW" and run.attempt_count == 0


def test_legacy_access_run_does_not_auto_retry(context):
    runtime, sessions = context
    run_id = runtime.create_run("legacy", "maya", "analytics")
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        run.request = {k: v for k, v in run.request.items() if not k.startswith("_ageis_")}
    runtime.prepare(runtime.claim("initial"))
    with sessions() as s:
        approval_id = s.scalar(select(Approval).where(Approval.run_id == run_id)).id
    runtime.decide(approval_id, "APPROVED", "operator")
    posts = []
    def handler(request):
        if request.method == "POST":
            posts.append(1)
            raise httpx.ReadTimeout("unknown", request=request)
        return httpx.Response(404)
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://synthetic") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
    assert len(posts) == 1


def test_contract_metadata_fails_closed_and_cannot_be_user_selected(context):
    runtime, sessions = context
    with pytest.raises(ContractMismatch):
        runtime.create_adapter_run("unregistered", {})
    with pytest.raises(ValueError):
        runtime.create_adapter_run("synthetic-access", {"tenant_id": "x", "user_id": "maya", "workspace_id": "analytics", "_ageis_contract": {}})
    run_id = runtime.create_run("tenant", "maya", "analytics")
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        run.request = {**run.request, "_ageis_contract": None}
    assert runtime.prepare(runtime.claim("worker")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "FAILED"


def test_tool_contract_defaults_do_not_authorize_retries_or_lookup():
    tool = ToolContract(name="unknown", version="1", send_path="/send", lookup_path="/status",
                        lookup_scope_fields=("org_id",), hash_algorithm="v1", side_effecting=True, idempotency_scope=("org_id", "key"))
    assert not tool.supports_safe_retry
    result = {"status": "committed", "action_id": "a", "request_hash": "h", "operation_id": "o"}
    assert tool.verified(result, {"action_id": "a"}, "h")
    assert not tool.verified(result, {"action_id": "a"}, "h", source="lookup")
    with pytest.raises(ValueError):
        replace(tool, send_path="//outside.example/send")


def test_retry_window_expiration_prevents_another_send(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        run.request = {**run.request, "_ageis_retry_until": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}
    posts = []
    def handler(request):
        if request.method == "POST":
            posts.append(request.headers["idempotency-key"])
            raise httpx.ReadTimeout("unknown", request=request)
        return httpx.Response(404)
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://synthetic") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
    assert len(posts) == 1


def test_legacy_access_run_still_completes_on_confirmed_response(context):
    runtime, sessions = context
    run_id = runtime.create_run("legacy", "maya", "analytics")
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        run.request = {k: v for k, v in run.request.items() if not k.startswith("_ageis_")}
    runtime.prepare(runtime.claim("initial"))
    with sessions() as s:
        approval_id = s.scalar(select(Approval).where(Approval.run_id == run_id)).id
    runtime.decide(approval_id, "APPROVED", "operator")
    def handler(request):
        with sessions() as s:
            run = s.get(Run, run_id)
            return httpx.Response(200, json={"status": "committed", "action_id": run.action["action_id"],
                                            "request_hash": run.action_hash, "operation_id": "legacy-op"})
    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://synthetic") as client:
        process(runtime, runtime.claim("worker"), client)
    with sessions() as s:
        assert s.get(Run, run_id).state == "COMPLETED"


def test_contract_change_on_unknown_outcome_requires_review(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    lease = runtime.claim("old-worker")
    intent = runtime.prepare(lease)
    assert intent["mode"] == "send"
    with sessions.begin() as s:
        s.get(Job, lease[0]).lease_expires_at -= timedelta(minutes=1)
    changed = NoticeAdapter()
    changed.tool = replace(changed.tool, version="2")
    replacement, _ = custom_runtime(context, changed)
    assert replacement.prepare(replacement.claim("replacement")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
        assert s.get(Run, run_id).action_status == "UNKNOWN"


def test_changed_policy_fingerprint_requires_review(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    changed = NoticeAdapter()
    changed.policy_fingerprint = fingerprint({"policy": "changed implementation"})
    replacement, _ = custom_runtime(context, changed)
    assert replacement.prepare(replacement.claim("worker")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
        assert s.get(Run, run_id).attempt_count == 0


def test_changed_adapter_implementation_requires_review(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    changed = NoticeAdapter()
    changed.implementation_fingerprint = fingerprint({"adapter": "modified"})
    replacement, _ = custom_runtime(context, changed)
    assert replacement.prepare(replacement.claim("worker")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "NEEDS_REVIEW"
        assert s.get(Run, run_id).attempt_count == 0


def test_malformed_persisted_adapter_identity_is_rejected(context):
    runtime, sessions = context
    run_id = runtime.create_run("tenant", "maya", "analytics")
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        binding = {**run.request["_ageis_contract"], "adapter": []}
        run.request = {**run.request, "_ageis_contract": binding}
    assert runtime.prepare(runtime.claim("worker")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "FAILED"


def test_partial_contract_binding_is_not_treated_as_legacy(context):
    runtime, sessions = context
    run_id = runtime.create_run("tenant", "maya", "analytics")
    with sessions.begin() as s:
        run = s.get(Run, run_id)
        run.request = {key: value for key, value in run.request.items() if key != "_ageis_contract"}
    assert runtime.prepare(runtime.claim("worker")) is None
    with sessions() as s:
        assert s.get(Run, run_id).state == "FAILED"


def test_missing_tool_route_stops_notice_for_review(context):
    runtime, sessions = custom_runtime(context)
    run_id = pending_notice(runtime, sessions)
    process(runtime, runtime.claim('worker'), {})
    with sessions() as session:
        run = session.get(Run, run_id)
        assert run.state == 'NEEDS_REVIEW' and run.operation is None
