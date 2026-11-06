"""Trusted scripted adapter for synthetic notifications."""
import inspect
from mock_notice.schema import NoticeRequest, request_hash
from .contracts import PolicyResult, ToolContract, fingerprint


class NoticeAdapter:
    name, version, policy_version = "synthetic-notice", "1", "notice-policy-v1"
    clarification_question = "Specify a recipient and message."
    tool = ToolContract(name="send_notice", version="1", send_path="/notices", lookup_path="/notices/status",
        lookup_scope_fields=("org_id",), hash_algorithm="notice-sha256-v1", side_effecting=True,
        idempotency_scope=("org_id", "key"), idempotency_atomic=True, same_key_same_request_safe=True,
        retention_seconds=3600, lookup_consistency="authoritative", lookup_positive_final=True)

    def build_action(self, request, action_id):
        return NoticeRequest(org_id=request["org_id"], recipient=request["recipient"], message=request["message"], action_id=action_id).model_dump()

    def validate_action(self, action):
        return NoticeRequest.model_validate(action).model_dump()

    def hash_action(self, action):
        return request_hash(NoticeRequest.model_validate(action))

    def assess(self, action):
        self.validate_action(action)
        return PolicyResult("APPROVAL_REQUIRED", (("policy_checked", {"policy_version": self.policy_version, "result": "APPROVAL_REQUIRED"}),))

    def initial_model_action(self, tenant_id, action_id):
        raise NotImplementedError("Synthetic notices are scripted only")

    def validate_model_result(self, result):
        return False

    def model_action(self, result, messages, current_action):
        return None

    def model_proposal_evidence(self, action):
        return {}


NoticeAdapter.policy_fingerprint = fingerprint({"version": NoticeAdapter.policy_version, "implementation": inspect.getsource(NoticeAdapter.assess)})
NoticeAdapter.implementation_fingerprint = fingerprint({"adapter": inspect.getsource(NoticeAdapter),
    "action_schema": NoticeRequest.model_json_schema(), "hash_implementation": inspect.getsource(request_hash)})
NOTICE_ADAPTER = NoticeAdapter()
