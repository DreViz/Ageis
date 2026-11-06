"""The existing synthetic access workflow expressed as a trusted adapter."""
from datetime import timedelta
import inspect
from typing import Any, Mapping
from mock_identity.app import GrantRequest, request_hash
from .contracts import AdapterRegistry, PolicyResult, ToolContract, fingerprint
from pydantic import ValidationError
from .model import ModelResult, Proposal, Question, grounded_proposal
from .workflow import POLICY_VERSION, USERS, WORKSPACES, evaluate


class AccessAdapter:
    name = "synthetic-access"
    version = "1"
    policy_version = POLICY_VERSION
    policy_fingerprint = fingerprint({"version": POLICY_VERSION, "users": USERS, "workspaces": WORKSPACES,
                                      "implementation": inspect.getsource(evaluate)})
    clarification_question = "Please specify the synthetic user and workspace for this access request."
    tool = ToolContract(name="grant_access", version="1", send_path="/grants",
                        lookup_path="/operations/by-key", lookup_scope_fields=("tenant_id",),
                        hash_algorithm="mock-grant-sha256-v1", side_effecting=True, idempotency_scope=("tenant_id", "operation_type", "key"),
                        idempotency_atomic=True, same_key_same_request_safe=True,
                        retention_seconds=int(timedelta(hours=24).total_seconds()), lookup_consistency="authoritative", lookup_positive_final=True)

    def build_action(self, request: Mapping[str, Any], action_id: str) -> dict[str, Any]:
        return GrantRequest(tenant_id=request["tenant_id"], user_id=request["user_id"],
                            workspace_id=request["workspace_id"], action_id=action_id).model_dump()

    def validate_action(self, action: Mapping[str, Any]) -> dict[str, Any]:
        return GrantRequest.model_validate(action).model_dump()

    def hash_action(self, action: Mapping[str, Any]) -> str:
        return request_hash(GrantRequest.model_validate(action))

    def assess(self, action: Mapping[str, Any]) -> PolicyResult:
        result = evaluate(self.validate_action(action))
        return PolicyResult(result, (("directory_checked", {"user_id": action["user_id"], "resolved": result != "UNKNOWN_USER"}),
                                     ("policy_checked", {"policy_version": self.policy_version, "result": result})))

    def initial_model_action(self, tenant_id, action_id):
        return self.build_action({"tenant_id": tenant_id, "user_id": "unresolved", "workspace_id": "unresolved"}, action_id)

    def validate_model_result(self, result):
        if not isinstance(result, ModelResult) or result.kind not in ("propose_access", "request_input"):
            return False
        try:
            (Proposal if result.kind == "propose_access" else Question).model_validate(result.arguments)
            return True
        except ValidationError:
            return False

    def model_proposal_evidence(self, action):
        return {"user_id": action["user_id"], "workspace_id": action["workspace_id"]}

    def model_action(self, result, messages, current_action):
        proposal = grounded_proposal(result, messages)
        return self.validate_action({**current_action, **proposal}) if proposal is not None else None


# Bind running work to the adapter code and the action's wire schema/hash logic.
AccessAdapter.implementation_fingerprint = fingerprint({"adapter": inspect.getsource(AccessAdapter),
    "action_schema": GrantRequest.model_json_schema(), "hash_implementation": inspect.getsource(request_hash)})
ACCESS_ADAPTER = AccessAdapter()
DEFAULT_REGISTRY = AdapterRegistry([ACCESS_ADAPTER], default=ACCESS_ADAPTER.name)
