"""Contracts for trusted, project-owned agents and external actions.

Registration is application configuration, not a user-controlled plugin mechanism.
"""
from dataclasses import asdict, dataclass
import hashlib
import inspect
import json
import re
from pathlib import Path
from typing import Any, Mapping, Protocol


def fingerprint(value: object) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ToolContract:
    name: str
    version: str
    send_path: str
    lookup_path: str
    lookup_scope_fields: tuple[str, ...]
    hash_algorithm: str
    side_effecting: bool
    idempotency_scope: tuple[str, ...]
    idempotency_atomic: bool = False
    same_key_same_request_safe: bool = False
    retention_seconds: int = 0
    lookup_consistency: str = "unknown"
    lookup_positive_final: bool = False

    def __post_init__(self):
        if (not self.name or not self.version or not self.hash_algorithm or self.side_effecting is not True or
            not re.fullmatch(r"/[A-Za-z0-9_/-]+", self.send_path) or "//" in self.send_path or
            not re.fullmatch(r"/[A-Za-z0-9_/-]+", self.lookup_path) or "//" in self.lookup_path or
            type(self.retention_seconds) is not int or self.retention_seconds < 0 or
            self.lookup_consistency not in ("authoritative", "eventual", "unknown") or
            len(set(self.lookup_scope_fields)) != len(self.lookup_scope_fields) or
            "idempotency_key" in self.lookup_scope_fields):
            raise ValueError("Invalid tool contract")

    @property
    def fingerprint(self) -> str:
        # Source is normalized so local and container checkouts bind identically.
        runtime_sources = {name: Path(__file__).with_name(name).read_text(encoding="utf-8")
                           for name in ("service.py", "worker.py")}
        return fingerprint({"metadata": asdict(self), "implementation": inspect.getsource(ToolContract),
                            "fingerprint_implementation": inspect.getsource(fingerprint),
                            "runtime_sources": runtime_sources})

    @property
    def supports_safe_retry(self) -> bool:
        return (self.idempotency_atomic and self.same_key_same_request_safe and
                self.retention_seconds > 0 and "key" in self.idempotency_scope)

    def lookup_params(self, action: Mapping[str, Any], key: str) -> dict[str, Any]:
        return {**{field: action[field] for field in self.lookup_scope_fields}, "idempotency_key": key}

    def verified(self, outcome: object, action: Mapping[str, Any], action_hash: str, *, source: str = "direct") -> bool:
        return (source in ("direct", "lookup") and (source != "lookup" or self.lookup_positive_final) and
                isinstance(outcome, dict) and outcome.get("status") == "committed" and
                outcome.get("action_id") == action.get("action_id") and
                outcome.get("request_hash") == action_hash and
                isinstance(outcome.get("operation_id"), str) and bool(outcome["operation_id"]))


@dataclass(frozen=True)
class PolicyResult:
    decision: str
    evidence_events: tuple[tuple[str, dict], ...] = ()


class AgentAdapter(Protocol):
    name: str
    version: str
    implementation_fingerprint: str
    policy_version: str
    policy_fingerprint: str
    tool: ToolContract
    clarification_question: str

    def build_action(self, request: Mapping[str, Any], action_id: str) -> dict[str, Any]: ...
    def validate_action(self, action: Mapping[str, Any]) -> dict[str, Any]: ...
    def hash_action(self, action: Mapping[str, Any]) -> str: ...
    def assess(self, action: Mapping[str, Any]) -> PolicyResult: ...
    def initial_model_action(self, tenant_id: str, action_id: str) -> dict[str, Any]: ...
    def validate_model_result(self, result: Any) -> bool: ...
    def model_action(self, result: Any, messages: list[dict], current_action: Mapping[str, Any]) -> dict[str, Any] | None: ...
    def model_proposal_evidence(self, action: Mapping[str, Any]) -> dict[str, Any]: ...


class ContractMismatch(Exception):
    pass


class AdapterRegistry:
    def __init__(self, adapters: list[AgentAdapter], default: str):
        self._adapters = {adapter.name: adapter for adapter in adapters}
        if len(self._adapters) != len(adapters) or default not in self._adapters:
            raise ValueError("Invalid adapter registry")
        for adapter in adapters:
            identities = (adapter.name, adapter.version, adapter.implementation_fingerprint, adapter.policy_version, adapter.policy_fingerprint)
            methods = ("build_action", "validate_action", "hash_action", "assess",
                       "initial_model_action", "validate_model_result", "model_action", "model_proposal_evidence")
            if (any(not isinstance(value, str) or not value for value in identities) or
                not isinstance(adapter.tool, ToolContract) or
                not isinstance(adapter.clarification_question, str) or not adapter.clarification_question or
                any(not callable(getattr(adapter, method, None)) for method in methods)):
                raise ValueError("Incomplete trusted adapter")
        self.default = default

    def get(self, name: str) -> AgentAdapter:
        if not isinstance(name, str):
            raise ContractMismatch("Invalid adapter identity")
        try:
            return self._adapters[name]
        except KeyError as error:
            raise ContractMismatch("Unknown adapter") from error

    def resolve(self, request: dict) -> tuple[AgentAdapter, bool]:
        if not isinstance(request, dict):
            raise ContractMismatch("Invalid persisted request")
        if "_ageis_contract" not in request:
            if any(isinstance(key, str) and key.startswith("_ageis_") for key in request):
                raise ContractMismatch("Incomplete contract binding")
            # Existing access runs predate pinned contracts. No other adapter can
            # claim legacy records and legacy records never gain automatic retries.
            return self.get("synthetic-access"), True
        binding = request["_ageis_contract"]
        if not isinstance(binding, dict):
            raise ContractMismatch("Invalid contract binding")
        adapter = self.get(binding.get("adapter", ""))
        expected = {"adapter": adapter.name, "adapter_version": adapter.version,
                    "adapter_fingerprint": adapter.implementation_fingerprint,
                    "policy_version": adapter.policy_version, "policy_fingerprint": adapter.policy_fingerprint,
                "tool_fingerprint": adapter.tool.fingerprint}
        if binding != expected:
            raise ContractMismatch("Pinned adapter or tool contract changed")
        return adapter, False

    @staticmethod
    def binding(adapter: AgentAdapter) -> dict[str, str]:
        return {"adapter": adapter.name, "adapter_version": adapter.version,
                "adapter_fingerprint": adapter.implementation_fingerprint,
                "policy_version": adapter.policy_version, "policy_fingerprint": adapter.policy_fingerprint,
                "tool_fingerprint": adapter.tool.fingerprint}
