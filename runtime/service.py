"""Durable state transitions. Every leased update locks and fences the job."""
from datetime import datetime, timedelta
import uuid
from sqlalchemy import and_, func, or_, select
from .db import Approval, Event, Job, ModelSession, Run
from .contracts import ContractMismatch
from .access_adapter import DEFAULT_REGISTRY
from .model import PROMPT_HASH, TOOL_SCHEMA_HASH
from pydantic import ValidationError


def uid():
    return str(uuid.uuid4())


def db_now(session):
    return session.scalar(select(func.clock_timestamp()))


def event(session, run, kind, payload=None):
    run.event_seq += 1
    session.add(Event(id=uid(), run_id=run.id, sequence=run.event_seq,
                      kind=kind, payload=payload or {}, created_at=db_now(session)))


def new_job(run_id, current_time, approval_id=None):
    return Job(id=uid(), run_id=run_id, approval_id=approval_id, state="READY", available_at=current_time)


class Conflict(Exception):
    pass


class LeaseLost(Exception):
    pass


class Runtime:
    def __init__(self, sessions, *, registry=DEFAULT_REGISTRY):
        self.sessions = sessions
        self.registry = registry

    def _binding(self, adapter, now, deadline):
        # The key's guaranteed lifetime starts no earlier than run creation. Use
        # this conservative bound rather than guessing when a remote commit occurred.
        until = min(deadline, now + timedelta(seconds=adapter.tool.retention_seconds))
        return {"_ageis_contract": self.registry.binding(adapter), "_ageis_retry_until": until.isoformat()}

    def _resolve(self, run):
        adapter, legacy = self.registry.resolve(run.request)
        try:
            action = adapter.validate_action(run.action)
            if adapter.hash_action(action) != run.action_hash:
                raise ContractMismatch("Action does not match its persisted hash")
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ContractMismatch("Invalid persisted action") from error
        return adapter, legacy

    def create_run(self, tenant_id, user_id, workspace_id):
        return self.create_adapter_run(self.registry.default, {"tenant_id": tenant_id, "user_id": user_id,
                                                               "workspace_id": workspace_id})

    def create_adapter_run(self, adapter_name, request):
        adapter = self.registry.get(adapter_name)
        if not isinstance(request, dict) or any(not isinstance(key, str) or key.startswith("_ageis_") for key in request):
            raise ValueError("Invalid adapter request")
        with self.sessions.begin() as s:
            now = db_now(s)
            deadline = now + timedelta(hours=1)
            action_id = uid()
            action = adapter.validate_action(adapter.build_action(request, action_id))
            if not isinstance(action, dict) or action.get("action_id") != action_id:
                raise ValueError("Adapter must preserve the generated action ID")
            if any(field not in action for field in adapter.tool.lookup_scope_fields):
                raise ValueError("Action is missing a lookup scope field")
            run = Run(id=uid(), state="PENDING", request={**request, **self._binding(adapter, now, deadline)},
                      action=action, action_hash=adapter.hash_action(action), idempotency_key=uid(),
                      action_status="NOT_SENT", deadline=deadline, max_attempts=3, attempt_count=0, event_seq=0)
            s.add(run)
            event(s, run, "run_created")
            s.add(new_job(run.id, now))
            return run.id

    def create_model_run(self, tenant_id, text, model_id):
        if not tenant_id or len(tenant_id) > 128 or not text.strip() or len(text) > 2000 or not model_id or len(model_id) > 128:
            raise ValueError("Invalid model run request")
        with self.sessions.begin() as s:
            now = db_now(s)
            adapter = self.registry.get(self.registry.default)
            deadline = now + timedelta(hours=1)
            action = adapter.initial_model_action(tenant_id, uid())
            run = Run(id=uid(), state="PENDING_MODEL", request={"tenant_id": tenant_id, **self._binding(adapter, now, deadline)}, action=action,
                      action_hash=adapter.hash_action(action), idempotency_key=uid(), action_status="NOT_SENT",
                      deadline=deadline, max_attempts=3, attempt_count=0, event_seq=0)
            s.add(run)
            s.flush()
            s.add(ModelSession(run_id=run.id, model_id=model_id, prompt_hash=PROMPT_HASH, tool_schema_hash=TOOL_SCHEMA_HASH,
                               messages=[{"role": "user", "content": text}], model_calls=0, max_calls=4,
                               input_tokens=0, output_tokens=0, usage_missing=False, accounted_calls=0))
            event(s, run, "model_run_created", {"model_id": model_id, "prompt_hash": PROMPT_HASH, "tool_schema_hash": TOOL_SCHEMA_HASH})
            s.add(new_job(run.id, now))
            return run.id

    def claim(self, owner, lease_seconds=30):
        with self.sessions.begin() as s:
            now = db_now(s)
            job = s.scalar(select(Job).where(or_(and_(Job.state == "READY", Job.available_at <= now),
                and_(Job.state == "CLAIMED", Job.lease_expires_at <= now))).order_by(Job.available_at, Job.id)
                .with_for_update(skip_locked=True).limit(1))
            if job is None:
                return None
            if job.claims >= 10:
                run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
                run.state = "NEEDS_REVIEW" if run.action_status == "UNKNOWN" else "FAILED"
                job.state = "DONE"
                event(s, run, "claim_limit_reached", {"action_status": run.action_status})
                return None
            job.state, job.lease_owner = "CLAIMED", owner
            job.lease_expires_at = now + timedelta(seconds=lease_seconds)
            job.fence += 1
            job.claims += 1
            return (job.id, owner, job.fence)

    def locked_job(self, s, lease):
        job_id, owner, fence = lease
        # Use the database clock after acquiring the row lock, including on a blocked lock.
        job = s.scalar(select(Job).where(Job.id == job_id).with_for_update())
        if not job or job.state != "CLAIMED" or job.lease_owner != owner or job.fence != fence or job.lease_expires_at <= db_now(s):
            raise LeaseLost()
        return job

    def renew(self, lease, seconds=30):
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            job.lease_expires_at = db_now(s) + timedelta(seconds=seconds)

    def prepare(self, lease):
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            now = db_now(s)
            if run.deadline <= now:
                run.state = "NEEDS_REVIEW" if run.action_status == "UNKNOWN" else "FAILED"
                job.state = "DONE"
                event(s, run, "deadline_exceeded", {"action_status": run.action_status})
                return None
            try:
                adapter, legacy = self._resolve(run)
            except ContractMismatch:
                run.state = "NEEDS_REVIEW" if run.action_status == "UNKNOWN" or run.state in ("RUNNING", "RECONCILING") else "FAILED"
                job.state = "DONE"
                event(s, run, "contract_mismatch")
                return None
            if run.state == "PENDING_MODEL":
                model = s.get(ModelSession, run.id)
                if not model or model.prompt_hash != PROMPT_HASH or model.tool_schema_hash != TOOL_SCHEMA_HASH:
                    run.state, job.state = "FAILED", "DONE"
                    event(s, run, "model_configuration_changed")
                    return None
                if model.model_calls >= model.max_calls:
                    run.state, job.state = "FAILED", "DONE"
                    event(s, run, "model_call_limit_reached")
                    return None
                model.model_calls += 1  # Reserve before the request, even if the worker crashes.
                event(s, run, "model_call_started", {"call": model.model_calls, "model_id": model.model_id})
                return {"mode": "model", "model_id": model.model_id, "messages": model.messages}
            if run.state == "PENDING":
                assessment = adapter.assess(run.action)
                policy = assessment.decision
                for kind, payload in assessment.evidence_events:
                    event(s, run, kind, payload)
                if policy != "APPROVAL_REQUIRED":
                    run.state, job.state = "REJECTED", "DONE"
                    event(s, run, "request_denied", {"reason": policy})
                    return None
                approval = Approval(id=uid(), run_id=run.id, action=run.action, action_hash=run.action_hash,
                                    status="PENDING", expires_at=now + timedelta(minutes=15))
                s.add(approval)
                run.state, job.state = "WAITING_FOR_APPROVAL", "DONE"
                event(s, run, "approval_requested", {"approval_id": approval.id, "action_hash": run.action_hash})
                return None
            if run.state not in ("RUNNING", "RECONCILING"):
                job.state = "DONE"
                event(s, run, "job_stopped", {"state": run.state})
                return None
            approval = s.get(Approval, job.approval_id)
            current_hash = adapter.hash_action(run.action)
            if not approval or approval.status != "APPROVED" or approval.expires_at <= now or approval.action_hash != current_hash or run.action_hash != current_hash:
                run.state, job.state = "NEEDS_REVIEW", "DONE"
                event(s, run, "approval_invalid")
                return None
            if adapter.assess(run.action).decision != "APPROVAL_REQUIRED":
                run.state, job.state = "NEEDS_REVIEW", "DONE"
                event(s, run, "policy_revalidation_failed", {"policy_version": adapter.policy_version})
                return None
            if run.action_status == "UNKNOWN":
                run.state = "RECONCILING"
                event(s, run, "reconciliation_started")
                return {"mode": "reconcile", "action": run.action, "key": run.idempotency_key, "hash": run.action_hash, "tool": adapter.tool}
            if run.attempt_count >= run.max_attempts:
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "attempt_limit_reached")
                return None
            # Commit an unknown intent and the stable key before initiating the remote request.
            run.attempt_count += 1
            run.action_status = "UNKNOWN"
            event(s, run, "action_started", {"attempt": run.attempt_count, "action_id": run.action["action_id"]})
            return {"mode": "send", "action": run.action, "key": run.idempotency_key, "hash": run.action_hash, "tool": adapter.tool}

    def finish_model(self, lease, result, error=None):
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            model = s.get(ModelSession, run.id)
            if run.state != "PENDING_MODEL" or model is None:
                raise Conflict("Run is not awaiting model output")
            now = db_now(s)
            if run.deadline <= now:
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "deadline_exceeded")
                return
            try:
                adapter, _ = self._resolve(run)
            except ContractMismatch:
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "contract_mismatch")
                return
            valid_output = adapter.validate_model_result(result)
            if error or not valid_output:
                model.last_error = error if error in ("provider_incomplete", "invalid_tool_call", "provider_or_output_error", "model_not_configured") else "invalid_model_output"
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "model_call_failed", {"error": model.last_error})
                return
            if (type(result.input_tokens) is not int or result.input_tokens < 0 or
                    type(result.output_tokens) is not int or result.output_tokens < 0):
                model.usage_missing = True
            else:
                model.input_tokens += result.input_tokens
                model.output_tokens += result.output_tokens
                model.accounted_calls += 1
            proposal_action = adapter.model_action(result, model.messages, run.action)
            event(s, run, "model_call_completed", {"tool": result.kind,
                  "usage_available": result.input_tokens is not None and result.output_tokens is not None})
            if proposal_action is None and model.model_calls >= model.max_calls:
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "model_call_limit_reached")
                return
            if proposal_action is None:
                # Do not display arbitrary model text or accept ungrounded targets.
                question = adapter.clarification_question
                model.question = question
                model.messages = [*model.messages, {"role": "assistant", "content": question}]
                run.state, job.state = "WAITING_FOR_INPUT", "DONE"
                event(s, run, "input_requested")
                return
            action = adapter.validate_action(proposal_action)
            if (action.get("action_id") != run.action.get("action_id") or
                any(action.get(field) != run.action.get(field) for field in adapter.tool.lookup_scope_fields)):
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "model_proposal_changed_identity")
                return
            run.action, run.action_hash = action, adapter.hash_action(action)
            model.question = None
            run.state, job.state = "PENDING", "DONE"
            event(s, run, "model_proposal_validated", adapter.model_proposal_evidence(action))
            s.add(new_job(run.id, now))

    def provide_input(self, run_id, text, actor):
        if not text.strip() or len(text) > 2000:
            raise ValueError("Input must contain 1 to 2000 characters")
        with self.sessions.begin() as s:
            run = s.scalar(select(Run).where(Run.id == run_id).with_for_update())
            if run is None:
                raise LookupError()
            model = s.get(ModelSession, run.id)
            now = db_now(s)
            if not model or run.state != "WAITING_FOR_INPUT" or run.deadline <= now or model.model_calls >= model.max_calls:
                raise Conflict("Run is not accepting input")
            model.messages = [*model.messages, {"role": "user", "content": text}]
            model.question = None
            run.state = "PENDING_MODEL"
            event(s, run, "input_received", {"actor": actor})
            s.add(new_job(run.id, now))

    def check_lease(self, lease):
        with self.sessions.begin() as s:
            self.locked_job(s, lease)

    def record_tool_event(self, lease, kind, payload=None):
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            event(s, run, kind, payload)

    def reserve_retry(self, lease):
        """Bound retries of this same logical action to the run and approval deadlines."""
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            approval = s.get(Approval, job.approval_id)
            now = db_now(s)
            try:
                adapter, legacy = self._resolve(run)
                retry_until = datetime.fromisoformat(run.request.get("_ageis_retry_until", ""))
                if retry_until.tzinfo is None:
                    return False
            except (ContractMismatch, TypeError, ValueError):
                return False
            current_hash = adapter.hash_action(run.action)
            if (legacy or not adapter.tool.supports_safe_retry or now >= retry_until or
                    run.action_status != "UNKNOWN" or now >= run.deadline or
                    run.attempt_count >= run.max_attempts or not approval or
                    approval.status != "APPROVED" or approval.expires_at <= now or
                    approval.action_hash != current_hash or run.action_hash != current_hash or
                    adapter.assess(run.action).decision != "APPROVAL_REQUIRED"):
                return False
            run.attempt_count += 1
            event(s, run, "action_retry", {"attempt": run.attempt_count,
                  "action_id": run.action["action_id"]})
            return True

    def finish(self, lease, outcome, *, source="direct"):
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            try:
                adapter, _ = self._resolve(run)
                verified = adapter.tool.verified(outcome, run.action, run.action_hash, source=source)
            except ContractMismatch:
                verified = False
            if verified:
                run.state, run.action_status, run.operation = "COMPLETED", "COMMITTED", outcome
                event(s, run, "action_verified", {"operation_id": outcome["operation_id"]})
            else:
                run.state, run.action_status = "NEEDS_REVIEW", "UNKNOWN"
                event(s, run, "outcome_unknown", {"action_id": run.action["action_id"]})
            job.state = "DONE"

    def decide(self, approval_id, decision, actor):
        with self.sessions.begin() as s:
            approval = s.scalar(select(Approval).where(Approval.id == approval_id).with_for_update())
            if approval is None:
                raise LookupError()
            run = s.scalar(select(Run).where(Run.id == approval.run_id).with_for_update())
            now = db_now(s)
            if approval.status != "PENDING" or run.state != "WAITING_FOR_APPROVAL" or approval.expires_at <= now or run.deadline <= now:
                raise Conflict("Approval is no longer pending or has expired")
            try:
                adapter, _ = self._resolve(run)
                current_hash = adapter.hash_action(run.action)
            except ContractMismatch as error:
                raise Conflict("Action contract changed or is invalid") from error
            if current_hash != approval.action_hash or current_hash != run.action_hash:
                raise Conflict("Action changed; approval is invalid")
            approval.status, approval.actor, approval.decided_at = decision, actor, now
            run.state = "RUNNING" if decision == "APPROVED" else "REJECTED"
            event(s, run, "approval_decided", {"approval_id": approval.id, "actor": actor, "decision": decision})
            if decision == "APPROVED":
                s.add(new_job(run.id, now, approval.id))

    def renew_approval(self, approval_id, actor):
        with self.sessions.begin() as s:
            approval = s.scalar(select(Approval).where(Approval.id == approval_id).with_for_update())
            if approval is None:
                raise LookupError()
            run = s.scalar(select(Run).where(Run.id == approval.run_id).with_for_update())
            now = db_now(s)
            try:
                adapter, _ = self.registry.resolve(run.request)
                current_hash = adapter.hash_action(adapter.validate_action(run.action))
            except (ContractMismatch, KeyError, TypeError, ValueError, ValidationError) as error:
                raise Conflict("Action contract changed or is invalid") from error
            if (approval.status not in ("PENDING", "APPROVED") or run.state not in ("WAITING_FOR_APPROVAL", "NEEDS_REVIEW")
                    or run.action_status != "NOT_SENT" or run.deadline <= now
                    or (approval.expires_at > now and approval.action_hash == current_hash)):
                raise Conflict("Approval cannot be replaced")
            approval.status = "SUPERSEDED"
            run.action_hash, run.state = current_hash, "WAITING_FOR_APPROVAL"
            replacement = Approval(id=uid(), run_id=run.id, action=run.action, action_hash=current_hash,
                                   status="PENDING", expires_at=now + timedelta(minutes=15))
            s.add(replacement)
            event(s, run, "approval_replaced", {"previous_approval_id": approval.id,
                  "approval_id": replacement.id, "actor": actor, "action_hash": current_hash})
            return replacement.id
