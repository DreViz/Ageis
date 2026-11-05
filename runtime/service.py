"""Durable state transitions. Every leased update locks and fences the job."""
from datetime import timedelta
import uuid
from sqlalchemy import and_, func, or_, select
from .db import Approval, Event, Job, ModelSession, Run
from .workflow import POLICY_VERSION, evaluate
from .model import PROMPT_HASH, TOOL_SCHEMA_HASH, ModelResult, Proposal, Question, grounded_proposal
from pydantic import ValidationError
from mock_identity.app import GrantRequest, request_hash


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
    def __init__(self, sessions):
        self.sessions = sessions

    def create_run(self, tenant_id, user_id, workspace_id):
        with self.sessions.begin() as s:
            now = db_now(s)
            action = GrantRequest(tenant_id=tenant_id, user_id=user_id, workspace_id=workspace_id,
                                  action_id=uid()).model_dump()
            run = Run(id=uid(), state="PENDING", request={"tenant_id": tenant_id, "user_id": user_id,
                      "workspace_id": workspace_id}, action=action, action_hash=request_hash(GrantRequest(**action)),
                      idempotency_key=uid(), action_status="NOT_SENT", deadline=now + timedelta(hours=1),
                      max_attempts=3, attempt_count=0, event_seq=0)
            s.add(run)
            event(s, run, "run_created")
            s.add(new_job(run.id, now))
            return run.id

    def create_model_run(self, tenant_id, text, model_id):
        if not tenant_id or len(tenant_id) > 128 or not text.strip() or len(text) > 2000 or not model_id or len(model_id) > 128:
            raise ValueError("Invalid model run request")
        with self.sessions.begin() as s:
            now = db_now(s)
            action = GrantRequest(tenant_id=tenant_id, user_id="unresolved", workspace_id="unresolved", action_id=uid()).model_dump()
            run = Run(id=uid(), state="PENDING_MODEL", request={"tenant_id": tenant_id}, action=action,
                      action_hash=request_hash(GrantRequest(**action)), idempotency_key=uid(), action_status="NOT_SENT",
                      deadline=now + timedelta(hours=1), max_attempts=3, attempt_count=0, event_seq=0)
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
                policy = evaluate(run.action)
                event(s, run, "directory_checked", {"user_id": run.action["user_id"],
                      "resolved": policy != "UNKNOWN_USER"})
                event(s, run, "policy_checked", {"policy_version": POLICY_VERSION, "result": policy})
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
            current_hash = request_hash(GrantRequest(**run.action))
            if not approval or approval.status != "APPROVED" or approval.expires_at <= now or approval.action_hash != current_hash or run.action_hash != current_hash:
                run.state, job.state = "NEEDS_REVIEW", "DONE"
                event(s, run, "approval_invalid")
                return None
            if evaluate(run.action) != "APPROVAL_REQUIRED":
                run.state, job.state = "NEEDS_REVIEW", "DONE"
                event(s, run, "policy_revalidation_failed", {"policy_version": POLICY_VERSION})
                return None
            if run.action_status == "UNKNOWN":
                run.state = "RECONCILING"
                event(s, run, "reconciliation_started")
                return {"mode": "reconcile", "action": run.action, "key": run.idempotency_key, "hash": run.action_hash}
            if run.attempt_count >= run.max_attempts:
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "attempt_limit_reached")
                return None
            # Commit an unknown intent and the stable key before initiating the remote request.
            run.attempt_count += 1
            run.action_status = "UNKNOWN"
            event(s, run, "action_started", {"attempt": run.attempt_count, "action_id": run.action["action_id"]})
            return {"mode": "send", "action": run.action, "key": run.idempotency_key, "hash": run.action_hash}

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
            valid_output = isinstance(result, ModelResult) and result.kind in ("propose_access", "request_input")
            if valid_output:
                try:
                    (Proposal if result.kind == "propose_access" else Question).model_validate(result.arguments)
                except ValidationError:
                    valid_output = False
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
            proposal = grounded_proposal(result, model.messages)
            event(s, run, "model_call_completed", {"tool": result.kind,
                  "usage_available": result.input_tokens is not None and result.output_tokens is not None})
            if proposal is None and model.model_calls >= model.max_calls:
                run.state, job.state = "FAILED", "DONE"
                event(s, run, "model_call_limit_reached")
                return
            if proposal is None:
                # Do not display arbitrary model text or accept ungrounded targets.
                question = "Please specify the synthetic user and workspace for this access request."
                model.question = question
                model.messages = [*model.messages, {"role": "assistant", "content": question}]
                run.state, job.state = "WAITING_FOR_INPUT", "DONE"
                event(s, run, "input_requested")
                return
            action = {**run.action, **proposal}
            run.action, run.action_hash = action, request_hash(GrantRequest(**action))
            model.question = None
            run.state, job.state = "PENDING", "DONE"
            event(s, run, "model_proposal_validated", proposal)
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

    def reserve_retry(self, lease):
        """Bound retries of this same logical action to the run and approval deadlines."""
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            approval = s.get(Approval, job.approval_id)
            now = db_now(s)
            current_hash = request_hash(GrantRequest(**run.action))
            if (run.action_status != "UNKNOWN" or now >= run.deadline or
                    run.attempt_count >= run.max_attempts or not approval or
                    approval.status != "APPROVED" or approval.expires_at <= now or
                    approval.action_hash != current_hash or run.action_hash != current_hash or
                    evaluate(run.action) != "APPROVAL_REQUIRED"):
                return False
            run.attempt_count += 1
            event(s, run, "action_retry", {"attempt": run.attempt_count,
                  "action_id": run.action["action_id"]})
            return True

    def finish(self, lease, outcome):
        with self.sessions.begin() as s:
            job = self.locked_job(s, lease)
            run = s.scalar(select(Run).where(Run.id == job.run_id).with_for_update())
            if isinstance(outcome, dict) and outcome.get("status") == "committed" and outcome.get("request_hash") == run.action_hash and outcome.get("action_id") == run.action["action_id"]:
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
            current_hash = request_hash(GrantRequest(**run.action))
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
            current_hash = request_hash(GrantRequest(**run.action))
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
