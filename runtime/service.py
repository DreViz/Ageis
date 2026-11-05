"""Durable state transitions. Every leased update locks and fences the job."""
from datetime import timedelta
import uuid
from sqlalchemy import and_, func, or_, select
from .db import Approval, Event, Job, Run
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
            if run.state == "PENDING":
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

    def check_lease(self, lease):
        with self.sessions.begin() as s:
            self.locked_job(s, lease)

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
