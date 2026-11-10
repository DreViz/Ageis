"""Durable, fenced execution of trusted cross-workflow evaluations.

An abandoned in-flight scenario is never automatically replayed: its external
outcome and synthetic fixture may still be live. Checkpointed scenarios survive.
"""
from datetime import timedelta
import hashlib
from pathlib import Path
import threading
import uuid
from urllib.parse import urlparse

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError

from runtime.db import EvaluationJob, EvaluationJobEvent
from .cross import cross_manifest, run_cross_case
from .faults import HOSTS
from .runner import digest
from .store import EvaluationStore, STATUSES


class JobConflict(Exception):
    pass


class LeaseLost(Exception):
    pass


def valid_urls(urls):
    return set(urls) == {'access', 'notice'} and all(
        isinstance(url, str) and urlparse(url).scheme in ('http', 'https') and urlparse(url).hostname in HOSTS
        for url in urls.values())


def now(session):
    return session.scalar(select(func.clock_timestamp()))


def job_manifest(candidate):
    _, manifest = cross_manifest(candidate)
    source = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
              for name in ('jobs.py', 'store.py')}
    manifest['code_hash'] = digest({'cross': manifest['code_hash'], 'durable': source})
    manifest['candidate_hash'] = digest({'candidate': candidate, 'code_hash': manifest['code_hash']})
    manifest['runner_version'] = 'durable-cross-v1'
    return manifest


def summary(job):
    state = 'CANCELLED' if job.error == 'cancelled_by_operator' else job.state
    return {'id': job.id, 'suite': 'cross-v1', 'candidate': job.candidate, 'state': state,
            'created_at': job.created_at, 'deadline': job.deadline, 'completed': len(job.results),
            'total': len(job.manifest['scenario_ids']), 'current_scenario': job.in_flight, 'current_fixture': job.fixture,
            'results': job.results, 'error': job.error, 'result_id': job.result_id}


def checkpoints_valid(job):
    if not isinstance(job.results, list) or len(job.results) > len(job.manifest['scenario_ids']):
        return False
    for index, item in enumerate(job.results):
        if not isinstance(item, dict) or item.get('scenario') != job.manifest['scenario_ids'][index]:
            return False
        if item.get('checkpoint_hash') != digest({key: value for key, value in item.items() if key != 'checkpoint_hash'}):
            return False
    return True


class JobService:
    def __init__(self, sessions, urls):
        if not valid_urls(urls):
            raise ValueError('Synthetic services are not configured')
        self.sessions, self.urls = sessions, dict(urls)
        self.config_hash = digest(self.urls)

    def create(self, request_key, candidate):
        if not isinstance(request_key, str) or not request_key or len(request_key) > 128 or not request_key.isascii():
            raise ValueError('Invalid idempotency key')
        manifest = job_manifest(candidate)
        try:
            with self.sessions.begin() as session:
                current = now(session)
                job = EvaluationJob(id=str(uuid.uuid4()), request_key=request_key, candidate=candidate,
                    manifest=manifest, config_hash=self.config_hash, state='QUEUED', created_at=current,
                    deadline=current + timedelta(minutes=30), fence=0, claims=0, results=[])
                session.add(job)
                session.flush()
                return summary(job), True
        except IntegrityError:
            with self.sessions() as session:
                job = session.scalar(select(EvaluationJob).where(EvaluationJob.request_key == request_key))
                if job is None:
                    raise
                if job.candidate != candidate:
                    raise JobConflict('Idempotency key belongs to another request')
                if not checkpoints_valid(job):
                    raise JobConflict('Stored checkpoints are invalid')
                return summary(job), False

    def get(self, job_id):
        with self.sessions() as session:
            job = session.get(EvaluationJob, job_id)
            if job is not None and not checkpoints_valid(job):
                raise JobConflict('Stored checkpoints are invalid')
            return summary(job) if job else None

    def list(self, limit=50):
        with self.sessions() as session:
            rows = session.scalars(select(EvaluationJob).order_by(EvaluationJob.created_at.desc(), EvaluationJob.id.desc()).limit(limit)).all()
            if any(not checkpoints_valid(job) for job in rows):
                raise JobConflict('Stored checkpoints are invalid')
            return [summary(job) for job in rows]

    def cancel(self, job_id, actor='demo-operator'):
        with self.sessions.begin() as session:
            job = session.scalar(select(EvaluationJob).where(EvaluationJob.id == job_id).with_for_update())
            if job is None:
                raise LookupError('Evaluation job not found')
            if job.error == 'cancelled_by_operator':
                return summary(job)
            if job.state not in ('QUEUED', 'RUNNING', 'INTERRUPTED'):
                raise JobConflict('Job cannot be cancelled')
            session.add(EvaluationJobEvent(id=str(uuid.uuid4()), job_id=job.id, kind='cancelled', actor=actor,
                created_at=now(session), payload={'scenario': job.in_flight, 'fixture': job.fixture}))
            # Keep an uncertain in-flight fixture for inspection. An already-sent
            # remote request can still complete after cancellation.
            job.state = 'INTERRUPTED' if job.in_flight else 'FAILED'
            job.error = 'cancelled_by_operator'
            job.fence += 1
            job.lease_owner, job.lease_expires_at = None, None
            return summary(job)

    def resolve_interrupted(self, job_id, actor):
        with self.sessions.begin() as session:
            job = session.scalar(select(EvaluationJob).where(EvaluationJob.id == job_id).with_for_update())
            if job is None:
                raise LookupError('Evaluation job not found')
            current = now(session)
            if job.state != 'INTERRUPTED' or job.error not in ('scenario_outcome_unconfirmed', 'deadline_during_scenario'):
                raise JobConflict('Job is not awaiting interruption review')
            if not job.in_flight or not job.fixture or not checkpoints_valid(job):
                raise JobConflict('Interrupted job evidence is incomplete')
            if len(job.results) >= len(job.manifest['scenario_ids']) or job.in_flight != job.manifest['scenario_ids'][len(job.results)]:
                raise JobConflict('Interrupted scenario does not match the checkpoint')
            if job.deadline <= current or (job.claims >= 3 and len(job.results) + 1 < len(job.manifest['scenario_ids'])):
                raise JobConflict('Job deadline or claim limit has been reached')
            if job_manifest(job.candidate) != job.manifest or job.config_hash != self.config_hash:
                raise JobConflict('Execution contract changed')
            result = {'scenario': job.in_flight, 'status': 'inconclusive', 'failed_assertions': [],
                      'reason': 'interrupted_outcome_unconfirmed', 'fixture': job.fixture,
                      'recovery': {'actor': actor, 'recorded_at': current.isoformat(), 'action': 'continue_inconclusive'}}
            session.add(EvaluationJobEvent(id=str(uuid.uuid4()), job_id=job.id, kind='continued_inconclusive',
                actor=actor, created_at=current, payload={'scenario': job.in_flight, 'fixture': job.fixture}))
            job.results = [*job.results, {**result, 'checkpoint_hash': digest(result)}]
            job.in_flight, job.fixture = None, None
            job.error = None
            job.fence += 1
            job.lease_owner, job.lease_expires_at = None, None
            if len(job.results) == len(job.manifest['scenario_ids']):
                self.finish(session, job)
            else:
                job.state = 'QUEUED'
            return summary(job)

    def events(self, job_id):
        with self.sessions() as session:
            if session.get(EvaluationJob, job_id) is None:
                raise LookupError('Evaluation job not found')
            rows = session.scalars(select(EvaluationJobEvent).where(EvaluationJobEvent.job_id == job_id)
                                   .order_by(EvaluationJobEvent.created_at, EvaluationJobEvent.id)).all()
            return [{'kind': row.kind, 'actor': row.actor, 'created_at': row.created_at, 'payload': row.payload} for row in rows]

    def claim(self, owner, lease_seconds=30):
        with self.sessions.begin() as session:
            current = now(session)
            job = session.scalar(select(EvaluationJob).where(or_(EvaluationJob.state == 'QUEUED',
                and_(EvaluationJob.state == 'RUNNING', EvaluationJob.lease_expires_at <= current)))
                .order_by(EvaluationJob.created_at, EvaluationJob.id).with_for_update(skip_locked=True).limit(1))
            if job is None:
                return None
            if job.in_flight is not None:
                job.state, job.error = 'INTERRUPTED', 'scenario_outcome_unconfirmed'
                job.lease_owner, job.lease_expires_at = None, None
                return None
            if job.deadline <= current or job.claims >= 3:
                job.state, job.error = 'FAILED', 'deadline_exceeded' if job.deadline <= current else 'claim_limit_reached'
                job.lease_owner, job.lease_expires_at = None, None
                return None
            if not checkpoints_valid(job):
                job.state, job.error = 'FAILED', 'checkpoint_integrity_failure'
                return None
            manifest = job_manifest(job.candidate)
            if manifest != job.manifest or job.config_hash != self.config_hash:
                job.state, job.error = 'FAILED', 'execution_contract_changed'
                return None
            job.state, job.lease_owner = 'RUNNING', owner
            job.lease_expires_at = current + timedelta(seconds=lease_seconds)
            job.fence += 1
            job.claims += 1
            return (job.id, owner, job.fence)

    def locked(self, session, lease):
        job = session.scalar(select(EvaluationJob).where(EvaluationJob.id == lease[0]).with_for_update())
        current = now(session)
        if (job is None or job.state != 'RUNNING' or job.lease_owner != lease[1] or job.fence != lease[2]
            or job.lease_expires_at is None or job.lease_expires_at <= current):
            raise LeaseLost()
        return job, current

    def renew(self, lease, lease_seconds=30):
        with self.sessions.begin() as session:
            job, current = self.locked(session, lease)
            if not checkpoints_valid(job):
                job.state, job.error = 'FAILED', 'checkpoint_integrity_failure'
                return None
            if current >= job.deadline:
                raise LeaseLost()
            job.lease_expires_at = min(current + timedelta(seconds=lease_seconds), job.deadline)

    def reserve(self, lease):
        with self.sessions.begin() as session:
            job, current = self.locked(session, lease)
            if not checkpoints_valid(job):
                job.state, job.error = 'FAILED', 'checkpoint_integrity_failure'
                return
            if current >= job.deadline:
                job.state, job.error = 'FAILED', 'deadline_exceeded'
                return None
            if job.in_flight is not None:
                raise JobConflict('Scenario is already in flight')
            scenarios = job.manifest['scenario_ids']
            if len(job.results) == len(scenarios):
                self.finish(session, job)
                return None
            job.in_flight = scenarios[len(job.results)]
            job.fixture = {'schema': 'demo_' + uuid.uuid4().hex, 'scope': uuid.uuid4().hex}
            return job.in_flight, job.candidate, dict(job.fixture)

    def checkpoint(self, lease, scenario, result):
        with self.sessions.begin() as session:
            job, current = self.locked(session, lease)
            if not checkpoints_valid(job):
                job.state, job.error = 'FAILED', 'checkpoint_integrity_failure'
                return
            if current >= job.deadline:
                job.state, job.error = 'INTERRUPTED', 'deadline_during_scenario'
                return
            if (job.in_flight != scenario or result.get('scenario') != scenario or
                result.get('status') not in STATUSES):
                raise JobConflict('Invalid scenario checkpoint')
            if 'checkpoint_hash' in result:
                raise JobConflict('Reserved checkpoint field')
            job.results = [*job.results, {**result, 'checkpoint_hash': digest(result)}]
            job.in_flight = None
            job.fixture = None
            if len(job.results) == len(job.manifest['scenario_ids']):
                self.finish(session, job)

    @staticmethod
    def finish(session, job):
        counts = {status: sum(item['status'] == status for item in job.results) for status in STATUSES}
        artifact = {'kind': 'evaluation', 'id': job.id, 'created_at': job.created_at.isoformat(),
                    'manifest': job.manifest, 'results': job.results, 'counts': counts, 'total': len(job.results)}
        artifact['evidence_hash'] = digest(artifact)
        EvaluationStore.save_in_transaction(session, artifact)
        job.state, job.result_id = 'COMPLETED', job.id
        job.lease_owner, job.lease_expires_at = None, None

    def process(self, lease, database_url, lease_seconds=30):
        stop, lost = threading.Event(), threading.Event()
        def heartbeat():
            while not stop.wait(lease_seconds / 3):
                try:
                    self.renew(lease, lease_seconds)
                except Exception:
                    lost.set()
                    return
        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            while not lost.is_set():
                item = self.reserve(lease)
                if item is None:
                    return
                scenario, candidate, fixture = item
                workflow, profile = scenario.split('::', 1)
                result = {**run_cross_case(database_url, self.urls[workflow], workflow, profile, candidate, fixture=fixture),
                          'fixture': fixture}
                if lost.is_set():
                    return
                self.checkpoint(lease, scenario, result)
        except LeaseLost:
            return
        finally:
            stop.set()
            thread.join()
