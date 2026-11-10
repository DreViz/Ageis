from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from runtime.db import EvaluationJob
from evaluation.jobs import JobConflict, JobService, LeaseLost
from evaluation.store import EvaluationStore
import runtime.api as api
from test_runtime import context
from test_cross_evaluation import mock_server, notice_server

URL = os.getenv('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not URL, reason='PostgreSQL TEST_DATABASE_URL required')


def service(context, urls=None):
    return JobService(context[1], urls or {'access':'http://localhost:8000', 'notice':'http://localhost:8002'})


def result(scenario):
    return {'scenario': scenario, 'status': 'passed', 'failed_assertions': [], 'evidence': {'synthetic': True}}


def expire(sessions, job_id):
    with sessions.begin() as session:
        session.get(EvaluationJob, job_id).lease_expires_at -= timedelta(minutes=1)


def test_idempotent_creation_conflict_and_concurrent_claim(context):
    jobs = service(context)
    with ThreadPoolExecutor(max_workers=2) as pool:
        created = list(pool.map(lambda _: jobs.create('same-key', 'safe-v1'), range(2)))
    assert len({item[0]['id'] for item in created}) == 1 and sorted(item[1] for item in created) == [False, True]
    with pytest.raises(JobConflict):
        jobs.create('same-key', 'unsafe-retry-v1')
    with ThreadPoolExecutor(max_workers=2) as pool:
        leases = list(pool.map(jobs.claim, ['worker-a', 'worker-b']))
    assert sum(lease is not None for lease in leases) == 1


def test_recovery_from_checkpoint_fences_old_worker(context):
    jobs, sessions = service(context), context[1]
    job, _ = jobs.create('resume', 'safe-v1')
    old = jobs.claim('old')
    scenario, _, _ = jobs.reserve(old)
    jobs.checkpoint(old, scenario, result(scenario))
    expire(sessions, job['id'])
    replacement = jobs.claim('replacement')
    assert replacement[2] > old[2]
    with pytest.raises(LeaseLost):
        jobs.reserve(old)
    next_scenario, _, _ = jobs.reserve(replacement)
    assert next_scenario != scenario and jobs.get(job['id'])['completed'] == 1


@pytest.mark.parametrize('position', [0, 7])
def test_abandoned_inflight_never_replayed_for_either_workflow(context, position):
    jobs, sessions = service(context), context[1]
    job, _ = jobs.create('abandoned-' + str(position), 'safe-v1')
    lease = jobs.claim('old')
    for _ in range(position):
        scenario, _, _ = jobs.reserve(lease)
        jobs.checkpoint(lease, scenario, result(scenario))
    scenario, _, _ = jobs.reserve(lease)
    assert scenario.startswith('access::' if position == 0 else 'notice::')
    expire(sessions, job['id'])
    assert jobs.claim('replacement') is None
    assert jobs.get(job['id'])['state'] == 'INTERRUPTED'
    assert jobs.get(job['id'])['error'] == 'scenario_outcome_unconfirmed'
    assert jobs.get(job['id'])['current_fixture']['schema'].startswith('demo_')
    with pytest.raises(LeaseLost):
        jobs.checkpoint(lease, scenario, result(scenario))
    assert EvaluationStore(sessions).load(job['id']) is None


def test_complete_artifact_is_atomic_and_compatible(context):
    jobs, sessions = service(context), context[1]
    job, _ = jobs.create('complete', 'safe-v1')
    lease = jobs.claim('worker')
    while jobs.get(job['id'])['state'] == 'RUNNING':
        scenario, _, _ = jobs.reserve(lease)
        jobs.checkpoint(lease, scenario, result(scenario))
    final = jobs.get(job['id'])
    assert final['state'] == 'COMPLETED' and final['result_id'] == job['id'] and final['completed'] == final['total'] == 14
    artifact = EvaluationStore(sessions).load(job['id'])
    assert artifact['counts']['passed'] == 14 and artifact['manifest']['runner_version'] == 'durable-cross-v1'


def test_last_checkpoint_and_artifact_roll_back_together(context, monkeypatch):
    jobs, sessions = service(context), context[1]
    job, _ = jobs.create('rollback', 'safe-v1')
    lease = jobs.claim('worker')
    for _ in range(13):
        scenario, _, _ = jobs.reserve(lease)
        jobs.checkpoint(lease, scenario, result(scenario))
    scenario, _, _ = jobs.reserve(lease)
    original = EvaluationStore.save_in_transaction
    def fail(session, artifact):
        original(session, artifact)
        raise RuntimeError('forced failure')
    monkeypatch.setattr(EvaluationStore, 'save_in_transaction', fail)
    with pytest.raises(RuntimeError):
        jobs.checkpoint(lease, scenario, result(scenario))
    assert jobs.get(job['id'])['completed'] == 13 and jobs.get(job['id'])['current_scenario'] == scenario
    assert EvaluationStore(sessions).load(job['id']) is None


def test_deadline_and_contract_drift_fail_closed(context):
    jobs, sessions = service(context), context[1]
    job, _ = jobs.create('expired', 'safe-v1')
    with sessions.begin() as session:
        session.get(EvaluationJob, job['id']).deadline -= timedelta(hours=1)
    assert jobs.claim('worker') is None and jobs.get(job['id'])['error'] == 'deadline_exceeded'
    job, _ = jobs.create('drift', 'safe-v1')
    changed = service(context, {'access':'http://localhost:8001', 'notice':'http://localhost:8002'})
    assert changed.claim('worker') is None and jobs.get(job['id'])['error'] == 'execution_contract_changed'


def test_corrupted_checkpoint_cannot_finish_or_be_read(context):
    jobs, sessions = service(context), context[1]
    job, _ = jobs.create('corrupt', 'safe-v1')
    lease = jobs.claim('worker')
    scenario, _, _ = jobs.reserve(lease)
    jobs.checkpoint(lease, scenario, result(scenario))
    with sessions.begin() as session:
        row = session.get(EvaluationJob, job['id'])
        row.results = [{**row.results[0], 'status': 'failed'}]
    with pytest.raises(JobConflict):
        jobs.get(job['id'])
    assert jobs.reserve(lease) is None
    with sessions() as session:
        assert session.get(EvaluationJob, job['id']).error == 'checkpoint_integrity_failure'
    assert EvaluationStore(sessions).load(job['id']) is None


def test_authenticated_job_api_and_idempotency(context, monkeypatch):
    _, sessions = context
    monkeypatch.setattr(api, 'make_database', lambda url: (sessions.kw['bind'], sessions))
    app = api.create_app(URL, operator_token='secret', identity_url='http://localhost:8000', notice_url='http://localhost:8002')
    headers = {'Authorization':'Bearer secret', 'Idempotency-Key':'api-key'}
    with TestClient(app) as client:
        assert client.get('/evaluation-jobs').status_code == 401
        assert client.post('/evaluation-jobs', json={'candidate':'safe-v1'}).status_code == 401
        assert client.post('/evaluation-jobs', headers={'Authorization':'Bearer secret'}, json={'candidate':'safe-v1'}).status_code == 422
        one = client.post('/evaluation-jobs', headers=headers, json={'candidate':'safe-v1'})
        two = client.post('/evaluation-jobs', headers=headers, json={'candidate':'safe-v1'})
        assert one.status_code == two.status_code == 202 and one.json()['id'] == two.json()['id']
        assert one.json()['created'] is True and two.json()['created'] is False
        assert client.post('/evaluation-jobs', headers=headers, json={'candidate':'unsafe-retry-v1'}).status_code == 409
        assert client.get('/evaluation-jobs/' + one.json()['id'], headers=headers).json()['state'] == 'QUEUED'
        assert client.get('/evaluation-jobs', headers=headers).json()[0]['id'] == one.json()['id']
        assert client.get('/evaluation-jobs/missing', headers=headers).status_code == 404


def test_real_cross_workflow_job(context, mock_server, notice_server):
    jobs, sessions = service(context, {'access':mock_server, 'notice':notice_server}), context[1]
    job, _ = jobs.create('real-' + uuid.uuid4().hex, 'unsafe-retry-v1')
    jobs.process(jobs.claim('worker'), URL)
    final = jobs.get(job['id'])
    assert final['state'] == 'COMPLETED', final
    artifact = EvaluationStore(sessions).load(job['id'])
    assert artifact['counts'] == {'passed':12,'failed':2,'error':0,'inconclusive':0}
    for item in artifact['results']:
        assert item['fixture']['schema'] == item['evidence']['schema']
        assert item['fixture']['scope'] in item['evidence']['action'].values()
    assert {item['scenario'] for item in artifact['results'] if item['status'] == 'failed'} == {
        'access::timeout-after-commit', 'notice::timeout-after-commit'}
    # These are test-owned, uniquely generated schema names.
    from sqlalchemy import create_engine, text
    engine = create_engine(URL)
    with engine.begin() as connection:
        for item in artifact['results']:
            schema = item.get('evidence', {}).get('schema')
            if schema:
                assert schema.startswith('demo_') and len(schema) == 37 and all(c in '0123456789abcdef' for c in schema[5:])
                connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()
