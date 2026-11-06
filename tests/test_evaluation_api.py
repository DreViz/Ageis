from datetime import datetime, timezone
import copy
import os
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text
import evaluation.api as evaluation_api
import runtime.api as api
from evaluation.runner import digest
from evaluation.store import EvaluationStore
from test_evaluation import artifact
from test_runtime import context

pytestmark = pytest.mark.skipif(not os.getenv('TEST_DATABASE_URL'), reason='PostgreSQL TEST_DATABASE_URL required')


def make_artifact(candidate='safe-v1'):
    value = artifact(candidate)
    value['id'] = str(uuid.uuid4())
    value['created_at'] = datetime.now(timezone.utc).isoformat()
    value['evidence_hash'] = digest({k: v for k, v in value.items() if k != 'evidence_hash'})
    return value


def client_for(context, monkeypatch, configured=True):
    _, sessions = context
    monkeypatch.setattr(api, 'make_database', lambda url: (sessions.kw['bind'], sessions))
    return TestClient(api.create_app('postgresql://unused', operator_token='secret',
        identity_url='http://localhost' if configured else None, notice_url='http://localhost' if configured else None))


def test_auth_required_on_all_evaluation_routes(context, monkeypatch):
    with client_for(context, monkeypatch) as client:
        for method, path, body in [('GET', '/evaluations', None), ('GET', '/evaluations/x', None),
            ('POST', '/evaluations', {'candidate':'safe-v1'}),
            ('POST', '/evaluation-comparisons', {'baseline_id':'x','candidate_id':'y'}),
            ('GET', '/evaluation-comparisons', None),
            ('GET', '/evaluation-comparisons/x', None),
            ('POST', '/evaluation-comparisons/x/decision', {'decision':'approved'}),
            ('GET', '/evaluation-comparisons/x/decision', None)]:
            assert client.request(method, path, json=body).status_code == 401
            assert client.request(method, path, json=body, headers={'Authorization':'Bearer wrong'}).status_code == 401


def test_start_compare_and_record_durable_decision(context, monkeypatch):
    _, sessions = context
    def fake_run(database_url, urls, candidate):
        return make_artifact(candidate)
    monkeypatch.setattr(evaluation_api, 'evaluate_cross', fake_run)
    auth = {'Authorization':'Bearer secret'}
    with client_for(context, monkeypatch) as client:
        baseline = client.post('/evaluations', headers=auth, json={'candidate':'safe-v1'} )
        candidate = client.post('/evaluations', headers=auth, json={'candidate':'unsafe-retry-v1'})
        assert baseline.status_code == candidate.status_code == 201
        baseline_id, candidate_id = baseline.json()['id'], candidate.json()['id']
        assert client.get('/evaluations', headers=auth).json()[0]['id'] == candidate_id
        assert client.get('/evaluations/' + baseline_id, headers=auth).json()['id'] == baseline_id
        assert client.get('/evaluations/missing', headers=auth).status_code == 404
        response = client.post('/evaluation-comparisons', headers=auth, json={'baseline_id':baseline_id, 'candidate_id':candidate_id})
        assert response.status_code == 201, response.text
        comp = response.json()
        assert comp['deployment_performed'] is False
        assert client.get('/evaluation-comparisons/' + comp['id'], headers=auth).json() == comp
        path = '/evaluation-comparisons/' + comp['id'] + '/decision'
        assert client.get(path, headers=auth).status_code == 404
        decision = client.post(path, headers=auth, json={'decision':'needs_review'})
        assert decision.status_code == 201
        assert decision.json()['actor'] == 'demo-operator' and decision.json()['deployment_performed'] is False
        assert client.post(path, headers=auth, json={'decision':'approved'}).status_code == 409
        assert client.get(path, headers=auth).json() == decision.json()
    with client_for(context, monkeypatch) as client:
        assert client.get('/evaluation-comparisons/' + comp['id'] + '/decision', headers=auth).json() == decision.json()
    assert EvaluationStore(sessions).load(baseline_id)['id'] == baseline_id


def test_invalid_inputs_incompatible_comparison_and_unconfigured(context, monkeypatch):
    _, sessions = context
    auth = {'Authorization':'Bearer secret'}
    left, right = make_artifact(), make_artifact()
    right['manifest']['fixture_hash'] = 'different'
    right['evidence_hash'] = digest({k:v for k,v in right.items() if k != 'evidence_hash'})
    store = EvaluationStore(sessions)
    store.save(left)
    store.save(right)
    with client_for(context, monkeypatch, configured=False) as client:
        assert client.post('/evaluations', headers=auth, json={'candidate':'safe-v1'}).status_code == 503
        assert client.post('/evaluations', headers=auth, json={'candidate':'other'}).status_code == 422
        assert client.post('/evaluations', headers=auth, json={'candidate':'safe-v1','url':'http://example.com'}).status_code == 422
        assert client.get('/evaluations?limit=101', headers=auth).status_code == 422
        assert client.post('/evaluation-comparisons', headers=auth, json={'baseline_id':left['id'],'candidate_id':right['id']}).status_code == 409
        assert client.post('/evaluation-comparisons', headers=auth, json={'baseline_id':left['id'],'candidate_id':'missing'}).status_code == 404


def test_concurrent_evaluation_rejected_by_database_lock(context, monkeypatch):
    _, sessions = context
    engine = sessions.kw['bind']
    with engine.connect() as connection:
        connection.execute(text('SELECT pg_advisory_lock(:key)'), {'key':evaluation_api.EVALUATION_LOCK})
        try:
            with client_for(context, monkeypatch) as client:
                response = client.post('/evaluations', headers={'Authorization':'Bearer secret'}, json={'candidate':'safe-v1'})
                assert response.status_code == 409
        finally:
            connection.execute(text('SELECT pg_advisory_unlock(:key)'), {'key':evaluation_api.EVALUATION_LOCK})


def test_corrupt_stored_evidence_cannot_be_listed_or_read(context, monkeypatch):
    from runtime.db import EvaluationScenario
    _, sessions = context
    value = make_artifact()
    EvaluationStore(sessions).save(value)
    with sessions.begin() as session:
        row = session.get(EvaluationScenario, (value['id'], 0))
        row.result = {**row.result, 'evidence': {'tampered': True}}
    auth = {'Authorization':'Bearer secret'}
    with client_for(context, monkeypatch) as client:
        assert client.get('/evaluations/' + value['id'], headers=auth).status_code == 409
        assert client.get('/evaluations', headers=auth).status_code == 409


def test_comparison_history_order_limit_decision_and_restart(context, monkeypatch):
    _, sessions = context
    first, second = make_artifact(), make_artifact('unsafe-retry-v1')
    store = EvaluationStore(sessions)
    store.save(first)
    store.save(second)
    auth = {'Authorization': 'Bearer secret'}
    payload = {'baseline_id': first['id'], 'candidate_id': second['id']}
    with client_for(context, monkeypatch) as client:
        assert client.get('/evaluation-comparisons', headers=auth).json() == []
        old = client.post('/evaluation-comparisons', headers=auth, json=payload).json()
        new = client.post('/evaluation-comparisons', headers=auth, json=payload).json()
        decision = client.post(f"/evaluation-comparisons/{new['id']}/decision", headers=auth,
                               json={'decision': 'needs_review'}).json()
        rows = client.get('/evaluation-comparisons', headers=auth).json()
        assert [r['id'] for r in rows] == [new['id'], old['id']]
        assert rows[0]['decision']['decision'] == decision['decision']
        assert rows[0]['decision']['actor'] == 'demo-operator'
        assert rows[1]['decision'] is None
        assert rows[0]['baseline_id'] == first['id'] and rows[0]['candidate_id'] == second['id']
        assert rows[0]['suite_id'] == first['manifest']['suite_id']
        assert rows[0]['candidate_counts'] == second['counts']
        assert len(client.get('/evaluation-comparisons?limit=1', headers=auth).json()) == 1
        assert client.get('/evaluation-comparisons?limit=0', headers=auth).status_code == 422
        assert client.get('/evaluation-comparisons?limit=101', headers=auth).status_code == 422
    with client_for(context, monkeypatch) as client:
        assert client.get('/evaluation-comparisons', headers=auth).json() == rows


def test_comparison_history_rejects_tampered_records(context, monkeypatch):
    from runtime.db import EvaluationComparison, EvaluationDecision
    _, sessions = context
    left, right = make_artifact(), make_artifact('unsafe-retry-v1')
    store = EvaluationStore(sessions)
    store.save(left)
    store.save(right)
    auth = {'Authorization': 'Bearer secret'}
    with client_for(context, monkeypatch) as client:
        result = client.post('/evaluation-comparisons', headers=auth,
                             json={'baseline_id': left['id'], 'candidate_id': right['id']}).json()
        client.post(f"/evaluation-comparisons/{result['id']}/decision", headers=auth, json={'decision': 'approved'})
        with sessions.begin() as session:
            row = session.scalar(select(EvaluationDecision).where(EvaluationDecision.comparison_id == result['id']))
            original = row.artifact
            row.artifact = {**original, 'decision': 'rejected'}
        assert client.get('/evaluation-comparisons', headers=auth).status_code == 409
        with sessions.begin() as session:
            row = session.scalar(select(EvaluationDecision).where(EvaluationDecision.comparison_id == result['id']))
            row.artifact = original
            comparison = session.get(EvaluationComparison, result['id'])
            comparison.artifact = {**comparison.artifact, 'created_at': 'tampered'}
        assert client.get('/evaluation-comparisons', headers=auth).status_code == 409
