import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os
import uuid

import pytest
from sqlalchemy import func, select
from evaluation.runner import digest
from evaluation.store import EvaluationConflict, EvaluationStore
from runtime.db import EvaluationRun, EvaluationScenario
from test_runtime import context

pytestmark = pytest.mark.skipif(not os.getenv('TEST_DATABASE_URL'), reason='PostgreSQL TEST_DATABASE_URL required')


def artifact():
    names = ['access::normal', 'access::bad', 'notice::error', 'notice::uncertain']
    value = {'kind': 'evaluation', 'id': str(uuid.uuid4()), 'created_at': datetime.now(timezone.utc).isoformat(),
             'manifest': {'suite_id': 'cross-v1', 'candidate': 'safe-v1', 'scenario_ids': names},
             'results': [
                 {'scenario': names[0], 'status': 'passed', 'failed_assertions': [], 'evidence': {'events': ['ok']}},
                 {'scenario': names[1], 'status': 'failed', 'failed_assertions': ['duplicate'], 'evidence': {'operations': [1, 2]}},
                 {'scenario': names[2], 'status': 'error', 'failed_assertions': [], 'reason': 'TimeoutError'},
                 {'scenario': names[3], 'status': 'inconclusive', 'failed_assertions': [], 'reason': 'missing_evidence'}],
             'counts': {'passed': 1, 'failed': 1, 'error': 1, 'inconclusive': 1}, 'total': 4}
    value['evidence_hash'] = digest(value)
    return value


def rehash(value):
    value['evidence_hash'] = digest({key: item for key, item in value.items() if key != 'evidence_hash'})
    return value


def test_round_trip_statuses_and_embedded_evidence(context):
    _, sessions = context
    store, value = EvaluationStore(sessions), artifact()
    assert store.save(value) is True
    assert store.save(value) is False
    assert store.load(value['id']) == value
    assert store.load('missing') is None
    with sessions() as session:
        run = session.get(EvaluationRun, value['id'])
        rows = session.scalars(select(EvaluationScenario).order_by(EvaluationScenario.position)).all()
        assert run.status == 'error' and run.suite_id == 'cross-v1' and run.candidate == 'safe-v1'
        assert [row.status for row in rows] == ['passed', 'failed', 'error', 'inconclusive']
        assert rows[0].evidence_hash == digest(value['results'][0]['evidence'])
        assert rows[2].evidence_hash is None


def test_duplicate_id_conflict_and_concurrent_replay(context):
    _, sessions = context
    store, value = EvaluationStore(sessions), artifact()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: store.save(value), range(2)))
    assert sorted(results) == [False, True]
    changed = copy.deepcopy(value)
    changed['results'][0]['evidence']['events'].append('changed')
    rehash(changed)
    with pytest.raises(EvaluationConflict):
        store.save(changed)
    assert store.load(value['id']) == value


def test_invalid_artifact_is_never_partially_stored(context):
    _, sessions = context
    store, value = EvaluationStore(sessions), artifact()
    value['counts']['error'] = 0
    rehash(value)
    with pytest.raises(ValueError, match='counts'):
        store.save(value)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(EvaluationRun)) == 0
        assert session.scalar(select(func.count()).select_from(EvaluationScenario)) == 0


def test_corrupt_scenario_evidence_is_detected(context):
    _, sessions = context
    store, value = EvaluationStore(sessions), artifact()
    store.save(value)
    with sessions.begin() as session:
        row = session.get(EvaluationScenario, (value['id'], 0))
        row.result = {**row.result, 'evidence': {'events': ['tampered']}}
    with pytest.raises(ValueError, match='inconsistent'):
        store.load(value['id'])


def test_inconsistent_outcome_and_missing_evidence_rejected(context):
    _, sessions = context
    store, value = EvaluationStore(sessions), artifact()
    value['results'][0]['failed_assertions'] = ['cannot pass']
    with pytest.raises(ValueError, match='outcome'):
        store.save(rehash(value))
    value = artifact()
    del value['results'][0]['evidence']
    with pytest.raises(ValueError, match='missing'):
        store.save(rehash(value))
