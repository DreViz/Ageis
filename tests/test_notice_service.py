import concurrent.futures

from fastapi.testclient import TestClient
from mock_notice.app import create_app


def test_notice_idempotency_conflict_scope_and_fault_isolation(tmp_path):
    app = create_app(f"sqlite:///{tmp_path / 'notice.db'}", enable_faults=True, fault_delay=0)
    body = {"org_id": "one", "recipient": "dev", "message": "hello", "action_id": "action"}
    with TestClient(app) as client:
        assert client.put('/test/scenario/one', params={'scenario': 'after_commit'}).status_code == 200
        first = client.post('/notices', json=body, headers={'Idempotency-Key': 'key'})
        again = client.post('/notices', json=body, headers={'Idempotency-Key': 'key'})
        assert first.status_code == again.status_code == 200
        assert first.json() == again.json()
        assert client.post('/notices', json={**body, 'message': 'changed'}, headers={'Idempotency-Key': 'key'}).status_code == 409
        other = client.post('/notices', json={**body, 'org_id': 'two'}, headers={'Idempotency-Key': 'key'})
        assert other.status_code == 200 and other.json()['operation_id'] != first.json()['operation_id']
        lookup = client.get('/notices/status', params={'org_id': 'one', 'idempotency_key': 'key'})
        assert lookup.json() == first.json()
        evidence = client.get('/test/evidence', params={'org_id': 'one'}).json()
        assert len(evidence['notices']) == 1 and len(evidence['attempts']) == 3
        assert client.put('/test/scenario/one', params={'scenario': 'normal'}).status_code == 409
    app.state.engine.dispose()


def test_notice_concurrent_same_key_commits_once(tmp_path):
    app = create_app(f"sqlite:///{tmp_path / 'concurrent.db'}")
    body = {"org_id": "org", "recipient": "dev", "message": "hello", "action_id": "action"}
    with TestClient(app) as client:
        def send(_):
            return client.post('/notices', json=body, headers={'Idempotency-Key': 'key'})
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(send, range(4)))
        assert all(r.status_code == 200 for r in responses)
        assert len({r.json()['operation_id'] for r in responses}) == 1
        assert client.get('/test/evidence', params={'org_id': 'org'}).status_code == 404
        assert client.put('/test/scenario/org', params={'scenario': 'normal'}).status_code == 404
    app.state.engine.dispose()
