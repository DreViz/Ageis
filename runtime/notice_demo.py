"""Exercise the running Docker API and worker against the synthetic notice service."""
import os
import time
import uuid
import httpx


def wait_for(read, predicate, timeout=25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = read()
        if predicate(value):
            return value
        time.sleep(.25)
    raise AssertionError(f"Timed out waiting for deployed worker: {value}")


def main():
    api_url = os.environ.get("AGEIS_API_URL", "http://runtime-api:8001")
    notice_url = os.environ.get("MOCK_NOTICE_URL", "http://mock-notice:8002")
    headers = {"Authorization": "Bearer " + os.environ.get("OPERATOR_TOKEN", "local-demo-only-token")}
    with httpx.Client(timeout=5) as client:
        def get(url, **kwargs):
            response = client.get(url, **kwargs)
            response.raise_for_status()
            return response.json()
        for scenario in ("normal", "before_commit", "after_commit", "unresolved"):
            org = "deployed-" + uuid.uuid4().hex
            response = client.put(f"{notice_url}/test/scenario/{org}", params={"scenario": scenario})
            response.raise_for_status()
            response = client.post(f"{api_url}/notice-runs", headers=headers,
                                   json={"org_id": org, "recipient": "synthetic-user", "message": "test notice"})
            response.raise_for_status()
            run_id = response.json()["run_id"]
            approval = wait_for(lambda: get(f"{api_url}/approvals", headers=headers),
                                lambda rows: any(row["run_id"] == run_id for row in rows))
            approval_id = next(row['id'] for row in approval if row['run_id'] == run_id)
            response = client.post(f"{api_url}/approvals/{approval_id}/decision", headers=headers, json={"decision": "APPROVED"})
            response.raise_for_status()
            run = wait_for(lambda: get(f"{api_url}/runs/{run_id}", headers=headers),
                           lambda row: row['state'] in ('COMPLETED', 'NEEDS_REVIEW', 'FAILED'))
            events = get(f"{api_url}/runs/{run_id}/events", headers=headers)
            evidence = get(f"{notice_url}/test/evidence", params={"org_id": org})
            attempts, notices = evidence['attempts'], evidence['notices']
            expected_state = 'NEEDS_REVIEW' if scenario == 'unresolved' else 'COMPLETED'
            assert run['state'] == expected_state, (scenario, run, events)
            assert run['contract']['adapter'] == 'synthetic-notice'
            assert len(notices) == 1 and len(attempts) == (2 if scenario == 'before_commit' else 1), evidence
            assert len({a['key'] for a in attempts}) == len({a['request_hash'] for a in attempts}) == 1
            assert all(a['action_id'] == run['action']['action_id'] for a in attempts)
            kinds = [e['kind'] for e in events]
            if scenario != 'normal':
                assert 'tool_timeout' in kinds, kinds
            if scenario == 'after_commit':
                assert 'lookup_succeeded' in kinds, kinds
            if scenario == 'unresolved':
                assert 'lookup_unavailable' in kinds and run['operation'] is None and run['action_status'] == 'UNKNOWN'
            else:
                assert run['operation']['operation_id'] == notices[0]['operation_id']
            print(scenario, run['state'], f"attempts={len(attempts)} notices={len(notices)}", flush=True)


if __name__ == '__main__':
    main()
