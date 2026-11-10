import copy
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn
from sqlalchemy import create_engine, text
from evaluation.cross import assess_cross, evaluate_cross
from evaluation.faults import FaultBoundary
from evaluation.runner import compare
from mock_notice.app import create_app
from test_workflow import URL, mock_server


@pytest.fixture
def notice_server(tmp_path):
    app = create_app(f"sqlite:///{(tmp_path / 'notice.db').as_posix()}", enable_faults=True, fault_delay=.7)
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen(128)
    server = uvicorn.Server(uvicorn.Config(app, log_level='error'))
    thread = threading.Thread(target=server.run, kwargs={'sockets': [sock]}, daemon=True)
    thread.start()
    limit = time.monotonic() + 5
    while not server.started and time.monotonic() < limit:
        time.sleep(.01)
    assert server.started
    yield f'http://127.0.0.1:{sock.getsockname()[1]}'
    server.should_exit = True
    thread.join(5)
    sock.close()
    app.state.engine.dispose()


def test_fault_boundary_rejects_external_destinations():
    with httpx.Client(base_url='http://example.com') as client:
        with pytest.raises(ValueError):
            FaultBoundary(client, 'access', 'scope', 'before-send')


def test_missing_fault_evidence_is_inconclusive():
    report = {'mock_evidence': {'attempts': [], 'operations': [], 'grants': []}, 'events': [],
              'action': {}, 'fault_injections': []}
    result = assess_cross(report, 'access', 'timeout-after-commit')
    assert result == {'status': 'inconclusive', 'failed_assertions': [], 'reason': 'fault_not_injected'}


@pytest.mark.skipif(not URL, reason='PostgreSQL TEST_DATABASE_URL required')
def test_cross_suite_detects_unsafe_retries_with_isolated_concurrent_runs(mock_server, notice_server):
    reports = []
    try:
        urls = {'access': mock_server, 'notice': notice_server}
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(evaluate_cross, URL, urls, candidate) for candidate in ('safe-v1', 'unsafe-retry-v1')]
            reports = [future.result() for future in futures]
        safe, unsafe = reports
        assert safe['counts'] == {'passed': 22, 'failed': 0, 'error': 0, 'inconclusive': 0}, safe['results']
        assert unsafe['counts'] == {'passed': 20, 'failed': 2, 'error': 0, 'inconclusive': 0}, unsafe['results']
        recovery_cases = [item for item in safe['results'] if item['scenario'].endswith(('restart-before-send', 'lease-loss-after-commit'))]
        assert len(recovery_cases) == 4
        for item in recovery_cases:
            workflow, profile = item['scenario'].split('::')
            evidence = item['evidence']
            assert evidence['recovery']['stale_completion_fenced'] is True
            assert len(evidence['mock_evidence']['attempts']) == 1
            damaged = copy.deepcopy(evidence)
            damaged['recovery']['stale_completion_fenced'] = False
            assessment = assess_cross(damaged, workflow, profile)
            assert assessment['status'] == 'failed' and 'stale_worker_fenced' in assessment['failed_assertions']
            damaged['recovery'] = None
            assert assess_cross(damaged, workflow, profile)['status'] == 'inconclusive'
        crash_cases = [item for item in safe['results'] if '::process-crash-' in item['scenario']]
        assert len(crash_cases) == 4
        for item in crash_cases:
            workflow, profile = item['scenario'].split('::')
            evidence = item['evidence']
            recovery = evidence['recovery']
            assert recovery['original_exitcode'] != 0 and recovery['replacement_exitcode'] == 0
            assert recovery['stale_completion_fenced'] is True
            assert len(evidence['mock_evidence']['attempts']) == 1
            damaged = copy.deepcopy(evidence)
            damaged['recovery']['stale_completion_fenced'] = False
            assessment = assess_cross(damaged, workflow, profile)
            assert assessment['status'] == 'failed' and 'stale_worker_fenced' in assessment['failed_assertions']
            damaged['recovery'] = None
            assert assess_cross(damaged, workflow, profile)['status'] == 'inconclusive'
        failures = [item for item in unsafe['results'] if item['status'] == 'failed']
        assert {item['scenario'] for item in failures} == {'access::timeout-after-commit', 'notice::timeout-after-commit'}
        for failure in failures:
            assert 'committed_operation_count' in failure['failed_assertions']
            evidence = failure['evidence']['mock_evidence']
            assert len(evidence.get('operations', evidence.get('notices'))) == 2
        scopes = [item['evidence']['action'].get('tenant_id', item['evidence']['action'].get('org_id'))
                  for report in reports for item in report['results']]
        assert len(scopes) == len(set(scopes))
        comparison = compare(safe, unsafe)
        assert sum(change['candidate'] == 'failed' for change in comparison['changes']) == 2
    finally:
        engine = create_engine(URL)
        with engine.begin() as connection:
            for report in reports:
                for item in report['results']:
                    schema = item.get('evidence', {}).get('schema')
                    if schema:
                        assert schema.startswith('demo_') and len(schema) == 37 and all(c in '0123456789abcdef' for c in schema[5:])
                        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()
