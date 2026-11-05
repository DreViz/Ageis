import os
import socket
import threading
import time

import pytest
import uvicorn
from sqlalchemy import create_engine, text

from mock_identity.app import create_app
from runtime.demo import run_demo, validate_report

URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="PostgreSQL TEST_DATABASE_URL required")


@pytest.fixture
def mock_server(tmp_path):
    app = create_app(f"sqlite:///{(tmp_path / 'mock.db').as_posix()}", enable_faults=True, fault_delay=0.7)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    limit = time.monotonic() + 5
    while not server.started and time.monotonic() < limit:
        time.sleep(.01)
    assert server.started
    yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    server.should_exit = True
    thread.join(5)
    sock.close()
    app.state.engine.dispose()


@pytest.mark.parametrize("scenario", ["normal", "timeout-before-commit", "timeout-after-commit", "unresolved", "rejected", "denied"])
def test_complete_scripted_workflow(scenario, mock_server):
    report = run_demo(URL, mock_server, scenario)
    try:
        validate_report(report)
        kinds = [event["kind"] for event in report["events"]]
        assert "directory_checked" in kinds and "policy_checked" in kinds
        if scenario == "timeout-before-commit":
            assert "action_retry" in kinds
        if scenario == "unresolved":
            assert "outcome_unknown" in kinds
    finally:
        # Only the random schema created by this test is removed.
        engine = create_engine(URL)
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{report["schema"]}" CASCADE'))
        engine.dispose()
