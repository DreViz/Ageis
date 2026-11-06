"""Single-job executor. The same persisted key is reused after worker replacement."""
import logging
import os
import threading
import time
import uuid
import httpx
from .db import make_database
from .service import LeaseLost, Runtime
from .model import ModelError, OpenAIModel

log = logging.getLogger(__name__)


def process(runtime, lease, client, *, lease_seconds=30, model_client=None):
    stop = threading.Event()
    lost = threading.Event()

    def renew():
        while not stop.wait(lease_seconds / 3):
            try:
                runtime.renew(lease, lease_seconds)
            except Exception:
                log.exception("Lease renewal failed; worker will not commit a result")
                lost.set()
                return

    thread = threading.Thread(target=renew, daemon=True)
    thread.start()
    try:
        action = runtime.prepare(lease)
        if action is None:
            return
        if lost.is_set():
            return
        runtime.check_lease(lease)
        if lost.is_set():
            return
        if action["mode"] == "model":
            try:
                if model_client is None:
                    raise ModelError("model_not_configured")
                result = model_client.call(action["model_id"], action["messages"])
                error = None
            except ModelError as exc:
                result, error = None, str(exc)
            if not lost.is_set():
                runtime.finish_model(lease, result, error)
            return
        result = None
        result_source = "direct"
        mode = action["mode"]
        while not lost.is_set():
            runtime.check_lease(lease)
            if lost.is_set():
                return
            if mode == "send":
                try:
                    response = client.post(action["tool"].send_path, json=action["action"],
                                           headers={"Idempotency-Key": action["key"]})
                    response.raise_for_status()
                    result = response.json()
                    break
                except (httpx.HTTPError, ValueError) as error:
                    # Capture the observation; it does not establish the remote outcome.
                    if lost.is_set():
                        return
                    runtime.record_tool_event(lease, "tool_timeout" if isinstance(error, httpx.TimeoutException) else "tool_error")
            if lost.is_set():
                return
            runtime.check_lease(lease)
            if lost.is_set():
                return
            try:
                response = client.get(action["tool"].lookup_path,
                                      params=action["tool"].lookup_params(action["action"], action["key"]))
                if response.status_code == 404:
                    runtime.record_tool_event(lease, "lookup_absent")
                    # Absence proves nothing. Retry is safe only because this specific
                    # mock atomically honors the still-valid, persisted key and hash.
                    if not runtime.reserve_retry(lease):
                        break
                    mode = "send"
                    continue
                response.raise_for_status()
                result = response.json()
                result_source = "lookup"
                runtime.record_tool_event(lease, "lookup_succeeded")
                break
            except (httpx.HTTPError, ValueError):
                if lost.is_set():
                    return
                runtime.record_tool_event(lease, "lookup_unavailable")
                break
        if not lost.is_set():
            runtime.finish(lease, result, source=result_source)
    except LeaseLost:
        log.warning("Lease lost; stale worker cannot commit state")
    finally:
        stop.set()
        thread.join()


def main():
    logging.basicConfig(level=logging.INFO)
    engine, sessions = make_database(os.environ["RUNTIME_DATABASE_URL"])
    runtime = Runtime(sessions)
    owner = str(uuid.uuid4())
    model_client = OpenAIModel(os.environ["OPENAI_API_KEY"]) if os.getenv("OPENAI_API_KEY") else None
    try:
        with httpx.Client(base_url=os.environ["MOCK_IDENTITY_URL"], timeout=5.0) as client:
            while True:
                lease = runtime.claim(owner)
                if lease:
                    process(runtime, lease, client, model_client=model_client)
                else:
                    time.sleep(1)
    finally:
        if model_client:
            model_client.close()
        engine.dispose()


if __name__ == "__main__":
    main()
