"""Single-job executor. The same persisted key is reused after worker replacement."""
import logging
import os
import threading
import time
import uuid
import httpx
from .db import make_database
from .service import LeaseLost, Runtime

log = logging.getLogger(__name__)


def process(runtime, lease, client, *, lease_seconds=30):
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
        result = None
        try:
            if action["mode"] == "send":
                response = client.post("/grants", json=action["action"], headers={"Idempotency-Key": action["key"]})
                response.raise_for_status()
                result = response.json()
            else:
                response = client.get("/operations/by-key", params={"tenant_id": action["action"]["tenant_id"],
                                      "idempotency_key": action["key"]})
                response.raise_for_status()
                result = response.json()
        except (httpx.HTTPError, ValueError):
            # A failed request may have committed; lookup is the only source of success.
            if lost.is_set():
                return
            runtime.check_lease(lease)
            if lost.is_set():
                return
            try:
                response = client.get("/operations/by-key", params={"tenant_id": action["action"]["tenant_id"],
                                      "idempotency_key": action["key"]})
                response.raise_for_status()
                result = response.json()
            except (httpx.HTTPError, ValueError):
                result = None
        if not lost.is_set():
            runtime.finish(lease, result)
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
    try:
        with httpx.Client(base_url=os.environ["MOCK_IDENTITY_URL"], timeout=5.0) as client:
            while True:
                lease = runtime.claim(owner)
                if lease:
                    process(runtime, lease, client)
                else:
                    time.sleep(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
