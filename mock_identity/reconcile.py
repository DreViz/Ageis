"""Small synchronous protocol client; durable orchestration belongs to Phase 2."""
import httpx
from .app import GrantRequest, request_hash


def execute(client: httpx.Client, body: dict, key: str, *, fault: str = "none", lookup_fault: str = "none"):
    headers = {"Idempotency-Key": key, "X-Test-Fault": fault}
    try:
        response = client.post("/grants", json=body, headers=headers)
        response.raise_for_status()
        return {"status": "committed", "operation": response.json()}
    except (httpx.TimeoutException, httpx.TransportError):
        pass
    # No absence-based retries: a 404 may race an in-flight request.
    try:
        response = client.get("/operations/by-key", params={"tenant_id": body["tenant_id"],
                              "idempotency_key": key}, headers={"X-Test-Fault": lookup_fault})
        response.raise_for_status()
        operation = response.json()
        if operation.get("request_hash") != request_hash(GrantRequest(**body)):
            return {"status": "needs_review"}
        return {"status": "committed", "operation": operation}
    except (httpx.TransportError, httpx.HTTPStatusError):
        return {"status": "needs_review"}
