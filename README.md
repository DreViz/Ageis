# Ageis — Phase 1 mock identity service

A synthetic, local-only service for testing uncertain access-grant outcomes. It does not integrate with real identity providers or provide authentication. Do not expose it publicly or use real data.

## Start

Install Docker and start its daemon, then run `docker compose up --build`. The API is bound to localhost at `http://127.0.0.1:8000`; interactive API documentation is at `/docs`. The Compose database credentials and fault injection are for local testing only. Database data persists in a Docker volume.

For offline tests, install Python 3.11+ and the project test dependencies with `pip install -e ".[test]"`, then run `pytest`. Tests use isolated SQLite files by default. Set `TEST_DATABASE_URL` to a dedicated empty PostgreSQL test database to additionally run the same protocol tests against PostgreSQL. The tests create tables and synthetic rows in that database; do not point it at a shared or production database.

## Protocol

Send `POST /grants` with `Idempotency-Key` and a JSON body containing `tenant_id`, `action_id`, `user_id`, and `workspace_id`. All values are required strings and preserved exactly. The request hash is SHA-256 over UTF-8 JSON with sorted keys and compact separators, containing those fields and `operation_type=grant_access`. Missing and null fields are invalid. Idempotency is scoped to tenant, operation type, and key. The operation and grant commit in one database transaction. A same-key, same-payload replay returns the original operation; a different payload returns 409. Records are retained without automatic deletion: after 24 hours replay is rejected rather than silently starting a new operation. Status remains available for reconciliation. The caller must bound retries to that window.

`GET /operations/by-key` performs an authoritative database read. A 404 is **not** proof of non-commit, because a request may be in flight. `GET /operations/{id}` and `GET /grants` inspect committed operation and final state. The minimal client in `mock_identity/reconcile.py` reconciles timeouts, verifies the request hash, and returns `needs_review` if evidence is inconclusive. It does not automatically retry; a caller may repeat the same request with the same key while the contract is valid.

When explicitly enabled using `ENABLE_TEST_FAULTS=true`, the `X-Test-Fault` header accepts `before_commit` (abort then delay) and `after_commit` (commit then delay) on grant requests, and `unavailable` on status lookups. These controls are for isolated synthetic tests only. Attempt records reflect server-observed processing, not proof that a client received the response. The fixture does not currently simulate stale replicas.

The Phase 1 test suite covers real HTTP timeouts, retry, conflicting keys, concurrent calls, committed operation counts and final grants. The runtime and mock schema use `create_all` for this initial disposable demo; this is not a migration system.

## Phase 2: durable approvals and worker

Compose also starts the authenticated control API at `http://127.0.0.1:8001` and a worker. The local demo token is `local-demo-only-token` (the Compose configuration is for local synthetic testing only). Requests to the control API use `Authorization: Bearer local-demo-only-token`. Create a run with `POST /runs` and the JSON fields `tenant_id`, `user_id`, and `workspace_id`; inspect it with `GET /runs/{id}` and `/runs/{id}/events`. Use `GET /approvals` to find its pending approval, then `POST /approvals/{id}/decision` with `{"decision":"APPROVED"}` or `{"decision":"REJECTED"}`.

The worker persists a logical action and key before making a request. It uses PostgreSQL row locks, renewable leases and fencing to prevent a stale worker from committing state; a reclaimed unknown action is reconciled with the same key. An inconclusive lookup stops in `NEEDS_REVIEW`. Approval, event, run transition and resume job commit together. Approvals expire after 15 minutes; a changed or expired action is blocked. Repeated worker claims and run duration are bounded. The runtime uses a separate PostgreSQL schema, but shares the mock database and demo credentials in Compose.

This is a local single-operator demonstration: the shared demo token is not a production identity system, and the mock service and runtime are not multi-tenant security boundaries. The current workflow always requests approval and does not yet resolve users or implement a general policy engine. An operator can request a replacement for an expired or changed approval with `POST /approvals/{id}/renew` while the action has not been sent. Model conversations and missing-input handling belong to later phases. The runtime tests require `TEST_DATABASE_URL` with permission to create and remove uniquely named test schemas in a dedicated test database.
