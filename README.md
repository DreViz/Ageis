# Ageis — Phase 1 mock identity service

A synthetic, local-only service for testing uncertain access-grant outcomes. It does not integrate with real identity providers or provide authentication. Do not expose it publicly or use real data.

## Start

Install Docker and start its daemon, then run `docker compose up --build`. The API is bound to localhost at `http://127.0.0.1:8000`; interactive API documentation is at `/docs`. The Compose database credentials and fault injection are for local testing only. Database data persists in a Docker volume.

For offline tests, install Python 3.11+ and the project test dependencies with `pip install -e ".[test]"`, then run `pytest`. Tests use isolated SQLite files by default. Set `TEST_DATABASE_URL` to a dedicated empty PostgreSQL test database to additionally run the same protocol tests against PostgreSQL. The tests create tables and synthetic rows in that database; do not point it at a shared or production database.

## Protocol

Send `POST /grants` with `Idempotency-Key` and a JSON body containing `tenant_id`, `action_id`, `user_id`, and `workspace_id`. All values are required strings and preserved exactly. The request hash is SHA-256 over UTF-8 JSON with sorted keys and compact separators, containing those fields and `operation_type=grant_access`. Missing and null fields are invalid. Idempotency is scoped to tenant, operation type, and key. The operation and grant commit in one database transaction. A same-key, same-payload replay returns the original operation; a different payload returns 409. Records are retained without automatic deletion: after 24 hours replay is rejected rather than silently starting a new operation. Status remains available for reconciliation. The caller must bound retries to that window.

`GET /operations/by-key` performs an authoritative database read. A 404 is **not** proof of non-commit, because a request may be in flight. `GET /operations/{id}` and `GET /grants` inspect committed operation and final state. The minimal client in `mock_identity/reconcile.py` reconciles timeouts, verifies the request hash, and returns `needs_review` if evidence is inconclusive. It does not automatically retry; a caller may repeat the same request with the same key while the contract is valid.

When explicitly enabled using `ENABLE_TEST_FAULTS=true`, the `X-Test-Fault` header accepts `before_commit` (abort then delay) and `after_commit` (commit then delay) on grant requests, and `unavailable` on status lookups. These controls are for isolated synthetic tests only. Attempt records reflect server-observed processing, not proof that a client received the response. The fixture does not currently simulate stale replicas.

The Phase 1 test suite covers real HTTP timeouts, retry, conflicting keys, concurrent calls, committed operation counts and final grants. Worker leases, durable action intent, approvals, migrations, and real model calls belong to later phases. Schema creation uses `create_all` for this initial disposable mock and is not a migration system.
