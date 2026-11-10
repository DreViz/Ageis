# Ageis — Phase 1 mock identity service

A synthetic, local-only runtime for testing uncertain access-grant outcomes. It does not integrate with real identity providers. The mock identity API is unauthenticated; the control API uses a local demo token. Do not expose the services publicly or use real data.

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

This is a local single-operator demonstration: the shared demo token is not a production identity system, and the mock service and runtime are not multi-tenant security boundaries. The scripted workflow uses a small static synthetic directory and policy, not a general identity or policy system. An operator can request a replacement for an expired or changed approval with `POST /approvals/{id}/renew` while the action has not been sent. Model runs have a separate opt-in path described below. The runtime tests require `TEST_DATABASE_URL` with permission to create and remove uniquely named test schemas in a dedicated test database.

## Phase 3: scripted offline scenarios

The deterministic fixture recognizes synthetic user `maya`; workspace `analytics` requires approval and `admin` is denied. Unknown users and workspaces are rejected. No model API or credentials are used.

Start the Compose services, then run e.g. `docker compose run --rm --no-deps worker python -m runtime.demo --scenario timeout-after-commit`. Available scenarios are `normal`, `timeout-before-commit`, `timeout-after-commit`, `unresolved`, `rejected`, and `denied`. The demo uses a scripted approval, creates an isolated PostgreSQL schema and unique synthetic tenant, and prints a JSON report with the event sequence, attempts, committed operations and final grants. The schema and synthetic mock records persist for inspection. The `/test/evidence` endpoint is only enabled with the mock's test fault controls and must remain private.

A missing operation on lookup does not establish non-commit. The worker may retry with the persisted key and unchanged arguments because this mock guarantees atomic idempotency; retries are bounded by the original run deadline, approval expiration and attempt limit. An unavailable lookup stops for review. This retry guarantee does not extend to arbitrary external APIs.

## Phase 4: optional model-driven requests

The default Compose setup stays offline and does not call a model. To opt in, copy `.env.example` to `.env` and set `AGEIS_MODEL_ID` to an available OpenAI model identifier and `OPENAI_API_KEY` to your own API key, then restart Compose. Choose and pin an appropriate model for your account; access, cost, quality, and model availability have not been verified here. Configure an account-level spend limit before using the live API.

Use the control API's `POST /model-runs` with `{"tenant_id":"synthetic-tenant","text":"Request analytics access for Maya"}`. Inspect `GET /runs/{id}` for state and a clarification question. If state is `WAITING_FOR_INPUT`, submit `POST /runs/{id}/input` with `{"text":"..."}`. Approval and subsequent execution use the same endpoints and checks as scripted runs. The bearer token is required on all these endpoints.

The OpenAI Responses adapter requests exactly one of two strict function calls: propose a synthetic user/workspace or ask for input. The runtime validates each result, requires both proposed identifiers to appear in user messages, and separately enforces the directory, policy, approval, action hash, and durable execution protocol. Model-supplied text never authorizes a grant. This simple grounding check does not establish intent or protect against every prompt-injection attempt; the operator must review the exact action.

A model run persists its configured model ID, prompt and tool-schema hashes, bounded conversation, model call reservations and available provider token usage. Each run allows at most four model calls, 2,000 characters per user input, 256 output tokens per call, a 15-second provider timeout, and the existing one-hour run deadline. A crashed or timed-out call still consumes a call reservation; missing usage is explicitly reported. These limits bound requests but **do not enforce a dollar-denominated spend cap**. The provider account's spend controls remain necessary. The adapter uses `store: false`; synthetic conversation content is still sent to the configured provider and stored locally in PostgreSQL. No live-provider result is implied by the offline tests.

The adapter follows the [OpenAI function-calling guide](https://developers.openai.com/api/docs/guides/function-calling). Run offline tests with the existing `pytest` command; the provider HTTP requests are mocked and require no API key.

## Phase 5: offline evaluations and release evidence

The versioned `evaluation/suites/v1.json` suite runs six deterministic scenarios against isolated synthetic tenants and runtime schemas. Two trusted scripted candidates are available: `safe-v1` and `unsafe-retry-v1`. The latter is a **test-only seeded regression** that retries a timed-out grant with a new key; the evaluator detects two committed operations. Never point the evaluator at a real identity service.

From PowerShell, create the local artifact directory and run the comparison:

```powershell
New-Item -ItemType Directory -Force evaluation/results | Out-Null
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation run --candidate safe-v1 --output /results/safe.json
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation run --candidate unsafe-retry-v1 --output /results/unsafe.json
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation compare --baseline /results/safe.json --candidate /results/unsafe.json --output /results/comparison.json
```

Inspect the comparison and full run artifacts. A human can record a local decision with `python -m evaluation decide --comparison /results/comparison.json --decision needs_review --actor demo-operator --output /results/decision.json` in the same mounted container. Recording a decision never deploys anything. Actor identity is a local label, not authenticated identity. Artifact outputs are create-only; choose new filenames to avoid overwriting previous evidence. Generated artifacts are Git-ignored. A successful `run` command means the report was written, not that every scenario passed: inspect its counts and individual results.

Each report records a suite hash, fixture and policy hashes, code and candidate hashes, environment metadata, per-scenario status, duration, assertions and evidence. Comparison rejects incompatible fixture, policy, suite or tool-schema inputs and distinguishes behavioral failures, execution errors and inconclusive evidence; execution errors identify dependency and runner errors separately. Artifact hashes detect accidental changes; they are not signatures or tamper-proof audit records. Image digests are currently unavailable and flagged in the comparison. Scenario runs create persistent synthetic schemas and mock records for inspection; no cleanup is performed automatically.

This first evaluator tests scripted candidates only. It does not yet run hosted models, provide a held-out dataset, collect model cost, expose evaluation APIs, or prove real-world agent safety.

## Local dashboard

Start the backend with `docker compose up -d --build`, then in a second terminal run `cd frontend`, `npm install`, and `npm run dev`. Open the Vite URL shown in the terminal (normally `http://127.0.0.1:5173`). The Vite development proxy forwards `/api` to the local control API. Enter the configured operator token; the Compose demo token is `local-demo-only-token`. The token is held only in the current page's memory. This is a local synthetic demo, not a production authentication setup.

The console lists the most recent 50 runs, shows recorded events and confirmed operation IDs, creates synthetic runs, and reviews pending approvals. It can submit clarification for model runs started through the API. The evaluations workspace starts synthetic suites, shows scenario evidence, compares persisted runs and records a human decision. Saved comparisons appear in comparison history with decision state and can also be reopened by ID. The static production build requires an equivalent same-origin `/api` reverse proxy; `vite preview` alone does not provide one.

## Trusted agent adapters and tool contracts

The execution loop uses `runtime/contracts.py` for a versioned, project-owned `AgentAdapter` and immutable `ToolContract`; `runtime/access_adapter.py` implements the existing synthetic access workflow. The registry is constructed in code: there is no API for uploading, selecting, or running arbitrary adapters. The synthetic notification adapter uses a separate mock service, action shape, and endpoints. The worker routes the two trusted tool names to their configured mock-service URLs; there is no user-supplied destination.

New runs persist the adapter version and implementation fingerprint, policy version and fingerprint, tool-contract fingerprint, action and a conservative retry deadline. Contract or policy drift blocks further execution; a changed contract with an uncertain action requires review. Explicit atomic same-key/same-request guarantees and a valid retry window are required for retry. A positive status lookup is trusted only when the tool contract declares it final. These declarations must be verified against the service: a contract cannot make an unsafe external API safe.

Existing access runs created before contract binding remain readable and can complete from a verified response or final lookup. They have no persisted retry window, so an ambiguous 404 does **not** trigger an automatic retry; such runs stop for review. The shared model path still uses the existing access-specific model schema; general multi-agent or model adapters remain future work.


## Synthetic notification service

Compose starts `mock-notice`, a private synthetic service that records notices in PostgreSQL without delivering messages. `POST /notice-runs` on the authenticated control API accepts `org_id`, `recipient`, and `message`; every notice requires operator approval. The worker routes `send_notice` to `MOCK_NOTICE_URL`, separate from `MOCK_IDENTITY_URL`. Missing worker configuration stops the action for review.

To exercise the actual running API and worker, run `docker compose exec -T worker python -m runtime.notice_demo`. It creates uniquely scoped synthetic records, approves each action, and checks normal execution, a timeout before commit, a timeout after commit, and an unavailable lookup. It verifies a single committed notice and reuse of the same key for a safe retry. Synthetic records remain in the local database. Test scenario and evidence endpoints are enabled only by `ENABLE_TEST_FAULTS` and the mock is not published to the host. The demo token and faults are for local testing, not production.

## Cross-workflow fault evaluation

The existing access-only suite remains the default. The `cross-v1` suite evaluates both trusted synthetic adapters using the same fault boundary and assertions. It covers normal and rejected actions, failure before send, timeouts before and after commit, unavailable lookup, and a stale 404 simulated at the client boundary. A stale 404 is not proof of non-commit; the safe candidate retries only with the same key under the declared contract. The boundary records injections and the evaluator also checks service-observed attempts and committed operations. Missing fault evidence is inconclusive. These tests do not simulate an actual stale database replica.

With Compose running, write fresh local artifacts (choose unused filenames):

```powershell
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation run --suite cross-v1 --candidate safe-v1 --output /results/cross-safe.json
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation run --suite cross-v1 --candidate unsafe-retry-v1 --output /results/cross-unsafe.json
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation compare --baseline /results/cross-safe.json --candidate /results/cross-unsafe.json --output /results/cross-comparison.json
```

The unsafe candidate deliberately retries a committed action with a new key; the expected behavioral failure is two committed operations in each workflow's timeout-after-commit case. Evaluation uses isolated random synthetic scopes and PostgreSQL schemas, which remain for inspection. Fault injection is restricted to configured local synthetic hosts; never use this harness with real services. These scripted results do not measure hosted-model quality or replace the remaining evaluation APIs and dashboard work.

## Persisting completed evaluation runs

Add `--persist` to either evaluation `run` command to store the completed artifact in PostgreSQL as well as writing the requested JSON file. For example:

```powershell
docker compose run --rm --no-deps -v "${PWD}/evaluation/results:/results" worker python -m evaluation run --suite cross-v1 --candidate safe-v1 --persist --output /results/cross-persisted.json
```

The runtime schema contains a run row and an ordered row per scenario. Scenario status, full synthetic evidence and its content hash are stored together; reads through `EvaluationStore` verify the reconstructed artifact and evidence hashes. Repeating the same artifact ID and contents is idempotent; a different artifact with the same ID is rejected. Error, failed and inconclusive scenarios remain distinct; the aggregate status prioritizes error, then failure, then inconclusive, then passed.

Storage happens after evaluation finishes and the output file is written. If persistence fails, the file remains available for inspection and the CLI exits with an error. Runs interrupted before completion are not persisted or resumed. This is still a local synthetic demo: stored evidence can contain synthetic action details, there is no retention policy yet, and the existing `create_all` startup is not a migration system.


## Authenticated evaluation APIs

The local control API uses the same demo bearer token for evaluation endpoints. `POST /evaluations` accepts only a trusted `suite` (`access-v1` or `cross-v1`) and `candidate` (`safe-v1` or the intentionally unsafe `unsafe-retry-v1`). It runs synchronously, persists the completed result and returns its ID and counts. Requests may take tens of seconds; configure client timeouts accordingly. A PostgreSQL advisory lock allows one API-started evaluation per database at a time; a concurrent request receives HTTP 409. The CLI does not use that lock. An interrupted process cannot resume a partial evaluation; inspect persisted runs before retrying.

Use `GET /evaluations` and `GET /evaluations/{id}` to inspect evidence. `POST /evaluation-comparisons` with `baseline_id` and `candidate_id` validates compatibility and stores a comparison; `GET /evaluation-comparisons` lists the latest saved comparisons and decision summaries (default 50, maximum 100); `GET /evaluation-comparisons/{id}` retrieves one. `POST /evaluation-comparisons/{id}/decision` records one immutable `approved`, `rejected`, or `needs_review` decision, and GET on the same path reads it. The actor is the local authenticated operator label. No decision deploys a candidate.

These APIs are synthetic and local-only. Mock URLs come from trusted server configuration, not request input. `MOCK_IDENTITY_URL` and `MOCK_NOTICE_URL` must point to the local fault-enabled mock services; Compose configures both. The shared demo token is not production authentication or an authorization boundary for multiple users. The dashboard uses these API endpoints for its evaluations workspace.

## Durable evaluation jobs and recovery

`POST /evaluation-jobs` accepts `{"suite":"cross-v1","candidate":"safe-v1"}` with the operator bearer token and a required `Idempotency-Key` header. The response is HTTP 202 and includes a stable job ID. Reusing the key with the same candidate returns the same job; a different candidate conflicts. `GET /evaluation-jobs` and `GET /evaluation-jobs/{id}` expose state, recorded scenario results, progress, any current synthetic fixture and the final result ID. The existing synchronous `/evaluations` endpoint and CLI are unchanged.

The separate Compose `evaluation-worker` claims jobs with PostgreSQL row locks, renewable leases and fencing. It checkpoints completed scenarios with integrity hashes and publishes the final evaluation artifact in the same transaction as the completed job state. `COMPLETED` means execution finished; inspect the artifact counts to determine whether scenarios passed, failed, errored or were inconclusive. A replacement worker may resume from committed checkpoints only if there is no in-flight scenario. An expired lease during a scenario marks the job `INTERRUPTED`; it does not replay the uncertain action. The persisted fixture scope and schema support investigation. Jobs have a 30-minute deadline and at most three claims. Pinned code, suite, policy, tool and mock configuration changes fail closed. External synthetic requests are not exactly-once, and stale workers cannot finalize evidence.

The dashboard starts and tracks durable jobs for the cross-workflow suite. It stores a pending idempotency key in session storage until a start response is received, so retrying after a reload in the same tab reuses that key. Different tabs or clients can still deliberately create separate jobs. The legacy synchronous evaluation path remains available separately.

An operator may cancel a queued, running or interrupted job via `POST /evaluation-jobs/{id}/cancel`. Cancellation fences future checkpoints but cannot guarantee that an in-flight synthetic scenario will not send or finish a request; the fixture remains visible. `POST /evaluation-jobs/{id}/continue-inconclusive` records the uncertain scenario as inconclusive without replaying it and continues with the next scenario, subject to the original deadline, claim limit and pinned execution contract. Both actions are recorded in `GET /evaluation-jobs/{id}/events`. A completed job is not necessarily a passing evaluation.

This remains a trusted synthetic-only demonstration, not arbitrary code or user-selected endpoints. Interrupted fixtures are retained for operator inspection; do not remove one while a worker or remote synthetic request may still be active. Cleanup is manual and should occur only after evidence is preserved and the fixture is confirmed idle. There is no automatic cleanup or migration system; an execution-contract change prevents continuing an old job. Existing demo authentication and synthetic-only limits still apply.
