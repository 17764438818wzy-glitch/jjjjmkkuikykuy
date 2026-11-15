# Universal RAGFlow Work Pool Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a durable, bounded upload and parse queue that accepts verified artifact tasks and proves end-to-end operation with a new education batch.

**Architecture:** A local SQLite WAL database owns task identity, attempts and leases. A single state-writing scheduler uses separate bounded executors for upload, parse and polling; each API worker owns its own RAGFlow client. An education adapter supplies routing and admission interpretation while the core remains independent of education fields.

**Tech Stack:** Python 3 standard library `sqlite3` and `concurrent.futures`, existing `RagflowClient`, `unittest`/`pytest`, Windows local disk.

**Spec:** `docs/superpowers/specs/2026-09-26-universal-ragflow-ingest-work-pool-design.md`

## Global Constraints

- No API keys, passwords or raw private text in the queue database, logs or test fixtures.
- Preserve the existing education md and image dataset routing; do not change finance documents or Pipeline DSL.
- Only a verified education `ingest.md` may trigger the education paper Pipeline. Approved original images are location materials; auxiliary projections follow their existing role policy.
- Never repeat a network write with an uncertain acknowledgement until the exact remote dataset, document name, ID when known, and Pipeline binding are reconciled.
- A quality decision is a terminal first-pass result, not a technical retry or an admission bypass. Generic progress text maps to `outcome_unresolved`.
- SQLite WAL stays on one local host; no network share or multi-host worker is introduced in this plan.

## Review Focus

1. Automatic and manual requests for the same source, destination and role race: one task and one upload. Covered by Task 2.
2. Same logical source, new content hash: version conflict instead of silent reuse. Covered by Task 2.
3. POST succeeds remotely but the client loses acknowledgement: reconcile remote content or leave uncertain before any repeat. Covered by Task 3.
4. A parse reaches quality hold/reject while other tasks wait: release and refill the slot. Covered by Task 4.
5. A queued file changes or disappears before upload: stop that task before any network write. Covered by Task 3.

---

### Task 1: Define the transport-neutral task contract

**Files:**
- Create: `ingest_contracts.py`
- Create: `test_ingest_contracts.py`

**Interfaces:**
- Consumes: `upload_tracking.service_key(url: str) -> str` for canonical RAGFlow service identity.
- Produces: frozen `ArtifactSpec(service_url: str, dataset_id: str, source_id: str, role: str, lane: Literal['paper', 'image'], content_sha256: str, path: Path, remote_name: str, pipeline_id: str | None, parse_required: bool, priority: int, batch_id: str, policy_key: str, channel_key: str)`; `task_identity(spec: ArtifactSpec) -> tuple[str, str, str, str]`; `canonical_remote_name(title: str, source_id: str, content_sha256: str, suffix: str, legacy_id: str | None = None) -> str`; `Outcome` values `accepted`, `quality_hold`, `quality_reject`, `technical_review`, `outcome_unresolved`. `policy_key` chooses the domain adapter; `channel_key` is an opaque model/provider capacity label without a secret.

- [ ] **Step 1: Write failing tests.** Assert two URL spellings normalized by `service_key` yield the same task identity; dataset or role changes yield different identities; empty source ID, dataset ID, role, remote name, policy/channel key, invalid lane or non-64-character SHA-256 raises `ValueError`. For a long Chinese title, `canonical_remote_name()` stays within 255 UTF-8 bytes and retains the source ID, first 16 hash characters, optional required legacy ID and suffix.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_contracts.py`.** Expected: import or assertions fail before implementation.
- [ ] **Step 3: Implement the dataclass, enum and validation.** Keep adapter-specific education and finance fields out of the contract.
- [ ] **Step 4: Verify.** Run the focused test; expected PASS.
- [ ] **Step 5: Commit.** Commit `ingest_contracts.py` and `test_ingest_contracts.py` with `git commit -m "feat: define universal ingest task identity"`.

### Task 2: Persist identity, claims and attempts in SQLite

**Files:**
- Create: `ingest_queue.py`
- Create: `test_ingest_queue.py`

**Interfaces:**
- Consumes: Task 1 `ArtifactSpec`, `task_identity()` and `Outcome`.
- Produces: `QueueStore(path: Path)` with `enqueue(spec: ArtifactSpec) -> int`, `get(task_id: int) -> dict`, `claim(stage: str, owner: str, limit: int, now: float, lease_seconds: int) -> list[dict]`, `transition(task_id: int, expected: str, target: str, **fields) -> bool`, `append_attempt(task_id: int, operation: str, result: str, at: float) -> None`, and `request_review(task_id: int, reason: str) -> None` for a recorded second-pass attempt.

- [ ] **Step 1: Write failing database tests.** Same key, hash and route policy enqueued twice returns one ID; an initial upload-only task followed by the same verified task with `parse_required=True` may add its first Pipeline ID, keeps its ID/remote document and queues parse after upload; a later upload-only request does not cancel parse; a later manual request upgrades an unstarted automatic task's priority without duplicating it; changed hash, conflicting non-null Pipeline ID or policy under the same key raises `VersionConflict`; two store connections cannot claim the same ready task; a lease expires for reconciliation but does not clear `upload_intent` or `parse_intent`; with both priority classes waiting, repeated claims schedule at least one automatic task per four tasks; `request_review()` retains the earlier attempt and requires a nonempty reason before scheduling the next one.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_queue.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Implement local WAL schema and short `BEGIN IMMEDIATE` claims.** Use a unique constraint on normalized service, dataset, source ID and role. Persist content hash, target Pipeline, remote ID/name, status, lease and attempt history; keep API calls outside database transactions. Treat `priority=10` as manual and `priority=0` as automatic, and use a 3:1 manual/automatic claim cycle while both classes have ready work.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_ingest_contracts.py test_ingest_queue.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit only the queue and test files with `git commit -m "feat: persist universal ingest tasks"`.

### Task 3: Reconcile RAGFlow writes before retries

**Files:**
- Create: `ingest_gateway.py`
- Create: `test_ingest_gateway.py`

**Interfaces:**
- Consumes: Task 2 task dictionaries and existing `RagflowClient.upload()`, `.parse()`, `.documents()`.
- Produces: `RagflowGateway(client_factory)` with `lookup(task: dict) -> list[dict]`, `upload(task: dict) -> str`, `parse(task: dict) -> None`; `reconcile_write(task: dict, operation: Literal['upload', 'parse'], gateway: RagflowGateway) -> str` returning `confirmed`, `not_sent` or `uncertain`.

- [ ] **Step 1: Write failing gateway tests.** A confirmed remote ID with matching dataset/name/Pipeline is adopted; a unique canonical name containing source ID and hash prefix remains `uncertain` after upload acknowledgement loss when remote bytes or a trusted remote hash cannot be checked; when remote content can be verified against the local SHA-256, adopt its exact ID; a legacy name and multiple same-name matches remain `uncertain`; a parse already `RUNNING` or `DONE` is confirmed without calling parse again; `UNSTART` after a lost parse acknowledgement remains `uncertain`; network query failure remains `uncertain`; a queued file whose bytes changed or disappeared is stopped before `client.upload()`.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_gateway.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Implement exact remote reconciliation and per-worker client creation.** Rehash the queued file before upload and compare it with `content_sha256`; use canonical name to locate candidates, then verify trusted remote content SHA-256 or downloaded bytes before adopting a lost upload acknowledgement. If the installed RAGFlow API cannot provide bytes or a trusted hash, keep the lost acknowledgement `uncertain` for review. `UNSTART` after a lost parse acknowledgement remains `uncertain`; do not infer absence from a timeout, 5xx or authentication error. Return `not_sent` only when a write is provably absent, not merely because the listing currently has no match.
- [ ] **Step 4: Verify.** Run the focused test plus `test_ragflow_connection.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit the gateway and tests with `git commit -m "feat: reconcile ragflow ingest writes"`.

### Task 4: Schedule independent bounded upload and parse pools

**Files:**
- Create: `ingest_scheduler.py`
- Create: `test_ingest_scheduler.py`

**Interfaces:**
- Consumes: `QueueStore`, `RagflowGateway`, task contract, `classify(task: dict, remote: dict) -> Outcome` supplied by a domain adapter.
- Produces: `WorkPool(queue: QueueStore, gateway_factory, classify_for, on_accepted_for=None, upload_limit: int = 2, parse_limit: int = 3, image_limit: int = 2, post_limit: int = 2, poll_limit: int = 6, service_limits: dict[str, int] | None = None, dataset_limits: dict[str, int] | None = None, pipeline_limits: dict[str, int] | None = None, channel_limits: dict[str, int] | None = None)` with `tick() -> dict` and `close() -> None`. `classify_for(task, remote) -> Outcome` and `on_accepted_for(task, remote) -> None` dispatch by the stored `policy_key`. Snapshot keys include `upload_waiting`, `parse_waiting`, `upload_active`, `parse_active`, `terminal_by_outcome`.

- [ ] **Step 1: Write failing fake-gateway tests.** With a backlog of five and limit three, three parse calls are in flight; one terminal result makes the next waiting paper start within the next tick; `quality_hold` and `quality_reject` release slots without retry; blocked image, upload or `on_accepted_for` calls do not block parse polling; expired `parse_intent` reconciles before dispatch; repeated ticks never exceed global, service, dataset, Pipeline or channel limits; a paused service or channel leaves an unrelated service/channel schedulable.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_scheduler.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Implement separate bounded executors and one state writer.** Persist intent before dispatch, return results through futures, route `lane='image'` tasks to their own executor and callbacks for accepted `role='paper_primary'` tasks to the post executor, poll only active IDs, and apply the Task 3 reconciliation rules. Use approximately 5-second polling while parse work waits and 15 seconds when idle; do not back off for quality outcomes. Reserve rate-limit/billing pauses for their affected service or `channel_key`.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_ingest_contracts.py test_ingest_queue.py test_ingest_gateway.py test_ingest_scheduler.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit only scheduler and test files with `git commit -m "feat: schedule bounded ragflow work pools"`.

### Task 5: Admit verified education tasks through the generic pool

**Files:**
- Create: `ingest_education.py`
- Create: `ingest_worker.py`
- Create: `test_ingest_education.py`
- Modify: `education_stream_status.py` only to read generic task status for a new-batch view; keep the active batch's JSON view.

**Interfaces:**
- Consumes: Tasks 1–4, `canonical_remote_name()`, existing `ready_items()`, `checked_copy()` and education `deployment.json` routing.
- Produces: `education_specs(item: dict, deployment: dict, canonical_package: Path, service_url: str, batch_id: str) -> list[ArtifactSpec]`; `classify_education(task: dict, remote: dict) -> Outcome`; CLI `ingest_worker.py --queue <path> --run-id <id> --policy education`.

- [ ] **Step 1: Write failing education adapter tests.** A verified education package produces one main parse task in the configured md dataset plus distinct image roles in the image dataset; new remote names contain stable `paper_id` and content hash; a source/hand-off hash mismatch produces no task; original images are upload-only; `DONE` with verified admission is accepted; generic queue progress on `FAIL` is unresolved; a clearly exposed hold/reject maps to the appropriate terminal outcome.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_education.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Implement the adapter and CLI on a new isolated education batch.** Preserve pilot IDs and exact target checks; set `policy_key='education'` and a non-secret model-channel label. Supply an `on_accepted_for` callback that calls the existing deterministic Tree/method Graph helpers with verified remote doc and chunks; image artifacts keep independent receipts. Initialize the CLI's RAGFlow clients from existing `auto_config.load_key(..., 'ragflow')` and settings rather than storing credentials in the queue. Do not migrate the already running batch within this task.
- [ ] **Step 4: Verify.** Run all four core test files plus `test_ingest_education.py`, `test_education_direct_tree.py` and `test_education_method_graph.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit the adapter, CLI, status-view change and tests with `git commit -m "feat: run education imports on universal work pool"`.

### Task 6: Verify persistence, throughput and the education cutover boundary

**Files:**
- Create: `docs/superpowers/specs/2026-09-26-ragflow-work-pool-core-verification.md`
- Read: live education receipts, new SQLite queue and exact remote document states.

**Interfaces:**
- Consumes: Task 5 CLI and the existing education worker.
- Produces: a reproducible verification record for a new education sample and a separate recommendation for when the active batch may migrate.

- [ ] **Step 1: Run an isolated 30-paper sample with stable identities.** Record time queued, uploaded, parse submitted, remotely started and terminal; count actual overlapping remote operations, quality versus technical outcomes and images. Stop and restart the new worker once while tasks are in flight; confirm no duplicate remote writes.
- [ ] **Step 2: Compare concurrency 3 then 6 on prepared backlog.** Measure end-to-end papers/hour, median and 95th-percentile execution time, model rate limits and remote overlap. If 6 slots remain stable and add real throughput, test 9 under the same limits; otherwise retain the measured lower setting.
- [ ] **Step 3: Document exact evidence and limits.** Include dataset/pipeline IDs, task counts, mismatches and any unresolved writes, omitting secrets and private paper text. Keep the active education batch on its current reconciled worker until a batch boundary or separately verified migration checkpoint.
- [ ] **Step 4: Commit the verification record only after its evidence exists.** Commit the one documentation file with `git commit -m "docs: verify universal ingest core"`.
