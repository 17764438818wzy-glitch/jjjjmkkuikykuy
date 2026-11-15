# Workbench Upload Pool Adoption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route every future workbench automatic and manual upload through the verified universal queue while retaining each button's routing, parse intent and professional Pipeline behavior.

**Architecture:** Intake adapters turn existing approved manifest items into `ArtifactSpec` tasks. Existing UI and automatic entry points call the adapters rather than writing to RAGFlow directly; a compatibility layer preserves synchronous callers during migration. Queue-backed status queries show upload and parse progress without relabeling queued tasks as uploaded.

**Tech Stack:** Python 3, Tkinter workbench, existing `Store`, `RagflowClient`, `auto_config`, universal SQLite work pool from `2026-09-26-ragflow-work-pool-core.md`, `unittest`/`pytest`.

**Spec:** `docs/superpowers/specs/2026-09-26-universal-ragflow-ingest-work-pool-design.md`

## Global Constraints

- Complete and verify `2026-09-26-ragflow-work-pool-core.md` before this plan.
- Finance knowledge bases, finance Pipeline DSL, existing remote documents and prior upload receipts remain unchanged.
- Preserve manual `app.py` upload-only intent, while `rag_results_ui.py` “导入选中项” and automatic handoff retain upload-and-parse intent.
- Use the manually selected md/image targets when supplied; otherwise use the existing major and batch mapping. Do not infer a target from a document title.
- Main paper and each auxiliary image/projection have independent destination receipts; original images are upload-only unless their existing role policy explicitly requires more.
- Queue credentials are loaded through existing Windows DPAPI configuration and never serialized into SQLite or logs.

## Review Focus

1. A manual upload-only click: uploads the primary and approved assets without submitting parse. Covered by Task 1.
2. A manual “导入选中项” with fixed targets: ignores a conflicting major default map and parses only the main document. Covered by Task 1.
3. Upload-only manual work followed by automatic parse of one stable paper in the same dataset: one upload task, then a new parse intent on its confirmed remote ID. Covered by Task 2.
4. A queued or uncertain task in the UI: never displayed as confirmed uploaded. Covered by Task 3.
5. A finance quality rejection: recorded distinctly from technical failure without altering the finance Pipeline. Covered by Task 2.

---

### Task 1: Build the workbench intake and finance policy

**Files:**
- Create: `ingest_workbench.py`
- Create: `test_ingest_workbench.py`

**Interfaces:**
- Consumes: `ArtifactSpec`, `canonical_remote_name()` and `QueueStore.enqueue()` from the core plan; existing `auto_ragflow._route_datasets()`, `ragflow_client.artifact_route()`, `upload_tracking.paper_id_for_item()`, `package_fingerprint()` and `Store` manifest access.
- Produces: `workbench_specs(store: Store, run_id: str, item_ids: list[str], targets: dict | None, mode: Literal['upload_only', 'upload_and_parse'], config: dict, service_url: str) -> list[ArtifactSpec]`; `classify_finance(task: dict, remote: dict) -> Outcome`.

- [ ] **Step 1: Write failing adapter tests.** An upload-only manual task sets `parse_required=False`; selected md/image targets override major mapping; upload-and-parse creates exactly one `role='paper_primary'` parse task and upload-only auxiliary tasks under their role destinations; each new remote name is at most 255 UTF-8 bytes and includes stable source ID, content-hash prefix and any finance Pipeline-required legacy task ID, with that ID still matching `CodeExec_PrepareAccounting.py`'s filename pattern; an unchanged source hashes to the same queue identity; changed source or package hash is rejected before enqueue; `accounting_admission=入库/暂缓/剔除` maps to the matching outcome when verified, and generic progress-only `FAIL` is unresolved.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_workbench.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Implement intake using existing preflight and route helpers.** Load each selected manifest once per group and index item IDs in memory; use queue uniqueness instead of scanning `Store.history()` per paper. Preserve automatic-approval versus manually reviewed entry conditions. Set `policy_key='finance'` for finance items and a non-secret `channel_key` for each configured model route; use the corresponding education policy for education items. Set `priority=10` for manual and `priority=0` for automatic tasks, matching the core 3:1 claim cycle. Keep finance Pipeline binding on the existing remote document; the adapter must not PATCH finance DSL or datasets.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_ingest_workbench.py test_routing.py test_upload_dedup.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit only intake and tests with `git commit -m "feat: adapt workbench uploads to universal queue"`.

### Task 2: Route automatic and selected-import entry points to one queue

**Files:**
- Modify: `auto_ragflow.py`
- Modify: `auto_pipeline.py`
- Modify: `rag_results_ui.py`
- Create: `test_ingest_entrypoints.py`

**Interfaces:**
- Consumes: Task 1 `workbench_specs()` and core `QueueStore.enqueue()`, `WorkPool` status and existing `handoff()` signature.
- Produces: `enqueue_workbench_items(store: Store, run_id: str, item_ids: list[str], targets: dict | None, mode: str, queue: QueueStore, config: dict, service_url: str) -> list[int]`; an `auto_ragflow.handoff()` compatibility path that submits these tasks and, for callers requiring its old completion semantics, waits for upload terminal receipts without issuing a second direct RAGFlow write.

- [ ] **Step 1: Write failing entry-point tests.** A completed, automatically approved paper is enqueued immediately after its manifest item is durably saved, before the rest of the preprocessing batch ends; the final batch sweep does not duplicate it. Manual upload-only followed by auto handoff of the same paper/destination returns one queue task and adds parsing on its existing remote ID; batch-fixed targets remain fixed; only the main document has `parse_required=True`; `handoff()` retains progress and error reporting; an uncertain prior upload waits for remote reconciliation and never calls `client.upload()` again.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_entrypoints.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Replace direct upload/parse calls in the normal automatic and selected-import paths.** Enqueue each approved automatic item after its manifest save; retain the batch-end handoff as an idempotent sweep. Resolve `classify_for` and `on_accepted_for` from each task's `policy_key`; finance admission reads its own verified `accounting_admission` fields. Keep existing manifest receipt fields as a queue-backed compatibility projection during migration, so earlier results remain readable. Do not rewrite already confirmed remote documents.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_ingest_entrypoints.py test_routing.py test_derived_upload_port.py test_deferred_ragflow.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit only the listed code and tests with `git commit -m "feat: queue automatic and selected ragflow imports"`.

### Task 3: Route manual upload-only and show queue-backed status

**Files:**
- Modify: `app.py`
- Modify: `rag_results_ui.py`
- Modify: `upload_tracking.py`
- Create: `test_ingest_status_ui.py`

**Interfaces:**
- Consumes: Task 2 `enqueue_workbench_items()` and core `QueueStore.get()`/summary data.
- Produces: `queued_upload_status(task_ids: list[int], queue: QueueStore) -> dict` for existing UI panels; queued, uploading, uploaded, parsing, terminal and uncertain labels derived from durable task state.

- [ ] **Step 1: Write failing status tests.** Clicking the existing manual upload button enqueues `mode='upload_only'`; queued does not display “已上传”; confirmed remote upload does; parse-in-progress is distinct from upload-in-progress; unknown acknowledgement displays “待核查”; manual fixed targets and approved images remain visible in separate destination receipts.
- [ ] **Step 2: Run `C:\Python314\python.exe -m pytest -q test_ingest_status_ui.py`.** Expected: the new tests fail before implementation.
- [ ] **Step 3: Connect the manual button and existing status views.** The current background UI task can wait for confirmed upload count when it must keep the old callback return; the new status view queries paged queue summaries without loading every historical manifest or blocking Tkinter's main thread.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_ingest_status_ui.py test_upload_dedup.py test_rag_results.py test_workbench.py`; expected PASS.
- [ ] **Step 5: Commit.** Commit only the listed code and tests with `git commit -m "feat: show queued upload and parse status"`.

### Task 4: Confirm all workbench ingress paths and staged rollout

**Files:**
- Create: `docs/superpowers/specs/2026-09-26-universal-ingest-adoption-verification.md`
- Read: current workbench config and exact remote md/image document inventories.

**Interfaces:**
- Consumes: Tasks 1–3 and the core verification report.
- Produces: a read-only rollout report and an explicit switch point for automatic, selected-import and manual-upload paths.

- [ ] **Step 1: Search every normal `client.upload()` and `client.parse()` call path reachable from workbench buttons and automatic handoff.** Confirm each future write now passes through `QueueStore`; legacy recovery paths either use the queue or are disabled as duplicate writers. Record any exceptional path in the report.
- [ ] **Step 2: Test a small approved finance sample and a manual upload-only sample on their exact intended datasets.** Compare stable/source IDs, remote names, parse intent and receipts. Check existing finance document count and Pipeline DSL before and after; account for intentional sample additions only.
- [ ] **Step 3: Exercise restart and mixed load.** Queue the same source from automatic and manual entry, restart the worker between intent and acknowledgement, and verify a single remote destination document and correct status in both UI panels. Measure whether manual priority still lets older automatic work progress.
- [ ] **Step 4: Write evidence and commit.** Record task counts, dataset IDs, throughput, actual remote concurrency, errors and unresolved items without secrets or private paper text. Commit the verification report only after direct checks with `git commit -m "docs: verify universal workbench ingest adoption"`.
