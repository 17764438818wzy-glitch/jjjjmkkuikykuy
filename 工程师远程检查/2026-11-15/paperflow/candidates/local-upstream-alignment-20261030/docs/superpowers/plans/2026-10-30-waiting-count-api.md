# 等待前处理数量接口 Implementation Plan

> **For agentic workers:** Execute in this existing isolated candidate. The user has approved implementation and narrowed this delivery to the waiting count. Use test-driven development and do not ask for another implementation approval.

**Goal:** Persist real, verified local receipt of eligible groups and expose one truthful, availability-aware count for the frontend.

**Architecture:** Reuse `local_handoff.read_registered_item` to validate the source receipt and delivery; store only verified group identity, file references and reception state in an independent SQLite ledger. A FastAPI GET returns an atomic local waiting-count snapshot and explicit source availability; discovery can be invoked by the caller against the existing first-step API, with no background work started by import. An unconnected source returns null, never a fabricated zero.

**Tech Stack:** Python 3, SQLite, FastAPI, pytest; existing first-step receipt reader.

**Spec:** `docs/superpowers/specs/2026-10-30-intake-queue-api-design.md` (user scope update in its first section takes precedence).

## Global Constraints

- Deliver only the waiting-count interface; do not publish the processing-count API now.
- Never process real papers, call models, modify the first-step service, or alter the frontend during development.
- Do not copy or move delivery files. Garbage never appears in the downstream input list.
- A first-step registered result is not itself a local acceptance. Retain receipt and file identity; do not accept a partial current observation.
- Local state must not be mistaken for an exhaustive current view if upstream scan or source availability is unknown.
- No git repository exists here; retain files in the isolated candidate rather than creating a git repository or commits.

## Review Focus

- An originally successful receipt whose current observation is incomplete must not enter waiting.
- Duplicate observations, changed versions and cross-task identity conflicts must not duplicate or silently merge work.
- A failed/incomplete scan or missing source must not be reported as a known zero.
- A deleted file or receipt must not be reported as a verified usable input when discovered.
- One group's body and attachments are preserved and garbage never enters the usable-input list.

### Task 1: Validated, persistent local acceptance

**Files:** Create `local_queue.py`; test `tests/test_local_queue.py`.

**Interfaces:** `LocalQueue(path).receive(task_id, item, receipt_root)` uses `read_registered_item`, persists a complete eligible observation once, and returns its waiting record; `LocalQueue.snapshot()` reads a consistent state. A changed receipt or cross-task active identity conflict is retained for review, not silently counted or merged.

- [ ] Write tests using the existing synthetic registered-delivery fixture: verified body and attachments enter exactly once; a damaged attachment or body does not; garbage is absent from usable inputs; changed identities and receipts do not duplicate or overwrite.
- [ ] Run the tests and confirm they fail because the module/behavior is absent.
- [ ] Implement transactional SQLite persistence and strict acceptance checks.
- [ ] Re-run task tests and the pre-existing 14 receipt-reader tests.

### Task 2: Discover eligible upstream groups without pretending pagination is a permanent cursor

**Files:** Create `upstream_discovery.py`; test `tests/test_upstream_discovery.py`.

**Interfaces:** `discover_once(source, queue, receipt_root)` enumerates task and group pages using a source adapter, sends eligible rows to `LocalQueue.receive`, and records complete/incomplete scan status without starting preprocessing. A read-only GET adapter may be provided for the existing first-step API but must not run automatically or be invoked against live tasks during development.

- [ ] Write tests with an isolated representative source: multiple pages, registered and unregistered groups, repeated scans, scan failure, and changed result order.
- [ ] Confirm RED; implement resilient repeated enumeration and explicit failure/freshness, preserving failure evidence and status.
- [ ] Confirm tests pass without opening live URLs.

### Task 3: Waiting-count HTTP contract

**Files:** Create `waiting_api.py`; test `tests/test_waiting_api.py`; update candidate `README.md` with integration and limitations.

**Interfaces:** `create_app(queue)` provides `GET /api/v1/local-intake/preprocessing/summary`; response is `{scope, waiting_count, waiting_available, as_of, upstream_scan_completed_at}` and does not expose `processing_count`. Unavailable or incomplete source returns `waiting_count: null`, not 0. It does not start discovery or a server on import.

- [ ] Write FastAPI TestClient tests for unavailable state, known waiting counts after real synthetic receipt validation, repeated reads, and no processing-count field.
- [ ] Confirm RED; implement route and document the concrete API, source setup, test-only verification, and still-unwired downstream start/removal signal.
- [ ] Run the complete isolated candidate test suite and obtain independent read-only review of files and test results.
