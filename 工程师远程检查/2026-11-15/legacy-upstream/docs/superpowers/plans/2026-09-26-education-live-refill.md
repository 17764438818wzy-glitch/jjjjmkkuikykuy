# Education Live Ingest Refill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the false 120-second delay and keep the running education batch's existing parse slots filled before image and artifact work.

**Architecture:** Retain the current single worker, `worker.lock`, JSON receipts and remote document IDs. Correct ambiguous failure classification, then reorder one tick so terminal papers release slots and newly ready primary documents take them before auxiliary work. Reconcile in-flight writes before a controlled worker restart.

**Tech Stack:** Python 3, `unittest`/`pytest`, existing RAGFlow client, Windows PowerShell.

**Spec:** `docs/superpowers/specs/2026-09-26-universal-ragflow-ingest-work-pool-design.md`

## Global Constraints

- Only the education worker and its tests change in this plan. Preserve finance datasets, finance Pipeline and the live education destination IDs.
- Do not reinterpret generic `progress_msg` queue text as a proven queue outage or a quality verdict.
- Quality and unresolved outcomes release the first-pass slot; `Arrearage` remains an explicit model-channel stop condition.
- Never resubmit a `parse_intent`, `submitted` or `unknown` write without checking the matching remote document.
- Current batch keeps one JSON receipt writer; do not migrate its storage while the old process runs.

## Review Focus

1. Three generic queue messages that hide quality outcomes: no long sleep and no automatic retry. Covered by Task 1.
2. An `Arrearage` message amid generic failures: preserve the explicit stop. Covered by Task 1.
3. A completed older paper before a newly ready paper in manifest order: submit the ready primary first. Covered by Task 2.
4. A `parse_intent` whose POST acknowledgement was lost: count it as occupying a slot and do not submit another paper over the cap. Covered by Task 2.
5. Worker restart while RAGFlow still processes a document: adopt remote state without submitting it again. Covered by Task 3.

---

### Task 1: Correct ambiguous failure and polling delay

**Files:**
- Modify: `test_education_stream_ingest.py`
- Modify: `education_stream_ingest.py:28-51,417-456,803-812`

**Interfaces:**
- Consumes: Existing `Stream.poll_primary()`, `classify_parse_failure(progress_msg: str) -> str` and `recovery_poll_delay(queue_failure_streak: int) -> int`.
- Produces: `classify_parse_failure()` returns `outcome_unresolved` for a bare `N tasks are ahead...` progress string; `recovery_poll_delay()` returns `POLL_SECONDS` for every stale streak value. `poll_primary()` no longer increments a queue-failure streak from ambiguous text.

- [ ] **Step 1: Change the tests to assert the corrected behavior.** In `test_education_stream_ingest.py`, assert `recovery_poll_delay(3) == recovery_poll_delay(10) == 15`; three `FAIL` documents with generic queue text become `failure_kind == 'outcome_unresolved'`, release their slots and do not grow `queue_failure_streak`; `Arrearage` still raises `ArrearageRecoveryHalted` after its receipt is saved.
- [ ] **Step 2: Run the focused tests and observe the old behavior fail.** Run `C:\Python314\python.exe -m pytest -q test_education_stream_ingest.py -k 'backoff or classification or consecutive or arrearage'`. Expected: the new assertions fail on the existing 120-second delay and `queue_only` classification.
- [ ] **Step 3: Implement the minimal classification and fixed delay.** Update the three functions above; preserve existing explicit `annotation_invalid`, `input_invalid` and `embedding_arrearage` recognition. Do not infer `quality_hold` or `quality_reject` from `progress_msg` alone.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_education_stream_ingest.py`. Expected: all tests pass.
- [ ] **Step 5: Commit.** Commit only the two intended files with `git add -- education_stream_ingest.py test_education_stream_ingest.py` and `git commit -m "fix: remove false education queue backoff"`.

### Task 2: Give freed primary slots first claim on each tick

**Files:**
- Modify: `test_education_stream_ingest.py`
- Modify: `education_stream_ingest.py:378-416,680-760,803-812`

**Interfaces:**
- Consumes: Task 1's failure behavior and existing `Stream.primary()`, `Stream.images()`, `Stream.direct_tree()` and `Stream.method_graph()`.
- Produces: `Stream.tick()` polls primary outcomes, submits up to `self.max_active` fresh or already uploaded primaries, then performs image and artifact work. `parse_intent` and `submitted` both count toward the cap. The tick summary includes `parse_waiting: int`; `poll_delay(parse_waiting: int) -> int` returns 5 when positive and 15 when zero.

- [ ] **Step 1: Write failing scheduling tests.** Add tests with a completed paper preceding a new ready paper in manifest order; assert `primary()` runs before `images()`/`direct_tree()`. Add a test with one existing `parse_intent` and `max_active=1`; assert the next primary is not parsed. Assert `poll_delay(1)==5` and `poll_delay(0)==15`.
- [ ] **Step 2: Run the new tests and confirm their intended failures.** Run `C:\Python314\python.exe -m pytest -q test_education_stream_ingest.py -k 'primary_before_auxiliary or parse_intent_counts or poll_delay'`. Expected: the new ordering and delay assertions fail.
- [ ] **Step 3: Refactor the tick into two bounded passes.** First process only new or `uploaded` primary receipts until `MAX_UPLOADS_PER_TICK` and the parse cap are reached; then process `done` receipts for images, Tree and Graph. Keep the existing hash/identity check before any upload. Add `parse_waiting` to the summary and use `poll_delay` in `main()`.
- [ ] **Step 4: Verify.** Run `C:\Python314\python.exe -m pytest -q test_education_stream_ingest.py test_education_method_graph.py test_education_direct_tree.py`. Expected: all tests pass.
- [ ] **Step 5: Commit.** Commit only the worker and its tests with `git commit -m "perf: refill education parse slots before auxiliary work"`.

### Task 3: Controlled deployment to the active education batch

**Files:**
- Read: `data/private/education_full_ingest/AUTO_20260924_215028_e9790d/receipts.json`
- Read: `data/runs/AUTO_20260924_215028_e9790d/manifest.json`
- Read: `data/private/education_full_ingest/AUTO_20260924_215028_e9790d/worker.lock`
- Create: a timestamped worker stdout/stderr pair in the same private batch directory

**Interfaces:**
- Consumes: Tasks 1–2 and the existing CLI `education_stream_ingest.py <run_id>`.
- Produces: One running worker on the same batch, with all existing remote IDs retained and no duplicate parse submission.

- [ ] **Step 1: Snapshot and reconcile.** Record process PID, lock ownership, counts of `upload_intent`, `parse_intent`, `submitted` and `unknown`; for every uncertain or in-flight receipt query RAGFlow by its exact remote ID, name and Pipeline ID. Save a timestamped copy of the receipt before stopping the old worker. Resolve any remote mismatch before proceeding.
- [ ] **Step 2: Stop only the identified worker, then verify lock release.** Do not kill the preprocessing GUI or the read-only monitor. Confirm the old worker PID is gone and `worker.lock` can be acquired.
- [ ] **Step 3: Start one new worker hidden.** Run `C:\Python314\python.exe -u E:\uv\ragflow_upstream\education_stream_ingest.py AUTO_20260924_215028_e9790d` via PowerShell `Start-Process -WindowStyle Hidden` with stdout/stderr redirected to new batch log files. Do not launch a second worker when the lock is held.
- [ ] **Step 4: Verify live behavior.** Over at least five new terminal papers, compare receipts with exact remote document IDs: one terminal per submitted paper, no repeat upload/parse, no 120-second wait caused by `outcome_unresolved`, and `parse_waiting` falls when a slot is free. If remote service capacity is unavailable, report the measured limit rather than claiming a throughput gain.
