# 正文画像工程恢复 Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this approved plan inline; independent reviewer checks the final candidate.

**Goal:** Restore saved response formatting where deterministic, preserve request history, block unknown resends, and report safe specific engineering errors.

**Architecture:** Reuse native parse_saved_candidate, request_observation, adopt_saved_response, create_revision and request_once. Add a small Prefect adapter recovery entry against the exact original body workspace. Keep native files, source manifest, template and successful profile identities unchanged.

**Tech Stack:** Existing Python 3.14, unittest, filelock and native adapters; no new dependency.

**Spec:** User-approved conversation: only invocation/format recovery; 95 successes must not invoke models; saved parsing adds zero requests; unknown requests must not resend; downstream failures must not regenerate profiles. One additional attempt at most per original request chain.

## Global Constraints

- No content quality inspection, semantic repair, template edits or native release changes.
- Original request/response/events remain byte-identical; append diagnostics, diffs and attempt receipts.
- ECONNRESET and IDLE_TIMEOUT behind upstream_stream_error remain unknown until reconciled.
- Retry only a positively classified eligible failure; preserve paper/source/request identity and original request bytes.
- Real five-case processing is local inspection/format recovery only while their upstream results remain unknown. No batch replay, RAG or NAS action.
- This is a non-Git workspace. Candidate and before-copy live under prefect-paperflow/runtime/development/profile-recovery-20261028; integration compares hashes before replacing authorized files.

## Review Focus

- A changed source manifest must not silently bypass 95 successful caches: do not change that manifest or runtime config.
- Unknown transport with explicit gateway error, partial stream or misleading finish_reason must never become retryable.
- Repeated recovery, concurrent callers and a crash after reserving an attempt must not create a second request.
- Syntax parse failures must retain exact source bytes, safe locations and durable diagnostics without copying private text into UI errors.
- Cached success and RAG/NAS failure must invoke zero models; retry selection cannot escape original workspace/source lineage.

### Task 1: Recovery evidence, deterministic parsing and bounded retry

Files: new paperflow_prefect/profile_recovery.py; tests/test_profile_recovery.py.
Interfaces: inspect_request(work) -> safe observation; repair_saved_profile(work) -> immutable receipt; retry_saved_request(work, source, template_path, connection, invoke) -> receipt; saved_response(work) -> str or None. All recovery targets bind exact original work/model evidence. No automatic retry from extract_profile.

- [x] Add failing tests for unknown/reset/timeout classification, supported syntax recovery without requests, ambiguous quotes rejection, immutable evidence, adopted saved response, and one-attempt cap.
- [x] Implement wrappers around the existing native functions; preserve native rules and original evidence.
- [x] Run targeted tests, including tampered evidence, crash reservation and exact request replay.

### Task 2: Integrate safe errors and append-only profile history

Files: body.py, records.py, flows.py; new profile_recovery_cli.py; tests/test_profile_recovery.py, test_backend.py, test_safety.py.

- [x] Add failing tests for safe UI causes, preserved prior stage history, successful cache reuse and downstream failure without model calls.
- [x] Keep original cached body work identity. Consume restored/adopted/retry response through extract_profile with unchanged validator semantics; format-only CLI never runs validator or downstream stages.
- [x] Add inspect/repair/retry CLI, scoped to one existing work directory and existing single-paper lock; local recovery does not load model credentials.
- [x] Repair baseline test isolation: test disabled execution with explicit disabled Settings, independent of live configuration.
- [x] Run full suite.

### Task 3: Five-case evidence, independent review and integrate

- [x] Capture hashes of existing 100 request records and 95 successful profile receipts/artifacts before inspection.
- [x] Inspect the three unknown results; attempt only native deterministic parsing of the two complete saved answers; write independent diagnostics, no semantic guessing.
- [x] Independently review candidate, resolve important findings with regression tests, run full suite.
- [x] Compare source hashes against before-copy, install only scoped adapter/test/documentation files, verify installed suite and original evidence hashes.
- [x] Report implementation vs real recovery separately, with explicit unrecoverable/unknown counts and no runtime-load claim.
