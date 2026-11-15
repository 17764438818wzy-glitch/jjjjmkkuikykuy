# Four delivery checks implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement the approved change in the existing workflow.

**Goal:** Trust DeepSeek's content decisions; only incomplete delivery, body loss, incorrect library routing, and unconfirmed ingestion can block completion.

**Architecture:** Reuse the current producer, deterministic package utilities, LangGraph checkpoints and per-specialty receipts. Normalize mechanical format differences before immutable sealing. Keep unmapped targets explicit and never report partial ingestion as complete.

**Tech Stack:** Existing Python, PyYAML, LangGraph, SQLite and pytest; no new dependencies.

**Spec:** User approval in this conversation on 2026-10-17, following the agreed retained/relaxed rules.

## Global constraints

- Keep STOP_TRIAL, paused state, checkpoints, receipts, original papers and existing immutable packages unchanged.
- Do not call DeepSeek, the live RAGFlow server, or any dispatcher during implementation/verification.
- Do not add quality scoring, privacy approval, or semantic review. Normal redaction remains part of the existing DeepSeek stage.
- At most one additional targeted delivery correction per paper, including same-session JSON correction; service retries do not rewrite content.
- Keep every selected specialty independent. Successful target receipts must survive retries of another target.

## Review focus

- Malformed auxiliary files must not block otherwise usable delivery.
- Format normalization must preserve all body text and retain duplicate-ID records rather than deleting content.
- One invalid specialty must not prevent valid targets being written or allow the article to appear complete.
- A repaired LLM stage must not invoke the same business stage again.
- Remote identity/readback failures must never call the content repair model.

## Task 1: Package compatibility and four delivery boundaries

- [x] Write failing tests for advisory privacy/model flags, optional files, IDs/parents, canonical specialty names, pending mappings and retained-image requirements.
- [x] Run tests and confirm the failures describe the approved change.
- [x] Reuse package normalization and sealing; preserve source/target identity, file bytes and body coverage checks.
- [x] Verify the relevant package tests.

## Task 2: One targeted correction and reliable target delivery

- [x] Write failing producer/graph tests for one shared correction budget, no repeated business-stage call, remote retries and pending-target persistence.
- [x] Reuse existing Harness sessions and graph error handling; keep small issues advisory and auxiliary JSON outside syntax repair.
- [x] Verify a local end-to-end ingestion with real package/graph/publisher logic and an in-memory RAG server substitute.

## Task 3: Verification and handoff

- [x] Run the full local pytest suite and inspect all failures.
- [x] Obtain independent read-only review and fix material issues within the approved scope.
- [x] Record results in docs/progress.md, verify STOP_TRIAL and absence of ingestion processes, and report without resuming.

## Baseline

- Before changes: `python -m pytest -q`: 129 passed in 18.66s.
- No Git repository exists here; pre-change source/tests/docs snapshot is stored under runtime/rule-change-backup-*.

## Final evidence

- Initial approved-rule regressions: 16 expected failures, followed by passing targeted tests.
- Review findings reproduced and repaired: activation bypass, stale predecessor output, budget reset on service errors, optional manifest files, value-changing JSON normalization, storage spelling, preserved text aliases, changed mappings and null auxiliary references.
- Full offline suite: 157 passed, 0 failures/errors, 0 external connection attempts; independent read-only review passed.
- Evidence: runtime/rule-change-verification-20261017/result.json, pytest.xml and paused-check.json.
- STOP_TRIAL preserved; no ingestion processes; no real model or RAGFlow ingestion was started.
