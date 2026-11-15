# RAG lock reduction implementation plan

> **For agentic workers:** Use an isolated candidate and the existing Prefect adapter. Work does not authorize deploying or dispatching production tasks.

**Goal:** Eliminate the shared folder lock on verified-folder reads, preserving remote-write safety and retaining the existing local RAG gate until its safety role can be replaced.

**Architecture:** A verified folder can be reused after a fresh, unique, identity-matching readback; first creation and unknown outcomes retain the existing protected path. Prefect 3.9 strict mode does not prevent a disabled global limit from being skipped, so the existing batch-local gate cannot yet be proven redundant.

**Tech stack:** Python 3, Prefect 3, filelock, unittest.

**Spec:** `docs/superpowers/specs/2026-10-31-rag-lock-reduction.md`.

## Constraints

- Candidate configuration disables real execution and targets inert loopback endpoints.
- No changes to the active worker's source, configuration, scheduling, locks, or business data.
- Preserve prior RAG receipts, intent markers, immutable output checks, and source identities.

## Tasks

1. Confirm the existing relevant fixture tests pass in the isolated candidate.
2. Retain the existing local RAG gate after independent review found inactive-limit and cancelled-waiter cases in Prefect 3.9.
3. Add failing tests for read-only reuse while the folder-creation lock is held, plus rejection or safe waiting for unknown, missing, duplicate, and mismatched folder identities.
4. Add the verified-folder fast path without weakening creation, uncertainty, or identity checks; re-run attachment regressions.
5. Run relevant offline regressions and obtain independent review. Report candidate-only scope and any cutover impact accurately.
