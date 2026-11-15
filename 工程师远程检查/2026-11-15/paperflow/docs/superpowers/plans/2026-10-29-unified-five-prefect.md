# Unified five-stage Prefect implementation plan

> **For agentic workers:** Use superpowers:executing-plans for inline implementation; reuse-first-dev independent reviewers inspect the final changes and evidence.

**Goal:** Make the five existing capabilities usable through one Prefect 3 batch deployment and provide a verified frontend handoff.

**Architecture:** Merge the existing H2.1 receipt/attachment MD path, RAG recovery and 20-slot gate, and Stage5Receiver into the latest main source. Keep the original task/receipt/model identities. Use Prefect native deployments and concurrency limits; do not create a second scheduler.

**Tech Stack:** Windows, Python 3.14, Prefect 3.9.0, filelock, existing native Paperflow adapters.

**Spec:** ../../../prefect-paperflow/docs/handoffs/stage45-20261029/前端与Prefect3接线设计.md and 代码与验收交付清单.md.

## Authorization and execution

The user explicitly requested on 2026-10-29: unify all five, configure them usable, then report for frontend coordination. This supersedes the prior read-only and candidate-only boundary for this integration. Reuse the existing agreed design; no additional design approval is needed. The current directory has no Git repository, so use an isolated candidate plus SHA256 baseline/backup rather than inventing Git history. Main implementation is inline; independent agents perform read-only reuse and acceptance review. Their tool capabilities are not technically restricted.

## Global constraints

- RAG uses the same global resource `paperflow-connected-rag`, target 20; NAS remains 2.
- Completed model results are reused. Unknown requests or uploads are reconciled and never blindly resent.
- Keep H2.1 identities, receipt history, original model requests/responses, final MD, packages and evidence.
- Stage 5 accepts only complete whole-paper RAG proof. NAS readback and a committed receipt precede any local removal.
- Cleanup is restricted to frozen admitted files under the configured input root and proven exclusive intermediate files. Original library/history paths cannot expand the scope.
- An isolated acceptance uses disposable copied inputs; do not resubmit the old 100-paper run for verification.
- Do not message other user chats. Deliver the frontend handoff here.

## Review focus

1. Legacy completed/unknown requests and migrated H2.1 receipts keep identity without duplicate model or RAG writes.
2. No attachments bypasses the attachment RAG gate; attachment-only/partially received groups preserve independent results.
3. A failed or unknown RAG target blocks only that paper's NAS and cleanup, not other papers.
4. Cleanup recovery after source removal starts at Stage 5 and keeps request/response and retained evidence.
5. Runtime identity, deployment schema, local gates and remote limits agree after switching the idle executor.

### Task 1: Baseline and upstream integration

- [x] Capture exact current source/config hashes and isolate the candidate.
- [x] Add failing integration tests for H2.1 batch receipt routing and legacy cached identity preservation.
- [x] Reuse receipt and third-stage modules through a shared batch entry; preserve the independent stage-3 entry.
- [x] Verify the new tests and existing H2.1/attachment/profile regressions.

### Task 2: RAG and Stage 5 integration

- [x] Bring in existing RAG recovery/status modules and meaningful candidate tests; demonstrate missing wiring failures.
- [x] Merge shared files by capability. Wire MD + original publication, unique whole-paper handoff, Stage5Receiver, recovery and actual completion results.
- [x] Test empty attachments, partial RAG, uncertain writes, cancellation, cleanup recovery, immutable evidence and retained files.
- [x] Run the combined offline suite and isolated real NAS acceptance with disposable material.

### Task 3: Runtime configuration and verified deployment

- [x] Prepare production-safe configuration, supported parameters and runtime identity readback before switching.
- [x] Check active runs and global slots; switch only when the existing executor is idle.
- [x] Back up changed main files; apply hash-checked changes, align local/remote RAG=20, keep NAS=2, restart the managed executor hidden.
- [x] Verify a new read-only deployment check run, actual loaded hashes, native dependency/credential readiness, and service heartbeat.
- [x] Execute a minimal bounded integration acceptance using existing verified results or disposable sample inputs; preserve all request identities and report exact evidence scope.

### Task 4: Frontend handoff and review

- [x] Provide concrete deployment parameters, business status/receipt sources, supported operations and examples that match the implementation.
- [x] Distinguish actual live interfaces from frontend work remaining; avoid claiming existing HTTP routes automatically map to Prefect.
- [x] Independent reviewer checks the actual candidate/main diff and acceptance artifacts; resolve important findings.
- [x] Update the existing handoff documentation and Prefect artifact with verified deployed version; report concise results and limitations.

## Progress ledger

- Baseline audit confirmed the main deployment uses the old folder intake and description publisher; Stage5Receiver is absent; remote RAG limit is 4; current runner is PID 31896 with no observed resource occupancy in the prior turn.
- Existing candidate source and acceptance modules are the implementation source, not copied runtime data or old candidate configs.

- Final candidate regression: 380 tests passed. Legacy candidate wiring tests were ported to the actual H2.1 unified entry; native cancellation, history, partial-input, immutable evidence and recovery contracts remain checked.
- Independent review found and closed input keyword dispatch, self-polluting legacy inventory discovery, stale-success overwrite, native pending/supplement upload history, cancellation at write/delete boundaries, duplicate paper identity and missing receipt-root guards.
- Live readiness: all 883 formal majors and both libraries matched remote ID/name/embedding; model credential readable, local Nimble completed actual classification/H2.1 sample receipt, and NAS accessible.
- Real NAS isolated acceptance: two generated originals and one proven cache, interruption after unlink, exact journal reconciliation and repeat completion; synthetic RAG scope preserved explicitly. An initial fixture placed a cache outside data_root and was correctly retained; a new correctly scoped fixture passed without changing the guard.
- Main source baseline had zero drift. Idle old runner stopped; exact changed-file backup saved at prefect-paperflow/runtime/backups/unified-five-1793286324011403200. Formal inputs/data/receipts/classification are distinct existing directories; old trial and historical model/receipt roots retained.
- Existing deployment 3c968368-58fc-40a0-9966-0ba2fde8c92c now uses paperflow_prefect.unified_flows.paperflow_batch. RAG20/NAS2 verified, runner PID38944, no schedules. Read-only check run 458c2e6f-9d01-4cf1-acf7-6e6773bc6522 completed with matching loaded source SHA d6c585eabad87e89c4a52c8ac11dcbad01a67c9e120b5c636bef72243c917dde.
- Actual deployment resume-rag run a8b315c8-d4ad-4f2d-9879-dc30ba47d860 completed one real no-attachment paper through original RAG IDs, native NAS readback and cleanup of only a copied inbox original. Original source and 10 model/profile/package files unchanged; zero new model requests and zero new RAG documents. It reused frozen trial targets; it does not prove a fresh-model or full-attachment production batch.
- Actual resume-stage5 run c106e30e-4a9e-4f08-8059-dee9b779dfa1 completed again after local-copy removal.
- Frontend handoff saved at prefect-paperflow/docs/统一五步-Prefect3-前端对接.md. It documents actual Prefect APIs/artifact schemas, not a nonexistent stage45-view HTTP service. Existing frontend controller still needs its adapter.
- Updated Prefect document keys with verified readback: frontend 4e92f0a4-035a-4b80-a5e4-cc257dd4f0d5; delivery de705f74-1715-443e-9f3f-1adb7e2eac6b. Old versioned document bundle retained as history, with a new current-deployment pointer.
