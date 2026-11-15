# Preprocessing triage repair implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task. User approved the preceding triage and repair order on 2026-09-27.

**Goal:** Repair evidenced preprocessing faults, verify existing outputs affected by cleanup, and resume recoverable work with bounded LAN AI requests.

**Architecture:** Keep native preparation, AI decisions, publication and RAGFlow ingestion as separate checkpoints. Fix deterministic faults at their source, then replay real saved inputs before enabling a bounded recovery run. Preserve original files and provenance.

**Tech Stack:** Python 3.14, pytest, native DOCX/PDF extraction, Tk, local JSON checkpoints and SQLite ingestion queue.

**Spec:** `data/diagnostics/failure-triage-20260927/分诊报告.md` and the user's approval.

## Global constraints

- Keep LAN text provider and low concurrency; retain configured vision fallback.
- No blanket approval, dropped research figures, fabricated cells, or disabled privacy checks.
- Preserve unrelated WIP and the separately running education parsing worker.
- Back up changed source and manifests before updating saved state; never modify originals.
- Do not force-close or restart the workbench.
- Count unique file identities and preserve retry ancestry, receipts and upload deduplication.

## Review focus

- A research title mentioning a teacher must pass, while explicit identity fields must fail.
- TOC acknowledgement entries must not remove body chapters; actual acknowledgements still disappear.
- Literal citations remain text; actual missing image/file targets still fail validation.
- An AI rejection remains rejection unless a fresh, bounded, validated decision resolves it.
- Old checkpoints containing removed body text must be regenerated, not trusted for AI resume.

## Task 1: Deterministic cleanup, title and browse fixes

Files: `content_rules.py`, `evidence.py`, `human_view.py`, `test_triage_cleanup_regressions.py`.

- [x] Add failing tests for all three real cleanup patterns, the teacher research title, and citation shapes; include identity and genuine-link counterexamples.
- [x] Run explicit tests and observe failures, then minimally fix shared cleanup rules and browse link handling.
- [x] Replay the three saved original documents and all ten browse errors. Run relevant existing tests.

## Task 2: Affected-output audit and checkpoint policy

Files: bounded diagnostic tools under `data/diagnostics/triage-repair-20260927`, production version/checkpoint code only if required by replay.

- [x] Compare native cleanup for all final approved outputs against saved preparation; identify recoverable body omissions and their published/upload state.
- [x] Record exact affected IDs and isolate their correction workflow; preserve old receipts and avoid duplicate uploads.
- [x] Refresh only derived browse results after verified link repair. Rebuild changed-body documents from original input.

## Task 3: AI and input recovery

Files: `agent_client.py`, `auto_evidence.py`, response/recovery modules only where a failing example proves a defect; corresponding targeted tests.

- [x] Verify existing black-image focused recheck and structured-response retry changes against saved failures.
- [x] Test precise identity masking and research-role decisions with small real requests; correct bounded prompts/validation without bypassing rejection.
- [x] Diagnose representative location/table/PDF/archive failures; handle attachments separately, recover supported original papers and report unrecoverable inputs accurately.
- [x] Verify LAN health and service retry behavior, keep low concurrency and bounded retries.

## Task 4: Resume, handoff and verification

- [x] Review final changes, run the relevant root test suite explicitly (exclude diagnostic/backups directories).
- [x] Run a representative recovery pilot, inspect saved outputs, then resume eligible groups using current snapshots and validated checkpoints.
- [x] Verify new passes reach the configured education upload queue exactly once; preserve other categories and existing accepted receipts.
- [x] Report fixed defects, sample and regression results, continuing work, input holds and real blockers with exact counts.

Execution note: verification and eligible resumption are complete; the bulk processing run continues asynchronously. Two live evidence mismatches and original-file holds remain explicitly recorded, not approved. See data/diagnostics/triage-repair-20260927/ledger.md and input-and-evidence-holds.json.
