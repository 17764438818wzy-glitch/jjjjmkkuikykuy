# Delivery rules v2 implementation plan

> **For agentic workers:** Use superpowers:executing-plans for the approved change in the current workflow.

**Goal:** Apply the approved A/B/C discussion draft: two targeted corrections, recover clear missing deliverables, narrowly handle explicit private identity, and show partial target completion.

**Architecture:** Reuse the existing producer budget, pre-seal normalizer, immutable validator, per-target receipts, SQLite metrics and UI. Do not add graph stages, schema migrations, a semantic reviewer, or a new parser/service.

**Tech Stack:** Current Python/LangGraph/SQLite/pytest/Tkinter only.

**Spec:** User-approved A1-A7, B1-B7, C1-C9 discussion draft plus the two clarified boundaries in this conversation.

## Global constraints

- STOP_TRIAL and paused live state remain in force; do not invoke a real model or RAG server, resume the trial, or migrate live state.
- Two extra corrections per paper, shared by JSON and delivery correction; versions or changing error messages do not reset the durable count.
- Trust DeepSeek's semantic choices. Do not require chapter trees for explicitly identified nonchaptered attachments or perform numeric/name-based privacy vetoes.
- Mechanical repair happens before sealing, preserves meaning, and must not conceal identity conflicts or changes to a sealed package.
- Actual stage/counts remain authoritative for dispatch; partial completion is derived from the current revision's target receipts and pending targets.

## Review focus

- Explicit author/student fields must not turn into blanket phone/name/email filtering.
- Known privacy edits to body text must preserve slice coverage and avoid editing immutable packages in place.
- Empty descriptions reuse only that asset's existing explanation; missing structure only reuses explicit headings/fields.
- Partial state must not count historical revisions, intents, or disabled indexed documents as fully usable success.
- Correction budget must remain durable through network failures and version changes.

## Task 1: Tests and implementation

- [x] Reproduce failures for the two-attempt budget, recoverable/unrecoverable structure and descriptions, private identity context/precise locations, and partial status.
- [x] Implement the smallest extensions to current interfaces; preserve identity/byte checks and successful target receipts.
- [x] Run targeted tests and update assertions that explicitly encoded the prior one-attempt/advisory-empty-description policy.

## Task 2: Verification and handoff

- [x] Run the full offline suite with external network connections blocked.
- [x] Obtain independent read-only review, fix material findings within scope, and update the existing rules/progress records.
- [x] Confirm STOP_TRIAL remains present and ingestion processes are absent; do not resume.

Evidence: `runtime/rules-v2-verification-20261017/result.json` (183 passed, no external connection attempts), `paused-check.json` (STOP_TRIAL present, automatic_resume=false, no ingestion processes, original prompt snapshots unchanged), and `review.md` (bounded independent review passed after regressions).
