# RAGFlow results page implementation plan

**Goal:** Preserve detailed upload and remote processing states with two filters, consistent selection controls, and stage-aware selected retry.

**Spec:** Approved conversation: first page owns script outcomes, second owns AI, third owns upload and RAGFlow. Targets chosen before starting remain read-only here. No live model or upload tests.

**Architecture:** Introduce a small remote state module and a RAGFlow UI mixin. Reuse receipt reconciliation and upload routing; restrict handoff to selected item IDs. Read and update results in their origin run across retry chains.

- [x] Test and implement remote status normalization, distinct query failures, asset partial failure, and saved-target resolution.
- [x] Test and implement selected-only recovery; query known documents before parse; skip running/queued/done; retain receipts and confirm uncertain uploads.
- [x] Build dual filters, search, all/none/invert/count above table, read-only destinations, details and single retry button. Hide shared script panel on this page; show remote stage/result information within the page.
- [x] Refresh origin-run records without rewriting processing operation or overwriting newer receipts; keep selection across refresh.
- [x] Update obsolete destination-selector tests to first-page/connection controls and add third-page behavior tests; run full suite.

**Review focus:** querying a server outage must not turn cached completion into remote rejection; queued/cancelled codes must not be guessed as running; saved targets outrank current unrelated dropdown; hidden selections do not run; linked successful results remain visible; partial asset upload never appears wholly complete.

**Boundary:** This change handles the third-page display and recovery. Existing batch-end automatic handoff remains; implementing a streaming upload queue is a separate pipeline change, not silently bundled into the UI redesign.

## Verification and review record

- Remote state and UI tests were observed failing before implementation.
- Independent read-only review identified legacy target mismatch, hidden asset processing exceptions, suppressed latest retry errors, and inconsistent URL alias handling. All four were corrected; targeted regression tests cover these paths.
- The final full root suite completed with 811 passed and the existing test_saved_runtime_config_enables_configured_fallback_channel failure (test_p0_2_fallback_config.py). User fallback settings were not changed.
- After clearing obsolete submission errors on confirmed remote progress, the final focused suite passed 78 tests including that new regression.
- Tests use synthetic files and fake remote interfaces; no real model or upload requests.
- Original scoped files, diffs and verification logs are under C:/Users/Administrator/Documents/uv/diagnostics/rag-page-20260920.
- Primary source for TaskStatus mapping: https://github.com/infiniflow/ragflow/blob/main/common/constants.py .
