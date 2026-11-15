# Education Handoff Identity Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a stable source-hash identity to the preprocessing handoff and verify it against the package manifest for education trials while preserving the finance Pipeline's legacy `paper_id` contract.

**Architecture:** Extend the two code paths that emit the handoff header with `stable_paper_id` and explicit `document_id`. Keep `paper_id=AUTO_...` for finance compatibility. Add an opt-in education identity check, then prepare corrected copies of already completed packages for the pilot; keep the active run's files and receipts unchanged until its batch has finished.

**Tech Stack:** Python 3, pytest through system `python`, local Markdown/JSON knowledge packages. The bundled artifact Python has no pytest; `E:\uv\.venv` lacks PyMuPDF. System Python has both and ran the related suite successfully.

**Spec:** `docs/superpowers/specs/2026-09-24-education-research-pipeline-design.md`

## Global Constraints

- Do not stop or modify the currently running `AUTO_20260924_215028_e9790d` batch.
- Do not alter the financial knowledge base or Pipeline.
- Preserve the user's existing uncommitted edits in `evidence.py`, `knowledge_package.py`, and tests.
- The canonical source-hash ID is `paper_identity(meta)[1]`, stored in `manifest.paper_id` and `handoff.stable_paper_id`; `meta['id']` remains `handoff.paper_id` and `handoff.document_id` for finance compatibility.
- Keep `paper-handoff/1.0`. Finance `CodeExec:PrepareAccounting` validates the legacy `AUTO_...` ID against filenames, so do not change that field or the finance Pipeline.

## Review Focus

- Existing finance packages with old `AUTO_` handoff IDs must remain readable and uploadable; the education trial's opt-in preflight rejects packages lacking or misreporting `stable_paper_id`.
- A package with no image projection must still validate.
- A package with no native chart or table must still validate.
- Repaired trial copies must preserve all source bytes and metadata resources, with new manifest and content hashes computed from the copy.
- The live batch may create more legacy packages after the code change; selection and repair must recheck each copied package immediately before upload.

---

### Task 1: Emit both legacy and stable identities in handoff blocks

**Files:**
- Modify: `test_evidence.py`
- Modify: `test_knowledge_package.py`
- Modify: `evidence.py`
- Modify: `knowledge_package.py`

**Interfaces:**
- Consumes: `knowledge_package.paper_identity(meta) -> tuple[str, str]`
- Produces: leading `ingest.md` handoff and `ragflow_assets.md` handoff with `stable_paper_id == manifest['paper_id']` and `paper_id == document_id == manifest['document_id']`.

- [ ] **Step 1: Write failing tests.** In `test_final_ingest_carries_only_safe_preprocessing_handoff_metadata`, assert the existing `paper_id == 'AUTO_0123456789abcdef'` and new `document_id` is the same; assert `stable_paper_id` equals the literal source-hash ID for that fixture. In `test_ingest_contains_machine_readable_chart_and_table_recipe_for_rag`, parse both handoff blocks and assert each `stable_paper_id == manifest['paper_id']` and each `paper_id == document_id == manifest['document_id']`.
- [ ] **Step 2: Run the two named tests and verify they fail because `stable_paper_id` and `document_id` are absent.**
- [ ] **Step 3: Change `ragflow_handoff_header` to add `stable_paper_id=paper_identity(meta)[1]` and `document_id=meta['id']`, preserving `paper_id=meta['id']`. Change `export_knowledge` to add the same fields using its existing `paper` variable.**
- [ ] **Step 4: Run the two named tests, then the `test_evidence.py` and `test_knowledge_package.py` suites.**

### Task 2: Add opt-in education identity preflight

**Files:**
- Modify: `test_knowledge_package.py`
- Modify: `knowledge_package.py`

**Interfaces:**
- Consumes: `knowledge_files(package)` and manifest inventory.
- Produces: `knowledge_files(package, require_stable_paper_id=True)` raises `ValueError` when leading `stable_paper_id` differs from `manifest.paper_id` or `paper_id`/`document_id` differs from `manifest.document_id`. The default `False` preserves finance behavior.

- [ ] **Step 1: Write a test that exports a valid sample, tampers only the leading `stable_paper_id` to a different well-formed ID, updates the manifest file hash and revision, and verifies opt-in `knowledge_files` rejects identity inconsistency while default `knowledge_files` still accepts it.** This exercises the semantic check rather than the checksum check.
- [ ] **Step 2: Run the named test and verify it fails because `knowledge_files` has no opt-in argument.**
- [ ] **Step 3: Parse the leading handoff block only when `require_stable_paper_id=True`, compare both IDs to the manifest, and raise a specific mismatch error. Do not treat a later asset copy of the handoff as the primary one.**
- [ ] **Step 4: Run the named test and all `test_knowledge_package.py` tests.**

### Task 3: Prepare corrected trial copies without changing the active run

**Files:**
- Create: `pilot_package_copy.py`
- Create: `test_pilot_package_copy.py`

**Interfaces:**
- Consumes: a completed, local package path and a new destination path.
- Produces: `prepare_pilot_copy(source: Path, destination: Path) -> dict` containing the rebuilt manifest; the source remains byte-identical.

- [ ] **Step 1: Write a test using `sample(tmp_path)` and `export_package` to create a package, then remove the new fields from its leading handoff to mimic the live legacy output. Record source hashes and call `prepare_pilot_copy`. Assert the source hashes do not change, both handoffs in the copy have the required dual IDs, opt-in `knowledge_files(copy)` succeeds, and the copy metadata `content_hashes` match the copied files.**
- [ ] **Step 2: Run the named test and verify failure because `prepare_pilot_copy` does not exist.**
- [ ] **Step 3: Copy only to a new destination, load copied `metadata.json`, run `evidence.export_package` on the copy, then validate it with opt-in `knowledge_files` and return its manifest. Reject existing destinations and incomplete or unapproved sources.**
- [ ] **Step 4: Run the named test, the identity tests, and then the full repository pytest suite. Report any unrelated baseline failures by test name.**

## Execution note

The active GUI process retains already loaded Python modules, so a source fix cannot be assumed to change packages it generates during this run. Prepare fresh corrected copies of the eight selected completed packages for the trial. A separate follow-up migration of the full run is only performed after the batch finishes and its live manifest is stable.
