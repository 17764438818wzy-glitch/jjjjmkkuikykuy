# Clean material and capacity planning implementation plan

> **For agentic workers:** Use superpowers:executing-plans with test-first changes; independent read-only review follows implementation.

**Goal:** Prepare a clean, source-bound body from a registered delivery and plan whole or split LLM input without sending requests.

**Architecture:** A candidate-only copy of the pinned native material module reuses the existing DOCX/PDF parsers, image handling, structure indexing, and capacity planner. Candidate adapters verify delivery identity, classify source format, enforce the agreed reading boundaries, and stage only verified derived files.

**Tech Stack:** Python 3.14, existing Paperflow modules, PyMuPDF, pytest.

**Spec:** User-approved design in the current chat: direct delivery paths; preserve originals; permit converter-owned temporary source copies; exclude pre-abstract, acknowledgements, references and body appendices; normalize blank/layout; plan segments after cleanup; no real LLM request or production deployment.

## Global constraints

- Do not copy the source or full delivery to a long-lived candidate output. Authorized conversion-only temporary source copies are allowed.
- Preserve source hashes, input identity and already bound requests; do not mutate historical work or call an LLM.
- All reading views, text, section/asset indices, formulas, and page evidence must enforce the same reading scope.
- Unknown boundaries or unverified visual content cannot pass as clean material. Do not discard tables, image-only research, or original whitespace values.
- No upload, Prefect sync, service change or production configuration change.

## Review focus

- Abstract title in a table of contents, missing/ambiguous abstract, same-block boundary and repeated headings.
- References/appendix/acknowledgements in different orders and research sentences containing these words.
- Blank text page containing an image/vector; multi-column and cross-page content; formulas, tables, and English spacing.
- Metadata, page images, orphan assets, and partition context revealing excluded text.
- Existing work, source drift, converter temp copies, capacity estimate, unsplittable objects and unknown requests.

## Tasks

### 1. Bound and preserve reading scope

**Files:** `reading_cleanup.py`, `tests/test_reading_cleanup.py`.

**Interface:** `clean_body_blocks(blocks) -> (kept, removed, decisions)`; block IDs and source positions survive; unknown critical boundaries raise `ScopeUncertain`.

- [ ] Write and run failing synthetic tests for clean sections, same-block abstract, heading ambiguity and reference retention in sentences.
- [ ] Implement source-aware scope filtering, conservative whitespace and explicit decisions; run those tests.

### 2. Build direct-source material from the registered handoff

**Files:** `desktop/preextract_material.py`, `direct_source.py`, `local_prepare.py`, `tests/test_direct_prepare.py`.

**Interfaces:** `prepare_registered(item, receipt_root, work_dir, catalog_path)` reuses `local_handoff.read_registered_item`; `prepare_source(source, expected_sha256, work_dir, catalog_path)` produces a cleaned `material.json` and validates source hashes.

- [ ] Write and run failing end-to-end tests using synthetic text/DOCX/PDF and registered paths.
- [ ] Reuse pinned native parsers and converters; eliminate long-lived raw copies and named input copies; dispatch by verified file format; reject unverified types.
- [ ] Apply cleanup before outline, formula and asset creation, and bind cleanup policy to all cache identities.
- [ ] Hold if source provenance, unsafe visuals or partition information cannot be verified; verify original hash after preparation.

### 3. Restore text continuity without fabricating content

**Files:** `reading_cleanup.py`, `tests/test_layout_cleanup.py`.

- [ ] Write and run failing tests for long blank runs, actual empty table cells, Chinese/English wrapped lines, cross-page paragraphs, headings, images and uncertain column order.
- [ ] Implement conservative join/mapping and blank handling; record nontrivial transformations; retain uncertain material unchanged.

### 4. Reuse capacity and durable partition planning

**Files:** `local_prepare.py`, `tests/test_capacity_plan.py`.

**Interface:** `plan_clean_material(material, template, catalog, config, work_dir)` returns a checked plan without invoking any model.

- [ ] Write and run failing tests for single request, multi-part input, continuous exact primary coverage, image/formula ownership, unsplittable material, no excluded content in a request and no HTTP call.
- [ ] Reuse existing image adaptation and `plan_requests`; record estimate basis, frozen input identity and policy; never overwrite existing requests or infer model capacity from config.

### 5. Independent review and verification

- [ ] Run all focused synthetic tests, inspect output and request projections, verify no duplicate raw originals, then request independent read-only review.
- [ ] Correct confirmed findings and rerun affected checks; record known limitations and unverified production integration.
- [ ] Maintain this plan and a concise progress ledger; no production service or data changes.
