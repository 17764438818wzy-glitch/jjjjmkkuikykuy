# Continue local processing despite localized problems

> **For agentic workers:** Use superpowers:executing-plans with test-first changes and an independent read-only review.

**Goal:** Implement the user-approved recommendations 1–37 and 39–46; item 38 is deferred. Localized ambiguity or failed OCR must not stop other locally processable content for the same paper.
**Architecture:** Extend the existing candidate. Preserve bound source and upstream identities, retain localized uncertainty and private provenance, reuse the parsers, CPU RapidOCR and capacity planner, and never expose unverified excluded content. An incomplete artifact must be explicitly labeled and cannot claim full-paper completion.
**Tech Stack:** Python 3.14, pytest, PyMuPDF, Pillow, existing Paperflow modules, RapidOCR.
**Spec:** User approval in this chat of the prior 46-rule table except item 38; prior approved file `2026-10-30-relaxed-material.md` and user instruction that small local problems must not stop the rest of a paper.

## Global constraints
- Operate in this isolated candidate; preserve upstream originals, receipts, hashes and historic artifacts. No whole-source or whole-delivery copy; conversion-only temporary copies allowed.
- Do not implement item 38, attachment-content downstream integration; do not send LLM requests, deploy, restart services, or update Prefect.
- Recognized cover, front matter, bibliography, acknowledgements and thesis appendices stay outside request material. Standalone attachments remain independent.
- Pending image or boundary content cannot be automatically forwarded as confirmed research. No manufactured review approval; unresolved issues and omitted content carry location and suggested next action.
- Critical unavailable source, inconsistent identity, and changed source must not be accepted as the paper. A batch must isolate these failures from other papers.
- Existing work and unknown external requests must not be overwritten or blindly resent. Deterministic local retry may use a new attempt directory.

## Review focus
- Front TOC entries and one-line abstract, or absent abstract and headings, do not disclose front matter or stop subsequent readable body.
- OCR failure/rotation/blank and figure-only pages do not abort other pages; ambiguous pixels are withheld, not silently discarded as complete.
- Boundary-page mixed columns, missing images, footnotes, inline images and equations retain location, private evidence, and pending actions without leaking excluded content.
- A clearly located normal image and isolated tail page do not trigger blanket whole-paper review; ambiguous images remain held.
- PDF tables and cross-page relationships are preserved with usable text/visual evidence and explicit limits; attachment previews remain independent and bound.
- Capacity respects real model limits, image readability and coverage; incomplete plans cannot claim whole-paper completion, and the candidate does not send requests.

## Tasks
### 1. Localized scope, warnings and recovery
**Files:** `reading_cleanup.py`, `review_requirements.py`, `local_prepare.py`, `desktop/preextract_material.py`, `tests/test_reading_cleanup.py`, `tests/test_direct_prepare.py`, `tests/test_capacity_plan.py`.
- [ ] Add behavioral tests for frontend TOC, short inline and missing abstracts, repeated markers, mixed table boundaries and missing notes; preserve clear body and location.
- [ ] Run the tests to establish failures, implement bounded recovery with explicit issues and unresolved local gaps, and rerun.
- [ ] Add behavioral tests separating harmless warnings, withheld content and hard identity checks; implement explicit incomplete readiness without fabricated approval.
- [ ] Implement recoverable local retry via new attempt directory, never overwrite source/request histories.

### 2. OCR, visual boundaries and tables
**Files:** `pdf_visual.py`, `visual_ocr.py`, `word_visual.py`, `desktop/preextract_material.py`, `review_requirements.py`, `tests/test_direct_prepare.py`.
- [ ] Add tests where blank/rotated/OCR-failed page and native visual gap do not stop adjacent text; keep doubtful images private and provide a warning.
- [ ] Add tests proving normal body graphics and unambiguous reference pages need no blanket review, while mixed-scope pixels remain held.
- [ ] Recover PDF tables using existing PyMuPDF recognition and bounded page crops when safe; preserve normal text and mark structural limits.
- [ ] Add tests for inline-image text preservation, equation/Word visual fallback and documented page mapping.

### 3. Whitespace, attachments and capacity
**Files:** `reading_cleanup.py`, `attachment_preview.py`, `desktop/preextract_images.py`, `local_prepare.py`, existing upstream partition/request modules through candidate adapters, relevant tests.
- [ ] Improve supported whitespace/margins and cross-page continuity without combining separate paragraphs.
- [ ] Make bounded attachment previews configurable, keep independent identity and mark truncation; defer item 38.
- [ ] Allow available capacity evidence and tokenizer to drive planning; preserve coverage, true image limits, local warnings and incomplete status.
- [ ] Handle larger images/tables and configurable segmentation within validated constraints, with behavioral tests.

### 4. Acceptance
- [ ] Run the full existing and new test suite. Document each changed expectation and any genuine remaining failure.
- [ ] Reuse the authorized selected real papers in a new isolated work directory, preserve source/receipt hashes, and inspect text, sample tables, images, pending states and results before/after. No model calls.
- [ ] Request independent read-only final review and resolve material findings. Report measured outcomes and limits; do not claim production integration or 80% accuracy without representative evidence.
