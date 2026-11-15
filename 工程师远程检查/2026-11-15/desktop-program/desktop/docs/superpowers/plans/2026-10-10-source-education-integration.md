# Source education integration implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for the preprocessing implementation and independent review. The user directly authorized implementation and delegation on 2026-10-10; no additional design approval is required.

**Goal:** Carry independently verifiable original education metadata through preprocessing, RAG, retrieval, and the actual 18291 Agent.

**Architecture:** Preserve the current preprocessing pipeline and immutable prepared checkpoints. Add deterministic source-only extraction, then carry the same object and canonical revision through existing identity, metadata and handoff boundaries. The existing RAG implementation chat owns its candidate and deployment.

**Tech Stack:** Python 3.14, existing DOCX compatibility parser/PyMuPDF, canonical JSON, existing RAG TypeScript/JavaScript adapter.

**Spec:** E:/uv/ragflow_upstream/docs/contracts/source-education-v1/contract.md and source-education.schema.json (frozen).

## Global Constraints

- Freeze source-education/1.0, source-education-extract/1.0, adult-to-correspondence/1.0; do not expand fields or aliases.
- Preserve original education, original study form, and business classification independently. Adult education maps to correspondence only as business_rule.
- Accept original-file identity evidence only; no inference from filenames, directories, advisor degrees, research participants, or body topics.
- Keep minimal privacy-safe exact source excerpts, original file SHA, evidence file SHA, and original PDF/Word/text locators. Selected DOCX compatibility branches retain indices counted before selection.
- Preserve existing dirty work. Develop/test in a snapshot of the current source, not the old Git HEAD. Apply only reviewed task files with before-hash comparison.
- Preserve historical audit packages/checkpoints, document/chunk IDs, text source_version and unaffected quality receipts. Never regenerate all profiles for metadata alone.
- No paid batch, full-library update, or app restart until the corresponding acceptance gate; 8091 unchanged. Current separate 18-paper human pause remains effective until reconciled.
- No secrets or original personal identifiers in logs, reports, source evidence or commits.

## Review Focus

- Cover degree plus advisor degree must retain the paper degree without an invented conflict.
- Adjacent table cells, empty paragraphs and alternate DOCX branches must not change original locator numbering.
- Adult-only covers keep degree unknown; body discussion of correspondence cannot establish study form.
- Retry of older checkpoints adds source metadata in a new checkpoint while the previous checkpoint bytes remain unchanged.
- Metadata-only changes invalidate relevant selection/cache revisions without changing body or quality evidence.

### Task 1: Preprocessing extraction and persistence

**Files:** Create source_education.py, test_source_education.py, test_source_education_pipeline.py; modify auto_pipeline.py, ai_resume.py, evidence.py, knowledge_package.py only where necessary.

**Interfaces:** source_education.extract_source_education(source, *, original_source=None, source_relation='original', derivation_method=None, derivation_record_id=None) -> dict; source_education.source_education_revision(payload) -> str; source_education.source_education_fields(payload) -> {'source_education': payload, 'source_education_revision': str}. The source argument names the actual evidence file; original_source supplies the binary input identity for converted/normalized cases. Follow frozen contract validation without adding producer verification fields.

- [ ] Add tests for real-format fixture files covering bachelor, adult bachelor, correspondence bachelor, adult-only, unknown, conflicting source labels, advisor/participant/body exclusions, minimal safe quotes, original paragraph/page/line locators, normalized/converted identities and deterministic revisions.
- [ ] Run the tests and record the expected missing-feature failure before implementation.
- [ ] Implement deterministic extraction using the existing DOCX compatibility selector and native PDF reader. Preserve unknown rather than guess. Do not duplicate full evidence/graphics processing or change classification and quality rules.
- [ ] Wire source metadata before cover cleaning into new tasks and retries, prepared identity/meta, immutable checkpoints, final item and both handoff outputs. Old checkpoint migration must not require a native rules bump or re-extract all figures.
- [ ] Verify fresh run, retry, stale native parser checkpoint, conversion, and packaging consistency. Source conflicts alone must not reject a paper. Retain old checkpoint bytes.
- [ ] Run targeted existing classification/checkpoint/package/privacy regressions and both frozen contract checkers. Independent reviewer inspects diff and counterexamples; fix substantiated findings before application.

### Task 2: Real source evidence and local application

**Files:** Task diagnostic directory only for original-source receipts, selection inventory and real handoff artifacts; approved source files applied to E:/uv/ragflow_upstream with backup and compare-before-write.

**Interfaces:** Actual source binary path stays in private local receipt; share sanitized object/revision, known task/paper identity, hashes and replayable locator proof with the RAG owner. A test conflict is explicitly labeled synthetic outside the frozen payload.

- [ ] Find actual PDF and Word sources; confirm original identity mapping. Select real bachelor, unknown and study-form samples where available. Do not call a synthetic fixture real.
- [ ] Reopen actual evidence files, verify each quote at its locator and hashes, produce new knowledge packages/checkpoints and rerun from a saved checkpoint. Use bounded existing preprocessing machinery; no full corpus retry.
- [ ] Apply reviewed changed files after ensuring original before-hashes still match. Back up originals and record task hashes and tests; do not replace unrelated files.
- [ ] Provide real artifact receipts to the existing RAG implementation thread for end-to-end validation.

### Task 3: RAG and actual 18291 integration

**Owner:** Existing chat 讨论财会文章学历分级 (2), thread 01a12118-53ea-7e01-a555-d35d6f3158b9. Root coordinates and checks delivered evidence; no concurrent edits in its candidate.

- [ ] Implement frozen schema, receiver validation/trusted original-file binding, metadata writes and rereads, profile compatibility, ordinary and located retrieval, metadata caches and saved selection revision handling.
- [ ] Validate real PDF/Word artifacts through RAG to actual 18291 ordinary-user Agent calls. Include unknown and explicit conflict fixture handling. Preserve independent quality evidence.
- [ ] Back up and pilot historical metadata-only updates; compare original/document/chunk IDs, body source_version, quality receipts and affected caches before and after.
- [ ] Incorporate acceptance into the original 18-paper regression once the direct human pause is reconciled. Only after chain/regression acceptance proceed with full-library source metadata backfill; do not regenerate valid profiles.

## Handoff and completion evidence

Record commands, receipts and counts in this plan's progress ledger. Report stages separately: implemented, applied, local verified, RAG read back, ordinary/located retrieval verified, actual Agent verified, historical pilot, 18-paper regression, full backfill. Missing real-source categories or paused work are outstanding, never passing by assumption.
