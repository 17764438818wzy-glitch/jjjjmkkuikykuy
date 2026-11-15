# Relaxed local material candidate implementation plan

> Historical plan: the subsequent `2026-10-30-continue-partial.md` and the user's
> instruction to continue usable material supersede this document wherever it
> calls for the whole offline planner to stop on local pending items.

> Execution: inline implementation, meaningful regression tests and an independent read-only review.

**Goal:** Improve local preparation of registered thesis materials without changing originals or silently forwarding excluded/uncertain content.
**Spec:** The ten recommendations approved in this chat; body and attachment preparation remain independent; unresolved partial LLM delivery is not authorized.
**Architecture:** Reuse the existing native parser, receipt binding, CPU RapidOCR and capacity planner. Isolated candidate only; no model requests, deployment, service restart, or batch processing. Preserve source identity, derivative hashes and prior work.

## Tasks
- [x] Extend scope/TOC boundary detection with multi-signal tests, retaining review for ambiguous input.
- [x] Replace blanket PDF visual blocking with page/region-aware extraction and CPU OCR fallback; standalone image bodies use transient in-memory page coordinates.
- [x] Preserve supported Word visual evidence as unverified crops; record unresolved geometry without losing original material.
- [x] Rejoin high-confidence page continuations with exact multi-source mappings; preserve uncertain boundaries and tables.
- [x] Enhance independently registered attachment previews with format-specific bounded readers, identities and evidence.
- [x] Adapt images and plan partitions using supplied channel limits; refuse unknown limits and overly small renditions.
- [x] Build a material-hash-bound review checklist and block the offline model planner until all pending decisions are recorded; no production sender is connected here.
- [x] Validate five selected registered real papers on candidate /15, compare source and receipt hashes, record outcomes and complete the independent read-only review.

## Critical safeguards
- Never infer that OCR is complete from confidence alone; do not forward a page/visual that crosses an unverified exclusion boundary.
- Unresolved content must not be silently marked complete or sent to a model; independent attachments and other preparations may proceed.
- A pending or failed boundary may still prevent a complete body material from being produced. A blank review template is not a review, and no review UI or reviewer authentication is installed.
- Neither local OCR nor offline request planning constitutes a downstream LLM call or end-to-end proof.
- Actual channel limits and image readability, review workflow, integration gates, and production acceptance remain to be established separately.
