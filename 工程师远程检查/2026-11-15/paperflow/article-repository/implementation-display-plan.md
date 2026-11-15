# Article warehouse display and filters implementation plan

> For agentic workers: use superpowers:executing-plans for inline implementation and an independent final review. User approved prototype A and explicitly authorized implementation in this chat.

**Goal:** Apply the approved list layout to the existing 8098 warehouse, with complete server-side combined filtering and richer existing profile details.

**Architecture:** Reuse the current Vue 3/Ant Design frontend, FastAPI catalog, PostgreSQL, Qdrant read adapters and NAS downloads. Add a small display/filter projection; do not regenerate profiles or modify the processing chain. Implement in this isolated directory because the source directory is not a Git repository.

**Spec:** `../../outputs/warehouse-prototype/README.txt` and approved prototype A. This production page has no sample data or variant switcher.

## Global constraints

- Professional codes are exact, existing memberships; one source folder remains one article.
- Level choices: 函授参考 / 本科参考 / 重点本科参考 / 层级待明确. Explicit 重点本科（参照） is included by default; reasons alone never add a level.
- Show explicit primary_label only. Preserve original labels, reasons and manual overrides; never fill in a grade.
- Available original, available attachment and available attachment format are separate conditions. Unknown remains unknown.
- Search title, summary, keywords, methods and reusable points; preserve title-only manual corrections and suppress obsolete manually overwritten text.
- Filtering and count happen before pagination, on the server, with parameterized SQL.
- Keep existing edit, synchronization and download capabilities. No synthetic LLM calls, reimport, reprocessing or broad service restart. One additive generated catalog search column is allowed based on the measured performance gap below.
- Deploy only the warehouse service after verification, with a source/image backup and an exact rollback command.

## Review focus

- A matching item beyond the original page boundary must be returned on filtered page one.
- Missing PDF plus available ZIP must not satisfy the PDF attachment filter.
- Manual reference labels/search edits override stale generated fields.
- Historical summary-only catalog profiles must retain their uncertainty; read full Qdrant profiles through the existing adapter when available.
- Slow, out-of-order list/detail reads must not replace the newest selection or clear the selected filters.

## Task 1: Query and display contract

Files: `article_repository/profiles.py`, `catalog.py`, `api.py`, `sync.py`, new `filters.py`; tests in `tests/test_filters.py`, `test_catalog.py`, `test_api.py`, `test_sync.py`.

- [x] Run existing relevant tests against a local isolated PostgreSQL cluster.
- [x] Add failing tests for explicit reference inclusion, unknown/missing files, override precedence, keyword search and filtering before pagination.
- [x] Add shared level patterns and lightweight file/reference summaries; SQL filters use the same declared patterns.
- [x] Expose whitelisted process/reuse/gap projections in detail. Reuse Qdrant source-manifest reading for legacy detail enrichment, without mutating raw profiles.
- [x] Verify tests and measure real read-only query timing on current catalog data.

## Task 2: Approved frontend A

Files: `frontend/ArticleRepository.vue`, `PaperProfile.vue`, `ArchiveCard.vue`, shared small `Icon.vue`, `warehouse.css` and an optional detail component if needed.

- [x] Retain request cancellation/sequence guards, selected filters, editing and sync controls.
- [x] Use the approved blue/white shell, search/profession/file toolbar, reference tabs, attachment formats, active chips and paginated table.
- [x] Add profile overview, research process, reuse/limits and real file download tabs; preserve historical empty states.
- [x] Build using existing Vue/Vite/Ant Design dependencies; browser-check the candidate against real read-only catalog data with imports disabled.

## Task 3: Review, deployment and acceptance

- [x] Independent review of actual diff, tests and representative workflow evidence.
- [x] Reconfirm deployed baseline hashes; preserve warehouse source and image rollback artifacts.
- [x] Upload only changed source and built frontend, rebuild/update only repository with no dependencies.
- [x] Verify live 8098 in browser and read APIs: filters, actual total, reference example, full detail, original/attachment availability and unchanged download route.
- [x] Report the live URL, verified behavior and any material limitation.

## Execution record

- Baseline: local API/catalog/profiles hashes equal running container. At inspection: 7,931 articles, 3,322 structured profiles and 4,586 historical flat profiles; background imports continue, so counts may rise.
- Independent reuse review completed; server-side pre-pagination filtering and legacy detail enrichment are necessary gaps.
- User approval covers implementation; no additional design approval gate is needed.
- Performance decision: live read-only keyword+level search took 8.779 seconds because it repeatedly decompressed large profile JSON. Add a PostgreSQL generated `search_text` column using the same effective field rules. New imports and manual edits update it automatically; no raw profile mutation or second indexing service is involved. Schema backup and post-deployment timing are required. Unclassified level matching now splits labels once per article instead of three times.
- Final independent review: four P2 findings reproduced and fixed with failing-then-passing tests: decoded multiline labels, explicit empty manual fields (learn/path/audience), historical research_process.overall_path, source empty summary/keyword fallback in generated search text. Full suite: 51 passed, one pre-existing dependency deprecation warning. Frontend build passes; candidate browser verified list-only layout, reference detail, six actual learning units and two actual file download links.

- Live acceptance: 7,945 articles at verification; explicit English/reference example, 7 route steps/6 units, combined file filters, real 124,041-byte PDF download, empty state, page-two DOM/API match and 390 px viewport without page overflow all verified. Real keyword+level query 1.650 s versus 8.779 s before. Local source synchronized; isolated preview/database helpers stopped after verification.
