# Year classification ledger — plan: docs/year-plan.md

Baseline: previous search feature is committed and deployed; 12,810 article records; six existing research types fully populated. Read-only year inventory saved in work/year-inventory.json outside the candidate repository.

Clarification: MD means the preextract template for future ingestion. Use the existing dedicated key. Confirmed +4, 2018 -> 2022.

Ruling: expose only a manuscript year string or null per article. Keep student numbers and evidence in private task receipts, not the public article label. The filter implementation is independent of the pending arithmetic choice.

Task 1: RED observed (9 expected failures); implementation GREEN (56 catalog/API/filter/file tests). Future import inference RED observed, then GREEN; added evidence cases await final run.

Ruling: year is article-directory metadata, so future new records derive it once at registration from the source names already retained. The versioned MD documents the same rule and shows a single year in its readable overview; its JSON shape is unchanged. This supports future ingestion without changing the shared preprocessing collector or frozen MD references. Existing null years are never silently inferred on refresh.

Model run: all 12,810 articles processed with the existing key and terminal stop receipts. One upstream 403 is a recorded failed refusal; only that known refusal was reattempted. A terminal result with numeric years was normalized to exact decimal strings without resending. One arithmetic mismatch (21 -> 2022) was rechecked by the same key and corrected to 2025. A non-year-encoded 1106059 was checked against its hashed original DOCX cover, which explicitly gives June 2023; the same key verified 2023.

Independent review: two Important future-inference cases reproduced RED and fixed: four-digit student years before 2010 and compact current-date timestamps. Added actual corpus encoding cases for short student IDs and Baoding YY-prefixed IDs; these are source-format rules, not article-specific overrides. Final relevant regression: 65 passed in 39.80 seconds; frontend build passed.

Deployment: only the independent repository service replaced; current final image and stable tag match. All eight canonical changed files and six live backend files match the tested candidate. Initial health check used loopback despite the service binding the LAN address; actual LAN health reconciled successfully without replaying deployment. A parser-fix image build using a raw SHA was rejected as a registry lookup; no service mutation happened before that build failure, and the build was corrected to a pinned local base tag.

Task 4: complete — 12,664 year fields written with revision/source/base/override CAS; 146 unknowns preserved. Readback of all 12,810 rows proves every other base/profile field, title override and update time unchanged. Receipt and narrow rollback backup saved locally and privately on the server.

Task 5: complete — v1.3 MD saved in outputs and program resources; JSON skeleton identical to v1.2; v1/v1.1/v1.2 byte hashes preserved. Shared preprocessor code/config/runtime and frozen template references unchanged. Future fresh catalog registrations supply a year from clear source student numbers; explicit unknowns and existing unknowns on refresh remain unknown.

Task 6: complete — live API verified all eight years plus unknown, all six original category counts, unchanged totals 12,810 / bachelor 8,128 / attachments 606, combined 2022+engineering=112, invalid-year rejection. Browser verified year/category combination, reset, year badges, and mobile width 390 without document overflow. Browser task space finished. Screenshot capture on the final desktop page timed out; DOM verification completed and mobile screenshot was visually inspected.
