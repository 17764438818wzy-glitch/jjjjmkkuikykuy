# Article year classification

Spec: one manuscript year per article, inferred with the existing dedicated WorkBuddy key; add year filtering and update the preextract MD for future articles. Do not add reasons, scores, or paragraph labels. Preserve article identity, existing research types, title overrides, source files, frozen requests and receipts.

The user confirmed +4 (2018 -> 2022) and that MD means the preextract template for future articles. Reuse the dedicated key. Missing or non-year-encoded student numbers remain unknown unless an original cover supplies an explicit completion year.

- [x] Add catalog/API year filter, year options and refresh retention tests; observe red.
- [x] Implement additive year projection/filter and retain a saved year for an unchanged original.
- [x] Add frontend year filter, reset/chip/pagination integration; build and inspect.
- [x] Infer years through the existing dedicated key, preserve terminal receipts and validate outputs before applying only article year.
- [x] Create a versioned preextract MD with the year rule/readable overview; retain its fixed JSON shape and old template bindings. The catalog owns the derived year, so future ingestion needs no shared preprocessing schema switch.
- [x] Fresh independent review and scoped deployment of the article service; verify live data and filtering.
