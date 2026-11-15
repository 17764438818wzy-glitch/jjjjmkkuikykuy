# Existing preextract chunk consolidation

User authorization: change the existing postprocessing flow, group chapter descriptions through level two, deduplicate chart descriptions, preserve full learning content and sources, keep required images, and compare four existing results without model calls or uploads. Do not change articles already being delivered.

## Scope and reuse

- Change only the publication generator and its regression tests. Reuse `_Budget.split`, existing source/metadata structures, image decisions and image processing, exact offline token counting, package sealing, validation and `RagPublisher.records`.
- Leave the shared Paperflow uploader, package reader, model scheduling, delivery queues and live configuration unchanged.
- Use actual `parent_id` paths. Keep the complete Tree and every node's fields; group descendants under their level-two ancestor. Missing/ambiguous hierarchy remains conservative rather than guessing from titles.
- Use the existing `asset_<id>` record for a chart's first text segment; generate chart continuations only when needed by the same token budget. Preserve linked limits and all source fields. Keep distinct descriptions for identical image pixels even though the uploader deduplicates those pixels.
- Keep learning units, source body, source map, other learning projections, asset storage decisions and retained image bytes unchanged. Do not reinterpret image choices without the model.
- Bump the publication version. Existing binding/receipt ownership continues to pin the old package and manifest.

## Verification and protected data

1. Snapshot source code and hashes of the four original materials, model results and sealed packages, plus other currently sealed trial packages.
2. Write failing tests for level-two grouping, complete node reconstruction and limits, short/long chart handling, duplicate image descriptions and legacy package mapping.
3. Make the minimum generator changes; run relevant publication, images, pipeline/cache, handoff and delivery regressions with external calls blocked by their existing fixtures.
4. Read candidate/material/validation directly for papers 14, 4, 2 and 7. Invoke `prepare_publication` into a fresh comparison directory; never invoke `run_single` on their existing work directories.
5. Use the existing offline exact tokenizer only. Block network calls during repacking. Compare complete metadata, learning unit text, body and source map, asset meaning/source/decision fields, retained image hashes, record counts and token counts. Verify old protected files still match.
6. Read back the existing remote profiles using GET document queries only; compare with the original sealed metadata. Do not upload the new comparison packages.
7. Obtain a read-only independent review of code and evidence. Report measured reduction and limitations; keep comparison packages out of the live queue.

## Decisions

- No separate UI or processing pipeline is introduced. Temporary comparison outputs do not replace live packages.
- A fresh package gets its own identity. Equality checks ignore only that expected top-level task identity, not article facts or learning fields.
- Retaining data and sources does not mean repeating the same derived text in multiple vector records. Any reduction must be attributable to grouping or duplicate removal, never omitted learning content.

## Progress

- Initial reuse review completed. Shared upload behavior and existing delivery ownership protection can remain unchanged.
- Existing four-paper baseline: 612 records per set of specialty destinations (74, 99, 184, 255 per individual specialty). Static reduction estimate is about 32%; actual counts require repacking and capacity checks.

## Approved formal integration, 2026-10-21

The user explicitly approved integrating the demonstrated merge into the original workflow, retaining profiles, complete learning process, full Tree and sources, reusing completed extraction and keeping ingestion stopped. This supersedes the earlier comparison-only boundary for code integration; it does not authorize ingestion, model reruns or changes to existing remote receipts.

- New publications continue through the existing `prepare_publication` level-two chapter grouping and chart/asset text reuse. No uploader, specialty routing, model prompt or first extraction changes.
- Existing learning/best-effort sealed publications upgrade through `preextract_reorganize.consolidate_publication`, extracted from the accepted four-paper trial. It copies body/source-map/images verbatim, freezes body/learning/research/continuity rows, preserves metadata and original projection context, rebuilds only derived chapter/chart organization and the normal structure plan, then seals and checks actual rendered token budgets.
- `run_single` validates the original candidate and seal before considering an upgrade. Bound/receipted or durably registered publications keep their old prepared pointer/task/manifest. An unregistered old publication receives a new directory and task; the old package and archived prepared metadata remain available. Changed privacy/admission policies still use their original policy-upgrade branch rather than treating reorganization as redaction.
- Queue ownership is read without creating a queue. Registration and packaging share the existing per-work `operation.lock`, closing the interval between prepared-pointer reads and durable registration. Unknown queue state stops replacement rather than replaying a possible write.
- All implementation verification is offline. The current four real packages again measure 612 to 425 records; 38 learning units, 91 learning rows and eight image files are preserved. The separate Agent-learning report remains the semantic evidence; no additional model learning run is required here.
- Evidence and before snapshots: `data/private/chunk-consolidation-integration-20261021/`. Existing old pointers, bindings and receipts are not migrated by the verification script. The original application window is not restarted and ingestion is not resumed.
- Another chat concurrently added the short registry-read lock in `default_work_dir`; retain that change. Rollback must target only the packaging/cache and queue-registration hunks from this integration, not overwrite the whole pipeline file with the earlier snapshot.
