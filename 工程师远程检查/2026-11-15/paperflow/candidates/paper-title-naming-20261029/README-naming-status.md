# Naming and filename-title candidate

This directory remains a candidate workspace with real execution disabled in its
own config. On 2026-10-29 the reviewed naming and automatic-title modules were
installed in the live Prefect runtime, without replacing its config or data.
The previous module files and the exact installation manifest are retained in
`prefect-paperflow/runtime/installations/title-naming-20261029-174306-897169/`.

Fresh body input can be copied under a title-based name when the title is
confirmed or when an independently journaled generated title is explicitly
available. Fresh attachments receive the same title and an attachment sequence.
Originals, frozen receipts, existing material, previous model requests, and
already sealed delivery identities remain unchanged.

The local Nimble filename selector chooses an exact offered filename fragment.
Its output is an observation or an unconfirmed candidate; the confirmed body
title wins if they differ. A real local probe returned the expected fragments for
eight sampled filenames, but selected a major in a major-only negative case.
Therefore filename-only output is not treated as a verified paper title. The
candidate builder produced options for 85 of the scoped morning 100 filenames;
this is coverage, not measured accuracy.

`staged-corrections/manifest.json` lists six title corrections checked against
source evidence in the 310-group batch. They have not been placed into the live
data root. An older conversion is reused for one correction using source,
conversion and material hashes. Seventeen groups could not be reconfirmed by the
source audit; a generated title must not be represented as the original title.

Four of those seventeen have a separate, uniquely identified cover in the same
group. The cover title, student name and number, original body and cover hashes,
and frozen handoff were checked. Their independently staged corrections are in
`staged-separate-cover-corrections/manifest.json`; none is installed live.
One other PDF has a two-line first-page title now preserved as an unconfirmed
candidate in `wrapped-cover-candidate.json`. This leaves thirteen of the original
seventeen without a confirmed title; one of the thirteen has that PDF candidate.

At the user's instruction, the other twelve received one separate title-only
request each, based on their saved extracted body text (not their images). All
twelve responses completed and are marked `model_generated`. Journal records
live outside `papers/`; the resulting named copies and their manifest are under
`staged-generated-title-inputs/`. Existing requests, receipts and original files
were not changed. The installed executor can consume a completed generated title.
For a newly prepared paper without a title, it can issue one separately journaled
title-only request; existing uncertain or completed requests are not re-sent.

The deployment was restarted and Prefect check run
`13d10233-ea58-44b0-945a-a16dee232695` loaded the installed files. This is a
load/configuration check, not evidence of real LLM or RAG/NAS completion. No
completed groups were reclassified, model requests resent, or sealed RAG/NAS
receipts mutated during installation.
