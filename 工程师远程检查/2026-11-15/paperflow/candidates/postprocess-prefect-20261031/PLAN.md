# Prefect Folder Postprocessing Implementation Plan

> 2026-11-01 update: the user subsequently authorized checking and connecting
> the live frontend, holding RAG, opening the postprocessing stage and running
> real postprocessing to record issues. The earlier staging-only restriction
> below is historical. This authorization does not include resending uncertain
> model requests, uploading to RAG, NAS operations or deleting originals.

**Goal:** Process one existing folder ticket's verified body and attachment LLM responses into validated handoff artifacts and record its waiting-for-RAG state, using Prefect for scheduling and resource limits.

**Architecture:** Separate deterministic body and attachment adapters from a folder-level coordinator. Preserve the upstream receipt, existing material, exact raw responses, request identities, source hashes and ticket identities. Bind to the new LLM handoff only after its real contract is ready; keep the candidate isolated and all automatic business execution disabled in the meantime.

**Tech Stack:** Python, existing Paperflow business scripts and local tokenizer, Prefect 3.9 process/task scheduling, SQLite only where required for durable local state and atomic transition.

**Postprocessing concurrency:** 8, explicitly specified by the user. This is a separate Prefect-managed processing limit and does not change any LLM request limits.

**Later user instruction:** Prepare the mainline connection and local verification only; do not connect it to Prefect until the user explicitly instructs. Before that instruction, the separate `paperflow-folder-postprocessing-v2` limit was created and read back as 8 with zero occupied slots. It does not dispatch work. See `接线状态.md` for the current state and remaining boundaries.

**Spec:** The user-approved previous chat design: one large task contains folder-grain small orders; LLM ends at saved complete raw answers; local postprocessing parses, validates, redacts, chunks, packages and awaits RAG. Reuse scripts rather than former controller/wiring. No new model request or RAG upload is authorized.

## Global constraints

- Never run live papers, LLM, RAG, resume paused tasks, or alter other active task's files while upstream is being built.
- Do not rerun pre-processing or mutate originals, upstream deliveries, previous response or package evidence.
- Missing, conflicting, interrupted, unverified or unknown input never advances the order and never triggers automatic model resend.
- Body and all attachment results belong to the same existing folder ticket, with exact identities and source hashes bound to each result.
- Single-body and partitioned-body replies must be handled with no additional model request; preserve cross-part uncertainty and do not falsely claim semantic whole-paper synthesis.
- Do not reuse legacy global desktop orchestration, model dispatch, upload workers or task-wide serialization. Cross-process collision and atomic recording require explicit mechanisms; Prefect status alone is not mutual exclusion.
- Do not treat transient READY.json alone as completed handoff. File/version integrity, final checks and persisted stage receipt are required.
- Existing source classification, attachment restrictions and learning/best_effort redaction caveats remain explicit.

## Review focus and required cases

- Changed raw answer, model request, image, source or receipt: reject without replacing previous evidence.
- Missing part, wrong parent/folder/attachment identity or unknown request outcome: do not process or advance.
- Two workers claiming same output, pause, deletion or crash between seal and receipt: no duplicate publication or unsafe admission.
- Incomplete professional routing or tokenizer resources: retain actionable diagnosis rather than report waiting for RAG.
- Partially complete folders and unavailable observer inputs: preserve limitations and show unknown rather than false zero.

## Tasks

### 1. Body adapter

- [ ] Write and run failing focused synthetic tests for complete single and multi-part saved replies, errors, conflicting provenance and no model dispatch.
- [ ] Reuse the existing parser, source/image reference validation, candidate validation and publication functions; persist diagnostics and preserve partial results.
- [ ] Verify generated complete package, hashes, exact scope and explicit multi-part limitations; reject pre-existing unregistered targets.

### 2. Attachment adapter

- [ ] Write and run failing tests covering saved raw result, incorrect evidence references, source changes and immutable duplicate output.
- [ ] Reuse parser, structured description validation, retained evidence and attachment-card renderer; bind MD/original identities and version.
- [ ] Verify per-attachment independence, no model call, no default named copies and no unsupported claim of automatic privacy redaction.

### 3. Folder coordination and Prefect

- [ ] Write failing tests for same-ticket atomic claim, pause/delete, failure, duplicate/concurrent execution, checkpoint/recovery and folder-level counts.
- [ ] Implement a candidate coordinator whose persisted state is keyed to upstream identity and whose processing lease is controlled by Prefect. Never run automatically while upstream is unavailable.
- [ ] Integrate body and attachment adapters; only promote after all required evidence and final outputs pass verification. Keep existing archive and unrelated prior workflows untouched.
- [ ] Integrate UI metrics through a trustworthy folder-stage observer; report unavailable/unknown as null.

### 4. Integration, independent review and readiness

- [ ] Exercise the actual business adapters with synthetic data and temporary Prefect/control services, without network to models or RAG.
- [ ] Independently review lock scope, error/unknown handling, evidence integrity and preserved pause semantics; correct findings and run focused regression checks.
- [ ] Reinspect the upstream task and actual service state. Only align an operational handoff after the upstream contract is stable and its process/code state is verified. Do not resume real requests.
