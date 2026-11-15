# Generated Title Prefect Integration Plan

> **For agentic workers:** Use superpowers:executing-plans and verify each step before installation.

**Goal:** Install the approved title-based naming into the production Prefect deployment and automatically make one separate generated-title request for a new body that has no known title.

**Architecture:** Reuse the existing native material preparation, Prefect body/total LLM concurrency, request journal, named-delivery copy and generated-title records. Generate only for new unprofiled bodies; reuse any saved generated result and preserve all older requests and sealed deliveries. A title-generation failure is recorded and does not stop body or attachment work. Install only after confirming the existing runner and deployment are idle, and verify the actual loaded source identity from a Prefect check flow.

**Tech Stack:** Python, Prefect 3, native paperflow request and receipt journals, filelock.

**Spec:** User approved a title-only request based on the paper's text, no evidence in the response, then authorized installing it in the production program and Prefect. The twelve already completed requests must not be repeated.

## Constraints

- Preserve originals, all unknown model outcomes and prior RAG/NAS receipt identities.
- No automatic historical replay or additional requests for bodies with an existing profile request.
- Use one durable identity per paper, body file and content hash, and shared concurrency limits.
- Preserve production config and its paths; never copy the candidate's disabled config.
- Respect the user's pause; a check flow is allowed but must not process papers.

## Tasks

1. Test that unnamed fresh bodies generate once, known or previously requested bodies do not, and errors do not block processing.
2. Connect the existing title-only journal to a Prefect task with shared model quota, name the new body and attachment inputs and preserve generated-title origin.
3. Test resume/idempotency, naming, existing batches and actual native saved-response verification.
4. Independently review, install only verified files, restart the idle runner, synchronize the same deployment, and verify the loaded identity with a no-paper Prefect check.
