# RAG lock reduction

The user wants unnecessary locks in the reusable RAG processing code removed, with Prefect responsible for work scheduling and shared RAG concurrency. Work is isolated because the existing preprocessing worker checks hashes of the shared Python package while running.

An independent review found that Prefect 3.9 can skip an inactive global limit even in strict mode, and cancelled slot waiters can remain queued. Therefore the existing batch-local RAG admission remains as a bounded safety fallback until equivalent global enforcement and waiter cancellation are established. Preserve Prefect's strict global concurrency limit, cancellation draining, empty-attachment behavior, existing records and remote identities.

Allow read-only reuse of a previously verified attachment folder without acquiring its creation lock, after validating the saved binding and reading back exactly one folder with the same ID. First creation, unknown outcomes, and existing unverified receipts must continue through the existing creation lock and intent reconciliation.

Do not weaken the per-attachment ingest lock, file or paper ownership, local output consistency, RAGFlow database consistency, or unknown remote-write protections. No actual paper, RAG, LLM, NAS, or production scheduling requests are permitted. The new RAG connection is outside this change.
