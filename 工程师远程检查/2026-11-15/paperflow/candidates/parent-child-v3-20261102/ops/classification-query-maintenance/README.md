# Classification query maintenance

Applied to the existing Prefect 3.9 / PostgreSQL 18.6 database after isolated validation on 2026-11-03. The application and live workers use these indexes immediately; no restart, queue replacement, concurrency change, or new business dispatch is required.

## Online indexes

`query_indexes.py` adds only:

```sql
CREATE INDEX CONCURRENTLY ix_pfv3_artifact_key_trgm
ON public.artifact USING gin (key gin_trgm_ops);
CREATE INDEX CONCURRENTLY ix_pfv3_collection_flow_type
ON public.artifact_collection USING btree (flow_run_id, type);
```

The existing `pg_trgm` extension is reused. Source-history filters, cross-task unknown protection, pagination, and the Prefect ArtifactCollection latest-by-key contract are unchanged. The file is a server-side operator script for the verified Docker container; it is not imported by the running application. `preflight` is read-only. `measure --options <json>` performs only SQL EXPLAIN ANALYZE SELECT and Prefect read/filter API calls. `apply --options <json>` requires `approved: true` and checks online index validity, short transactions, disk space, and bounded waits.

Rollback, only if needed, drops these two added indexes with `DROP INDEX CONCURRENTLY`; do not drop the shared extension or any Prefect indexes. Do not wrap concurrent index commands in a transaction.

## Obsolete snapshot cleanup

`snapshot_maintenance.py preview --options <json>` reports candidates for explicitly selected native parent task UUIDs and their direct native children. Save the returned UTC cutoff into the options before applying. `apply` also requires `approved: true`.

The options format is:

```json
{
  "task_ids": ["native-parent-flow-run-uuid"],
  "cutoff": "ISO-8601 timestamp returned by preview",
  "approved": true
}
```

Deletion is limited to superseded derived snapshot rows with the expected prefix and matching task/flow ownership. The current collection head must exist; latest IDs, raw receipts, folder completion facts, pending/indexed receipt markers, and native FlowRunState/TaskRunState result references are excluded. Candidate UUIDs are read once into the operator process memory, then each batch uses at most 250 primary keys and rechecks every protection condition while locking the collection references. Active writers are skipped. After a pass, candidates are selected again so newly superseded pre-cutoff records are included. A nonempty final candidate set is reported as partial, never completed. The run is bounded to 200 batches, including zero-deletion batches, and at most 50,000 deletions. Each confirmed committed batch is logged; interruption reports the known committed total.

Preflight identified 22 existing collection references whose artifact head was already missing in the older `de360c0e-d4fc-4261-8c10-3925bb6f2790` task. These keys have no surviving artifact history, are excluded by the head join, and are not repaired or deleted by this maintenance. Verification compares the exact missing-reference identity set before and after and rejects any newly missing head. A dedicated isolated fixture covers this preexisting condition.

The first live cleanup attempt stopped during its initial read-only integrity query with a PostgreSQL parallel-worker shared-memory allocation error, before any DELETE. Maintenance sessions now set `max_parallel_workers_per_gather=0` locally. This limits only the maintenance connection; server defaults, worker settings, and application concurrency are unchanged.

An initial full-selection-per-batch implementation later hit its 15-second statement limit; the failing transaction rolled back. It was replaced with the tested UUID-bounded implementation above. The production EXPLAIN confirmed that each batch locates at most 250 artifact primary keys instead of rescanning all snapshot candidates. Read-only candidate selection/inventory has a 45-second timeout; each deleting transaction retains its 15-second statement and 1-second lock limits. The interrupted early run logged at least 2,500 committed deletions; its unlogged tail is not represented as an exact count.

This is one-time maintenance, not an automatic retention service. New superseded derived snapshots can appear as running tasks progress. There is no scheduled deletion or background monitoring. RAG/NAS/cleanup continue using the existing folder-level completion record behavior.

## Verification

The isolated database used the production PostgreSQL image with 1 CPU and 768 MiB, 200,000 synthetic full-payload artifact rows, and real query shapes. Full-row results, source pagination with an old-task unknown, pending snapshot pagination, and two-table write transactions were checked. Cleanup tests cover a concurrent writer replacing the latest head, cutoff and ownership mismatches, native state references, raw unknown and indexed tombstones, idempotent cleanup, and unchanged collection values. Application regressions: four classification request/source-state tests and eleven selected stage-snapshot tests passed using an isolated local Prefect test harness.

SQL and API speed measurements establish query improvement, not full-batch model throughput. GIN has storage and write costs; the stress test observed occasional write latency spikes. Live metrics and confirmed deletion counts are recorded in the maintenance JSON evidence.

The online build completed with both indexes valid and ready (GIN about 119.8 seconds, collection index about 1.2 seconds). Three sequential samples of the same production API filters before/after indexing gave these medians:

| Query | Before | After indexing |
| --- | ---: | ---: |
| Source history | 2146.489 ms | 761.346 ms |
| Latest state, one child run | 1246.257 ms | 921.194 ms |
| Latest state, 200 child runs | 926.989 ms | 797.674 ms |
| Pending state, 200 child runs | 1612.234 ms | 517.914 ms |

These were live observations under changing load, not a controlled throughput comparison. Source-history and one-child full API response hashes matched before/after. The 200-child and pending results changed as tasks progressed; equivalence for those query shapes was verified separately against fixed isolated data.

Final verification after cleanup measured API medians of 1011.028 ms (source history), 823.982 ms (one child), 913.301 ms (200 children), and 954.976 ms (pending). The large latest-state read therefore showed little final latency improvement; do not present the early measurements as a stable whole-system speedup. Both indexes remained valid and ready, and the two current parent flows remained RUNNING.

The final bounded pass committed 21,095 deletions across 85 batches, with zero remaining eligible rows at verification for cutoff `2026-11-03T17:20:30.962406+00:00`. An earlier interrupted pass separately confirmed at least 2,500 deletions; its unlogged tail is unknown, so the total is stated only as a lower bound of 23,595. Final deletion types: 20,378 stage snapshots, 698 resolved snapshots, 14 history-check snapshots, and 5 task summaries. All 112,717 pre-cutoff raw receipt/completion IDs and their identity digest were unchanged. The 22 preexisting missing references were unchanged, with no new missing heads. Both isolated server test containers were removed. No service restart, model/RAG dispatch, or application concurrency change was performed.
