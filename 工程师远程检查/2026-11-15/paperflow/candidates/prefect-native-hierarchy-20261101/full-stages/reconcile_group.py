"""Record and explicitly continue the observed pre-upload lookup failure.

The caller supplies independently collected read-only evidence for the original
detached Prefect task. This module does not create a scheduler.
"""
from contextlib import closing
import json
import time
from uuid import UUID

from group_prefect import _check_run, _current, _db
from group_stage import FIELDS, prefect_task_identity


FAILURE = 'formal library ownership or embedding is unavailable'


def record_reconciliation(database, task_id, group_id, actual, evidence):
    """Preserve the failed attempt and its pre-upload evidence in the control DB."""
    with closing(_db(database)) as db, db:
        db.execute('BEGIN IMMEDIATE')
        job = _current(db, task_id, group_id, running=True, resume_unknown=True)
        run_id = job['rag_task_run_id']
        binding = {key: job[key] for key in FIELDS}
        binding.update(rag_task_run_id=run_id,
                       upstream_manifest_sha256=job['upstream_manifest_sha256'],
                       postprocess_receipt_sha256=job['postprocess_receipt_sha256'])
        checks = evidence.get('checks', {})
        barrier = db.execute("SELECT blocked FROM paperflow_queue_barriers WHERE name='rag_upload'").fetchone()
        if (job['stage'] != 'rag' or job['state'] != 'unknown'
                or job['rag_submission'] != 'submitted' or job['error_type'] != 'ValueError'
                or not barrier or barrier['blocked'] != 0
                or not _check_run(actual, prefect_task_identity('rag', run_id),
                                  job['task_run_id'], allow_detached=True)
                or not actual.state.is_failed() or FAILURE not in (actual.state.message or '')
                or evidence.get('binding') != binding
                or checks.get('all_routes_checked') is not True
                or checks.get('all_test_targets_ready') is not True
                or checks.get('matching_group_documents') != 0
                or checks.get('group_upload_effects') != 0
                or checks.get('test_copy_receipt_exists') is not False):
            raise ValueError('the pre-upload failure has not been reconciled')
        db.execute('CREATE TABLE IF NOT EXISTS paperflow_rag_nas_reconciliations ('
                   'task_id TEXT NOT NULL, group_id TEXT NOT NULL, task_run_id TEXT NOT NULL, '
                   'prior_state TEXT NOT NULL, prior_error_type TEXT NOT NULL, prior_message TEXT NOT NULL, '
                   'evidence_json TEXT NOT NULL, recorded_at_ns INTEGER NOT NULL, '
                   'continuation_state TEXT NOT NULL, PRIMARY KEY(task_id,group_id,task_run_id))')
        db.execute('INSERT INTO paperflow_rag_nas_reconciliations VALUES(?,?,?,?,?,?,?,?,?)',
                   (task_id, group_id, run_id, job['state'], job['error_type'], actual.state.message,
                    json.dumps(evidence, ensure_ascii=False, sort_keys=True), time.time_ns(), 'verified'))
        # Existing claim_stage already recognizes unknown/unknown for the same
        # TaskRun. Preserve unknown until that TaskRun is actually reclaimed.
        db.execute("UPDATE paperflow_rag_nas_jobs SET rag_submission='unknown',updated_at_ns=? "
                   'WHERE task_id=? AND group_id=?', (time.time_ns(), task_id, group_id))
        return dict(db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                               (task_id, group_id)).fetchone())


async def execute_reconciled_prefect_stage(database, job, operation, slot, evidence):
    """Execute only the reconciled RAG stage using the existing group operation."""
    import asyncio
    from prefect.client.orchestration import get_client
    from group_prefect import advance_stage, build_task, controlled_executor, make_worker

    task_id, group_id = job['task_id'], job['group_id']
    execute = controlled_executor(database, task_id, group_id, operation, slot)
    task = build_task('rag', job['rag_task_run_id'], execute)
    worker = make_worker([task])
    def prepare():
        with get_client(sync_client=True) as api:
            actual = api.read_task_run(UUID(job['rag_task_run_id']))
        record_reconciliation(database, task_id, group_id, actual, evidence)
        with closing(_db(database)) as db, db:
            db.execute("UPDATE paperflow_rag_nas_reconciliations SET continuation_state='executing' "
                       'WHERE task_id=? AND group_id=? AND task_run_id=?',
                       (task_id, group_id, job['rag_task_run_id']))
        return actual
    actual = await asyncio.to_thread(prepare)
    try:
        # Prefect's public one-shot TaskWorker entry executes this existing
        # TaskRun directly; no Scheduled message goes to stale subscriptions.
        await worker.execute_task_run(actual)
        def finish():
            with get_client(sync_client=True) as api:
                current = api.read_task_run(UUID(job['rag_task_run_id']))
            if not current.state.is_completed():
                raise RuntimeError('reconciled Prefect folder stage did not complete')
            advance_stage(database, task_id, group_id, 'rag', current)
        await asyncio.to_thread(finish)
    except BaseException:
        with closing(_db(database)) as db, db:
            db.execute("UPDATE paperflow_rag_nas_reconciliations SET continuation_state='unknown' "
                       'WHERE task_id=? AND group_id=? AND task_run_id=?',
                       (task_id, group_id, job['rag_task_run_id']))
        raise
    with closing(_db(database)) as db, db:
        db.execute("UPDATE paperflow_rag_nas_reconciliations SET continuation_state='completed' "
                   'WHERE task_id=? AND group_id=? AND task_run_id=?',
                   (task_id, group_id, job['rag_task_run_id']))


async def continue_one(bridge, group_id, evidence):
    """Explicitly continue one reconciled group, leaving NAS to the main loop."""
    import asyncio

    def current():
        with closing(_db(bridge.database)) as db:
            row = db.execute('SELECT * FROM paperflow_rag_nas_jobs '
                             'WHERE task_id=? AND group_id=?', (bridge.task_id, group_id)).fetchone()
        return dict(row)

    job = await asyncio.to_thread(current)
    await bridge.execute(job, 'rag', {'explicit_reconciled': True, 'reconciliation_evidence': evidence})
