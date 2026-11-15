"""Real RAG/NAS test for one folder using an isolated temporary Prefect server."""
import asyncio
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from uuid import UUID, uuid5

from live_prepare_group import main as prepare
from folder_native_client import observe_native_request


def main(*, resume_thread_fix=False, image_group=False):
    task_id = '6cb5a766-94f9-4613-aa5e-17a883be9016'
    group_id = ('4ede358f59b08e033e7983f11aca1ff7' if image_group
                else '723f1c8cac67472ff77fa7f27e8caa6d')
    run_id = 'fc240b92da874cca9a05f9630aeb073e'
    here = Path(__file__).resolve().parent
    batch = here / 'live-tests' / ('batch-309-' + run_id)
    case = batch / ('prefect-single-' + group_id)
    if not resume_thread_fix and case.exists():
        raise ValueError('this isolated group test already exists')
    stage, prepared, access, raw, config = prepare(task_id, group_id, create_targets=image_group)
    from paperflow_prefect.records import read_json, sha_file, write_json
    from paperflow_prefect.settings import Settings
    from preextract_batch_delivery import NativeMemoryGate, _local_native_target
    from preextract_native_runtime import readiness_for_client
    from effect_journal import EffectJournal
    from isolated_files_folder import ensure_test_files_folder
    from group_stage import FIELDS, initialize, reserve_group
    from group_prefect import (advance_stage, build_task, controlled_executor, controlled_prefect_slots,
                               make_worker, observed_concurrency, submit_stage)
    from isolated_group_operation import execute_group_stage
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.testing.utilities import prefect_test_harness

    workspace = here.parents[1]
    if resume_thread_fix:
        if not case.is_dir() or (case / 'thread-fix-continuation.json').exists():
            raise ValueError('the original stopped test or continuation state is unavailable')
    else:
        case.mkdir(exist_ok=False)
        write_json(case / 'attempt.json', {'schema': 'paperflow-isolated-prefect-attempt/1',
            'task_id': task_id, 'group_id': group_id, 'at_ns': time.time_ns(),
            'scope': 'isolated Prefect and isolated RAG/NAS targets'})
    folder = read_json(batch / 'rag-files-folder.json')
    journal = EffectJournal(batch / 'group-effects.sqlite3')
    if folder['binding']['journal_id'] != journal.identity() or folder['binding']['run_id'] != run_id:
        raise ValueError('test Files folder and group journal differ')
    def no_new_folder(*_):
        raise ValueError('single-group test may only use the existing isolated Files folder')
    if ensure_test_files_folder(raw, journal, no_new_folder, run_id, batch / 'rag-files-folder.json') != folder['folder_id']:
        raise ValueError('isolated Files folder no longer matches its receipt')
    selected = {route[key] for route in prepared['plan']['routes']
                for key in ('paper_dataset_id', 'image_dataset_id') if key in route}
    targets = [row for row in raw.datasets() if row['id'] in selected]
    if len(targets) != len(selected):
        raise ValueError('selected single-group test targets are unavailable')
    if not resume_thread_fix:
        from paperflow.native_delivery import NativeDeliveryPublisher
        publisher = NativeDeliveryPublisher(raw, lambda _: None, config)
        records = publisher.records(prepared['packet'])
        if image_group and not records['images']:
            raise ValueError('the selected paper has no deliverable image records')
        for route in prepared['plan']['routes']:
            for role, rows in records.items():
                if not rows:
                    continue
                ds = route['paper_dataset_id' if role == 'body' else 'image_dataset_id']
                names = publisher.document_names(prepared['prepared']['task'], prepared['packet'], route['code'], role)
                if any(publisher._document_lookup(ds, name=name) for name in set(names)):
                    raise ValueError('this paper already has a document in the test target')
    cache = workspace / 'prefect-paperflow/runtime/rag-nas-test/private/preextract_delivery/native-runtime-observation.json'
    config.native_runtime_observation = lambda: read_json(cache)
    memory = NativeMemoryGate(config)
    def require_native():
        if not memory.can_admit(0) or _local_native_target(config) == 0:
            return {'ready': False}
        ready = readiness_for_client(raw, config)
        if ready.get('ready') is True:
            ready['ready_valid_until'] = min(ready['ready_valid_until'], memory.last_sample['observed_at'] + 10)
        return ready
    if require_native().get('ready') is not True:
        raise ValueError('native RAG executor, upload policy or memory is not ready')
    native_requests, native_observations = [], []
    raw.request = observe_native_request(raw.request, config, native_requests, native_observations)

    database = case / 'isolated-control.sqlite3'
    ticket = {key: stage[key] for key in FIELDS}
    if not resume_thread_fix:
        with closing(sqlite3.connect(database)) as db, db:
            db.execute('CREATE TABLE tasks(task_id TEXT, prefect_run_id TEXT, prefect_order_model TEXT, '
                       'desired_state TEXT, start_pending INTEGER, start_generation INTEGER, task_status TEXT)')
            db.execute('CREATE TABLE task_deletions(task_id TEXT)')
            db.execute('CREATE TABLE paperflow_queue_barriers(name TEXT, blocked INTEGER)')
            db.execute("INSERT INTO paperflow_queue_barriers VALUES('rag_upload',0)")
            fields = ('task_id TEXT, group_id TEXT, paper_id TEXT, receipt_id TEXT, attempt_id TEXT, '
                      'parent_run_id TEXT, task_run_id TEXT')
            db.execute(f'CREATE TABLE postprocessing_claims({fields}, generation INTEGER, state TEXT, '
                       'claim_id TEXT, manifest_sha256 TEXT)')
            db.execute(f'CREATE TABLE paperflow_postprocess_stage_jobs({fields}, job_id TEXT, state TEXT, '
                       'manifest_sha256 TEXT, generation INTEGER)')
            db.execute(f'CREATE TABLE postprocessing_results({fields}, claim_id TEXT, state TEXT, code TEXT, '
                       'stage_receipt_sha256 TEXT, manifest_sha256 TEXT, generation INTEGER)')
            db.execute('CREATE TABLE postprocessing_receipts(claim_id TEXT, receipt_sha256 TEXT)')
            db.execute(f'CREATE TABLE llm_folder_queue({fields}, state TEXT, manifest_sha256 TEXT)')
            values = [ticket[key] for key in FIELDS]
            db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                       (task_id, ticket['parent_run_id'], 'v2', 'running', 0, stage['run_generation'], 'complete'))
            db.execute('INSERT INTO postprocessing_claims VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (*values, stage['source_generation'], 'waiting_rag', stage['claim_id'], stage['upstream_manifest_sha256']))
            db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (*values, stage['postprocess_job_id'], 'finished', stage['upstream_manifest_sha256'], stage['source_generation']))
            db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (*values, stage['claim_id'], 'passed', 'passed', stage['postprocess_receipt_sha256'],
                        stage['upstream_manifest_sha256'], stage['source_generation']))
            db.execute('INSERT INTO postprocessing_receipts VALUES(?,?)',
                       (stage['claim_id'], stage['postprocess_receipt_sha256']))
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                       (*values, 'waiting_postprocessing', stage['upstream_manifest_sha256']))
        initialize(database)
        job = reserve_group(database, prepared['artifact_map'], stage['run_generation'])
    else:
        from paperflow.native_delivery import NativeDeliveryPublisher
        with closing(sqlite3.connect(database.resolve(strict=True).as_uri() + '?mode=rw', uri=True)) as db:
            db.row_factory = sqlite3.Row
            job = dict(db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                                  (task_id, group_id)).fetchone())
        with closing(journal._connect()) as db:
            unresolved = db.execute("SELECT COUNT(*) FROM remote_effects WHERE state='intent'").fetchone()[0]
        rag_dir = case / group_id / 'rag'
        group_receipt, body_receipt = read_json(rag_dir / 'rag-group.json'), read_json(rag_dir / 'body.json')
        attachment_paths = list((rag_dir / 'attachments').glob('*.json'))
        if (job['state'] != 'unknown' or job['error_type'] != 'ValueError' or job['rag_submission'] != 'submitted'
                or job['nas_submission'] != 'reserved' or unresolved or len(attachment_paths) != 1
                or group_receipt.get('status') != 'pending' or group_receipt['binding']['ticket'] != ticket
                or group_receipt['binding']['copy_receipt_sha256'] != sha_file(prepared['copy_receipt_path'])
                or group_receipt['binding']['manifest_sha256'] != prepared['packet']['manifest_sha256']
                or group_receipt['binding']['journal_id'] != journal.identity()
                or body_receipt.get('body_verified') is not True):
            raise ValueError('original stopped test is not the known counter-thread failure')
        attachment = read_json(attachment_paths[0])
        original = attachment['raw']
        if attachment.get('status') != 'pending_verification' or original.get('status') != 'verified' or attachment['publications']:
            raise ValueError('attachment has a different prior publication history')
        publisher = NativeDeliveryPublisher(raw, lambda _: None, config)
        packet = dict(prepared['packet'], routes=prepared['plan']['routes'])
        publisher.verify(prepared['prepared']['task'], packet, body_receipt['targets'])
        publisher.states(body_receipt['targets'], enabled=True)
        for value in body_receipt['targets'].values():
            body = value['body']
            remote = raw.documents(body['dataset_id'], document_id=body['document_id'])
            if len(remote) != 1 or str(remote[0].get('run')) not in ('3', 'DONE') or float(remote[0].get('progress', 0)) < 1:
                raise ValueError('original body native processing is not complete')
        remote = raw.documents(original['dataset_id'], document_id=original['document_id'])
        if (len(remote) != 1 or str(remote[0].get('run')) not in ('0', 'UNSTART')
                or str(remote[0].get('status')) != '0'
                or hashlib.sha256(raw.download_file(original['file_id'])).hexdigest() != original['source_sha256']):
            raise ValueError('original test attachment is not the recorded unparsed file')
        prior_run = job['rag_task_run_id']
        next_run = str(uuid5(UUID(prior_run), 'counter-thread-continuation-1'))
        write_json(case / 'thread-fix-continuation.json', {'schema': 'paperflow-thread-fix-continuation/1',
            'previous_task_run_id': prior_run, 'new_task_run_id': next_run,
            'previous_error': 'offline counter belongs to its worker thread',
            'unresolved_effects': unresolved, 'body_targets_read_back': len(body_receipt['targets']),
            'original_attachment_read_back': True, 'attachment_native_submissions': 0, 'at_ns': time.time_ns()})
        with closing(sqlite3.connect(database)) as db, db:
            db.execute("UPDATE paperflow_rag_nas_jobs SET state='waiting',rag_submission='reserved',"
                       "rag_task_run_id=?,error_type=NULL,updated_at_ns=? WHERE task_id=? AND group_id=?",
                       (next_run, time.time_ns(), task_id, group_id))
            db.row_factory = sqlite3.Row
            job = dict(db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                                  (task_id, group_id)).fetchone())
    settings = Settings.load()
    nas_root = Path(r'\\zwjy-nas\paper_ai\paperflow-selftest') / ('batch-309-' + run_id) / 'prefect-single'
    test_root = Path(prepared['copy_receipt_path']).parent.parent
    context = {**prepared, 'database': access.database, 'folder': folder, 'raw': raw, 'journal': journal,
               'require_runtime': require_native, 'config': config,
               'runtime': read_json(settings.path('runtime_config')),
               'paperflow_root': settings.path('release_root') / 'core',
               'test_root': test_root, 'nas_root': nas_root, 'work_root': case}
    stop_mirror = False

    async def mirror_parent():
        nonlocal stop_mirror
        last = None
        while not stop_mirror:
            with closing(sqlite3.connect(access.database.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                parent = db.execute('SELECT desired_state,start_pending,start_generation,task_status FROM tasks WHERE task_id=?',
                                    (task_id,)).fetchone()
                deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
            current = (parent, bool(deleted))
            if current != last:
                with closing(sqlite3.connect(database)) as db, db:
                    if parent:
                        db.execute('UPDATE tasks SET desired_state=?,start_pending=?,start_generation=?,task_status=?', parent)
                    if deleted and not db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
                        db.execute('INSERT INTO task_deletions VALUES(?)', (task_id,))
                last = current
            await asyncio.sleep(.5)

    async def run_stage(name):
        execute = controlled_executor(database, task_id, group_id,
            lambda selected_stage, row: execute_group_stage(selected_stage, row, context),
            controlled_prefect_slots(database, task_id, group_id,
                                    {'rag': ('isolated-rag', 20), 'nas': ('isolated-nas', 2)}))
        task = build_task(name, job[name + '_task_run_id'], execute)
        worker = make_worker([task])
        async with worker:
            subscription = asyncio.create_task(worker.astart())
            try:
                await asyncio.sleep(0)
                def submit():
                    with get_client(sync_client=True) as api:
                        return submit_stage(database, task_id, group_id, name, api, task)
                from task_effect_admission import TaskNotRunning
                while True:
                    try:
                        await asyncio.to_thread(submit)
                        break
                    except TaskNotRunning:
                        with closing(sqlite3.connect(database)) as db:
                            if db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
                                raise
                        await asyncio.sleep(1)
                print('isolated Prefect stage started:', name, flush=True)
                while not worker.finished_task_runs.get(task.task_key, 0):
                    await asyncio.sleep(1)
                def finish():
                    with get_client(sync_client=True) as api:
                        actual = api.read_task_run(UUID(job[name + '_task_run_id']))
                    if not actual.state.is_completed():
                        raise RuntimeError('isolated Prefect stage did not complete: ' + str(actual.state.type))
                    advance_stage(database, task_id, group_id, name, actual)
                await asyncio.to_thread(finish)
                print('isolated Prefect stage completed:', name, flush=True)
            finally:
                subscription.cancel()
                try: await subscription
                except asyncio.CancelledError: pass

    async def run():
        nonlocal stop_mirror
        def create_limits():
            with get_client(sync_client=True) as api:
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name='isolated-rag', limit=20))
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name='isolated-nas', limit=2))
        await asyncio.to_thread(create_limits)
        mirror = asyncio.create_task(mirror_parent())
        try:
            await run_stage('rag')
            await run_stage('nas')
        finally:
            stop_mirror = True
            await mirror

    with prefect_test_harness():
        asyncio.run(run())
    with closing(sqlite3.connect(database)) as db:
        db.row_factory = sqlite3.Row
        finished = dict(db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                                   (task_id, group_id)).fetchone())
    rag, archive, cleanup = (read_json(finished[key]) for key in
                             ('rag_receipt_path', 'nas_receipt_path', 'cleanup_receipt_path'))
    body = read_json(rag['body_receipt_path'])
    attachments = [read_json(row['receipt_path']) for row in rag['attachments']]
    events = [(row['started_at_ns'], 1) for row in native_requests] + [
        (row['ended_at_ns'], -1) for row in native_requests]
    current = request_peak = 0
    for _, change in sorted(events):
        current += change
        request_peak = max(request_peak, current)
    active, observed_peak = set(), 0
    for row in native_observations:
        if row['run'] in ('1', 'RUNNING'):
            active.add(row['document_id'])
        elif row['run'] in ('3', 'DONE', 'FAIL', '4', '-1', '2', 'CANCEL'):
            active.discard(row['document_id'])
        observed_peak = max(observed_peak, len(active))
    write_json(case / 'native-observations.json', {'requests': native_requests, 'documents': native_observations})
    proof = {'schema': 'paperflow-isolated-prefect-group-result/1', 'task_id': task_id, 'group_id': group_id,
             'scope': 'one group on an isolated Prefect server', 'state': finished['state'],
             'mainline_stage_created': False, 'rag_group_count': rag['group_count'],
             'rag_started_at_ns': finished['rag_started_at_ns'], 'rag_completed_at_ns': finished['rag_completed_at_ns'],
             'nas_started_at_ns': finished['nas_started_at_ns'], 'nas_completed_at_ns': finished['nas_completed_at_ns'],
             'completed_at_ns': finished['completed_at_ns'],
             'rag_peak_groups': observed_concurrency(database, task_id, 'rag')['peak'],
             'nas_peak_groups': observed_concurrency(database, task_id, 'nas')['peak'],
             'continued_after_counter_thread_fix': resume_thread_fix,
             'prior_body_native_targets_reused': 3 if resume_thread_fix else 0,
             'native_http_request_count': len(native_requests), 'native_http_request_peak': request_peak,
             'observed_running_document_peak': observed_peak,
             'running_document_peak_scope': 'sampled RAGFlow document run state',
             'body_targets': {code: {'chunks': len(value['body']['chunks']),
                                     'image_chunks': len(value.get('images', {}).get('chunks', {})),
                                     'retrieval_verified': value.get('retrieval_verified') is True}
                              for code, value in body['targets'].items()},
             'attachment_targets': [{code: {'chunks': len(value['chunks']),
                                             'status': value['status']}
                                      for code, value in attachment['publications'][attachment['current_md_sha256']].items()}
                                     for attachment in attachments],
             'archive_files': [{'name': row['name'], 'bytes': row['bytes'], 'sha256': row['sha256']}
                               for row in archive['files']],
             'deleted_test_copies': [row['path'] for row in cleanup['files'].values()],
             'reported_privacy_redaction': prepared['reported_privacy_redaction'],
             'completed_at_utc': datetime.now(timezone.utc).isoformat()}
    write_json(case / 'result.json', proof)
    print(json.dumps(proof, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    if len(sys.argv) > 2 or (len(sys.argv) == 2 and sys.argv[1] not in ('--resume-thread-fix', '--image-group')):
        raise ValueError('unsupported isolated group test')
    main(resume_thread_fix='--resume-thread-fix' in sys.argv, image_group='--image-group' in sys.argv)
