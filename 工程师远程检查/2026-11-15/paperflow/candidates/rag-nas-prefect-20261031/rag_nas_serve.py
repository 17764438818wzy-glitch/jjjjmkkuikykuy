"""Folder-level RAG/NAS admissions for the existing Prefect service loop."""
import asyncio
from contextlib import closing
from pathlib import Path
import sqlite3


def snapshot(database, task_id):
    with closing(sqlite3.connect(Path(database).resolve(strict=True).as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        parent = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
        state = {'generation': None, 'rag': [], 'nas': []}
        if (not parent or deleted or parent['desired_state'] != 'running' or parent['start_pending']
                or parent['prefect_order_model'] != 'v2' or not parent['prefect_run_id']
                or parent['task_status'] in {'deleting', 'delete_failed', 'unknown'}):
            return state
        state['generation'] = parent['start_generation']
        state['nas'] = [dict(row) for row in db.execute(
            "SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND stage='nas' "
            "AND ((state='waiting' AND nas_submission='reserved') "
            "OR (state IN ('waiting','paused') AND nas_submission='submitted')) ORDER BY group_id", (task_id,))]
        barrier = db.execute("SELECT blocked FROM paperflow_queue_barriers WHERE name='rag_upload'").fetchone()
        if barrier and barrier['blocked'] == 0:
            state['rag'] = [row[0] for row in db.execute(
                "SELECT group_id FROM paperflow_rag_nas_jobs WHERE task_id=? AND stage='rag' "
                "AND ((state='waiting' AND rag_submission='reserved') "
                "OR (state IN ('waiting','paused') AND rag_submission='submitted')) ORDER BY group_id", (task_id,))]
            state['rag'] += [row[0] for row in db.execute(
                "SELECT c.group_id FROM postprocessing_claims c "
                "JOIN paperflow_postprocess_stage_jobs s ON s.task_id=c.task_id AND s.group_id=c.group_id "
                "AND s.state='finished' JOIN postprocessing_results r ON r.task_id=c.task_id "
                "AND r.group_id=c.group_id AND r.state='passed' AND r.code='passed' "
                "WHERE c.task_id=? AND c.state='waiting_rag' "
                'AND NOT EXISTS(SELECT 1 FROM paperflow_rag_nas_jobs j '
                'WHERE j.task_id=c.task_id AND j.group_id=c.group_id) ORDER BY c.group_id', (task_id,))]
        return state


class Bridge:
    def __init__(self, database, task_id, prepare, execute, nas_context, log):
        self.database, self.task_id = database, task_id
        self.prepare, self.execute, self.nas_context, self.log = prepare, execute, nas_context, log
        self.active, self.preparing, self.skipped = {}, {}, set()

    def pending(self):
        return set(self.active) | set(self.preparing)

    async def tick(self):
        for task in [item for item in self.active if item.done()]:
            stage, group, generation = self.active.pop(task)
            try:
                task.result()
            except Exception as exc:
                if not isinstance(exc, InterruptedError):
                    self.skipped.add((stage, group, generation))
                self.log('rag_nas_folder_unconfirmed', task_id=self.task_id, group_id=group,
                         stage=stage, error_type=type(exc).__name__)
            else:
                self.log('rag_nas_folder_stage_complete', task_id=self.task_id, group_id=group, stage=stage)
        for task in [item for item in self.preparing if item.done()]:
            group, generation = self.preparing.pop(task)
            try:
                job, context = task.result()
            except Exception as exc:
                if not isinstance(exc, InterruptedError):
                    self.skipped.add(('rag', group, generation))
                self.log('rag_nas_folder_unconfirmed', task_id=self.task_id, group_id=group,
                         stage='rag_prepare', error_type=type(exc).__name__)
            else:
                item = asyncio.create_task(self.execute(job, 'rag', context))
                self.active[item] = ('rag', group, generation)
        state = await asyncio.to_thread(snapshot, self.database, self.task_id)
        generation = state['generation']
        if generation is None:
            return
        occupied = {(stage, group) for stage, group, _ in self.active.values()}
        occupied.update(('rag', group) for group, _ in self.preparing.values())
        nas_count = sum(stage == 'nas' for stage, _, _ in self.active.values())
        for job in state['nas']:
            group = job['group_id']
            if nas_count >= 2:
                break
            if ('nas', group) in occupied or ('nas', group, generation) in self.skipped:
                continue
            try:
                context = self.nas_context(job)
            except Exception as exc:
                self.skipped.add(('nas', group, generation))
                self.log('rag_nas_folder_unconfirmed', task_id=self.task_id, group_id=group,
                         stage='nas_prepare', error_type=type(exc).__name__)
                continue
            item = asyncio.create_task(self.execute(job, 'nas', context))
            self.active[item] = ('nas', group, generation)
            nas_count += 1
        rag_count = sum(stage == 'rag' for stage, _, _ in self.active.values()) + len(self.preparing)
        for group in state['rag']:
            if rag_count >= 20:
                break
            if ('rag', group) in occupied or ('rag', group, generation) in self.skipped:
                continue
            item = asyncio.create_task(asyncio.to_thread(self.prepare, group, generation))
            self.preparing[item] = (group, generation)
            rag_count += 1


def build(runtime, access, log, *, batch_config):
    """Bind the authorized isolated batch to the caller's existing service."""
    from uuid import UUID, uuid5
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.settings import Settings
    from paperflow_prefect.records import read_json, write_json
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.exceptions import ObjectNotFound

    settings = Settings.load()
    bind(settings)
    from auto_config import load_key
    from ragflow_client import RagflowClient
    from preextract_batch_delivery import NativeMemoryGate, _local_native_target, load_native_batch_config
    from preextract_native_runtime import readiness_for_client
    import paramiko
    from effect_journal import EffectJournal, EffectUnknown
    from folder_native_client import observe_native_request
    from group_prefect import HandoffNeedsReview, controlled_prefect_slots, execute_prefect_stage
    from group_stage import initialize, reserve_local_group, verify_remote_handoff
    from isolated_files_folder import ensure_test_files_folder
    from isolated_group_operation import execute_group_stage
    from isolated_group_prepare import prepare_group
    from isolated_targets import TargetCreations
    from read_only_rpc import UnreadReviewRequired
    from task_effect_admission import task_effect_admission

    here = Path(__file__).resolve().parent
    workspace = here.parents[1]
    database = Path(runtime.original_runtime['database']).resolve(strict=True)
    if database != Path(access.database).resolve(strict=True):
        raise ValueError('the postprocessing and RAG services use different task controls')
    task_id, run_id = batch_config['task_id'], batch_config['run_id']
    batch = Path(batch_config['batch_root'])
    test_root = Path(batch_config['test_root'])
    work_root = batch / 'prefect-groups-work'
    nas_root = Path(batch_config['nas_root'])
    cache = workspace / 'prefect-paperflow/runtime/rag-nas-test/private/preextract_delivery/native-runtime-observation.json'
    limits = {'rag': ('paperflow-connected-rag', 20), 'nas': ('paperflow-connected-nas', 2)}
    target_creations = TargetCreations()
    initialize(database)
    with get_client(sync_client=True) as api:
        for name, limit in limits.values():
            try:
                row = api.read_global_concurrency_limit_by_name(name)
            except ObjectNotFound:
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name, limit=limit))
            else:
                if not row.active or row.limit != limit or row.slot_decay_per_second:
                    raise ValueError('existing Prefect concurrency limit does not match the authorized batch')

    def reused(group):
        case = batch / ('prefect-single-' + group)
        if not case.exists():
            return None
        result_path = case / 'result.json'
        if not result_path.is_file():
            raise EffectUnknown('previous isolated group test has not completed')
        result = read_json(result_path)
        if result.get('state') != 'completed' or result.get('task_id') != task_id or result.get('group_id') != group:
            raise ValueError('previous isolated result belongs to a different folder')
        folder = case / group
        return {'rag': str(folder / 'rag/rag-group.json'),
                'copy': str(test_root / group / 'test-copy-receipt.json'),
                'archive': str(folder / 'archive/archive-complete.json'),
                'cleanup': str(folder / 'cleanup.json')}

    def prepare(group, generation):
        with closing(sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            found = db.execute('SELECT * FROM paperflow_rag_nas_jobs WHERE task_id=? AND group_id=?',
                               (task_id, group)).fetchone()
        job = dict(found) if found else reserve_local_group(database, task_id, group, generation)
        return job, {}

    def prepare_payload(job, artifact_map):
        group, generation = job['group_id'], job['run_generation']
        raw = RagflowClient(settings['rag_url'], load_key(settings.path('settings_root'), 'ragflow'))
        config = load_native_batch_config(Path(r'E:\uv\ragflow_upstream\data'), raw)
        config.native_runtime_observation = lambda: read_json(cache)
        def admit(*_):
            return task_effect_admission(database, task_id, generation, started=True)
        target_run, target_dir = run_id, batch
        if group in batch_config['fresh_target_groups']:
            target_run = uuid5(UUID(run_id), 'fresh-targets:' + group).hex
            target_dir = batch / 'fresh-targets' / group
            target_dir.mkdir(parents=True, exist_ok=True)
        dataset_journal = EffectJournal(target_dir / 'rag-dataset-effects.sqlite3')
        if target_dir != batch:
            dataset_journal.initialize()
        ssh = paramiko.SSHClient()
        ssh.load_host_keys(config.memory_settings['known_hosts'])
        ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        ssh.connect(config.memory_settings['host'], port=config.memory_settings['port'],
                    username=config.memory_settings['username'],
                    key_filename=str(workspace / 'prefect-paperflow/runtime/remote-postprocessing/server-rag-readonly-ed25519'),
                    look_for_keys=False, allow_agent=False, timeout=5, auth_timeout=5, banner_timeout=5)
        try:
            with ssh.open_sftp() as sftp:
                prepared = prepare_group(job, access, sftp,
                    pool_database=workspace / 'local-intake-runtime/waiting-pool.sqlite3',
                    test_root=test_root, work_root=batch / 'prefect-preparation',
                    catalog_path=settings.path('catalog_path'),
                    catalog_sha256=batch_config['catalog_sha256'],
                    run_id=target_run, raw=raw, dataset_journal=dataset_journal, admit=admit,
                    dataset_receipts=target_dir, config=config, artifact_map=artifact_map,
                    coordinate_targets=target_creations)
        finally:
            ssh.close()
        journal = EffectJournal(batch / 'group-effects.sqlite3')
        folder = read_json(batch / 'rag-files-folder.json')
        if prepared['attachments']:
            def no_new_folder(*_):
                raise ValueError('the existing isolated Files folder is required')
            if ensure_test_files_folder(raw, journal, no_new_folder, run_id, batch / 'rag-files-folder.json') != folder['folder_id']:
                raise ValueError('the isolated Files folder differs from its receipt')
        memory = NativeMemoryGate(config)
        def require_native():
            with get_client(sync_client=True) as api:
                current = api.read_global_concurrency_limit_by_name(limits['rag'][0])
            if (not current.active or current.limit != limits['rag'][1]
                    or not memory.can_admit(max(0, current.active_slots - 1)) or _local_native_target(config) == 0):
                return {'ready': False}
            ready = readiness_for_client(raw, config)
            if ready.get('ready') is True:
                ready['ready_valid_until'] = min(ready['ready_valid_until'], memory.last_sample['observed_at'] + 10)
            return ready
        requests, observations = [], []
        raw.request = observe_native_request(raw.request, config, requests, observations)
        context = {**prepared, 'database': database, 'folder': folder, 'raw': raw, 'journal': journal,
                   'require_runtime': require_native, 'config': config,
                   'runtime': read_json(settings.path('runtime_config')),
                   'paperflow_root': settings.path('release_root') / 'core',
                   'test_root': test_root, 'nas_root': nas_root, 'work_root': work_root,
                   'native_requests': requests, 'native_observations': observations}
        return context

    def nas_context(job):
        receipts = reused(job['group_id'])
        if receipts:
            return {'reuse_receipts': receipts}
        return {'database': database, 'copy_receipt_path': job['copy_receipt_path'],
                'test_root': test_root, 'nas_root': nas_root, 'work_root': work_root}

    async def execute(job, stage, context):
        def operation(selected, current):
            try:
                if selected == 'rag':
                    if 'artifact_map' not in context:
                        try:
                            artifact_map = access.artifact_map(task_id, current['group_id'])
                        except UnreadReviewRequired as exc:
                            raise HandoffNeedsReview('UnreadReviewRequired') from exc
                        verify_remote_handoff(current, artifact_map)
                        context['artifact_map'] = artifact_map
                    if 'reuse_receipts' not in context and 'prepared' not in context:
                        receipts = reused(current['group_id'])
                        if receipts:
                            context['reuse_receipts'] = receipts
                        else:
                            context.update(prepare_payload(current, context['artifact_map']))
                return execute_group_stage(selected, current, context)
            finally:
                if selected == 'rag' and 'native_requests' in context:
                    write_json(work_root / current['group_id'] / 'native-observations.json',
                               {'requests': context['native_requests'], 'documents': context['native_observations']})
        slot = controlled_prefect_slots(database, task_id, job['group_id'], limits)
        await execute_prefect_stage(database, job, stage, operation, slot)

    return Bridge(database, task_id, prepare, execute, nas_context, log)
