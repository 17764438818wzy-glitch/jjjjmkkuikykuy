"""Folder-scoped stage admission and durable evidence of pause drainage."""
import asyncio
import hashlib
from threading import Event
from uuid import UUID, uuid4

import httpx

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import ArtifactCreate
from prefect.client.schemas.filters import ArtifactCollectionFilter, ArtifactFilter, FlowRunFilter, VariableFilter
from prefect.client.schemas.sorting import ArtifactCollectionSort, ArtifactSort
from prefect.variables import Variable

from .admission import _SESSION, control_gate, read_variables_sync
from .receipts import read_receipts
from .retry import POSTPROCESSING_NETWORK_UNKNOWN, uncertain_model_receipt
from paperflow_v3.stages.gateway_admission import has_proof

ACTIVE = 'paperflow-v3-stage-active'
SETTLED = 'paperflow-v3-stage-settled'
UNKNOWN = 'paperflow-v3-stage-unknown'
STAGES = {'classification', 'preprocessing', 'llm', 'postprocessing', 'delivery'}


def stage_exit_status(exc, uncertain):
    from .stage_queue import StageQueueWait
    return ('unknown' if uncertain else 'waiting' if isinstance(exc, StageQueueWait)
            else 'failed' if exc else 'complete')


def _prefix(task_id):
    return 'paperflow-v3-stage-' + str(task_id).replace('-', '') + '-'


async def _stage_rows(task_id, *, key=None, pending_only=True):
    result = []
    selected = {'key': {'any_': [key]} if key else {'like_': _prefix(task_id) + '%'},
                'type': {'any_': [ACTIVE, UNKNOWN] if pending_only else [ACTIVE, UNKNOWN, SETTLED]}}
    async with get_client() as client:
        while True:
            rows = await client.read_latest_artifacts(artifact_filter=ArtifactCollectionFilter(**selected),
                sort=ArtifactCollectionSort.KEY_ASC, limit=200, offset=len(result))
            result.extend(rows)
            if len(rows) < 200:
                return result


def unsettled_requests(records, stage):
    """Keep original unknown requests distinct from failed or queued work."""
    if stage == 'llm':
        from .attempts import active_model_records
        records = active_model_records(records)
    latest = {}
    unknown_model_requests = set()
    for row in records:
        kind, data = row['kind'], row.get('data') or {}
        if stage == 'classification' and kind == 'classification_model':
            latest.setdefault(('classification', data.get('request_id')), data.get('state'))
        elif stage == 'classification' and kind == 'child_dispatch':
            latest.setdefault(('dispatch', row.get('folder_id')), data.get('state'))
        elif stage == 'preprocessing' and kind in {'preprocessing_started', 'preprocessing_complete', 'preprocessing_failed'}:
            value = ('unknown' if kind == 'preprocessing_started' or kind == 'preprocessing_failed'
                     and data.get('failure_reason') not in {'local_io', 'local_parse', 'plan_unavailable'} else kind)
            latest.setdefault(('preprocessing', row.get('folder_id')), value)
        elif stage == 'llm' and kind in {'llm_request_intent', 'llm_request_started',
                'llm_request_unknown', 'llm_request_not_sent', 'llm_request_failed', 'llm_response_saved',
                'llm_gateway_not_dispatched'}:
            if uncertain_model_receipt(kind, data):
                unknown_model_requests.add((data.get('component_id'), data.get('request_id')))
            if kind == 'llm_gateway_not_dispatched':
                kind = 'llm_request_not_sent' if has_proof(data) else 'llm_request_unknown'
            latest.setdefault(('llm', data.get('component_id'), data.get('request_id')), kind)
        elif stage == 'postprocessing' and kind.startswith('postprocessing_component_'):
            value = ('unknown' if kind == 'postprocessing_component_failed'
                     and data.get('error_type') in POSTPROCESSING_NETWORK_UNKNOWN else kind)
            latest.setdefault(('post', data.get('component_id')), value)
        elif stage == 'delivery' and kind == 'folder_completion_pending':
            latest.setdefault(('completion', data.get('stage')), 'unknown')
        elif stage == 'delivery' and kind.startswith('rag_effect_'):
            latest.setdefault(('effect', data.get('effect_id')), kind)
        elif stage == 'delivery' and kind in {'nas', 'cleanup', 'rag-body', 'rag-attachments',
                                              'rag-qdrant-body', 'rag-qdrant-attachments'}:
            latest.setdefault(('files', kind), data)
    pending = {'intent', 'unknown', 'llm_request_intent', 'llm_request_started', 'llm_request_unknown',
               'postprocessing_component_started', 'rag_effect_intent'}

    def unknown_file(value):
        if isinstance(value, dict):
            if any(value.get(field) in {'in_flight', 'unknown'} for field in
                   ('status', 'body_delivery_state', 'delivery_state')):
                return True
            return any(unknown_file(item) for item in value.values())
        return False

    return bool(unknown_model_requests) or any(unknown_file(value) if isinstance(value, dict) else value in pending
                                             for value in latest.values())


def _explicit_same_attempt_resume(records, identity, retry_row, previous):
    """Consume a new exact user grant for this immutable unknown owner version.

    A frozen input plan keeps its processing revision. The new manual event
    authorizes only reviewed interrupted requests; it cannot replay a later
    unknown owner or hide an unlisted/late request outcome.
    """
    from .attempts import authorization, active_model_records, digest
    latest = next((row for row in records if row.get('kind') == 'manual_retry'), None)
    if latest != retry_row:
        return False
    retry = retry_row.get('data') or {}
    references = [{'key': row.key, 'version_id': str(getattr(row, 'latest_id', row.id)),
                   'token': row.data.get('token'), 'data_sha256': digest(row.data)} for row in previous]
    if retry.get('resume_unknown_stage') != {'stage': 'llm', 'owners': references}:
        return False
    selected = authorization(records, identity)
    if selected is None or selected[0] != retry_row:
        return False
    covered = {(item['component_id'], item['request_id']) for item in selected[1]['requests']}
    if not any(row.get('kind') == 'llm_request_unknown' and
               (row.get('data', {}).get('component_id'), row.get('data', {}).get('request_id')) in covered for row in records):
        return False
    if any(row.get('kind') == 'llm_response_saved' and
           (row.get('data', {}).get('component_id'), row.get('data', {}).get('request_id')) in covered for row in records):
        return False
    return not unsettled_requests(active_model_records(records, identity), 'llm')


class FolderStage:
    """An admitted folder drains this stage, retaining the original session."""
    def __init__(self, parent, identity, stage, *, deferred=False, read_records=None, retry_id=None,
                 wait_resources=None):
        if stage not in STAGES:
            raise ValueError('unknown folder stage')
        self.parent, self.identity, self.stage = parent, dict(identity), stage
        self.task_id = str(identity['task_id'])
        self.folder_id = identity['group_id']
        self.child_id = str(identity['task_run_id'])
        digest = hashlib.sha256((self.child_id + ':' + self.folder_id).encode()).hexdigest()[:32]
        self.key = _prefix(self.task_id) + digest + '-' + stage
        self.deferred = deferred
        self.loop = None
        self.read_records = read_records
        self.retry_id = retry_id
        self.session = None
        self.admitted = False
        self.token = str(uuid4())
        self.closed = False
        self.stop = Event()
        self.unknown_reason = None
        self.wait_resources = wait_resources

    async def _save(self, state, reason=None):
        kind = ACTIVE if state == 'started' else UNKNOWN if state == 'unknown' else SETTLED
        data = {'task_id': self.task_id, 'folder_id': self.folder_id, 'child_flow_run_id': self.child_id,
                'stage': self.stage, 'session': self.session, 'token': self.token, 'state': state,
                'processing_attempt_id': self.retry_id}
        if reason:
            data['reason'] = reason
        async with get_client() as client:
            await client.create_artifact(ArtifactCreate(key=self.key, type=kind,
                flow_run_id=self.child_id, data=data))

    async def __aenter__(self):
        self.loop = asyncio.get_running_loop()
        if self.wait_resources:
            from .admission import wait_for_resources
            await wait_for_resources(self.parent, self.wait_resources)
        else:
            await self.parent.wait()
        if not self.deferred:
            await self.ensure_admitted()
        return self

    async def admit(self):
        """Called while holding the actual first-operation resource, never a probe."""
        if self.closed:
            raise InterruptedError('文件夹阶段已结束，未开始新操作')
        if self.unknown_reason:
            raise RuntimeError(self.unknown_reason)
        if self.admitted:
            await self.wait()
            return True
        async with control_gate(self.task_id):
            if self.unknown_reason:
                raise RuntimeError(self.unknown_reason)
            if self.stop.is_set() and not self.admitted:
                raise InterruptedError('未入场文件夹已停止等待')
            if self.admitted:
                await self.wait()
                return True
            session = await Variable.aget(_SESSION)
            if not await self.parent.running():
                return False
            if session is None or await Variable.aget(_SESSION) != session:
                raise InterruptedError('控制会话未确认，未开始新阶段')
            previous = await _stage_rows(self.task_id, key=self.key)
            if previous:
                # The existing retry endpoint has already checked the original
                # requests. Reuse that authorization once, retaining its identity.
                allowed = False
                if self.retry_id and all(row.type == UNKNOWN for row in previous):
                    reader = self.read_records or read_receipts
                    records = await reader(self.child_id)
                    retry_row = next((row for row in records
                        if row.get('kind') == 'manual_retry' and row.get('task_id') == self.task_id
                        and row.get('folder_id') == self.folder_id), {})
                    retry = retry_row.get('data') or {}
                    retry_stage = retry.get('stage')
                    if retry_stage in {'rag', 'nas', 'cleanup'}:
                        retry_stage = 'delivery'
                    allowed = (retry.get('retry_id') == self.retry_id and retry_stage == self.stage
                               and retry.get('state') in {'authorized', 'running'})
                    if allowed and any(row.data.get('processing_attempt_id') == self.retry_id for row in previous):
                        allowed = self.stage == 'llm' and _explicit_same_attempt_resume(
                            records, self.identity, retry_row, previous)
                if not allowed:
                    raise RuntimeError('本文件夹原阶段仍在处理或待核对，未重复开始')
            self.session = session
            try:
                await self._save('started')
            except BaseException:
                self.mark_unknown('阶段准入回执未确认')
                raise
            self.admitted = True
        return True

    def admit_sync(self):
        # The worker retains its synchronous resource CM. Only this short
        # admission coroutine runs on the owning flow's event loop.
        return asyncio.run_coroutine_threadsafe(self.admit(), self.loop).result()

    async def ensure_admitted(self):
        while not await self.admit():
            await self.parent.wait()

    async def running(self):
        from .stage_queue import phase_healthy
        if not phase_healthy():
            return False
        if self.closed:
            return False
        if not self.admitted:
            return await self.parent.running()
        if await Variable.aget(_SESSION) != self.session:
            return False
        state = await Variable.aget('paperflow_v3_task_' + self.task_id.replace('-', '_'))
        return not (isinstance(state, dict) and state.get('state') == 'deleting')

    def running_sync(self):
        return self.cleanup_running_sync()

    def cleanup_running_sync(self):
        """Fresh control fence for synchronous stages, including every unlink."""
        from .stage_queue import phase_healthy
        if not phase_healthy() or self.closed:
            return False
        if not self.admitted:
            return self.parent.running_sync()
        task_key = 'paperflow_v3_task_' + self.task_id.replace('-', '_')
        with get_client(sync_client=True) as client:
            values = read_variables_sync(client, [_SESSION, task_key])
        current, state = values.get(_SESSION), values.get(task_key)
        return (current is not None and current.value == self.session and
                not (state is not None and isinstance(state.value, dict) and state.value.get('state') == 'deleting'))

    async def wait(self):
        if self.stop.is_set() and not self.admitted:
            raise InterruptedError('未入场文件夹已停止等待')
        if not self.admitted and not self.closed:
            await self.parent.wait()
            return
        if not await self.running():
            raise InterruptedError('控制会话已改变，未继续原阶段的新操作')

    def wait_sync(self):
        if self.stop.is_set() and not self.admitted:
            raise InterruptedError('未入场文件夹已停止等待')
        if not self.admitted and not self.closed:
            return
        if not self.running_sync():
            raise InterruptedError('控制会话已改变，未继续原阶段的新操作')

    def mark_unknown(self, reason):
        self.unknown_reason = reason

    async def __aexit__(self, exc_type, exc, traceback):
        if self.session is None:
            self.closed = True
            return False
        try:
            reader = self.read_records or read_receipts
            records = await reader(self.child_id)
            records = [row for row in records if row.get('folder_id', self.folder_id) == self.folder_id]
            uncertain = (self.unknown_reason or not await self.running()
                         or unsettled_requests(records, self.stage)
                         or (exc is not None and not isinstance(exc, Exception)))
        except Exception:
            await self._save('unknown', '阶段结束证据待核对')
            self.closed = True
            raise
        status = stage_exit_status(exc, uncertain)
        await self._save(status, self.unknown_reason or (str(exc)[:500] if exc else None))
        self.closed = True
        if uncertain and exc is None:
            raise RuntimeError('文件夹当前阶段结果待核对，未开始下一阶段')
        return False


async def _legacy_pause_folders(task_id, snapshot=None, *, verify=True):
    """Reuse the existing projection to retain unknown outcomes from older runs."""
    if snapshot is None:
        from .parent import _children
        from .snapshots import load_task_snapshot
        snapshot = await load_task_snapshot(task_id, await _children(task_id), repair=False)
    folders = snapshot.get('folders', [])
    child_groups = {row['folder_id'] for row in folders}
    unknown = {row['group_id'] for row in snapshot.get('items', [])
               if row.get('status') == 'unresolved' or
               (row.get('status') == 'passed' and row.get('registration_status') == 'registered'
                and row['group_id'] not in child_groups)}
    for row in folders:
        stage = row.get('stage', '')
        if (stage == 'unresolved' or stage.endswith('_in_progress')
                or stage in {'waiting_nas_archive', 'waiting_nas_archive_delete', 'waiting_cleanup', 'waiting_rag_completion',
                             'waiting_nas_completion', 'waiting_cleanup_completion'}
                or row.get('failure_detail') == '完成结果尚未保存'
                or any((view.get('pending_request_count') or 0) > 0
                       for view in row.get('stage_snapshots', {}).values())):
            unknown.add(row['folder_id'])
    targets = set(snapshot.get('history_targets') or [])
    uncertain_views = {str(row['ticket_id']): row['folder_id'] for row in folders if row.get('ticket_id')
        and any(not view.get('final') and view.get('pending_request_count') is None
                for view in row.get('stage_snapshots', {}).values())}
    targets.update(uncertain_views)
    if targets:
        if not verify:
            by_ticket = {str(row.get('ticket_id')): row['folder_id'] for row in folders}
            unknown.update(by_ticket.get(str(run_id), 'task') for run_id in targets)
            return unknown
        # Missing snapshot coverage is not proof that original requests settled.
        # Inspect the existing receipt history only for runs the snapshot cannot cover.
        from .receipts import read_folder_receipts
        from paperflow_v3.delivery.progress import local_records, merge_records
        original = await read_folder_receipts(sorted(targets))
        for run_id, remote in original.items():
            records = merge_records(local_records(run_id), remote)
            if not records and str(run_id) in uncertain_views:
                unknown.add(uncertain_views[str(run_id)])
            groups = {row['folder_id'] for row in records if row.get('folder_id') != 'task'}
            for group in groups:
                selected = [row for row in records if row.get('folder_id') == group
                            and not (row['kind'] == 'child_dispatch' and group in child_groups)]
                pre = next((row for row in selected if row['kind'] in {
                    'preprocessing_started', 'preprocessing_complete', 'preprocessing_failed'}), None)
                classification = next((row for row in selected if row['kind'] in {
                    'classification_started', 'classification'}), None)
                if (any(unsettled_requests(selected, stage) for stage in STAGES)
                        or pre and (pre['kind'] == 'preprocessing_started' or
                            pre['kind'] == 'preprocessing_failed' and pre['data'].get('failure_reason')
                            not in {'local_io', 'local_parse', 'plan_unavailable'})
                        or classification and classification['kind'] == 'classification_started'):
                    unknown.add(group)
    return unknown


async def pause_progress(task_id, *, snapshot=None, verify_legacy=True):
    """Count admitted folders and retain uncertainty from pre-journal tasks."""
    rows = await _stage_rows(task_id)
    session = await Variable.aget(_SESSION) if rows else None
    unknown = [row for row in rows if row.type == UNKNOWN or row.data.get('session') != session]
    active = {row.data['folder_id'] for row in rows if row not in unknown}
    unresolved = {row.data['folder_id'] for row in unknown}
    # An active, current-session grant owns its original requests until it settles.
    if not active:
        unresolved.update(await _legacy_pause_folders(task_id, snapshot, verify=verify_legacy))
    return {'state': 'unresolved' if unresolved else 'pausing' if active else 'paused',
            'active_folders': len(active), 'unresolved_folders': len(unresolved)}


async def pause_execution_progress(task_id, *, snapshot=None, verify_legacy=True):
    """Confirm stopped execution without resolving old paper or request outcomes."""
    task_id = UUID(str(task_id))
    unknown = {'state': 'unresolved', 'active_folders': None, 'unresolved_folders': None}
    try:
        suffix = str(task_id).replace('-', '_')
        gate_name, intent_name = 'paperflow_v3_task_' + suffix, 'paperflow_v3_intent_' + suffix
        gate = await Variable.aget(gate_name)
        if not isinstance(gate, dict) or gate.get('state') != 'paused':
            return unknown
        session, intent = await Variable.aget(_SESSION), await Variable.aget(intent_name)

        async def checked(progress):
            # The dashboard and control API have different process locks. Bind
            # the observation to the durable intent as well as the gate/session;
            # paused -> running -> paused must not pass as an unchanged read.
            if (await Variable.aget(gate_name) != gate or await Variable.aget(_SESSION) != session
                    or await Variable.aget(intent_name) != intent):
                return unknown
            return progress

        async with get_client() as client:
            active_count = await client.count_flow_runs(flow_run_filter=FlowRunFilter(
                parent_flow_run_id={'any_': [task_id]},
                state={'type': {'any_': ['PENDING', 'RUNNING', 'CANCELLING', 'PAUSED']}}))
            if active_count:
                return await checked({'state': 'pausing', 'active_folders': active_count, 'unresolved_folders': 0})
            rows = await _stage_rows(task_id)
            if not rows:
                # A closed stage is a small execution witness. Bind its
                # original artifact below; do not scan old business receipts.
                rows = await client.read_latest_artifacts(artifact_filter=ArtifactCollectionFilter(
                    key={'like_': _prefix(task_id) + '%'}, type={'any_': [SETTLED]}),
                    sort=ArtifactCollectionSort.KEY_ASC, limit=1)
            source_ids = [row.latest_id for row in rows]
            originals = {}
            for offset in range(0, len(source_ids), 200):
                page = source_ids[offset:offset + 200]
                originals.update({row.id: row for row in await client.read_artifacts(
                    artifact_filter=ArtifactFilter(id={'any_': page}), limit=len(page))})
            child_ids = list({row.flow_run_id for row in rows if row.flow_run_id and row.flow_run_id != task_id})
            children = {}
            for offset in range(0, len(child_ids), 200):
                page = child_ids[offset:offset + 200]
                children.update({row.id: row for row in await client.read_flow_runs(flow_run_filter=FlowRunFilter(
                    parent_flow_run_id={'any_': [task_id]}, id={'any_': page}), limit=len(page))})
            complete = None
            if not rows or any(row.flow_run_id == task_id for row in rows):
                saved = await client.read_artifacts(artifact_filter=ArtifactFilter(flow_run_id={'any_': [task_id]},
                    key={'like_': 'paperflow-v3-' + task_id.hex + 'classificationcomplete%'},
                    type={'any_': ['paperflow-v3-receipt']}), sort=ArtifactSort.CREATED_DESC, limit=1)
                if (saved and saved[0].flow_run_id == task_id and saved[0].data.get('kind') == 'classification_complete'
                        and saved[0].data.get('task_id') == str(task_id) and saved[0].data.get('folder_id') == 'task'):
                    complete = saved[0]
            if not rows:
                if complete:
                    return await checked({'state': 'paused', 'active_folders': 0, 'unresolved_folders': 0})
                # A fresh, never-started suspended parent has no stage journal
                # or classification exit receipt yet. Do not infer this from
                # old business snapshots or an empty count alone.
                parent = await client.read_flow_run(task_id)
                if parent.id == task_id and parent.run_count == 0 and parent.state.is_paused():
                    return await checked({'state': 'paused', 'active_folders': 0, 'unresolved_folders': 0})
                return unknown
            active, unresolved = set(), set()
            for row in rows:
                data = row.data or {}
                source = originals.get(row.latest_id)
                folder = data.get('folder_id') or row.key
                if (data.get('task_id') != str(task_id) or data.get('child_flow_run_id') != str(row.flow_run_id)
                        or source is None or source.key != row.key or source.type != row.type
                        or source.flow_run_id != row.flow_run_id or source.data != data):
                    unresolved.add(folder)
                    continue
                if row.flow_run_id == task_id and data.get('stage') == 'classification':
                    if row.type == SETTLED and data.get('state') in {'complete', 'failed', 'waiting'}:
                        continue
                    # This immutable receipt is saved after the classification
                    # TaskGroup has exited. Unknown answers remain unchanged.
                    if complete and source.created < complete.created:
                        continue
                    (active if row.type == ACTIVE and data.get('session') == session else unresolved).add(folder)
                    continue
                child = children.get(row.flow_run_id)
                if (child is None or child.parameters.get('task_id') != str(task_id)
                        or child.parameters.get('group_id') != data.get('folder_id')):
                    unresolved.add(folder)
                elif row.type == SETTLED and data.get('state') in {'complete', 'failed', 'waiting'}:
                    continue
                elif (child.state.is_completed() or child.state.is_failed() or child.state.is_cancelled()) and source.created < child.state.timestamp:
                    continue
                elif child.state.is_running() or child.state.is_pending() or child.state.is_cancelling() or child.state.is_paused():
                    active.add(folder)
                else:
                    unresolved.add(folder)
            return await checked({'state': 'pausing' if active else 'unresolved' if unresolved else 'paused',
                    'active_folders': len(active), 'unresolved_folders': len(unresolved)})
    except (httpx.HTTPError, OSError, TimeoutError):
        return unknown
