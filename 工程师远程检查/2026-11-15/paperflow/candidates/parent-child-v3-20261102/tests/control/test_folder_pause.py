"""Folder stages drain as a unit; coordination never uses a real service."""
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.variables import Variable
from prefect.states import Completed, Running


@pytest.fixture
def stage_store(monkeypatch):
    try:
        from paperflow_v3.control import stage_admission as stages
    except ImportError:
        return None
    from paperflow_v3.control import admission

    values = {'paperflow_v3_control_session': 'session-1'}
    latest, history, raw = {}, [], []
    locks = {}

    async def get(name, *args, **kwargs):
        return deepcopy(values.get(name))

    async def set_value(name, value, *args, **kwargs):
        values[name] = deepcopy(value)

    @asynccontextmanager
    async def gate(task_id):
        lock = locks.setdefault((id(asyncio.get_running_loop()), str(task_id)), asyncio.Lock())
        async with lock:
            yield

    class Client:
        async def create_artifact(self, artifact):
            row = SimpleNamespace(id=uuid4(), key=artifact.key, type=artifact.type,
                                  data=deepcopy(artifact.data), flow_run_id=artifact.flow_run_id,
                                  created=datetime.now(timezone.utc))
            row.latest_id = row.id
            latest[row.key] = deepcopy(row)
            history.append(deepcopy(row))
            return row

        async def read_latest_artifacts(self, *, artifact_filter, limit=200, offset=0, **kwargs):
            selected = list(latest.values())
            keys = artifact_filter.key
            if keys and keys.like_:
                selected = [row for row in selected if row.key.startswith(keys.like_.rstrip('%'))]
            if keys and keys.any_:
                selected = [row for row in selected if row.key in keys.any_]
            if artifact_filter.type and artifact_filter.type.any_:
                selected = [row for row in selected if row.type in artifact_filter.type.any_]
            return selected[offset:offset + limit]

        async def count_flow_runs(self, **kwargs):
            parents = {str(value) for value in kwargs['flow_run_filter'].parent_flow_run_id.any_}
            return sum(row.type == stages.ACTIVE and row.data.get('task_id') in parents
                       for row in latest.values())

        async def read_artifacts(self, *, artifact_filter, **kwargs):
            if artifact_filter.id and artifact_filter.id.any_:
                return [row for row in history if row.id in artifact_filter.id.any_]
            return []

        async def read_flow_runs(self, *, flow_run_filter, **kwargs):
            rows = [row for row in latest.values() if row.flow_run_id in flow_run_filter.id.any_]
            return [SimpleNamespace(id=row.flow_run_id,
                parameters={'task_id': row.data['task_id'], 'group_id': row.data['folder_id']},
                state=Running() if row.type == stages.ACTIVE else Completed()) for row in rows]

        def read_variable_by_name(self, name):
            value = values.get(name)
            return None if value is None else SimpleNamespace(value=deepcopy(value))

        def request(self, method, path, *, json):
            assert method == 'POST' and path == '/variables/filter'
            rows=[{'name':name,'value':deepcopy(values[name])}
                  for name in json['variables']['name']['any_'] if name in values]
            return SimpleNamespace(json=lambda:rows)

    class Context:
        async def __aenter__(self):
            return Client()
        async def __aexit__(self, *args):
            pass
        def __enter__(self):
            return Client()
        def __exit__(self, *args):
            pass

    async def records(_run):
        return list(reversed(raw))

    original_legacy = stages._legacy_pause_folders

    async def legacy(task_id, snapshot=None, *, verify=True):
        if snapshot is not None:
            return await original_legacy(task_id, snapshot, verify=verify)
        selected = [row for row in reversed(raw) if row.get('task_id', str(task_id)) == str(task_id)]
        return {row['folder_id'] for row in selected if any(
            stages.unsettled_requests([row], stage) for stage in stages.STAGES)}

    monkeypatch.setattr(Variable, 'aget', staticmethod(get))
    monkeypatch.setattr(Variable, 'aset', staticmethod(set_value))
    monkeypatch.setattr(admission, 'control_gate', gate, raising=False)
    monkeypatch.setattr(stages, 'control_gate', gate)
    monkeypatch.setattr(stages, 'get_client', lambda **kwargs: Context())
    monkeypatch.setattr(stages, 'read_receipts', records)
    monkeypatch.setattr(stages, '_legacy_pause_folders', legacy)
    return SimpleNamespace(stages=stages, admission=admission, values=values, raw=raw,
                           latest=latest, history=history, gate=gate)


def identity(task=None, folder='paper-a'):
    return {'task_id': str(task or uuid4()), 'group_id': folder, 'task_run_id': str(uuid4())}


@pytest.mark.parametrize(('stage', 'retry_stage'), [('preprocessing', 'preprocessing'),
    ('delivery', 'rag'), ('delivery', 'nas'), ('delivery', 'cleanup')])
def test_existing_manual_retry_can_replace_only_an_older_unknown_stage(stage_store, stage, retry_stage):
    async def exercise():
        store, ids = stage_store, identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        previous = store.stages.FolderStage(parent, ids, stage)
        store.latest[previous.key] = SimpleNamespace(key=previous.key, type=store.stages.UNKNOWN,
            data={'folder_id': ids['group_id'], 'session': 'session-1', 'processing_attempt_id': None})
        retry_id = str(uuid4())
        store.raw.append({'kind': 'manual_retry', 'task_id': ids['task_id'], 'folder_id': ids['group_id'],
            'data': {'retry_id': retry_id, 'stage': retry_stage, 'state': 'running'}})
        async with store.stages.FolderStage(parent, ids, stage, retry_id=retry_id) as admitted:
            assert admitted.admitted
            assert store.history[-1].data['processing_attempt_id'] == retry_id
            admitted.mark_unknown('synthetic completion not confirmed')
        raise AssertionError('unknown stage must not return normally')
    with pytest.raises(RuntimeError, match='结果待核对'):
        asyncio.run(exercise())
    # The same authorization cannot replay the newly unknown retry.
    async def replay():
        previous = stage_store.history[-1]
        ids = {'task_id': previous.data['task_id'], 'group_id': previous.data['folder_id'],
               'task_run_id': previous.data['child_flow_run_id']}
        parent = stage_store.admission.Admission(ids['task_id'])
        with pytest.raises(RuntimeError, match='原阶段'):
            async with stage_store.stages.FolderStage(parent, ids, stage,
                    retry_id=previous.data['processing_attempt_id']):
                pytest.fail('same retry entered twice')
    asyncio.run(replay())


@pytest.mark.parametrize(('prior_type', 'retry_stage', 'folder'), [
    ('active', 'preprocessing', 'paper-a'), ('unknown', 'llm', 'paper-a'),
    ('unknown', 'preprocessing', 'another-folder')])
def test_manual_retry_does_not_bypass_active_or_other_stage_or_folder(stage_store, prior_type, retry_stage, folder):
    async def exercise():
        store, ids = stage_store, identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        prior = store.stages.FolderStage(parent, ids, 'preprocessing')
        store.latest[prior.key] = SimpleNamespace(key=prior.key,
            type=store.stages.ACTIVE if prior_type == 'active' else store.stages.UNKNOWN,
            data={'folder_id': ids['group_id'], 'session': 'session-1'})
        retry_id = str(uuid4())
        store.raw.append({'kind': 'manual_retry', 'task_id': ids['task_id'], 'folder_id': folder,
            'data': {'retry_id': retry_id, 'stage': retry_stage, 'state': 'running'}})
        with pytest.raises(RuntimeError, match='原阶段'):
            async with store.stages.FolderStage(parent, ids, 'preprocessing', retry_id=retry_id):
                pytest.fail('invalid retry entered')
    asyncio.run(exercise())


def test_paused_folder_finishes_body_and_all_attachments_before_next_gate(stage_store):
    assert stage_store is not None, 'folder-level stage admission is missing'
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        work = []
        async with store.stages.FolderStage(parent, ids, 'llm') as admitted:
            work.append('body')
            await parent.pause()
            assert await admitted.running()
            assert admitted.running_sync()
            work.extend(['attachment-1', 'attachment-2'])
            assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'pausing'
        assert work == ['body', 'attachment-1', 'attachment-2']
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'paused'
        entered = asyncio.Event()

        async def next_stage():
            async with store.stages.FolderStage(parent, ids, 'postprocessing'):
                entered.set()

        queued = asyncio.create_task(next_stage())
        await asyncio.sleep(.03)
        assert not entered.is_set()
        await parent.start()
        await asyncio.wait_for(queued, 2)
        assert entered.is_set()
    asyncio.run(exercise())


def test_waiting_for_first_stage_resource_does_not_enter_after_pause(stage_store, monkeypatch):
    assert stage_store is not None, 'folder-level stage admission is missing'
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        available, waiting, started = asyncio.Event(), asyncio.Event(), asyncio.Event()

        @asynccontextmanager
        async def resource():
            waiting.set()
            await available.wait()
            yield

        async def work():
            async with store.stages.FolderStage(parent, ids, 'llm', deferred=True) as phase:
                async with store.admission.admitted_slot(phase, ['synthetic']):
                    started.set()

        @asynccontextmanager
        async def capacity(*args, **kwargs):
            async with resource():
                yield
        monkeypatch.setattr(store.admission, 'concurrency', capacity)

        queued = asyncio.create_task(work())
        await waiting.wait()
        await parent.pause()
        available.set()
        await asyncio.sleep(.03)
        assert not started.is_set()
        assert not store.history
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'paused'
        await parent.start()
        await asyncio.wait_for(queued, 2)
    asyncio.run(exercise())


def test_only_the_folder_holding_real_capacity_is_admitted_before_pause(stage_store, monkeypatch):
    async def exercise():
        store = stage_store
        first, second = identity(), identity(folder='paper-b')
        second['task_id'] = first['task_id']
        parent = store.admission.Admission(first['task_id'])
        await parent.start()
        resource = asyncio.Semaphore(1)
        entered, release = asyncio.Event(), asyncio.Event()
        started = []

        @asynccontextmanager
        async def capacity(*_args, **_kwargs):
            async with resource:
                yield

        monkeypatch.setattr(store.admission, 'concurrency', capacity)

        async def work(ids):
            async with store.stages.FolderStage(parent, ids, 'llm', deferred=True) as phase:
                async with store.admission.admitted_slot(phase, ['model-one']):
                    started.append(ids['task_run_id'])
                    if ids is first:
                        entered.set()
                        await release.wait()

        a = asyncio.create_task(work(first))
        await asyncio.wait_for(entered.wait(), 1)
        b = asyncio.create_task(work(second))
        await asyncio.sleep(.02)
        await parent.pause()
        release.set()
        await a
        await asyncio.sleep(.03)
        assert started == [first['task_run_id']]
        assert not b.done()
        assert (await store.stages.pause_progress(first['task_id']))['state'] == 'paused'
        await parent.start()
        await asyncio.wait_for(b, 1)
    asyncio.run(exercise())


def test_concurrent_folder_branches_share_only_a_persisted_admission(stage_store, monkeypatch):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        phase = store.stages.FolderStage(parent, ids, 'llm', deferred=True)
        await phase.__aenter__()
        saving, release = asyncio.Event(), asyncio.Event()
        original = phase._save
        async def slow_save(state, reason=None):
            if state == 'started':
                saving.set()
                await release.wait()
            return await original(state, reason)
        monkeypatch.setattr(phase, '_save', slow_save)
        a = asyncio.create_task(phase.admit())
        await saving.wait()
        b = asyncio.create_task(phase.admit())
        await asyncio.sleep(.02)
        assert not phase.admitted and not b.done()
        release.set()
        assert await a and await b
        assert sum(row.type == store.stages.ACTIVE for row in store.history) == 1
        await phase.__aexit__(None, None, None)
    asyncio.run(exercise())


def test_cancelling_a_folder_still_in_queue_creates_no_stage_journal(stage_store):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        waiting = asyncio.Event()
        async def queued():
            async with store.stages.FolderStage(parent, ids, 'delivery', deferred=True):
                waiting.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(queued())
        await waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert store.history == []
        await parent.pause()
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'paused'
    asyncio.run(exercise())


def test_failed_start_save_cannot_be_retried_by_another_folder_branch(stage_store, monkeypatch):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        phase = store.stages.FolderStage(parent, ids, 'llm', deferred=True)
        await phase.__aenter__()
        saving, release = asyncio.Event(), asyncio.Event()
        attempts = []
        async def failed_save(*args):
            attempts.append(args)
            saving.set()
            await release.wait()
            raise OSError('synthetic lost admission acknowledgement')
        monkeypatch.setattr(phase, '_save', failed_save)
        a = asyncio.create_task(phase.admit())
        await saving.wait()
        b = asyncio.create_task(phase.admit())
        await asyncio.sleep(.01)
        release.set()
        outcomes = await asyncio.gather(a, b, return_exceptions=True)
        assert isinstance(outcomes[0], OSError) and isinstance(outcomes[1], RuntimeError)
        assert len(attempts) == 1 and not phase.admitted
    asyncio.run(exercise())


def test_entry_receipt_and_pause_share_one_control_boundary(stage_store, monkeypatch):
    assert stage_store is not None, 'folder-level stage admission is missing'
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        entry = store.stages.FolderStage(parent, ids, 'classification')
        entered, release = asyncio.Event(), asyncio.Event()

        async def work():
            async with entry:
                entered.set()
                await release.wait()

        async with store.gate(ids['task_id']):
            queued = asyncio.create_task(work())
            pause = asyncio.create_task(parent.pause())
            await asyncio.sleep(.02)
            assert not entered.is_set() and not pause.done()
        await pause
        await entered.wait()
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'pausing'
        release.set()
        await queued
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'paused'
    asyncio.run(exercise())


def test_new_control_session_revokes_old_stage_permission(stage_store):
    assert stage_store is not None, 'folder-level stage admission is missing'
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        phase = store.stages.FolderStage(parent, ids, 'llm')
        await phase.__aenter__()
        store.values['paperflow_v3_control_session'] = 'session-2'
        assert not await phase.running()
        assert not phase.running_sync()
        with pytest.raises(InterruptedError):
            await phase.wait()
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'unresolved'
    asyncio.run(exercise())


def test_session_change_during_admission_cannot_upgrade_an_old_start(stage_store, monkeypatch):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        original = parent.running
        checks = 0

        async def restart_after_check():
            nonlocal checks
            checks += 1
            result = await original()
            if checks == 2:
                store.values['paperflow_v3_control_session'] = 'session-2'
            return result

        monkeypatch.setattr(parent, 'running', restart_after_check)
        with pytest.raises(InterruptedError):
            async with store.stages.FolderStage(parent, ids, 'llm'):
                pytest.fail('old start was upgraded to the new session')
        assert not store.history
    asyncio.run(exercise())


@pytest.mark.parametrize('kind,data,stage', [
    ('folder_completion_pending', {'stage': 'cleanup'}, 'delivery'),
    ('postprocessing_component_failed', {'component_id': 'body', 'error_type': 'TimeoutError'}, 'postprocessing'),
])
def test_unconfirmed_result_cannot_confirm_pause(stage_store, kind, data, stage):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        with pytest.raises(OSError):
            async with store.stages.FolderStage(parent, ids, stage):
                store.raw.append({'task_id': ids['task_id'], 'folder_id': ids['group_id'],
                                  'kind': kind, 'data': data})
                raise OSError('completion not confirmed')
        await parent.pause()
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'unresolved'
    asyncio.run(exercise())


def test_original_unknown_is_preserved_without_any_new_stage_journal(stage_store):
    async def exercise():
        store = stage_store
        ids = identity()
        store.raw.append({'task_id': ids['task_id'], 'folder_id': ids['group_id'],
                          'kind': 'llm_request_unknown', 'data': {'component_id': 'body', 'request_id': 'original'}})
        assert not store.latest
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'unresolved'
        snapshot = {'folders': [{'folder_id': ids['group_id'], 'stage': 'unresolved',
                                 'stage_snapshots': {'llm': {'pending_request_count': 1}}}]}
        assert (await store.stages.pause_progress(ids['task_id'], snapshot=snapshot))['state'] == 'unresolved'
    asyncio.run(exercise())


def test_revoked_folder_permission_does_not_wait_forever_for_sync_or_async_resources(stage_store):
    from paperflow_v3.stages.pipeline import _FolderAdmission
    from threading import Event
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        phase = store.stages.FolderStage(parent, ids, 'llm')
        await phase.__aenter__()
        permission = _FolderAdmission(phase, Event())
        store.values['paperflow_v3_control_session'] = 'session-2'
        with pytest.raises(InterruptedError):
            await asyncio.wait_for(permission.wait(), .1)
        with pytest.raises(InterruptedError):
            permission.wait_sync()
        with pytest.raises(InterruptedError):
            with store.admission.admitted_sync_slot(phase, ['unused']):
                pytest.fail('revoked folder obtained a resource slot')
    asyncio.run(exercise())


def test_sync_resource_wait_rechecks_revocation_after_its_first_check(stage_store, monkeypatch):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        phase = store.stages.FolderStage(parent, ids, 'delivery')
        await phase.__aenter__()
        original = phase.running_sync
        checks = 0
        def running():
            nonlocal checks
            checks += 1
            result = original()
            if checks == 1:
                store.values['paperflow_v3_control_session'] = 'new-session'
            return result
        def bounded_sleep(_seconds):
            assert checks < 8, 'revoked delivery kept waiting for admission'
        monkeypatch.setattr(phase, 'running_sync', running)
        monkeypatch.setattr(store.admission.time, 'sleep', bounded_sleep)
        with pytest.raises(InterruptedError):
            with store.admission.admitted_sync_slot(phase, ['unused']):
                pytest.fail('revoked folder entered shared resource')
    asyncio.run(exercise())


def test_failed_folder_can_settle_but_unknown_request_cannot(stage_store):
    assert stage_store is not None, 'folder-level stage admission is missing'
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        with pytest.raises(ValueError):
            async with store.stages.FolderStage(parent, ids, 'preprocessing'):
                raise ValueError('unreadable synthetic file')
        await parent.pause()
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'paused'
        await parent.start()
        with pytest.raises(RuntimeError):
            async with store.stages.FolderStage(parent, ids, 'llm'):
                store.raw.append({'folder_id': ids['group_id'], 'kind': 'llm_request_unknown',
                    'data': {'request_id': 'original', 'component_id': 'body'}})
                raise RuntimeError('original request outcome unknown')
        await parent.pause()
        assert (await store.stages.pause_progress(ids['task_id']))['state'] == 'unresolved'
    asyncio.run(exercise())


def test_task_pause_and_stage_records_are_isolated(stage_store):
    assert stage_store is not None, 'folder-level stage admission is missing'
    async def exercise():
        store = stage_store
        first, second = identity(), identity()
        a = store.admission.Admission(first['task_id'])
        b = store.admission.Admission(second['task_id'])
        await a.start()
        await b.start()
        async with store.stages.FolderStage(b, second, 'delivery'):
            await a.pause()
            assert await b.running()
            assert (await store.stages.pause_progress(first['task_id']))['state'] == 'paused'
            assert (await store.stages.pause_progress(second['task_id']))['state'] == 'pausing'
    asyncio.run(exercise())


def test_pause_api_and_operation_lookup_wait_for_the_folder_to_settle(stage_store, monkeypatch):
    assert stage_store is not None
    import httpx
    from test_operation_confirmation import control_app, TASK_ID

    async def exercise():
        store = stage_store
        async def ready(_settings):
            return {'ready': True, 'checks': []}
        app = control_app(monkeypatch, ready, variables=store.values, stage_backend=True)
        ids = identity(TASK_ID)
        parent = store.admission.Admission(TASK_ID)
        await parent.start()
        request_id = str(uuid4())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://isolated') as client:
            async with store.stages.FolderStage(parent, ids, 'classification'):
                response = await client.post(f'/api/v1/prefect/tasks/{TASK_ID}/pause',
                    json={'request_id': request_id, 'intent_seq': 1})
                assert response.status_code == 200
                assert response.json()['resolved'] is False
                progress = (await client.get(f'/api/v1/prefect/operations/{request_id}')).json()
                assert progress['resolved'] is False and progress['pause_state'] == 'pausing'
            progress = (await client.get(f'/api/v1/prefect/operations/{request_id}')).json()
            assert progress['resolved'] is True and progress['pause_state'] == 'paused'
    asyncio.run(exercise())


@pytest.mark.parametrize('damage', ['none', 'missing-original', 'changed-latest', 'changed-original-key', 'wrong-source-id'])
def test_execution_pause_requires_immutable_stage_proof(stage_store, damage):
    async def exercise():
        store = stage_store
        ids = identity()
        parent = store.admission.Admission(ids['task_id'])
        await parent.start()
        async with store.stages.FolderStage(parent, ids, 'postprocessing'):
            pass
        await parent.pause()
        saved = next(iter(store.latest.values()))
        if damage == 'missing-original':
            store.history.clear()
        elif damage == 'changed-latest':
            saved.data['folder_id'] = 'changed-folder'
        elif damage == 'changed-original-key':
            store.history[-1].key = 'another-stage'
        elif damage == 'wrong-source-id':
            saved.latest_id = uuid4()
        result = await store.stages.pause_execution_progress(ids['task_id'])
        assert result['state'] == ('paused' if damage == 'none' else 'unresolved')
        assert result['active_folders'] == 0
        assert result['unresolved_folders'] == (0 if damage == 'none' else 1)
    asyncio.run(exercise())


def test_task_projection_cannot_call_an_active_folder_paused():
    from prefect.states import Running
    from paperflow_v3.control.query_service import _view_task_state
    run = SimpleNamespace(state=Running(), run_count=1)
    snapshot = {'pause_progress': {'state': 'pausing'}}
    assert _view_task_state(run, False, snapshot) == 'pausing'
    snapshot['pause_progress'] = {'state': 'unresolved'}
    assert _view_task_state(run, False, snapshot) == 'unresolved'


@pytest.mark.parametrize('changes', [{'limit': 2}, {'active': False}, {'slot_decay_per_second': .5}])
def test_native_control_gate_rejects_a_configuration_that_cannot_exclude_other_workers(monkeypatch, changes):
    from paperflow_v3.control import admission
    fields = {'limit': 1, 'active': True, 'slot_decay_per_second': 0}
    fields.update(changes)
    class Client:
        async def read_global_concurrency_limit_by_name(self, _name):
            return SimpleNamespace(**fields)
    @asynccontextmanager
    async def client():
        yield Client()
    @asynccontextmanager
    async def must_not_acquire(*args, **kwargs):
        pytest.fail('misconfigured control gate was acquired')
        yield
    monkeypatch.setattr(admission, 'get_client', client)
    monkeypatch.setattr(admission, 'concurrency', must_not_acquire)
    async def exercise():
        with pytest.raises(RuntimeError, match='准入控制额度无效'):
            async with admission.control_gate(uuid4()):
                pytest.fail('business was admitted')
    asyncio.run(exercise())
