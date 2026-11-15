"""Exercise the delivery entry with isolated stage slots and synthetic operations."""

import copy
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Semaphore
from types import SimpleNamespace
from uuid import uuid4

import pytest



def observe_progress(monkeypatch, runtime, observer):
    original = runtime.FolderProgress.save
    def save(progress, kind, data, event_id=None):
        saved = observer(task_id=progress.identity['task_id'], folder_id=progress.identity['group_id'],
                         child_flow_run_id=progress.identity['task_run_id'], event_id=event_id,
                         kind=kind, data=data)
        return original(progress, kind, data, event_id) if saved else saved
    monkeypatch.setattr(runtime.FolderProgress, 'save', save)

@pytest.fixture
def delivery_harness(tmp_path, monkeypatch):
    import paperflow.package as native_package
    import paperflow_v3.delivery.publication as publication
    import paperflow_v3.delivery.runtime as runtime

    work = tmp_path / 'work'
    work.mkdir()
    stage_file = work / 'stage.json'
    stage_file.write_text('{}', encoding='utf8')
    paths = {'data_root': work, 'catalog_path': tmp_path / 'catalog.json',
             'nas_destination': tmp_path / 'nas', 'release_root': tmp_path / 'release'}
    settings = SimpleNamespace(values={}, path=lambda key: paths[key])
    route = {'code': '080901', 'name': 'Synthetic', 'paper_dataset_id': 'paper-ds',
             'image_dataset_id': 'attachment-ds'}
    packet = {'routes': [route], 'manifest_sha256': 'manifest',
              'profile': {'routing_candidates': [{'code': route['code']}]}, 'assets': []}
    records = {}

    def body_target(identity):
        return {'body_delivery_state': 'complete', 'verified': True, 'retrieval_verified': True,
                'formal_route': copy.deepcopy(route), 'ticket_identity': copy.deepcopy(identity),
                'body': {'dataset_id': route['paper_dataset_id'], 'document_id': 'document',
                         'identity': {'paperflow_source_sha256': 'source', 'paperflow_specialty': route['code']}}}

    def arguments(name, *, rag_done=False, nas_done=False):
        identity = {'task_id': 'task', 'group_id': name, 'paper_id': name, 'receipt_id': 'receipt-' + name,
                    'task_run_id': 'child-' + name, 'attempt_id': 'attempt', 'parent_run_id': 'parent'}
        owned = work / 'paperflow-v3' / 'task' / name
        owned.mkdir(parents=True)
        local_file = owned / 'stage.json'
        local_file.write_text('{}', encoding='utf8')
        paper = {'paper_id': name, 'group_id': name, 'person_dir': str(tmp_path / ('source-' + name)),
                 'body': {'file_id': 'body', 'sha256': 'source'}, 'attachments': [],
                 'archive_files': [{'file_id': 'body', 'relative': 'body.docx', 'path': str(local_file),
                                   'source_path': str(tmp_path / ('source-' + name) / 'body.docx')}]}
        handoff = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': identity,
                   'validation_status': 'passed', 'body': {'package_path': str(owned / 'publication'),
                                                          'manifest_sha256': 'manifest'}, 'attachments': []}
        rows = []
        if rag_done:
            rows.append({'task_id': 'task', 'folder_id': name, 'kind': 'rag-body',
                         'data': {route['code']: body_target(identity)}})
        if nas_done:
            rows.append({'task_id': 'task', 'folder_id': name, 'kind': 'nas', 'data': {
                'identity': copy.deepcopy(identity), 'status': 'complete',
                'files': {'body': {'status': 'complete', 'destination': str(tmp_path / 'nas' / 'task' / name / 'body.docx')}}}})
            for kind in ('rag_complete', 'nas'):
                rows.append({'task_id': 'task', 'folder_id': name, 'kind': kind, 'folder_completion': True,
                             'data': {'identity': copy.deepcopy(identity), 'status': 'complete'}})
        records[identity['task_run_id']] = rows
        registered = {**{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                      'paper': paper}
        return dict(identity=identity, registered=registered, settings=settings,
                    admission=SimpleNamespace(running_sync=lambda: True),
                    limits={'rag': ['rag'], 'nas': ['nas'], 'cleanup': ['cleanup']},
                    first_step_work_root=work / 'first-step' / 'task'), handoff

    monkeypatch.setattr(runtime.legacy, 'bind', lambda _: None)
    monkeypatch.setattr(runtime.legacy, 'rag_client', lambda _: object())
    monkeypatch.setattr(runtime, 'read_receipts_sync', lambda run_id: copy.deepcopy(records.get(run_id, [])))
    monkeypatch.setattr(runtime, 'save_folder_completion', lambda **_: 'synthetic-artifact')
    monkeypatch.setattr(runtime, '_registration', lambda *_args: tmp_path / 'first-step')
    monkeypatch.setattr(runtime, 'folder_settings', lambda _s, task, group, _p: SimpleNamespace(
        values={}, path=lambda key: Path(_p['archive_files'][0]['path']).parent if key == 'data_root' else paths[key]))
    monkeypatch.setattr(runtime, '_work_paths', lambda *_args: None)
    monkeypatch.setattr(runtime, 'owned_work_files', lambda roots, *_args: [str(Path(roots[-1]) / 'stage.json')])
    monkeypatch.setattr(runtime, 'read_json', lambda *_args: {'majors': [route]})
    monkeypatch.setattr(runtime, 'read_complete_handoff', lambda identity, paper, _records: {
        'handoff': {'body': {'package_path': str(Path(paper['archive_files'][0]['path']).parent / 'publication'), 'manifest_sha256': 'manifest'}},
        'body_result': {'task': {'source_sha256': 'source'}, 'validation': {},
                        'package_path': str(Path(paper['archive_files'][0]['path']).parent / 'publication')},
        'stage_work_files': [str(Path(paper['archive_files'][0]['path']))]})
    monkeypatch.setattr(runtime, 'UnifiedBackend', lambda *_args, **_kwargs: SimpleNamespace(root=work))
    monkeypatch.setattr(native_package, 'validate_package', lambda *_args, **_kwargs: packet)
    monkeypatch.setattr(publication, 'publish_body_targets', lambda paper, identity, handoff, prepared, packet,
                        client, targets, save, check, **_kwargs: targets.update({route['code']: body_target(identity)}))
    return runtime, publication, arguments, body_target, route, records


def test_local_stage_progress_tracks_waiting_and_releases_each_slot(delivery_harness, monkeypatch):
    from paperflow_v3.control.folder_status import folder_status

    runtime, publication, arguments, body_target, route, _records = delivery_harness
    args, _ = arguments('A')
    slots = {'rag': Semaphore(1), 'nas': Semaphore(1), 'cleanup': Semaphore(1)}
    slots['rag'].acquire()
    slots['nas'].acquire()
    slots['cleanup'].acquire()
    rag_held = nas_held = True
    cleanup_held = True
    stages = []
    signals = {name: Event() for name in ('waiting_rag_upload', 'rag_in_progress',
        'waiting_nas_archive', 'nas_in_progress', 'waiting_cleanup', 'cleanup_in_progress')}
    cleanup_entered, finish_cleanup, allow_cleanup_wait = Event(), Event(), Event()
    upstream = [{'kind': 'postprocessing_complete', 'data': {'validation_status': 'passed'}},
                {'kind': 'llm_folder_complete', 'data': {'status': 'raw_complete', 'ranking_complete': True}},
                {'kind': 'preprocessing_complete', 'data': {}}]

    def projected_stage():
        records = [{'kind': 'delivery_stage', 'data': {'identity': args['identity'], 'stage': stage}}
                   for stage in reversed(stages)]
        return folder_status(records + upstream, is_running=True, is_active=True,
                             identity=args['identity'])['stage']

    @contextmanager
    def slot(_admission, names):
        with slots[names[0]]:
            yield

    def save(**row):
        if row['kind'] == 'delivery_stage':
            assert row['child_flow_run_id'] == args['identity']['task_run_id']
            assert row['data']['identity'] == args['identity']
            stages.append(row['data']['stage'])
            signals[row['data']['stage']].set()
            if row['data']['stage'] == 'waiting_cleanup':
                assert allow_cleanup_wait.wait(5)
        return 'synthetic-artifact'

    def archive(_paper, identity, _handoff, _destination, state, _save, _check, **_kwargs):
        state.update(identity=identity, status='complete', files={})

    def cleanup(*_args, **_kwargs):
        _args[8].update(identity=_args[1], status='complete', files={})
        cleanup_entered.set()
        assert finish_cleanup.wait(5)

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(runtime, 'save_archive', archive)
    monkeypatch.setattr(runtime, 'cleanup_paper', cleanup)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runtime.deliver_paper, **args)
        try:
            assert signals['waiting_rag_upload'].wait(5)
            assert not signals['rag_in_progress'].is_set()
            assert projected_stage() == 'waiting_rag_upload'
            slots['rag'].release()
            rag_held = False
            assert signals['waiting_nas_archive'].wait(5)
            assert projected_stage() == 'waiting_nas_archive_delete'
            assert slots['rag'].acquire(blocking=False), 'waiting for NAS must not occupy RAG'
            slots['rag'].release()
            assert not signals['nas_in_progress'].is_set()
            slots['nas'].release()
            nas_held = False
            assert signals['waiting_cleanup'].wait(5)
            assert projected_stage() == 'waiting_cleanup'
            assert slots['nas'].acquire(blocking=False), 'archive must release NAS before waiting for cleanup'
            nas_held = True
            allow_cleanup_wait.set()
            assert not signals['cleanup_in_progress'].wait(0.2), 'cleanup must wait for its own capacity'
            slots['cleanup'].release()
            cleanup_held = False
            assert cleanup_entered.wait(5)
            assert signals['cleanup_in_progress'].is_set()
            assert projected_stage() == 'cleanup_in_progress'
            assert not slots['cleanup'].acquire(blocking=False), 'cleanup must hold its own slot'
        finally:
            if rag_held:
                slots['rag'].release()
            if nas_held:
                slots['nas'].release()
            if cleanup_held:
                slots['cleanup'].release()
            allow_cleanup_wait.set()
            finish_cleanup.set()
        assert future.result(timeout=5)['status'] == 'complete'
    assert slots['cleanup'].acquire(blocking=False), 'cleanup must release its own slot'
    slots['cleanup'].release()
    assert stages == ['waiting_rag_upload', 'rag_in_progress', 'waiting_nas_archive',
                      'nas_in_progress', 'waiting_cleanup', 'cleanup_in_progress']


@pytest.mark.parametrize(('rejected', 'expected'), [
    ('waiting_rag_upload', []), ('rag_in_progress', []),
    ('waiting_nas_archive', ['rag']), ('nas_in_progress', ['rag']),
    ('waiting_cleanup', ['rag', 'nas']), ('cleanup_in_progress', ['rag', 'nas']),
])
def test_unsaved_stage_receipt_blocks_next_effect(delivery_harness, monkeypatch, rejected, expected):
    runtime, publication, arguments, body_target, route, _records = delivery_harness
    args, _ = arguments('A')
    effects = []

    @contextmanager
    def slot(_admission, _names):
        yield

    def save(**row):
        if row['kind'] == 'delivery_stage' and row['data']['stage'] == rejected:
            return None
        return 'synthetic-artifact'

    def publish(_paper, identity, _handoff, _prepared, _packet, _client, targets, _save, _check, **_kwargs):
        effects.append('rag')
        targets[route['code']] = body_target(identity)

    def archive(_paper, identity, _handoff, _destination, state, _save, _check, **_kwargs):
        effects.append('nas')
        state.update(identity=identity, status='complete', files={})

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(publication, 'publish_body_targets', publish)
    monkeypatch.setattr(runtime, 'save_archive', archive)
    monkeypatch.setattr(runtime, 'cleanup_paper', lambda *_args, **_kwargs: effects.append('cleanup'))
    with pytest.raises(ValueError, match='local delivery state'):
        runtime.deliver_paper(**args)
    assert effects == expected


def test_incomplete_rag_does_not_enter_archive_waiting(delivery_harness, monkeypatch):
    runtime, publication, arguments, _body_target, _route, _records = delivery_harness
    args, _ = arguments('A')
    stages = []

    @contextmanager
    def slot(_admission, _names):
        yield

    def save(**row):
        if row['kind'] == 'delivery_stage':
            stages.append(row['data']['stage'])
        return 'synthetic-artifact'

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(publication, 'publish_body_targets', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, 'save_archive', lambda *_args, **_kwargs: pytest.fail('archive must not start'))
    with pytest.raises(ValueError, match='RAG.*complete'):
        runtime.deliver_paper(**args)
    assert stages == ['waiting_rag_upload', 'rag_in_progress']


@pytest.mark.parametrize('interruption', ['pause', 'restart', 'completion', 'network_unknown'])
def test_whole_delivery_gate_drains_pause_but_keeps_revocation_and_completion_unknown(
        delivery_harness, monkeypatch, stage_store, tmp_path, interruption):
    import json
    import httpx
    from paperflow_v3.control import flows
    runtime, publication, arguments, body_target, route, records = delivery_harness
    args, _ = arguments('A')
    ids = args['identity']
    old_run = ids['task_run_id']
    ids.update(task_id=str(uuid4()), task_run_id=str(uuid4()))
    records[ids['task_run_id']] = records.pop(old_run)
    config = tmp_path / 'isolated-config.json'
    config.write_text(json.dumps({'data_root': str(args['settings'].path('data_root'))}), encoding='utf8')
    monkeypatch.setenv('PAPERFLOW_V3_PROJECT_CONFIG', str(config))
    parent = stage_store.admission.Admission(ids['task_id'])
    effects = []

    @contextmanager
    def resource(stage, _limits):
        stage.wait_sync()
        assert stage.admit_sync()
        yield

    async def remote(run_id):
        return records.get(str(run_id), [])

    async def exercise():
        await parent.start()
        loop = asyncio.get_running_loop()

        def publish(_paper, identity, _handoff, _prepared, _packet, _client, targets, _save, check, **_kwargs):
            assert check()
            effects.append('rag')
            if interruption == 'network_unknown':
                raise httpx.ReadTimeout('synthetic lost upload reply')
            targets[route['code']] = body_target(identity)
            if interruption == 'restart':
                stage_store.values['paperflow_v3_control_session'] = 'new-session'
            else:
                asyncio.run_coroutine_threadsafe(parent.pause(), loop).result(2)
                assert check(), 'ordinary pause revoked the admitted delivery folder'

        def archive(_paper, identity, _handoff, _destination, state, _save, check, **_kwargs):
            assert check()
            effects.append('nas')
            state.update(identity=identity, status='complete', files={})

        def cleanup(*values, **_kwargs):
            assert values[10]()
            effects.append('cleanup')
            values[8].update(identity=ids, status='complete', files={})

        def completed(**row):
            if interruption == 'completion' and row['kind'] == 'cleanup':
                raise OSError('synthetic final completion save failure')
            return 'saved'

        monkeypatch.setattr(publication, 'publish_body_targets', publish)
        monkeypatch.setattr(runtime, 'save_archive', archive)
        monkeypatch.setattr(runtime, 'cleanup_paper', cleanup)
        monkeypatch.setattr(runtime, 'save_folder_completion', completed)
        operation = flows._delivery_step(ids, args['registered'], args['settings'], parent,
            args['limits'], args['first_step_work_root'], False)
        if interruption in {'restart', 'completion', 'network_unknown'}:
            with pytest.raises((InterruptedError, OSError, httpx.ReadTimeout)):
                await operation
        else:
            assert (await operation)['status'] == 'complete'
        assert effects == (['rag'] if interruption in {'restart', 'network_unknown'} else ['rag', 'nas', 'cleanup'])
        assert (await stage_store.stages.pause_progress(ids['task_id']))['state'] == (
            'paused' if interruption == 'pause' else 'unresolved')

    monkeypatch.setattr(flows, 'read_receipts', remote)
    monkeypatch.setattr(runtime, 'admitted_sync_slot', resource)
    asyncio.run(exercise())


def test_rag_and_archive_progress_independently_between_papers(delivery_harness, monkeypatch):
    runtime, publication, arguments, body_target, route, _records = delivery_harness
    a, _ = arguments('A')
    b, _ = arguments('B')
    slots = {'rag': Semaphore(1), 'nas': Semaphore(1), 'cleanup': Semaphore(1)}
    a_nas, b_rag, b_nas, a_cleanup = Event(), Event(), Event(), Event()
    finish_a_nas, finish_a_cleanup = Event(), Event()

    @contextmanager
    def slot(_admission, names):
        with slots[names[0]]:
            yield

    def publish(paper, identity, handoff, prepared, packet, client, targets, save, check, **_kwargs):
        targets[route['code']] = body_target(identity)
        if paper['paper_id'] == 'B':
            b_rag.set()

    def archive(paper, identity, handoff, destination, state, save, check, **_kwargs):
        if paper['paper_id'] == 'A':
            a_nas.set()
            assert finish_a_nas.wait(5)
        else:
            b_nas.set()
        state.update(identity=identity, status='complete', files={})

    def cleanup(paper, *args, **kwargs):
        args[7].update(identity=args[0], status='complete', files={})
        if paper['paper_id'] == 'A':
            a_cleanup.set()
            assert finish_a_cleanup.wait(5)

    def save(**row):
        if (row['kind'] == 'delivery_stage' and row['folder_id'] == 'A'
                and row['data']['stage'] == 'waiting_cleanup'):
            assert b_nas.wait(5), 'another paper can use NAS after A archives'
        return 'synthetic-artifact'

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(publication, 'publish_body_targets', publish)
    monkeypatch.setattr(runtime, 'save_archive', archive)
    monkeypatch.setattr(runtime, 'cleanup_paper', cleanup)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(runtime.deliver_paper, **a)
        try:
            assert a_nas.wait(5)
            second = pool.submit(runtime.deliver_paper, **b)
            assert b_rag.wait(5), 'B must enter RAG while A is waiting in NAS'
            finish_a_nas.set()
            assert b_nas.wait(5), 'B must enter NAS after A releases its archive slot'
            assert a_cleanup.wait(5)
        finally:
            finish_a_nas.set()
            finish_a_cleanup.set()
        assert first.result(timeout=5)['status'] == 'complete'
        assert second.result(timeout=5)['status'] == 'complete'


@pytest.mark.parametrize('nas_done', [False, True])
def test_completed_stages_do_not_reacquire_slots_on_resume(delivery_harness, monkeypatch, nas_done):
    runtime, _publication, arguments, _body_target, _route, _records = delivery_harness
    args, _ = arguments('A', rag_done=True, nas_done=nas_done)
    slots = {'rag': Semaphore(1), 'nas': Semaphore(1), 'cleanup': Semaphore(1)}
    slots['rag'].acquire()
    next_stage = Event()
    entered = []

    @contextmanager
    def slot(_admission, names):
        with slots[names[0]]:
            entered.append(names[0])
            yield

    def archive(_paper, identity, _handoff, _destination, state, _save, _check, **_kwargs):
        next_stage.set()
        state.update(identity=identity, status='complete', files={})

    def cleanup(*_args, **_kwargs):
        _args[8].update(identity=_args[1], status='complete', files={})
        next_stage.set()

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    monkeypatch.setattr(runtime, 'save_archive', archive)
    monkeypatch.setattr(runtime, 'cleanup_paper', cleanup)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runtime.deliver_paper, **args)
        try:
            progressed_without_completed_slots = next_stage.wait(2)
        finally:
            slots['rag'].release()
        assert future.result(timeout=5)['status'] == 'complete'
    assert progressed_without_completed_slots, 'resumption waited for an already completed stage'
    assert entered == (['cleanup'] if nas_done else ['nas', 'cleanup'])


def test_resuming_partial_cleanup_waits_for_its_own_slot(delivery_harness, monkeypatch):
    runtime, _publication, arguments, body_target, route, records = delivery_harness
    args, handoff = arguments('A', rag_done=True, nas_done=True)
    identity = args['identity']
    archive = next(row['data'] for row in records['child-A'] if row['kind'] == 'nas')
    records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A', 'kind': 'postprocessing_complete',
                                  'data': handoff})
    records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A', 'kind': 'cleanup', 'data': {
        'identity': identity, 'status': 'incomplete', 'routes': [route],
        'body_targets': {route['code']: body_target(identity)}, 'attachment_receipts': {},
        'archive': archive, 'files': {args['registered']['paper']['archive_files'][0]['path']: {'status': 'pending'}}}})
    nas = Semaphore(1)
    nas.acquire()
    waiting, cleaning = Event(), Event()
    stages = []

    @contextmanager
    def slot(_admission, names):
        assert names == ['cleanup']
        with nas:
            yield

    def save(**row):
        if row['kind'] == 'delivery_stage':
            stages.append(row['data']['stage'])
            if row['data']['stage'] == 'waiting_cleanup':
                waiting.set()
        return 'synthetic-artifact'

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(runtime, 'cleanup_paper', lambda *args, **kwargs: (args[8].update(identity=args[1], status='complete', files={}), cleaning.set()))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(runtime.deliver_paper, **args)
        try:
            assert waiting.wait(5)
            assert not cleaning.wait(0.2)
        finally:
            nas.release()
        assert future.result(timeout=5)['status'] == 'complete'
    assert cleaning.is_set()
    assert stages == ['waiting_cleanup', 'cleanup_in_progress']


@pytest.mark.parametrize(('stage', 'status', 'reason'), [
    ('nas', 'in_flight', 'unknown'), ('nas', 'unknown', 'unknown'), ('nas', 'failed', 'retry'),
    ('cleanup', 'in_flight', 'unknown'), ('cleanup', 'unknown', 'unknown'), ('cleanup', 'failed', 'retry'),
])
def test_unresolved_file_effect_never_appears_as_a_queue(delivery_harness, monkeypatch, stage, status, reason):
    runtime, _publication, arguments, _body_target, _route, records = delivery_harness
    args, handoff = arguments('A', rag_done=True, nas_done=stage == 'cleanup')
    identity = args['identity']
    records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A', 'kind': 'postprocessing_complete',
                                  'data': handoff})
    records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A', 'kind': stage, 'data': {
        'identity': identity, 'status': 'incomplete',
        **({'routes': [ _route], 'body_targets': {_route['code']: _body_target(identity)},
            'attachment_receipts': {}, 'archive': next(row['data'] for row in records['child-A'] if row['kind'] == 'nas'),
            } if stage == 'cleanup' else {}),
        'files': {args['registered']['paper']['archive_files'][0]['path']: {'status': status}}}})
    stages = []

    def save(**row):
        if row['kind'] == 'delivery_stage':
            stages.append(row['data']['stage'])
        return 'synthetic-artifact'

    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(runtime, 'admitted_sync_slot', lambda *_args: pytest.fail('no slot should be acquired'))
    with pytest.raises(ValueError, match=reason):
        runtime.deliver_paper(**args)
    assert stages == []


@pytest.mark.parametrize('bad_stage', ['rag', 'nas'])
def test_another_papers_completed_receipt_is_not_skipped(delivery_harness, monkeypatch, bad_stage):
    runtime, publication, arguments, _body_target, route, records = delivery_harness
    args, _ = arguments('A', rag_done=True, nas_done=bad_stage == 'nas')
    if bad_stage == 'rag':
        records['child-A'][0]['data'][route['code']]['ticket_identity']['paper_id'] = 'another-paper'
    else:
        records['child-A'][1]['data']['identity']['paper_id'] = 'another-paper'
    calls = []

    @contextmanager
    def slot(_admission, names):
        calls.append(names[0])
        yield

    def reject(*_args, **_kwargs):
        raise ValueError('receipt identity mismatch')

    monkeypatch.setattr(runtime, 'admitted_sync_slot', slot)
    monkeypatch.setattr(publication, 'publish_body_targets', reject)
    monkeypatch.setattr(runtime, 'save_archive', reject)
    monkeypatch.setattr(runtime, 'cleanup_paper', lambda *_args, **_kwargs: pytest.fail('cleanup must not start'))
    with pytest.raises(ValueError, match='identity mismatch|incomplete or unrelated'):
        runtime.deliver_paper(**args)
    assert calls == ([] if bad_stage == 'nas' else [bad_stage])


@pytest.mark.parametrize(('kind', 'reason'), [('rag_effect_intent', 'unknown'), ('rag_effect_failed', 'retry')])
def test_completed_checkpoints_do_not_bypass_unresolved_rag_effects(delivery_harness, monkeypatch, kind, reason):
    runtime, _publication, arguments, _body_target, _route, records = delivery_harness
    args, _ = arguments('A', rag_done=True)
    identity = args['identity']
    scope = {key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'task_run_id')}
    records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A', 'kind': kind,
        'data': {'scope': scope, 'effect_id': 'unresolved-effect'}})
    monkeypatch.setattr(runtime, 'admitted_sync_slot', lambda *_args: pytest.fail('no stage should start'))
    monkeypatch.setattr(runtime, 'cleanup_paper', lambda *_args, **_kwargs: pytest.fail('cleanup must not start'))
    with pytest.raises(ValueError, match=reason):
        runtime.deliver_paper(**args)


def test_temporary_prefect_slots_keep_archive_and_cleanup_independent(delivery_harness, monkeypatch):
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.testing.utilities import prefect_test_harness
    from paperflow_v3.control.admission import Admission, begin_control_session

    runtime, publication, arguments, body_target, route, _records = delivery_harness
    a, _ = arguments('A')
    b, _ = arguments('B')
    c, _ = arguments('C', rag_done=True)
    a_nas, b_rag, b_nas, c_nas, c_waiting, a_cleanup = (Event() for _ in range(6))
    finish_a_nas, finish_a_cleanup = Event(), Event()

    def publish(paper, identity, handoff, prepared, packet, client, targets, save, check, **_kwargs):
        targets[route['code']] = body_target(identity)
        if paper['paper_id'] == 'B':
            b_rag.set()

    def archive(paper, identity, handoff, destination, state, save, check, **_kwargs):
        if paper['paper_id'] == 'A':
            a_nas.set()
            assert finish_a_nas.wait(25)
        elif paper['paper_id'] == 'B':
            b_nas.set()
        else:
            c_nas.set()
        state.update(identity=identity, status='complete', files={})

    def cleanup(paper, *args, **kwargs):
        args[7].update(identity=args[0], status='complete', files={})
        if paper['paper_id'] == 'A':
            a_cleanup.set()
            assert finish_a_cleanup.wait(25)

    def save(**row):
        if (row['kind'] == 'delivery_stage' and row['folder_id'] == 'C'
                and row['data']['stage'] == 'waiting_nas_archive'):
            c_waiting.set()
        return 'synthetic-artifact'

    monkeypatch.setattr(publication, 'publish_body_targets', publish)
    observe_progress(monkeypatch, runtime, save)
    monkeypatch.setattr(runtime, 'save_archive', archive)
    monkeypatch.setattr(runtime, 'cleanup_paper', cleanup)

    async def exercise():
        await begin_control_session()
        admission = Admission('synthetic-task')
        await admission.start()
        suffix = uuid4().hex
        names = {stage: ['w3-isolated-' + stage + '-' + suffix] for stage in ('rag', 'nas', 'cleanup')}
        async with get_client() as client:
            for name in (value[0] for value in names.values()):
                await client.create_global_concurrency_limit(
                    GlobalConcurrencyLimitCreate(name=name, limit=1, active=True))
        for args in (a, b, c):
            args.update(admission=admission, limits=names)
        tasks = []
        try:
            tasks.append(asyncio.create_task(asyncio.to_thread(runtime.deliver_paper, **a)))
            assert await asyncio.to_thread(a_nas.wait, 20)
            tasks.append(asyncio.create_task(asyncio.to_thread(runtime.deliver_paper, **b)))
            assert await asyncio.to_thread(b_rag.wait, 20), 'B must start RAG while A is in NAS'
            finish_a_nas.set()
            assert await asyncio.to_thread(a_cleanup.wait, 20)
            tasks.append(asyncio.create_task(asyncio.to_thread(runtime.deliver_paper, **c)))
            assert await asyncio.to_thread(c_waiting.wait, 20)
            assert await asyncio.to_thread(c_nas.wait, 20), 'C must archive while A holds cleanup capacity'
            finish_a_cleanup.set()
            assert await asyncio.to_thread(b_nas.wait, 20)
        finally:
            finish_a_nas.set()
            finish_a_cleanup.set()
            outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(result, dict) and result['status'] == 'complete' for result in outcomes), outcomes

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
