"""Inert Prefect composition checks using only synthetic local tickets."""
import asyncio
import copy
from contextlib import nullcontext
import gc
import importlib
from pathlib import Path
import shutil
import sqlite3
import sys
import threading
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT.parent / 'postprocess-prefect-20261031'
for path in (ROOT, ROOT / 'tests', UPSTREAM, UPSTREAM / 'tests', UPSTREAM / 'runtime_next',
             UPSTREAM / 'results_next', UPSTREAM.parent.parent / 'prefect-paperflow'):
    sys.path.insert(0, str(path))

from test_remote_runtime_adapter import sample


def entry_module():
    try:
        return importlib.import_module('remote_prefect_entry')
    except ModuleNotFoundError as exc:
        if exc.name == 'remote_prefect_entry':
            pytest.fail('the server-only Prefect composition is not implemented')
        raise


def options(sample):
    values = sample['options']
    parts = {key: values[key] for key in ('base', 'stage_store', 'journal', 'load_inputs', 'snapshot_options')}
    transport = {key: values[key] for key in ('transfer_snapshot', 'validate_staging', 'submit_once',
                                              'wait_for_result', 'validate_result')}
    return dict(server_identity=copy.deepcopy(sample['server']), control_database=sample['database'],
                journal_database=sample['journal'].database, journal_id=sample['journal'].journal_id,
                original_base=sample['base'], components_factory=lambda: dict(parts),
                transport=transport)


def test_missing_remote_configuration_is_rejected_before_any_component_is_built(sample):
    module, values = entry_module(), options(sample)
    called = []
    values['components_factory'] = lambda: called.append(True)
    values['server_identity'] = None
    with pytest.raises(ValueError):
        module.RemoteCoordinatorFactory(**values)
    assert called == []
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT COUNT(*) FROM paperflow_postprocess_stage_jobs').fetchone()[0] == 0


def test_factory_rejects_missing_transport_or_mismatched_journal_without_dispatch(sample):
    module, values = entry_module(), options(sample)
    values['transport'].pop('submit_once')
    with pytest.raises(ValueError):
        module.RemoteCoordinatorFactory(**values)
    values = options(sample)
    values['journal_id'] = str(uuid4())
    factory = module.RemoteCoordinatorFactory(**values)
    with pytest.raises(ValueError):
        factory()
    assert sample['events'] == []


def test_missing_control_database_is_a_configuration_error(sample):
    module, values = entry_module(), options(sample)
    values['control_database'] = None
    with pytest.raises(ValueError):
        module.RemoteCoordinatorFactory(**values)
    assert sample['events'] == []


@pytest.mark.parametrize('api', [None, '', '   ', '///'])
def test_missing_prefect_identity_cannot_create_an_entry(sample, api):
    module = entry_module()
    with pytest.raises(ValueError):
        module.RemotePrefectEntry(factory=module.RemoteCoordinatorFactory(**options(sample)),
            client_factory=lambda: nullcontext(SimpleNamespace(api_url='')), expected_api_url=api)
    assert sample['events'] == []


def test_factory_does_not_switch_control_database_and_freezes_remote_identity(sample, tmp_path):
    module, values = entry_module(), options(sample)
    factory = module.RemoteCoordinatorFactory(**values)
    values['server_identity']['server_id'] = 'untrusted-replacement'
    values['transport'].clear()
    assert isinstance(factory(), module.RemoteRuntimeCoordinator)
    other = tmp_path / 'other-control.sqlite3'
    other.touch()
    values = options(sample)
    values['control_database'] = other
    with pytest.raises(ValueError):
        module.RemoteCoordinatorFactory(**values)()
    assert sample['events'] == []


@pytest.mark.parametrize('max_attempts', [3, None])
def test_factory_can_explicitly_pass_capacity_wait_without_dispatching(sample, max_attempts):
    module, values = entry_module(), options(sample)
    pause = lambda: None
    factory = module.RemoteCoordinatorFactory(**values, admission_wait={'wait': pause, 'max_attempts': max_attempts})
    remote = factory()
    assert remote._admission_wait == {'wait': pause, 'max_attempts': max_attempts}
    assert sample['events'] == []


def test_factory_rejects_another_server_or_a_local_coordinator(sample, monkeypatch):
    module, values = entry_module(), options(sample)
    values['server_identity']['server_id'] = 'other-server'
    with pytest.raises(ValueError):
        module.RemoteCoordinatorFactory(**values)()
    factory = module.RemoteCoordinatorFactory(**options(sample))
    monkeypatch.setattr(module, 'make_remote_coordinator', lambda **kwargs: sample['base'])
    with pytest.raises(ValueError):
        factory()
    assert sample['events'] == []


def test_factory_rejects_a_cloned_journal_even_if_its_id_is_the_same(sample, tmp_path):
    module, values = entry_module(), options(sample)
    from remote_coordinator import RemoteSubmissionJournal
    clone = RemoteSubmissionJournal(tmp_path / 'clone.sqlite3', control_database=sample['database'],
        admission_gate=sample['task_gate'], journal_id=sample['journal'].journal_id)
    clone.initialize()
    parts = values['components_factory']()
    parts['journal'] = clone
    values['components_factory'] = lambda: dict(parts)
    with pytest.raises(ValueError):
        module.RemoteCoordinatorFactory(**values)()
    assert sample['events'] == []


def test_factory_rejects_replacement_of_journal_at_the_same_path(sample):
    module = entry_module()
    factory = module.RemoteCoordinatorFactory(**options(sample))
    path = sample['journal'].database
    backup = path.with_suffix('.synthetic-backup')
    path.rename(backup)
    shutil.copyfile(backup, path)
    with pytest.raises(ValueError):
        factory()
    assert sample['events'] == []


def test_factory_rejects_replacement_of_control_database_at_the_same_path(sample):
    module = entry_module()
    factory = module.RemoteCoordinatorFactory(**options(sample))
    path = sample['database']
    backup = path.with_suffix('.synthetic-backup')
    gc.collect()  # Close unreachable temporary SQLite connections on Windows.
    path.rename(backup)
    shutil.copyfile(backup, path)
    with pytest.raises(ValueError):
        factory()
    assert sample['events'] == []


@pytest.mark.parametrize('field', ['task_gate', 'folder_gate', 'slot', 'verify_upstream', 'verify_execution',
                                   'postprocess_is_open', 'rag_is_held'])
def test_worker_factory_rejects_replaced_original_prefect_gates_and_slot(sample, field):
    module, values = entry_module(), options(sample)
    parts = values['components_factory']()
    changed = copy.copy(parts['base'])
    setattr(changed, field, lambda *args, **kwargs: True)
    parts['base'] = changed
    values['components_factory'] = lambda: dict(parts)
    factory = module.RemoteCoordinatorFactory(**values)
    with pytest.raises(ValueError):
        factory()
    assert sample['events'] == []


def test_rebuilding_original_runtime_with_new_closures_is_refused(sample):
    module, values = entry_module(), options(sample)
    parts = values['components_factory']()

    def rebuilt_components():
        # This mirrors build_runtime's fresh closure objects, even though those
        # closures delegate to the same synthetic underlying operations.
        rebuilt = copy.copy(parts['base'])
        for name in ('task_gate', 'folder_gate', 'slot', 'verify_upstream', 'verify_execution',
                     'postprocess_is_open', 'rag_is_held'):
            original = getattr(parts['base'], name)
            setattr(rebuilt, name, lambda *args, _original=original, **kwargs: _original(*args, **kwargs))
        return dict(parts, base=rebuilt)

    values['components_factory'] = rebuilt_components
    factory = module.RemoteCoordinatorFactory(**values)
    with pytest.raises(ValueError):
        factory()
    assert sample['events'] == []


@pytest.mark.parametrize('mode', ['one', 'reserved', 'batch', 'resume'])
def test_every_entry_passes_the_same_server_factory_to_the_prefect_worker(sample, monkeypatch, mode):
    module = entry_module()
    values = options(sample)
    factory = module.RemoteCoordinatorFactory(**values)
    client = SimpleNamespace(api_url='http://synthetic-prefect/api')
    entry = module.RemotePrefectEntry(factory=factory, client_factory=lambda: nullcontext(client),
                                     expected_api_url=client.api_url)
    identity = sample['evidence'][0]['identity']
    observed = []

    async def delegated(coordinator, *args, **kwargs):
        worker_coordinator = kwargs['runtime_factory']()
        observed.append((coordinator, worker_coordinator, kwargs['runtime_factory'], args, kwargs))
        return {'synthetic': True}

    monkeypatch.setattr(module.live_runtime, 'run_selected', delegated)
    monkeypatch.setattr(module.live_runtime, 'run_reserved', delegated)
    monkeypatch.setattr(module.live_runtime, 'resume_stopped', delegated)
    monkeypatch.setattr(module.batch_next, 'run_batch', delegated)
    if mode == 'one':
        value = asyncio.run(entry.run_one(identity, 1))
    elif mode == 'reserved':
        value = asyncio.run(entry.run_reserved({'job_id': str(uuid4())}))
    elif mode == 'batch':
        value = asyncio.run(entry.run_batch([(identity, 1)]))
    else:
        value = asyncio.run(entry.resume_stopped(str(uuid4()), 2))
    assert value == {'synthetic': True}
    outer, worker, actual_factory, _, kwargs = observed[0]
    assert isinstance(outer, module.RemoteRuntimeCoordinator)
    assert isinstance(worker, module.RemoteRuntimeCoordinator)
    assert actual_factory is factory
    assert outer.database == worker.database == sample['base'].database
    assert outer.task_gate is worker.task_gate is sample['task_gate']
    assert outer.slot is worker.slot is sample['base'].slot
    if mode == 'resume':
        assert kwargs['expected_api_url'] == client.api_url
    assert sample['events'] == []


def test_real_prefect_task_definition_executes_the_remote_not_the_local_path(sample, monkeypatch):
    module = entry_module()
    from prefect.context import TaskRunContext
    from stage_runtime import build_task

    values = options(sample)
    original_loader = values['components_factory']()['load_inputs']
    parts = values['components_factory']()
    parts['load_inputs'] = lambda identity, generation, item, context: original_loader(
        identity, generation, item, str(context.id))
    values['components_factory'] = lambda: dict(parts)
    sample['base'].verify_execution = lambda item, context: str(context.id) == item['job_id']
    factory = module.RemoteCoordinatorFactory(**values)
    outer = factory()
    job = outer.reserve(sample['evidence'][0]['identity'], 1)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted' WHERE job_id=?", (job['job_id'],))
    task_run = SimpleNamespace(id=UUID(job['job_id']))
    monkeypatch.setattr(TaskRunContext, 'get', classmethod(lambda cls: SimpleNamespace(task_run=task_run)))
    sample['wait_release'].set()
    result = asyncio.run(build_task(factory, job['job_id']).fn())
    assert result['state'] == 'waiting_rag'
    assert [event for event, _ in sample['events']] == ['transfer', 'submit']
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'


def test_factory_does_not_serialize_two_folders_or_replace_original_slots(sample):
    module, values = entry_module(), options(sample)
    barrier = threading.Barrier(2)
    transfer = values['transport']['transfer_snapshot']
    def overlapping(*args, **kwargs):
        barrier.wait(timeout=10)
        return transfer(*args, **kwargs)
    values['transport']['transfer_snapshot'] = overlapping
    factory = module.RemoteCoordinatorFactory(**values)
    first, second = factory(), factory()
    assert first.slot is second.slot is sample['base'].slot
    jobs = [coordinator.reserve(item['identity'], 1) for coordinator, item in
            zip((first, second), sample['evidence'])]
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")

    async def run():
        pending = [asyncio.create_task(coordinator.execute(job['job_id'], job['job_id']))
                   for coordinator, job in zip((first, second), jobs)]
        try:
            assert await asyncio.to_thread(sample['two_waiting'].wait, 12)
        finally:
            sample['wait_release'].set()
        return await asyncio.gather(*pending)

    results = asyncio.run(run())
    assert all(result['state'] == 'waiting_rag' for result in results)
    assert sample['slot_state']['maximum'] == 2
    assert [event for event, _ in sample['events']].count('submit') == 2


def test_single_entry_reuses_real_prefect_orchestration_and_remote_worker(sample, monkeypatch):
    module = entry_module()
    from prefect.context import TaskRunContext
    from stage_runtime import TAG

    values = options(sample)
    parts = values['components_factory']()
    original_loader = parts['load_inputs']
    parts['load_inputs'] = lambda identity, generation, item, context: original_loader(
        identity, generation, item, str(context.id))
    values['components_factory'] = lambda: dict(parts)
    sample['base'].verify_execution = lambda item, context: str(context.id) == item['job_id']
    factory = module.RemoteCoordinatorFactory(**values)
    selected = {}
    monkeypatch.setattr(TaskRunContext, 'get', classmethod(
        lambda cls: SimpleNamespace(task_run=SimpleNamespace(id=UUID(selected['job_id'])))))

    class Client:
        api_url = 'http://synthetic-prefect/api'
        def create_task_run(self, *, task, flow_run_id, id, dynamic_key, name, state):
            assert flow_run_id is None and state.state_details.deferred is True
            return SimpleNamespace(id=id, flow_run_id=None, task_key=task.task_key,
                                   dynamic_key=dynamic_key, tags=[TAG])

    class Worker:
        def __init__(self, stage, job_id):
            self.stage, self.job_id, self.finished_task_runs = stage, job_id, {}
            selected['job_id'] = job_id
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def astart(self):
            while True:
                with sqlite3.connect(sample['database']) as db:
                    row = db.execute('SELECT state FROM paperflow_postprocess_stage_jobs WHERE job_id=?',
                                     (self.job_id,)).fetchone()
                if row and row[0] in ('submitting', 'submitted'):
                    break
                await asyncio.sleep(.01)
            await self.stage.fn()
            self.finished_task_runs[self.stage.task_key] = 1
            await asyncio.Event().wait()

    entry = module.RemotePrefectEntry(factory=factory, client_factory=lambda: nullcontext(Client()),
                                     expected_api_url=Client.api_url)
    sample['wait_release'].set()
    result = asyncio.run(entry.run_one(sample['evidence'][0]['identity'], 1,
                                       worker_factory=Worker, poll_seconds=.01))
    assert result['state'] == 'finished'
    assert [event for event, _ in sample['events']] == ['transfer', 'submit']
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'


def test_changed_factory_cannot_route_worker_to_a_local_coordinator(sample, monkeypatch):
    module = entry_module()
    factory = module.RemoteCoordinatorFactory(**options(sample))
    entry = module.RemotePrefectEntry(factory=factory,
        client_factory=lambda: nullcontext(SimpleNamespace(api_url='http://synthetic-prefect/api')),
        expected_api_url='http://synthetic-prefect/api')
    with pytest.raises(AttributeError):
        entry.factory = lambda: sample['base']

    def change_after_outer_construction():
        entry._factory = lambda: sample['base']

    async def delegated(coordinator, *args, **kwargs):
        assert isinstance(coordinator, module.RemoteRuntimeCoordinator)
        assert kwargs['runtime_factory'] is factory
        assert isinstance(kwargs['runtime_factory'](), module.RemoteRuntimeCoordinator)
        return {'synthetic': True}

    monkeypatch.setattr(entry, '_check_client', change_after_outer_construction)
    monkeypatch.setattr(module.live_runtime, 'run_selected', delegated)
    assert asyncio.run(entry.run_one(sample['evidence'][0]['identity'], 1)) == {'synthetic': True}
    with pytest.raises(ValueError):
        asyncio.run(entry.run_one(sample['evidence'][0]['identity'], 1))
    assert sample['events'] == []


def test_prefect_client_identity_mismatch_cannot_submit_or_fall_back(sample):
    module = entry_module()
    factory = module.RemoteCoordinatorFactory(**options(sample))
    entry = module.RemotePrefectEntry(factory=factory,
        client_factory=lambda: nullcontext(SimpleNamespace(api_url='http://different/api')),
        expected_api_url='http://synthetic-prefect/api')
    with pytest.raises(ValueError):
        asyncio.run(entry.run_one(sample['evidence'][0]['identity'], 1))
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT COUNT(*) FROM paperflow_postprocess_stage_jobs').fetchone()[0] == 0
    assert sample['events'] == []
