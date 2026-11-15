"""Explicit launch checks using temporary configuration and a synthetic runtime."""
import asyncio
import copy
import hashlib
import importlib
import json
import os
from pathlib import Path
import py_compile
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4
from contextlib import nullcontext

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from test_remote_live_runtime import configured, sample


def module():
    try:
        return importlib.import_module('remote_live_launch')
    except ModuleNotFoundError as exc:
        if exc.name == 'remote_live_launch': pytest.fail('explicit launcher is missing')
        raise


def write_json(path, value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def launch_case(configured, tmp_path, monkeypatch):
    x = configured
    runtime_dir = tmp_path / 'workspace' / 'candidates' / 'original' / 'runtime_next'
    runtime_dir.mkdir(parents=True)
    project = tmp_path / 'workspace' / 'prefect-paperflow'
    project.mkdir()
    sources = {}
    for name in ('live_runtime.py', 'stage_runtime.py', 'batch_next.py'):
        raw = ('# synthetic ' + name).encode()
        (runtime_dir / name).write_bytes(raw)
        sources[name] = hashlib.sha256(raw).hexdigest()
    runtime_file = project / 'runtime-config.json'
    tokenizer_sha = write_json(runtime_file, x.tokenizer)
    Path(x.config['connection']['private_key']).write_text('synthetic placeholder; never read', encoding='utf8')
    settings = dict(prefect_api_url=x.original['api'], execution_enabled=True,
                    catalog_path=str(x.sample['catalog']),
                    runtime_config=str(runtime_file))
    settings_sha = write_json(project / 'config.json', settings)
    options = copy.deepcopy(x.options)
    options.pop('catalog')  # The original pinned catalog is read once, never duplicated in launch config.
    config = dict(schema='paperflow-remote-live/1', runtime_dir=str(runtime_dir),
        runtime_sources=sources, prefect_project=str(project), settings_sha256=settings_sha,
        tokenizer_runtime_sha256=tokenizer_sha, expected_prefect_api_url=x.original['api'],
        journal_database=str(x.args['journal_database']), journal_id=x.args['journal_id'],
        server_identity=copy.deepcopy(x.args['server_identity']),
        transport_config=copy.deepcopy(x.config), transport_config_sha256=x.args['transport_config_sha256'],
        snapshot_options=options, expected_catalog_path=str(x.sample['catalog']),
        expected_catalog_sha256=x.args['expected_catalog_sha256'], max_summary_receipts=50)
    path = tmp_path / 'remote-config.json'
    digest = write_json(path, config)
    calls = []
    live = SimpleNamespace(build_runtime=lambda: (calls.append('build') or x.original),
        select_folder=lambda database, task_id, group_id: (
            calls.append(('select', database, task_id, group_id)) or
            (x.sample['evidence'][0]['identity'], 1)))
    remote = importlib.import_module('remote_live_runtime')
    settings_module = SimpleNamespace(DEFAULT_CONFIG=project / 'config.json')
    launcher = module()
    monkeypatch.setattr(launcher, '_import_modules', lambda *_: (live, remote, settings_module))
    return SimpleNamespace(x=x, path=path, digest=digest, config=config, settings=settings,
        runtime_file=runtime_file, project=project, runtime_dir=runtime_dir, live=live,
        calls=calls, launcher=launcher)


def build(case, **changes):
    x = case.x
    return case.launcher.build_from_config(case.path, case.digest,
        stage_call=x.args['stage_call'], rpc_call=x.args['rpc_call'], **changes)


def test_check_builds_once_from_pinned_original_configuration_without_dispatch(launch_case):
    c = launch_case
    before = c.x.sample['database'].read_bytes(), c.x.sample['journal'].database.read_bytes()
    loaded = build(c)
    assert c.calls == ['build'] and c.x.events == []
    assert loaded.remote.entry.factory is loaded.remote.factory
    assert loaded.original_runtime is c.x.original
    assert loaded.remote.factory().slot is c.x.original['coordinator'].slot
    assert before == (c.x.sample['database'].read_bytes(), c.x.sample['journal'].database.read_bytes())


def test_prebuilt_original_runtime_cannot_bypass_the_clean_process_boundary(launch_case):
    c = launch_case
    with pytest.raises(TypeError): build(c, original_runtime=c.x.original)
    assert c.calls == [] and c.x.events == []


def test_explicit_single_ticket_uses_entry_and_selects_only_requested_identity(launch_case, monkeypatch):
    c = launch_case
    loaded = build(c)
    identity = c.x.sample['evidence'][0]['identity']
    seen = []
    async def run_one(actual, generation):
        seen.append((actual, generation))
        return dict(job_id=str(uuid4()), state='finished')
    monkeypatch.setattr(loaded.remote.entry, 'run_one', run_one)
    result = asyncio.run(loaded.run_one(identity['task_id'], identity['group_id']))
    assert result['state'] == 'finished' and seen == [(identity, 1)]
    assert c.calls[1:] == [('select', c.x.sample['database'], identity['task_id'], identity['group_id'])]
    assert c.x.events == []


@pytest.mark.parametrize('changed', ['launch_config', 'settings', 'tokenizer', 'source', 'catalog'])
def test_reused_entry_rechecks_pinned_local_identity_before_selection(launch_case, monkeypatch, changed):
    c = launch_case
    loaded = build(c)
    targets = dict(launch_config=c.path, settings=c.project / 'config.json', tokenizer=c.runtime_file,
                   source=c.runtime_dir / 'stage_runtime.py', catalog=c.x.sample['catalog'])
    with targets[changed].open('ab') as stream: stream.write(b' ')
    async def no_dispatch(*_): pytest.fail('changed config must not reach the entry')
    monkeypatch.setattr(loaded.remote.entry, 'run_one', no_dispatch)
    identity = c.x.sample['evidence'][0]['identity']
    with pytest.raises(ValueError): asyncio.run(loaded.run_one(identity['task_id'], identity['group_id']))
    assert c.calls == ['build'] and c.x.events == []


def test_configuration_change_while_selecting_is_rejected_before_entry(launch_case, monkeypatch):
    c = launch_case
    loaded = build(c)
    identity = c.x.sample['evidence'][0]['identity']
    def select(*_):
        with c.runtime_file.open('ab') as stream: stream.write(b' ')
        return identity, 1
    c.live.select_folder = select
    async def no_dispatch(*_): pytest.fail('changed config must not reach the entry')
    monkeypatch.setattr(loaded.remote.entry, 'run_one', no_dispatch)
    with pytest.raises(ValueError): asyncio.run(loaded.run_one(identity['task_id'], identity['group_id']))
    assert c.x.events == []


def test_invalid_selection_or_closed_original_gate_never_calls_entry(launch_case, monkeypatch):
    c = launch_case
    loaded = build(c)
    async def no_dispatch(*_): pytest.fail('entry must not be reached')
    monkeypatch.setattr(loaded.remote.entry, 'run_one', no_dispatch)
    with pytest.raises(ValueError): asyncio.run(loaded.run_one('bad-task', 'bad-group'))
    c.live.select_folder = lambda *_: (_ for _ in ()).throw(ValueError('task paused'))
    identity = c.x.sample['evidence'][0]['identity']
    with pytest.raises(ValueError): asyncio.run(loaded.run_one(identity['task_id'], identity['group_id']))
    assert c.calls == ['build'] and c.x.events == []


def test_disabled_original_execution_allows_check_but_blocks_single_job(launch_case, monkeypatch):
    c = launch_case
    c.config['settings_sha256'] = write_json(c.project / 'config.json', dict(c.settings, execution_enabled=False))
    c.digest = write_json(c.path, c.config)
    loaded = build(c)
    async def no_dispatch(*_): pytest.fail('disabled original setting must block entry')
    monkeypatch.setattr(loaded.remote.entry, 'run_one', no_dispatch)
    identity = c.x.sample['evidence'][0]['identity']
    with pytest.raises(ValueError): asyncio.run(loaded.run_one(identity['task_id'], identity['group_id']))
    assert c.calls == ['build'] and c.x.events == []


def batch_items(case, count):
    first = case.x.sample['evidence'][0]['identity']
    items = [first]
    fields = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
    with sqlite3.connect(case.x.sample['database']) as db:
        for number in range(1, count):
            item = dict(first, group_id=f'{number:032x}', paper_id='synthetic-' + str(number),
                        receipt_id=f'{number:064x}', attempt_id='attempt-' + str(number), task_run_id=str(uuid4()))
            db.execute('INSERT INTO llm_folder_queue VALUES (?,?,?,?,?,?,?,?,?,?)',
                       tuple(item[key] for key in fields) + ('c' * 64, 'waiting_postprocessing', 1))
            db.execute('INSERT INTO llm_request_admissions VALUES (?,?,?,?,?)',
                       (item['task_id'], item['task_run_id'], 'body-' + str(number), 'raw_complete', 'body'))
            items.append(item)
    return items


def test_explicit_batch_selects_only_one_task_and_passes_original_snapshot(launch_case, monkeypatch):
    c = launch_case
    items = batch_items(c, 3)
    loaded = build(c)
    seen = []
    async def accept(selections, **kwargs):
        seen.append((selections, kwargs))
        return dict(completed=[loaded.batch._job_id(identity['task_run_id']) for identity, _ in selections],
                    stopped=False, detail=None, attempted=len(selections), remaining=0)
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', accept)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 2, batch_size=2))
    assert result['stopped'] is False and len(seen) == 1
    selections, options = seen[0]
    assert len(selections) == 2 and [pair[0] for pair in selections] == items[:2]
    assert all(pair[0]['task_id'] == items[0]['task_id'] and pair[1] == 1 for pair in selections)
    assert options['batch_size'] == 2 and c.x.events == []


@pytest.mark.parametrize('phase', ['paused_before', 'paused_during', 'deleted_during', 'generation_changes_during',
                                    'gate_closes_during', 'config_changes_during'])
def test_batch_refuses_pause_gate_or_configuration_change_before_dispatch(launch_case, monkeypatch, phase):
    c = launch_case
    identity = batch_items(c, 2)[0]
    loaded = build(c)
    original_snapshot = loaded.batch.snapshot_waiting
    if phase == 'paused_before':
        with sqlite3.connect(c.x.sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
    else:
        def changed_snapshot(*args):
            selections = original_snapshot(*args)
            if phase == 'paused_during':
                with sqlite3.connect(c.x.sample['database']) as db:
                    db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
            elif phase == 'deleted_during':
                with sqlite3.connect(c.x.sample['database']) as db:
                    db.execute('INSERT INTO task_deletions VALUES (?)', (identity['task_id'],))
            elif phase == 'generation_changes_during':
                with sqlite3.connect(c.x.sample['database']) as db:
                    db.execute('UPDATE tasks SET start_generation=2 WHERE task_id=?', (identity['task_id'],))
            elif phase == 'gate_closes_during':
                loaded.original_runtime['coordinator'].postprocess_is_open = lambda: False
            else:
                with c.runtime_file.open('ab') as stream: stream.write(b' ')
            return selections
        monkeypatch.setattr(loaded.batch, 'snapshot_waiting', changed_snapshot)
    async def no_dispatch(*_args, **_kwargs): pytest.fail('batch must not dispatch')
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', no_dispatch)
    with pytest.raises(Exception): asyncio.run(loaded.run_batch(identity['task_id'], 2, batch_size=2))
    with sqlite3.connect(c.x.sample['database']) as db:
        assert db.execute('SELECT count(*) FROM paperflow_postprocess_stage_jobs').fetchone()[0] == 0
    assert c.x.events == []


@pytest.mark.parametrize('stop_kind', ['paused', 'execution_unknown'])
def test_original_batch_stops_later_tranches_after_pause_or_unknown(launch_case, monkeypatch, stop_kind):
    c = launch_case
    items = batch_items(c, 5)
    loaded = build(c)
    original_batch = loaded.batch
    from stage_runtime import _job_id
    states, submitted, worker_sizes = {}, [], []

    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, identity, generation):
            with sqlite3.connect(c.x.sample['database']) as db:
                state = db.execute('SELECT desired_state FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()[0]
            if state != 'running': raise ValueError('task paused')
            job_id = _job_id(identity['task_run_id'])
            states[job_id] = {'job_id': job_id, 'state': 'reserved'}
            return dict(states[job_id])
        def submit(self, job_id, client, stage):
            submitted.append(job_id)
            states[job_id]['state'] = ('execution_unknown' if stop_kind == 'execution_unknown'
                                       and len(submitted) == 2 else 'finished')
            if stop_kind == 'paused' and len(submitted) == 2:
                with sqlite3.connect(c.x.sample['database']) as db:
                    db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (items[0]['task_id'],))
        def job(self, job_id): return dict(states[job_id])

    class Worker:
        def __init__(self, stages, ids):
            worker_sizes.append(len(ids))
            self.finished_task_runs = {stage.task_key: 0 for stage in stages}
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def astart(self): await asyncio.Event().wait()

    async def fake_remote_entry(selections, **kwargs):
        return await original_batch.run_batch(Coordinator(), selections,
            client_factory=lambda: nullcontext(object()), runtime_factory=lambda: None,
            worker_factory=Worker, batch_size=kwargs['batch_size'], poll_seconds=.01)
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', fake_remote_entry)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 5, batch_size=2))
    assert result['stopped'] is True and len(submitted) == 2 and worker_sizes == [2]
    assert result['attempted'] == 2 and result['remaining'] == 3
    assert len(result['completed']) == (2 if stop_kind == 'paused' else 1) and c.x.events == []


def test_batch_reuses_real_remote_entry_and_original_batch_coordinator(launch_case, monkeypatch):
    c = launch_case
    items = batch_items(c, 3)
    loaded = build(c)
    from stage_runtime import _job_id
    states, worker_sizes, seen = {}, [], []

    class Coordinator:
        def postprocess_is_open(self): return True
        def reserve(self, identity, generation):
            job_id = _job_id(identity['task_run_id'])
            states[job_id] = {'job_id': job_id, 'state': 'reserved'}
            return dict(states[job_id])
        def submit(self, job_id, client, stage): states[job_id]['state'] = 'finished'
        def job(self, job_id): return dict(states[job_id])

    class Worker:
        def __init__(self, stages, ids):
            worker_sizes.append(len(ids))
            self.finished_task_runs = {stage.task_key: 0 for stage in stages}
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def astart(self): await asyncio.Event().wait()

    real_batch = loaded.batch.run_batch
    async def observed(coordinator, selections, **kwargs):
        seen.append(kwargs['runtime_factory'])
        kwargs['worker_factory'] = Worker
        return await real_batch(coordinator, selections, **kwargs)
    monkeypatch.setattr(loaded.batch, 'run_batch', observed)
    monkeypatch.setattr(loaded.remote.entry, '_coordinator', lambda: (loaded.remote.factory, Coordinator()))
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 3, batch_size=2, poll_seconds=.01))
    assert result['stopped'] is False and len(result['completed']) == 3
    assert seen == [loaded.remote.factory, loaded.remote.factory] and worker_sizes == [2, 1] and c.x.events == []


@pytest.mark.parametrize('kind', ['other_task', 'changed_generation', 'too_many'])
def test_untrusted_or_changed_batch_snapshot_never_reaches_entry(launch_case, monkeypatch, kind):
    c = launch_case
    first = batch_items(c, 2)[0]
    loaded = build(c)
    snapshot = loaded.batch.snapshot_waiting
    def wrong(*args):
        selected = snapshot(*args)
        if kind == 'other_task': return [(c.x.sample['evidence'][1]['identity'], 1)]
        if kind == 'changed_generation': return [(selected[0][0], 2)]
        return selected + selected
    monkeypatch.setattr(loaded.batch, 'snapshot_waiting', wrong)
    async def no_dispatch(*_args, **_kwargs): pytest.fail('batch must not dispatch')
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', no_dispatch)
    with pytest.raises(Exception): asyncio.run(loaded.run_batch(first['task_id'], 2, batch_size=2))
    assert c.x.events == []


def test_disabled_original_execution_blocks_batch_before_snapshot(launch_case, monkeypatch):
    c = launch_case
    c.config['settings_sha256'] = write_json(c.project / 'config.json', dict(c.settings, execution_enabled=False))
    c.digest = write_json(c.path, c.config)
    loaded = build(c)
    monkeypatch.setattr(loaded.batch, 'snapshot_waiting', lambda *_: pytest.fail('snapshot must not run'))
    first = c.x.sample['evidence'][0]['identity']
    with pytest.raises(ValueError): asyncio.run(loaded.run_batch(first['task_id'], 2))
    assert c.x.events == []


@pytest.mark.parametrize('kwargs', [dict(max_folders=0), dict(max_folders=True),
    dict(max_folders=2**63), dict(max_folders=1, batch_size=9), dict(max_folders=1, poll_seconds=float('nan'))])
def test_invalid_explicit_batch_limits_reject_before_snapshot(launch_case, monkeypatch, kwargs):
    c = launch_case
    loaded = build(c)
    monkeypatch.setattr(loaded.batch, 'snapshot_waiting', lambda *_: pytest.fail('snapshot must not run'))
    first = c.x.sample['evidence'][0]['identity']
    with pytest.raises(ValueError): asyncio.run(loaded.run_batch(first['task_id'], **kwargs))
    assert c.x.events == []


def test_config_change_between_tranches_preserves_prior_completion_and_stops(launch_case, monkeypatch):
    c = launch_case
    items = batch_items(c, 4)
    loaded = build(c)
    calls = []
    async def accept(group, **_kwargs):
        calls.append(group)
        with c.runtime_file.open('ab') as stream: stream.write(b' ')
        return dict(completed=[loaded.batch._job_id(identity['task_run_id']) for identity, _ in group],
                    stopped=False, attempted=len(group), remaining=0, detail=None)
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', accept)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 4, batch_size=2))
    assert result['stopped'] is True and result['detail']['reason'] == 'before_batch_check_failed'
    assert len(calls) == 1 and result['attempted'] == 2 and result['remaining'] == 2
    assert len(result['completed']) == 2 and c.x.events == []


def test_batch_exception_does_not_automatically_dispatch_later_tranches(launch_case, monkeypatch):
    c = launch_case
    items = batch_items(c, 4)
    loaded = build(c)
    calls = []
    async def unknown(group, **_kwargs):
        calls.append(group)
        raise TimeoutError('synthetic ambiguous result')
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', unknown)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 4, batch_size=2))
    assert result['stopped'] is True and result['last_group'] == 'unconfirmed'
    assert result['attempted'] is None and result['remaining'] is None and result['confirmed_attempted'] == 0
    assert result['snapshot_selected'] == 4 and len(calls) == 1 and c.x.events == []


def test_batch_exception_preserves_known_prior_results_without_inventing_current_counts(launch_case, monkeypatch):
    c = launch_case
    items = batch_items(c, 4)
    loaded = build(c)
    calls = []
    async def partial(group, **_kwargs):
        calls.append(group)
        if len(calls) == 2: raise TimeoutError('synthetic ambiguous result')
        return dict(completed=[loaded.batch._job_id(identity['task_run_id']) for identity, _ in group],
                    stopped=False, attempted=len(group), remaining=0, detail=None)
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', partial)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 4, batch_size=2))
    assert result['stopped'] is True and result['last_group'] == 'unconfirmed' and len(calls) == 2
    assert len(result['completed']) == 2 and result['confirmed_attempted'] == 2
    assert result['snapshot_selected'] == 4 and result['attempted'] is None and result['remaining'] is None


@pytest.mark.parametrize('kind', ['missing_completed', 'outside_attempted_prefix'])
def test_incomplete_batch_result_cannot_be_treated_as_success(launch_case, monkeypatch, kind):
    c = launch_case
    items = batch_items(c, 4)
    loaded = build(c)
    calls = []
    async def incomplete(group, **_kwargs):
        calls.append(group)
        if kind == 'missing_completed':
            return dict(completed=[], stopped=False, attempted=len(group), remaining=0, detail=None)
        second_job = loaded.batch._job_id(group[1][0]['task_run_id'])
        return dict(completed=[second_job], stopped=True, attempted=1, remaining=len(group)-1, detail=None)
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', incomplete)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 4, batch_size=2))
    assert result['stopped'] is True and result['last_group'] == 'unconfirmed'
    assert result['attempted'] is None and result['remaining'] is None and len(calls) == 1 and c.x.events == []


def test_later_explicit_batch_excludes_unknown_folder_but_keeps_independent_folders(launch_case, monkeypatch):
    c = launch_case
    items = batch_items(c, 3)
    loaded = build(c)
    old = loaded.remote.factory().reserve(items[0], 1)
    with sqlite3.connect(c.x.sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='execution_unknown' WHERE job_id=?",
                   (old['job_id'],))
    seen = []
    async def accept(group, **_kwargs):
        seen.extend(identity for identity, _ in group)
        return dict(completed=[loaded.batch._job_id(identity['task_run_id']) for identity, _ in group],
                    stopped=False, attempted=len(group), remaining=0, detail=None)
    monkeypatch.setattr(loaded.remote.entry, 'run_batch', accept)
    result = asyncio.run(loaded.run_batch(items[0]['task_id'], 3, batch_size=2))
    assert result['stopped'] is False and seen == items[1:]
    assert all(identity['task_run_id'] != items[0]['task_run_id'] for identity in seen)
    assert c.x.events == []


def test_configuration_bytes_must_match_independent_sha_and_exact_schema(launch_case):
    c = launch_case
    with pytest.raises(ValueError): c.launcher.build_from_config(c.path, '0' * 64)
    config = copy.deepcopy(c.config); config.pop('journal_id')
    changed = write_json(c.path, config)
    with pytest.raises(ValueError): c.launcher.build_from_config(c.path, changed)
    c.path.write_text('{"schema":"x","schema":"y"}', encoding='utf8')
    with pytest.raises(ValueError): c.launcher.build_from_config(c.path, hashlib.sha256(c.path.read_bytes()).hexdigest())
    assert c.calls == [] and c.x.events == []


def test_example_lists_exact_configuration_fields_without_real_credentials():
    launcher = module()
    example = json.loads((ROOT / 'remote_live_config.example.json').read_text(encoding='utf8'))
    assert set(example) == launcher._FIELDS
    assert set(example['runtime_sources']) == launcher._SOURCE_NAMES
    assert set(example['snapshot_options']) == launcher._SNAPSHOT_FIELDS
    assert example['snapshot_options']['allowed_roots'] == example['transport_config']['allowed_roots']
    assert example['expected_catalog_path'] == example['transport_config']['trusted_files'][0]['path']
    assert example['expected_catalog_sha256'] == example['transport_config']['trusted_files'][0]['sha256']


def test_changed_original_settings_tokenizer_or_source_fails_before_composition(launch_case):
    c = launch_case
    targets = [c.project / 'config.json', c.runtime_file, c.runtime_dir / 'stage_runtime.py']
    for path in targets:
        before = path.read_bytes()
        path.write_bytes(before + b' ')
        with pytest.raises(ValueError): build(c)
        path.write_bytes(before)
    assert c.calls == [] and c.x.events == []


@pytest.mark.parametrize('field', ['journal_id', 'server_identity', 'snapshot_options'])
def test_incomplete_identity_or_snapshot_never_builds_original_runtime(launch_case, field):
    c = launch_case
    if field == 'journal_id': c.config[field] = 'not-a-uuid'
    elif field == 'server_identity': c.config[field] = {'server_id': 'different', 'ledger_id': str(uuid4())}
    else: c.config[field].pop('code_sha256')
    c.digest = write_json(c.path, c.config)
    with pytest.raises(ValueError): build(c)
    assert c.calls == [] and c.x.events == []


def test_configuration_change_during_construction_is_rejected(launch_case):
    c = launch_case
    def replace_config():
        c.calls.append('build')
        with (c.project / 'config.json').open('ab') as stream: stream.write(b' ')
        return c.x.original
    c.live.build_runtime = replace_config
    with pytest.raises(ValueError): build(c)
    assert c.calls == ['build'] and c.x.events == []


def test_original_prefect_or_catalog_identity_cannot_be_switched(launch_case):
    c = launch_case
    for key, value in [('prefect_api_url', 'http://different/api'),
                       ('catalog_path', str(c.project / 'different.json'))]:
        settings = dict(c.settings, **{key: value})
        c.config['settings_sha256'] = write_json(c.project / 'config.json', settings)
        c.digest = write_json(c.path, c.config)
        with pytest.raises(ValueError): build(c)
    assert c.calls == [] and c.x.events == []


def test_catalog_is_read_from_pinned_original_and_detects_changed_bytes_before_build(launch_case):
    c = launch_case
    c.x.sample['catalog'].write_text('{"changed": true}', encoding='utf8')
    with pytest.raises(ValueError): build(c)
    assert c.calls == [] and c.x.events == []


def test_missing_key_or_original_journal_is_rejected_without_network_or_initialization(launch_case):
    c = launch_case
    key = Path(c.config['transport_config']['connection']['private_key'])
    key.unlink()
    with pytest.raises(ValueError): build(c)
    key.write_text('synthetic placeholder; never read', encoding='utf8')
    missing = c.project / 'never-create.sqlite3'
    c.config['journal_database'] = str(missing)
    c.digest = write_json(c.path, c.config)
    with pytest.raises(ValueError): build(c)
    assert not missing.exists()
    assert c.calls == [] and c.x.events == []


def test_cli_check_and_run_are_distinct_and_do_not_print_config(launch_case, monkeypatch, capsys):
    c = launch_case
    loaded = build(c)
    monkeypatch.setattr(c.launcher, 'build_from_config', lambda *_: loaded)
    identity = c.x.sample['evidence'][0]['identity']
    run_calls = []
    async def run_one(task_id, group_id):
        run_calls.append((task_id, group_id))
        return {'job_id': 'synthetic', 'state': 'finished', 'private': 'never-print'}
    monkeypatch.setattr(loaded, 'run_one', run_one)
    assert c.launcher.main(['check', '--config', str(c.path), '--config-sha256', c.digest]) == 0
    assert run_calls == []
    out = json.loads(capsys.readouterr().out)
    assert out['configuration'] == 'loaded' and out['execution_enabled'] is True
    assert out['business_work_started'] is False and out['server_connectivity'] == 'not_checked'
    assert c.launcher.main(['run-one', '--config', str(c.path), '--config-sha256', c.digest,
                            '--task-id', identity['task_id'], '--group-id', identity['group_id']]) == 0
    assert run_calls == [(identity['task_id'], identity['group_id'])]
    output = capsys.readouterr().out
    assert 'never-print' not in output and json.loads(output)['rag_handoff'] == 'not_checked'


def test_cli_unknown_result_is_not_reported_as_success(launch_case, monkeypatch, capsys):
    c = launch_case
    loaded = build(c)
    monkeypatch.setattr(c.launcher, 'build_from_config', lambda *_: loaded)
    identity = c.x.sample['evidence'][0]['identity']
    async def unknown(*_): return {'job_id': 'synthetic', 'state': 'execution_unknown'}
    monkeypatch.setattr(loaded, 'run_one', unknown)
    status = c.launcher.main(['run-one', '--config', str(c.path), '--config-sha256', c.digest,
                              '--task-id', identity['task_id'], '--group-id', identity['group_id']])
    assert status != 0 and json.loads(capsys.readouterr().out)['state'] == 'execution_unknown'


@pytest.mark.parametrize('stopped', [False, True])
def test_cli_batch_requires_explicit_task_and_limit_and_reports_stage_scope(launch_case, monkeypatch, capsys, stopped):
    c = launch_case
    loaded = build(c)
    monkeypatch.setattr(c.launcher, 'build_from_config', lambda *_: loaded)
    identity = c.x.sample['evidence'][0]['identity']
    calls = []
    async def run_batch(task_id, max_folders, *, batch_size):
        calls.append((task_id, max_folders, batch_size))
        return dict(completed=['stage-1'], stopped=stopped, attempted=2, remaining=1, detail=None,
                    snapshot_selected=3, confirmed_attempted=2, last_group='stopped' if stopped else 'finished')
    monkeypatch.setattr(loaded, 'run_batch', run_batch)
    code = c.launcher.main(['run-batch', '--config', str(c.path), '--config-sha256', c.digest,
                            '--task-id', identity['task_id'], '--max-folders', '3', '--batch-size', '2'])
    result = json.loads(capsys.readouterr().out)
    assert code == (1 if stopped else 0) and calls == [(identity['task_id'], 3, 2)]
    assert result['confirmed_stages_finished'] == 1 and result['rag_handoff'] == 'not_checked'
    assert result['snapshot_selected'] == 3 and result['snapshot_remaining'] == 1
    assert 'remaining' not in result


def test_cli_batch_unknown_keeps_counts_unknown_and_prior_completions_confirmed(launch_case, monkeypatch, capsys):
    c = launch_case
    loaded = build(c)
    monkeypatch.setattr(c.launcher, 'build_from_config', lambda *_: loaded)
    identity = c.x.sample['evidence'][0]['identity']
    async def run_batch(*_args, **_kwargs):
        return dict(completed=['stage-1'], stopped=True, attempted=None, remaining=None,
                    snapshot_selected=4, confirmed_attempted=2, last_group='unconfirmed')
    monkeypatch.setattr(loaded, 'run_batch', run_batch)
    code = c.launcher.main(['run-batch', '--config', str(c.path), '--config-sha256', c.digest,
                            '--task-id', identity['task_id'], '--max-folders', '4'])
    result = json.loads(capsys.readouterr().out)
    assert code == 1 and result['confirmed_stages_finished'] == 1
    assert result['snapshot_selected'] == 4 and result['confirmed_attempted'] == 2
    assert result['snapshot_attempted'] is None and result['snapshot_remaining'] is None
    assert result['last_group'] == 'unconfirmed' and result['rag_handoff'] == 'not_checked'


@pytest.mark.parametrize('variant', ['runtime_next', 'generation_next'])
def test_real_candidate_import_paths_share_canonical_types_without_loading_settings(variant, tmp_path):
    upstream = ROOT.parent / 'postprocess-prefect-20261031'
    runtime = upstream / variant
    project = ROOT.parent.parent / 'prefect-paperflow'
    script = '''
import hashlib
from pathlib import Path
import sys
import remote_live_launch as launch
runtime, project = map(Path, sys.argv[1:])
names = ('live_runtime.py', 'stage_runtime.py', 'batch_next.py')
cfg = dict(runtime_dir=str(runtime), prefect_project=str(project),
           runtime_sources={name: hashlib.sha256((runtime / name).read_bytes()).hexdigest() for name in names})
launch._sources(cfg)
live, remote, settings = launch._import_modules(cfg)
import stage_runtime, remote_runtime_adapter, remote_prefect_entry
assert remote.Coordinator is stage_runtime.Coordinator
assert remote_runtime_adapter.Coordinator is stage_runtime.Coordinator
assert remote_prefect_entry.Coordinator is stage_runtime.Coordinator
assert Path(settings.DEFAULT_CONFIG).resolve() == (project / 'config.json').resolve()
assert Path(live.__file__).resolve() == runtime / 'live_runtime.py'
'''
    env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE='1')
    result = subprocess.run([sys.executable, '-B', '-c', script, str(runtime), str(project)], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_pinned_source_loader_does_not_execute_stale_timestamp_pyc(tmp_path, monkeypatch):
    source = tmp_path / 'synthetic_module.py'
    source.write_bytes(b'VALUE = 1\n')
    timestamp = source.stat().st_mtime_ns
    py_compile.compile(str(source), doraise=True)
    source.write_bytes(b'VALUE = 2\n')
    os.utime(source, ns=(timestamp, timestamp))
    ordinary_spec = importlib.util.spec_from_file_location('synthetic_cached_old', source)
    ordinary = importlib.util.module_from_spec(ordinary_spec)
    ordinary_spec.loader.exec_module(ordinary)
    assert ordinary.VALUE == 1, 'the synthetic stale timestamp bytecode must be reproducible'
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    name = 'synthetic_pinned_new'
    try:
        pinned = module()._load_pinned_source(name, source, sha)
        assert pinned.VALUE == 2 and Path(pinned.__file__) == source
    finally:
        sys.modules.pop(name, None)


@pytest.mark.parametrize('preloaded', ['runtime_next', 'generation_next'])
def test_import_with_any_preloaded_original_stage_class_fails_closed(tmp_path, preloaded):
    upstream = ROOT.parent / 'postprocess-prefect-20261031'
    runtime = upstream / 'runtime_next'
    other = upstream / preloaded
    project = ROOT.parent.parent / 'prefect-paperflow'
    script = '''
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
import stage_runtime
import remote_live_launch as launch
config = dict(runtime_dir=sys.argv[2], prefect_project=sys.argv[3])
try:
    launch._import_modules(config)
except ValueError:
    pass
else:
    raise AssertionError('a second original class was accepted')
'''
    env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE='1')
    result = subprocess.run([sys.executable, '-B', '-c', script, str(other), str(runtime), str(project)],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
