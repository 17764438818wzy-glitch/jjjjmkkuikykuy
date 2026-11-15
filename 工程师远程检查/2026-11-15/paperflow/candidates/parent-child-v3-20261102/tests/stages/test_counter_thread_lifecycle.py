"""Synthetic regression coverage for the postprocessing counter's owner thread."""
import asyncio
from contextlib import asynccontextmanager
import inspect
from pathlib import Path
import sys
import threading

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_synthetic_mainline_integration import synthetic_folder
from test_body_adapter import FixtureCounter, RELEASE
from paperflow_prefect.records import sha_file


@pytest.fixture
def offline_loop(monkeypatch):
    # Windows needs its loopback socket pair before external calls are disabled.
    loop = asyncio.new_event_loop()
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *_a, **_k: pytest.fail('network forbidden'))
    yield loop.run_until_complete
    loop.close()


@pytest.fixture
def postprocessing_case(tmp_path, monkeypatch, offline_loop):
    from paperflow_v3.stages import llm_handoff, postprocessing

    identity, manifest, paper, admissions, catalog, attachment_work = synthetic_folder(tmp_path)
    originals = [Path(row['model_dir']) / 'response.txt' for row in admissions]
    hashes = [sha_file(path) for path in originals]
    events = [{'task_id': identity['task_id'], 'folder_id': identity['group_id'],
        'kind': 'llm_response_saved', 'data': {'component_id': row['component_id'],
            'request_id': row['component_id'], 'kind': row['kind'],
            'request_sha256': row['request_sha256'], 'response_sha256': row['response_sha256'],
            'response_text': (Path(row['model_dir']) / 'response.txt').read_bytes().decode('utf8'),
            'response_path': str(Path(row['model_dir']) / 'response.txt')}} for row in admissions]

    async def read_receipts(_child):
        return list(reversed(events))

    async def save_receipt(**row):
        events.append(row)

    class Backend:
        def attachment_work(self, _paper, _row):
            return Path(attachment_work)

    class Admission:
        async def running(self):
            return True
        async def wait(self):
            return None

    active = []
    @asynccontextmanager
    async def slot(_admission, _limits):
        if hasattr(_admission, 'admit'):
            assert await _admission.admit()
        active.append(True)
        try:
            yield
        finally:
            active.pop()

    for module in (llm_handoff, postprocessing):
        monkeypatch.setattr(module, 'read_receipts', read_receipts)
        monkeypatch.setattr(module, 'save_receipt', save_receipt)
    monkeypatch.setattr(postprocessing, 'admitted_slot', slot)
    inputs = offline_loop(llm_handoff.build_postprocessing_inputs(
        identity=identity, manifest=manifest, paper=paper, backend=Backend()))

    lifecycle = []
    class ThreadBoundCounter(FixtureCounter):
        def __init__(self):
            self.owner = threading.get_ident()
            lifecycle.append(('created', self.owner, bool(active)))

        def count_many(self, texts):
            if threading.get_ident() != self.owner:
                raise ValueError('offline counter belongs to its worker thread')
            lifecycle.append(('counted', threading.get_ident(), bool(active)))
            return super().count_many(texts)

        def close(self):
            assert threading.get_ident() == self.owner
            lifecycle.append(('closed', threading.get_ident(), bool(active)))

    def counter_argument():
        # Exercise the pre-fix call too, so this test reproduces the real bug.
        if 'counter_factory' in inspect.signature(postprocessing.process_folder).parameters:
            return {'counter_factory': ThreadBoundCounter}
        return {'counter': ThreadBoundCounter()}

    options = dict(inputs=inputs, catalog=catalog, paperflow_root=RELEASE / 'core',
                   output_root=tmp_path / 'output', admission=Admission(),
                   limit_names=['synthetic-postprocessing'])
    return postprocessing, options, counter_argument, lifecycle, events, originals, hashes


def test_counter_created_used_and_closed_inside_the_processing_worker(postprocessing_case, offline_loop):
    postprocessing, options, counter_argument, lifecycle, events, originals, hashes = postprocessing_case
    main_thread = threading.get_ident()
    result = offline_loop(postprocessing.process_folder(**options, **counter_argument()))
    assert result['validation_status'] == 'passed'
    assert [row[0] for row in lifecycle].count('created') == 1
    assert any(row[0] == 'counted' for row in lifecycle)
    assert [row[0] for row in lifecycle].count('closed') == 1
    owner = lifecycle[0][1]
    assert owner != main_thread
    assert all(thread == owner and in_slot for _, thread, in_slot in lifecycle)
    assert [sha_file(path) for path in originals] == hashes
    assert not any(row['kind'] == 'llm_request_started' for row in events)


def test_counter_is_closed_after_a_processing_exception(postprocessing_case, monkeypatch, offline_loop):
    postprocessing, options, counter_argument, lifecycle, events, originals, hashes = postprocessing_case

    def fail_body(*_args, **_kwargs):
        raise ValueError('synthetic execution failure')

    monkeypatch.setattr(postprocessing, 'process_body', fail_body)
    with pytest.raises(ValueError, match='synthetic execution failure'):
        offline_loop(postprocessing.process_folder(**options, **counter_argument()))
    assert [row[0] for row in lifecycle] == ['created', 'closed']
    assert lifecycle[0][1] == lifecycle[1][1] != threading.get_ident()
    assert all(row[2] for row in lifecycle)
    assert events[-1]['kind'] == 'postprocessing_component_failed'
    assert [sha_file(path) for path in originals] == hashes


def test_cancellation_waits_for_worker_to_close_counter(postprocessing_case, monkeypatch, offline_loop):
    postprocessing, options, counter_argument, lifecycle, events, originals, hashes = postprocessing_case
    started, release = threading.Event(), threading.Event()

    def slow_body(*_args, counter, **_kwargs):
        counter.count_many(['synthetic text'])
        started.set()
        assert release.wait(5)
        return {}

    monkeypatch.setattr(postprocessing, 'process_body', slow_body)

    async def cancel_while_running():
        task = asyncio.create_task(postprocessing.process_folder(**options, **counter_argument()))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            await asyncio.sleep(.05)
            assert not task.done()
            assert [row[0] for row in lifecycle] == ['created', 'counted']
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()

    offline_loop(cancel_while_running())
    assert [row[0] for row in lifecycle] == ['created', 'counted', 'closed']
    assert len({row[1] for row in lifecycle}) == 1
    assert all(row[2] for row in lifecycle)
    assert not any(row['kind'] == 'postprocessing_component_failed' for row in events)
    assert [sha_file(path) for path in originals] == hashes


def test_explicit_body_retry_reuses_original_model_responses(postprocessing_case, monkeypatch, offline_loop):
    postprocessing, options, counter_argument, lifecycle, events, originals, hashes = postprocessing_case
    original_body = postprocessing.process_body
    original_responses = [row['data'].copy() for row in events if row['kind'] == 'llm_response_saved']

    def fail_body(*_args, **_kwargs):
        raise ValueError('synthetic postprocessing failure')

    monkeypatch.setattr(postprocessing, 'process_body', fail_body)
    with pytest.raises(ValueError, match='synthetic postprocessing failure'):
        offline_loop(postprocessing.process_folder(**options, **counter_argument()))
    monkeypatch.setattr(postprocessing, 'process_body', original_body)
    with pytest.raises(RuntimeError, match='人工重试'):
        offline_loop(postprocessing.process_folder(**options, **counter_argument()))
    result = offline_loop(postprocessing.process_folder(
        **options, **counter_argument(), retry_id='synthetic-postprocessing-retry'))

    assert result['validation_status'] == 'passed'
    assert [row[0] for row in lifecycle].count('created') == 2
    assert [row[0] for row in lifecycle].count('closed') == 2
    assert [row['data'] for row in events if row['kind'] == 'llm_response_saved'] == original_responses
    assert not any(row['kind'] in {'llm_request_intent', 'llm_request_started'} for row in events)
    assert [sha_file(path) for path in originals] == hashes


def test_control_passes_lazy_factory_into_the_body_worker(tmp_path, monkeypatch, offline_loop):
    from paperflow_prefect import legacy, records
    from paperflow_prefect.resources import drain_thread
    from paperflow_v3.control import flows
    from paperflow_v3.stages import pipeline, postprocessing
    import preextract_pipeline

    lifecycle = []
    class Counter:
        def __init__(self):
            self.owner = threading.get_ident()
            lifecycle.append(('created', self.owner))

        def count_many(self, _texts):
            assert threading.get_ident() == self.owner
            lifecycle.append(('counted', threading.get_ident()))

        def close(self):
            assert threading.get_ident() == self.owner
            lifecycle.append(('closed', threading.get_ident()))

    class Scoped:
        def path(self, name):
            return tmp_path / name

    monkeypatch.setattr(flows, 'folder_settings', lambda *_args: Scoped())
    monkeypatch.setattr(legacy, 'bind', lambda *_args: None)
    monkeypatch.setattr(legacy, 'connection', lambda *_args: {'model': 'synthetic'})
    monkeypatch.setattr(records, 'read_json', lambda _path: {'synthetic': True})
    monkeypatch.setattr(preextract_pipeline, 'offline_counter', lambda *_args: Counter())

    def process_body(_item, _output, *, counter, **_kwargs):
        counter.count_many(['synthetic text'])
        return 'processed'

    async def process_paper(**kwargs):
        assert not lifecycle
        return await drain_thread(lambda: postprocessing._process_body(
            {}, tmp_path / 'output', identity=kwargs['identity'], catalog=kwargs['catalog'],
            paperflow_root=kwargs['paperflow_root'], counter_factory=kwargs['counter_factory']))

    monkeypatch.setattr(postprocessing, 'process_body', process_body)
    monkeypatch.setattr(pipeline, 'process_paper', process_paper)
    main_thread = threading.get_ident()
    identity = {'task_id': 'synthetic-task', 'group_id': 'synthetic-group'}
    result = offline_loop(flows._process_step(identity, {'paper': {}}, object(), object(), {}, None))
    assert result == 'processed'
    assert [row[0] for row in lifecycle] == ['created', 'counted', 'closed']
    assert len({row[1] for row in lifecycle}) == 1
    assert lifecycle[0][1] != main_thread
