"""Exercise the original one-request transport with a shared start budget."""
import io
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


class Reply(io.BytesIO):
    headers = {'Content-Type': 'application/json'}

    def __init__(self):
        super().__init__(json.dumps(dict(choices=[dict(message=dict(content='saved'), finish_reason='stop')],
                                        usage=dict(prompt_tokens=7, completion_tokens=9))).encode())


def connection(tmp_path, **changes):
    return dict(base_url='http://fixture.invalid/v1', api_key='private-test-key', model='selected',
                min_start_interval_seconds=.06, request_start_root=str(tmp_path / 'shared'), **changes)


def test_same_key_aliases_share_start_spacing_without_serializing_responses(tmp_path):
    from preextract_request import request_once
    starts = []
    lock = threading.Lock()
    all_started = threading.Event()
    release = threading.Event()

    class Opener:
        def open(self, request, timeout):
            with lock:
                starts.append(time.monotonic())
                if len(starts) == 5:
                    all_started.set()
            assert release.wait(5), 'response must not retain the start pacing lock'
            return Reply()

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(request_once, dict(model=f'alias-{i}', messages=[]),
                               connection(tmp_path), tmp_path / f'paper-{i}', opener=Opener()) for i in range(5)]
        try:
            assert all_started.wait(4)
        finally:
            release.set()
        assert [f.result(timeout=3) for f in futures] == ['saved'] * 5
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert min(gaps) >= .055, gaps
    for i in range(5):
        state = json.loads((tmp_path / f'paper-{i}' / 'request-state.json').read_text(encoding='utf8'))
        assert state['status'] == 'response_complete' and state['client_http_requests'] == 1
        assert state['http_started_at'] <= state['finished_at']
        assert state['dispatch_wait_seconds'] >= 0
    assert all('private-test-key' not in p.read_text(encoding='utf8') for p in (tmp_path / 'shared').glob('*.json'))


def test_cancelled_start_wait_has_no_request_intent_and_cache_skips_wait(tmp_path):
    from preextract_request import request_once
    calls = []

    class Opener:
        def open(self, *args, **kwargs):
            calls.append(True)
            return Reply()

    cfg = connection(tmp_path)
    cfg['min_start_interval_seconds'] = 4
    body = dict(model='selected', messages=[])
    assert request_once(body, cfg, tmp_path / 'first', opener=Opener()) == 'saved'
    began = time.monotonic()
    assert request_once(body, cfg, tmp_path / 'first', opener=Opener()) == 'saved'
    assert time.monotonic() - began < .2
    stop = threading.Event()
    ready = threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(request_once, body, cfg, tmp_path / 'cancelled', opener=Opener(), stop=stop,
                              on_request_ready=ready.set)
        time.sleep(.05)
        stop.set()
        with pytest.raises(InterruptedError):
            pending.result(timeout=1)
    assert not ready.is_set()
    assert not (tmp_path / 'cancelled' / 'request-state.json').exists()
    assert len(calls) == 1


def test_failed_send_keeps_shared_start_budget_and_does_not_retry(tmp_path):
    from preextract_request import request_once
    cfg = connection(tmp_path)
    starts = []

    class Opener:
        def open(self, *args, **kwargs):
            starts.append(time.monotonic())
            if len(starts) == 1:
                raise OSError('offline transport')
            return Reply()

    body = dict(model='selected', messages=[])
    with pytest.raises(OSError):
        request_once(body, cfg, tmp_path / 'failed', opener=Opener())
    assert request_once(body, cfg, tmp_path / 'next', opener=Opener()) == 'saved'
    assert starts[1] - starts[0] >= .055
    with pytest.raises(RuntimeError, match='已存在'):
        request_once(body, cfg, tmp_path / 'failed', opener=Opener())
    assert len(starts) == 2


def test_pacing_wait_does_not_acquire_the_original_model_gate(tmp_path):
    from types import SimpleNamespace
    from auto_pipeline import _ObservedGate
    from preextract_request import request_once
    from preextract_workbench import invoke_once
    cfg = connection(tmp_path)
    cfg['min_start_interval_seconds'] = .3
    body = dict(model='selected', messages=[])
    class Opener:
        def open(self, *args, **kwargs):
            return Reply()
    request_once(body, cfg, tmp_path / 'prior-batch', opener=Opener())
    gate = _ObservedGate(10)
    processor = SimpleNamespace(stop=threading.Event(), config={},
        agent=SimpleNamespace(llm_gate=gate, ledger=None, opener=Opener(), on_usage=None))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(invoke_once, processor, body, cfg, tmp_path / 'new-batch')
        time.sleep(.05)
        assert not future.done()
        assert gate.current == 0
        assert future.result(timeout=2) == 'saved'
    assert gate.peak == 1 and gate.current == 0


def test_intent_appearing_during_pacing_never_acquires_an_extra_model_permit(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from concurrent.futures import wait, FIRST_COMPLETED
    from types import SimpleNamespace
    from auto_pipeline import _ObservedGate
    from preextract_workbench import invoke_once
    import preextract_request as request
    barrier = threading.Barrier(2)
    original_window = request._request_start_window
    opened = threading.Event()
    release = threading.Event()
    @contextmanager
    def synchronized_window(*args):
        barrier.wait(timeout=3)
        with original_window(*args) as marker:
            yield marker
    monkeypatch.setattr(request, '_request_start_window', synchronized_window)
    class Opener:
        def open(self, *args, **kwargs):
            opened.set()
            assert release.wait(4)
            return Reply()
    gate = _ObservedGate(10)
    processor = SimpleNamespace(stop=threading.Event(), config={},
        agent=SimpleNamespace(llm_gate=gate, ledger=None, opener=Opener(), on_usage=None))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(invoke_once, processor, dict(model='selected', messages=[]),
                               connection(tmp_path), tmp_path / 'same-paper') for _ in range(2)]
        try:
            assert opened.wait(3)
            done, _ = wait(futures, timeout=2, return_when=FIRST_COMPLETED)
            assert len(done) == 1
            with pytest.raises(RuntimeError, match='已存在'):
                next(iter(done)).result()
            assert gate.current == gate.peak == 1
        finally:
            release.set()
        assert [f.result() for f in futures if f.exception() is None] == ['saved']
    assert gate.current == 0


@pytest.mark.parametrize('value', [-1, float('nan'), float('inf'), 'bad'])
def test_invalid_start_interval_is_rejected_by_existing_config(value):
    from auto_config import normalize_concurrency
    with pytest.raises(ValueError, match='min_start_interval_seconds'):
        normalize_concurrency(dict(min_start_interval_seconds=value))


def test_original_settings_roundtrip_ten_slots_and_sixteen_second_starts(ui):
    from auto_config import load_config
    from auto_ui import read_ai_form, save_form
    _, app = ui
    app.ai_vars['concurrency_papers'].set('10')
    app.ai_vars['concurrency_global_llm'].set('10')
    app.ai_vars['concurrency_min_start_interval_seconds'].set('16')
    save_form(app)
    cfg, _ = read_ai_form(app)
    saved = load_config(app.store.root)
    assert cfg['concurrency']['min_start_interval_seconds'] == saved['concurrency']['min_start_interval_seconds'] == 16
    assert saved['concurrency']['papers'] == saved['concurrency']['global_llm'] == 10
    assert '16' in app.concurrency_status.get() and '4 路' in app.concurrency_status.get()
    assert len(app.tabs.tabs()) == 6


from test_script_task_ui import ui
from test_preextract_workbench_flow import flow


def test_original_status_records_program_stage_durations(flow):
    processor, _, sources = flow
    result = processor.run(sources[:1])
    assert result['items'][0]['status'] == 'pass'
    work = Path(result['items'][0]['preextract_work_dir'])
    status = json.loads((work / 'status.json').read_text(encoding='utf8'))
    timeline = status['stage_timings']
    names = [row['stage'] for row in timeline]
    assert names.index('preparing') < names.index('extracting_once') < names.index('response_saved')
    assert names.index('response_saved') < names.index('validating_candidate') < names.index('package_verified')
    assert all(row['elapsed_seconds'] >= 0 for row in timeline[:-1])
    assert all(a['started_at'] <= b['started_at'] for a, b in zip(timeline, timeline[1:]))
