"""Real local persistence must stay fresh without rereading unrelated history."""
import json
import os
import sqlite3
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from billing import Ledger
from stage_cache import StageCache


def receipt(identifier, run='run', item='paper', tokens=11):
    return {'id': identifier, 'run_id': run, 'item_id': item, 'scope': 'processing',
            'model': 'fixture', 'task': 'screen', 'channel': 'primary',
            'usage': {'input_tokens': tokens - 1, 'output_tokens': 1,
                      'total_tokens': tokens, 'cached_tokens': 0},
            'cost_cny': '0.000000000000000001', 'seconds': 1.25,
            'padding': 'x' * 1000}


class ObservedFile:
    def __init__(self, stream, counts):
        self.stream, self.counts = stream, counts

    def __getattr__(self, name):
        return getattr(self.stream, name)

    def __enter__(self):
        self.stream.__enter__()
        return self

    def __exit__(self, *args):
        return self.stream.__exit__(*args)

    def _count(self, value):
        self.counts.append(len(value.encode('utf-8') if isinstance(value, str) else value))
        return value

    def read(self, *args):
        return self._count(self.stream.read(*args))

    def readline(self, *args):
        return self._count(self.stream.readline(*args))

    def __iter__(self):
        return self

    def __next__(self):
        return self._count(next(self.stream))


def observe_ledger(monkeypatch, path):
    original = Path.open
    counts = []
    def opened(self, mode='r', *args, **kwargs):
        stream = original(self, mode, *args, **kwargs)
        return ObservedFile(stream, counts) if self == path and 'r' in mode else stream
    monkeypatch.setattr(Path, 'open', opened)
    return counts


def test_ledger_instances_read_only_new_bytes_after_warming(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path)
    for n in range(100): ledger.append(receipt(str(n)))
    assert len(ledger.records()) == 100
    counts = observe_ledger(monkeypatch, ledger.path)
    assert len(Ledger(tmp_path).records()) == 100
    assert sum(counts) < 8192, 'A second instance reread the entire historical journal'
    counts.clear()
    ledger.append(receipt('new', tokens=21))
    assert len(Ledger(tmp_path).records()) == 101
    assert sum(counts) < 10000


def test_summary_deduplicates_begin_finish_and_keeps_decimal_precision(tmp_path):
    ledger = Ledger(tmp_path)
    pending = ledger.begin('fixture', 'text', 'screen', 1, {}, {'run_id': 'a', 'item_id': 'one'})
    assert ledger.summary()['unknown_usage'] == 1
    ledger.finish(pending, {'prompt_tokens': 10, 'completion_tokens': 1}, 'response')
    ledger.append(receipt('extra', run='b', item='two', tokens=21))
    ledger.append(receipt('priced', run='b', item='one', tokens=31))
    assert ledger.summary()['requests'] == 3
    assert ledger.summary()['total_tokens'] == 63
    assert ledger.summary()['unknown_usage'] == 0
    assert Decimal(ledger.summary()['cost_cny']) == Decimal('0.000000000000000002')
    assert ledger.summary(run_id='b')['total_tokens'] == 52
    assert ledger.summary(item_id='one')['total_tokens'] == 42
    assert ledger.summary(run_id='b', item_id='one')['total_tokens'] == 31
    assert ledger.summary(run_ids=['a', 'b', 'b'])['total_tokens'] == 63
    assert ledger.summary(run_ids=[])['requests'] == 0


@pytest.mark.parametrize('channel', ['primary', 'fallback'])
def test_cancelled_before_send_replaces_pending_request_in_every_summary(tmp_path, channel):
    from billing import summarize
    ledger = Ledger(tmp_path)
    pending = ledger.begin('gpt-5.5', 'vision', 'screen', 1, {},
                           {'run_id': 'cancelled-run', 'item_id': 'cancelled-paper'}, channel=channel)
    assert ledger.summary()['requests'] == 1
    assert ledger.summary()['unknown_usage'] == 1
    ledger.finish(pending, {'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0},
                  'cancelled_before_send', ok=False, error_type='cancelled_before_send')
    rows = ledger.records()
    assert len(rows) == 1 and rows[0]['id'] == pending['id']
    assert rows[0]['outcome'] == rows[0]['error_type'] == 'cancelled_before_send'
    summaries = [summarize(rows), ledger.summary(),
                 ledger.summary(run_id='cancelled-run'),
                 ledger.summary(item_id='cancelled-paper'),
                 ledger.summary(run_id='cancelled-run', item_id='cancelled-paper'),
                 ledger.summary(run_ids=['cancelled-run'])]
    for summary in summaries:
        assert summary['requests'] == summary['unknown_usage'] == 0
        assert summary['primary_requests'] == summary['fallback_requests'] == 0
        assert summary['input_tokens'] == summary['output_tokens'] == summary['total_tokens'] == 0
        assert summary['priced_requests'] == summary['unpriced'] == 0
        assert Decimal(summary['cost_cny']) == 0
    code = "from billing import Ledger; import json,sys; print(json.dumps(Ledger(sys.argv[1]).summary()))"
    restarted = subprocess.run([sys.executable, '-B', '-c', code, str(tmp_path)],
                               cwd=Path(__file__).parent, capture_output=True, text=True, check=True)
    assert json.loads(restarted.stdout)['requests'] == 0
    assert json.loads(restarted.stdout)['unknown_usage'] == 0


def test_external_append_partial_tail_rotation_and_regrowth_rebuild(tmp_path):
    ledger = Ledger(tmp_path)
    ledger.append(receipt('one'))
    assert ledger.summary()['total_tokens'] == 11
    with ledger.path.open('ab') as stream:
        stream.write((json.dumps(receipt('two', tokens=21)) + '\n').encode())
        stream.write(b'{"id":"unfinished')
    assert Ledger(tmp_path).summary()['total_tokens'] == 32
    ledger.append(receipt('three', tokens=31))
    assert ledger.summary()['total_tokens'] == 63
    replacement = ledger.path.with_suffix('.replacement')
    replacement.write_text(json.dumps(receipt('rotated', tokens=41)) + '\n', encoding='utf-8')
    os.replace(replacement, ledger.path)
    assert ledger.summary()['requests'] == 1
    assert ledger.records()[0]['id'] == 'rotated'
    # Truncation followed by regrowth can exceed the old cursor position.
    ledger.path.write_text(json.dumps({**receipt('regrown', tokens=51), 'padding': 'y' * 4000}) + '\n', encoding='utf-8')
    assert ledger.summary()['total_tokens'] == 51
    assert [row['id'] for row in ledger.records()] == ['regrown']


def test_legacy_complete_final_row_without_newline_remains_visible(tmp_path):
    ledger = Ledger(tmp_path)
    ledger.path.write_text(json.dumps(receipt('legacy')), encoding='utf-8')
    assert ledger.summary()['total_tokens'] == 11
    assert ledger.records()[0]['id'] == 'legacy'
    ledger.append(receipt('next', tokens=21))
    assert ledger.summary()['total_tokens'] == 32


def test_restarted_reader_reuses_persisted_cursor_and_rebuilds_corrupt_index(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path)
    for n in range(100): ledger.append(receipt(str(n)))
    assert ledger.summary()['requests'] == 100
    code = '''import json,sys
from pathlib import Path
from billing import Ledger
from test_audit_io_fixes import ObservedFile
counts=[]; original=Path.open; ledger=Ledger(sys.argv[1])
def opened(self,mode='r',*args,**kwargs):
    stream=original(self,mode,*args,**kwargs)
    return ObservedFile(stream,counts) if self==ledger.path and 'r' in mode else stream
Path.open=opened
print(json.dumps({'requests':ledger.summary()['requests'],'bytes':sum(counts)}))
'''
    result = subprocess.run([sys.executable, '-B', '-c', code, str(tmp_path)],
                            cwd=Path(__file__).parent, capture_output=True, text=True, check=True)
    assert json.loads(result.stdout)['requests'] == 100
    assert json.loads(result.stdout)['bytes'] < 8192
    (tmp_path/'private'/'ai_usage_index.sqlite3').write_bytes(b'corrupt derived index')
    assert Ledger(tmp_path).summary()['requests'] == 100
    assert len(ledger.records()) == 100


def test_damaged_derived_cursor_rebuilds_from_authoritative_journal(tmp_path):
    from contextlib import closing
    ledger = Ledger(tmp_path)
    ledger.append(receipt('durable'))
    assert ledger.summary()['total_tokens'] == 11
    with closing(sqlite3.connect(tmp_path/'private'/'ai_usage_index.sqlite3')) as connection:
        with connection: connection.execute("UPDATE journal SET metadata='{' WHERE id=1")
    assert Ledger(tmp_path).summary()['total_tokens'] == 11


def test_parallel_ledger_instances_keep_each_latest_receipt(tmp_path):
    def worker(n):
        ledger = Ledger(tmp_path)
        row = ledger.begin('fixture', 'text', 'screen', 1, {}, {'run_id': str(n % 2), 'item_id': str(n)})
        ledger.finish(row, {'prompt_tokens': 10, 'completion_tokens': 1}, 'response')
        return ledger.summary()['requests']
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(20)))
    summary = Ledger(tmp_path).summary()
    assert summary['requests'] == 20 and summary['total_tokens'] == 220
    assert summary['unknown_usage'] == 0


def test_ui_uses_batch_event_summary_and_incremental_total(tmp_path, monkeypatch):
    from billing_ui import update_progress
    import tkinter as tk
    interpreter = tk.Tcl()
    ledger = Ledger(tmp_path)
    ledger.append(receipt('child', run='child'))
    app = SimpleNamespace(store=SimpleNamespace(root=tmp_path),
                          billing_line=tk.StringVar(master=interpreter),
                          billing_total=tk.StringVar(master=interpreter))
    monkeypatch.setattr(Ledger, 'records', lambda *_args, **_kwargs: pytest.fail('UI reread ledger records'))
    value = {'run_id': 'parent', 'billing': {'requests': 1, 'input_tokens': 10,
             'output_tokens': 1, 'total_tokens': 11, 'cached_tokens': 0,
             'unknown_usage': 0, 'unpriced': 0, 'priced_requests': 1,
             'cost_cny': '0.01', 'primary_requests': 1, 'fallback_requests': 0,
             'circuit_breaker_trips': 0, 'seconds': 1.25}}
    update_progress(app, value)
    assert '11 Token' in app.billing_line.get() and '0.010000' in app.billing_line.get()
    assert '11 Token' in app.billing_total.get()
    ledger.append(receipt('second', run='child', tokens=21))
    update_progress(app, value)
    assert '32 Token' in app.billing_total.get()


def test_billing_details_include_still_running_child_receipts(tmp_path):
    from billing_ui import show_details
    from engine import Store
    store = Store(tmp_path)
    store.save({'run_id': 'parent', 'status': 'running', 'items': [],
                'child_runs': [], 'billing_run_ids': ['running-child'], 'billing': {}})
    ledger = Ledger(tmp_path)
    ledger.append(receipt('child', run='running-child', tokens=21))
    captured = []
    app = SimpleNamespace(store=store, billing_run_id='parent',
                          text_window=lambda title, body: captured.append(body))
    show_details(app)
    batch = captured[0].split('批次 parent\n', 1)[1].split('\n', 1)[0]
    assert '21 Token' in batch


def diagnostic(cache, source='a' * 64, value='original', version='v1', endpoint='https://fixture.invalid/v1'):
    request = json.dumps({'messages': [{'content': value}]}).encode()
    key = cache.key(source, version, endpoint, request)
    return key, cache.request_diagnostic(source, version, endpoint, request, key)


def test_cache_miss_uses_scope_metadata_without_opening_response_files(tmp_path, monkeypatch):
    cache = StageCache(tmp_path)
    key, saved = diagnostic(cache)
    assert cache.save(key, {'decision': 'pass', 'reason': 'PRIVATE_MODEL_RESPONSE'}, saved)
    for n in range(50):
        other_key, other = diagnostic(cache, source=f'{n + 1:064x}')
        assert cache.save(other_key, {'decision': 'pass', 'reason': 'unrelated'}, other)
    original = Path.read_text
    def reject_response_read(self, *args, **kwargs):
        if self.parent == cache.root and self.suffix == '.json':
            pytest.fail('Miss diagnosis opened a model response')
        return original(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', reject_response_read)
    changed_key, changed = diagnostic(cache, value='changed')
    report = StageCache(tmp_path).diagnose_miss(changed_key, changed)
    assert report['reason'] == 'request_field_changed'
    assert report['first_difference'] == 'messages[0].content'
    for path in cache.root.rglob('*'):
        if path.is_file() and path.suffix != '.json':
            raw = path.read_bytes()
            assert b'PRIVATE_MODEL_RESPONSE' not in raw and b'original' not in raw


def test_cache_diagnostic_scope_and_discard_cannot_invent_a_candidate(tmp_path):
    cache = StageCache(tmp_path)
    key, saved = diagnostic(cache)
    cache.save(key, {'decision': 'pass', 'reason': 'fixture'}, saved)
    for overrides in ({'source': 'b' * 64}, {'version': 'v2'}, {'endpoint': 'https://other.invalid/v1'}):
        missing_key, missing = diagnostic(cache, value='changed', **overrides)
        assert cache.diagnose_miss(missing_key, missing)['reason'] == 'cache_key_not_found'
    cache.discard(key)
    missing_key, missing = diagnostic(cache, value='changed')
    assert StageCache(tmp_path).diagnose_miss(missing_key, missing)['reason'] == 'cache_key_not_found'
