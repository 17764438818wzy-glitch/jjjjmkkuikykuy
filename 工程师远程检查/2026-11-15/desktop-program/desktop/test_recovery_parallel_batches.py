"""Recovery must actually enter the paper scheduler and retain source receipts."""
import copy
import importlib.util
import json
import io
from pathlib import Path

import pytest


@pytest.fixture
def runner():
    path = Path(__file__).parent / 'data/diagnostics/triage-repair-20260927/recover.py'
    spec = importlib.util.spec_from_file_location('parallel_recovery_runner', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('mode', ['normal', 'service_failure', 'stop_after_batch', 'legacy_console', 'mixed_versions'])
def test_main_dispatches_bounded_batches_using_saved_limits(runner, tmp_path, monkeypatch, mode):
    from engine import Store, write_json
    data = tmp_path / 'data'
    store = Store(data)
    items = [{'id': f'old{i}', 'source_path': f'paper¬{i}.docx', 'status': 'reject'} for i in range(4)]
    if mode == 'mixed_versions':
        for index, item in enumerate(items):
            path = data / 'runs/base/private' / str(index) / 'checkpoint.json'
            write_json(path, {'version': runner.VERSION if index % 2 == 0 else 'old-version'})
            item['prepared_checkpoint'] = {'path': str(path)}
    store.save({'run_id': 'base', 'items': items, 'inputs': [], 'status': 'completed'})
    write_json(tmp_path / 'plan.json', {'base_run': 'base', 'ids': [i['id'] for i in items]})
    write_json(data / 'settings.json', {'ragflow_url': 'https://example.invalid'})
    config = {'concurrency': {'enabled': True, 'papers': 2, 'images_per_paper': 2,
                              'global_llm': 2, 'max_retries': 3, 'backoff_base_seconds': 5}}
    calls = []

    class Processor:
        def __init__(self, store, cfg, *keys):
            self.cfg = copy.deepcopy(cfg)

        def run(self, selected, callback, operation):
            calls.append((copy.deepcopy(selected), self.cfg, operation))
            rid = f'batch{len(calls)}'
            callback({'run_id': rid})
            if mode == 'stop_after_batch':
                (tmp_path / 'STOP_RECOVERY').write_text('stop after saved batch')
            return {'run_id': rid, 'status': 'completed', 'items': [
                {'id': 'new' + row['path'], 'source_path': row['path'],
                 'status': 'reject' if mode == 'service_failure' else 'pass',
                 'reason_kind': 'technical' if mode == 'service_failure' else '',
                 'reason': '503 service unavailable' if mode == 'service_failure' else '',
                 'paper_id': row['path']} for row in selected]}

    monkeypatch.setattr(runner, 'OUT', tmp_path)
    monkeypatch.setattr(runner, 'DATA', data)
    monkeypatch.setattr(runner, 'AutoProcessor', Processor)
    monkeypatch.setattr(runner, 'load_config', lambda _: copy.deepcopy(config))
    monkeypatch.setattr(runner, 'load_key', lambda *args: 'fixture-key')
    def validate(record, *args):
        if mode == 'mixed_versions' and json.loads(Path(record['prepared_checkpoint']['path']).read_text(encoding='utf8'))['version'] != runner.VERSION:
            raise ValueError('old checkpoint version')
    monkeypatch.setattr(runner, 'validate_prepared', validate)
    monkeypatch.setattr(runner.sys, 'argv', ['recover', '--plan', 'plan.json', '--receipt', 'receipt.json'])
    if mode == 'legacy_console':
        monkeypatch.setattr(runner.sys, 'stdout', io.TextIOWrapper(io.BytesIO(), encoding='gbk'))
    runner.main()
    result = json.loads((tmp_path / 'receipt.json').read_text(encoding='utf8'))
    sizes = {'normal': [2, 2], 'legacy_console': [2, 2], 'mixed_versions': [2, 2], 'service_failure': [2, 1], 'stop_after_batch': [2]}[mode]
    assert [len(c[0]) for c in calls] == sizes
    assert [c[1]['concurrency']['global_llm'] for c in calls] == ([2, 1] if mode == 'service_failure' else [2] * len(sizes))
    assert [c[1]['retry_of'] for c in calls] == ['base', 'batch1'][:len(sizes)]
    count = sum(sizes)
    order = [0, 2, 1, 3] if mode == 'mixed_versions' else list(range(count))
    assert [r['old_id'] for r in result['items']] == [f'old{i}' for i in order]
    assert [r['results'][0]['source_path'] for r in result['items']] == [items[i]['source_path'] for i in order]
    if mode == 'mixed_versions': assert [c[2] for c in calls] == ['retry_ai', 'process']
    assert result['last_run'] == f'batch{len(sizes)}'
    assert result['status'] == {'normal': 'finished', 'legacy_console': 'finished', 'mixed_versions': 'finished', 'service_failure': 'service_blocked', 'stop_after_batch': 'stopped'}[mode]


def test_batch_receipts_keep_archive_members_separate(runner, tmp_path):
    records = [{'path': 'extracted/a.docx', 'input_source_path': 'source.zip', 'archive_member_path': 'a.docx'},
               {'path': 'extracted/b.docx', 'input_source_path': 'source.zip', 'archive_member_path': 'b.docx'}]
    class Processor:
        def run(self, selected, callback, operation):
            callback({'run_id': 'batch'})
            return {'run_id': 'batch', 'status': 'completed', 'items': [
                {'id': 'b', 'source_path': 'elsewhere/b.docx', 'input_source_path': 'source.zip',
                 'archive_member_path': 'b.docx', 'status': 'reject'},
                {'id': 'a', 'source_path': 'elsewhere/a.docx', 'input_source_path': 'source.zip',
                 'archive_member_path': 'a.docx', 'status': 'pass'}]}
    receipt = {'last_run': 'base', 'items': [], 'status': 'running'}
    runner.execute_batch(Processor(), records, 'retry_ai', ['old-a', 'old-b'], receipt,
                         tmp_path / 'receipt.json', lambda e: None)
    assert [(r['old_id'], r['new_id']) for r in receipt['items']] == [('old-a', 'a'), ('old-b', 'b')]


def test_batch_crash_keeps_every_pending_source_and_created_run(runner, tmp_path):
    class Processor:
        def run(self, selected, callback, operation):
            callback({'run_id': 'created'})
            raise RuntimeError('fixture interruption')
    receipt = {'last_run': 'base', 'items': [], 'status': 'running'}
    with pytest.raises(RuntimeError):
        runner.execute_batch(Processor(), [{'path': 'a'}, {'path': 'b'}], 'process',
                             ['old-a', 'old-b'], receipt, tmp_path / 'receipt.json', lambda e: None)
    saved = json.loads((tmp_path / 'receipt.json').read_text(encoding='utf8'))
    assert saved['status'] == 'failed' and saved['last_run'] == 'created'
    assert saved['active'] == {'old_ids': ['old-a', 'old-b'], 'run_id': 'created'}
    assert saved['items'] == []


def test_archive_expansion_and_duplicate_skip_have_individual_receipts(runner, tmp_path):
    class Processor:
        def run(self, selected, callback, operation):
            return {'run_id': 'batch', 'status': 'completed', 'items': [
                {'id': 'a', 'source_path': 'private/a.docx', 'input_source_path': 'source.zip', 'status': 'pass'},
                {'id': 'b', 'source_path': 'private/b.docx', 'input_source_path': 'source.zip', 'status': 'reject'}],
                'events': [{'status': 'duplicate', 'source': 'same.docx'}]}
    receipt = {'last_run': 'base', 'items': [], 'status': 'running'}
    rows = runner.execute_batch(Processor(), [{'path': 'source.zip'}, {'path': 'same.docx'}], 'process',
                                ['archive', 'duplicate'], receipt, tmp_path / 'receipt.json', lambda e: None)
    assert [r['status'] for r in rows] == ['mixed', 'skipped']
    assert len(rows[0]['results']) == 2 and len(rows[1]['events']) == 1


def test_legacy_archive_member_duplicate_event_can_match_its_exact_path(runner, tmp_path):
    record = {'path': 'private/archive/a.docx', 'input_source_path': 'source.zip', 'archive_member_path': 'a.docx'}
    class Processor:
        def run(self, selected, callback, operation):
            return {'run_id': 'batch', 'status': 'completed', 'items': [],
                    'events': [{'status': 'duplicate', 'source': record['path']}]}
    receipt = {'last_run': 'base', 'items': [], 'status': 'running'}
    rows = runner.execute_batch(Processor(), [record], 'retry_ai', ['old-member'], receipt,
                                tmp_path / 'receipt.json', lambda e: None)
    assert rows[0]['old_id'] == 'old-member' and rows[0]['status'] == 'skipped'
    assert rows[0]['events'][0]['status'] == 'duplicate'
