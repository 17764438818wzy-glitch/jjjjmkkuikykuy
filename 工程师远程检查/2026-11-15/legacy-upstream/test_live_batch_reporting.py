"""Regressions for the running-batch symptoms observed on 2026-09-27."""
import io
import json
import os
from types import SimpleNamespace
import zipfile

import pytest

from engine import Store
from script_task import task_snapshot, task_rows
from test_parallel_result_regressions import processor, record


def test_child_completion_does_not_announce_the_whole_batch_finished(tmp_path):
    paths = [tmp_path / name for name in ('one.txt', 'two.txt')]
    for path in paths:
        path.write_text('short synthetic source ' + path.name, encoding='utf-8')
    events = []
    result = processor(tmp_path).run([record(p) for p in paths], events.append)
    ended = [e for e in events if e.get('progress', {}).get('stage') == '任务结束']
    assert len(ended) == 1
    assert ended[0]['status'] == 'completed'
    assert ended[0]['processed_sources'] == result['source_count'] == 2


def test_running_retry_shows_queued_files_as_pending_and_keeps_prior_reason(tmp_path):
    store = Store(tmp_path / 'data')
    waiting, failed, done = [str(tmp_path / name) for name in ('waiting.txt', 'bad.txt', 'done.txt')]
    old = {'run_id': 'old', 'status': 'stopped', 'inputs': [{'path': p} for p in (waiting, failed, done)],
           'items': [{'id': 'bad', 'source_path': failed, 'status': 'reject',
                      'reason_kind': 'technical', 'reason': '读取失败'},
                     {'id': 'done', 'source_path': done, 'status': 'pass',
                      'automatic_approval': True, 'title': '企业财务管理研究', 'major': '会计学'}]}
    store.save(old)
    live = {'run_id': 'retry', 'retry_of': 'old', 'status': 'running',
            'inputs': [{'path': waiting}, {'path': failed}], 'items': []}
    rows = {r['path']: r for r in task_rows(task_snapshot(store, live))}
    assert rows[waiting]['script_status'] == rows[failed]['script_status'] == '待处理'
    assert '读取失败' in rows[failed]['previous_error']
    assert rows[done]['script_status'] == '通过'
    live['items'] = [{'id': 'bad-new', 'source_path': failed, 'status': 'reject',
                      'reason_kind': 'technical', 'reason': '本次读取失败'}]
    rows = {r['path']: r for r in task_rows(task_snapshot(store, live))}
    assert rows[failed]['script_status'] == '不通过'
    assert '本次读取失败' in rows[failed]['script_reason']
    live['status'] = 'stopped'
    assert next(r for r in task_rows(task_snapshot(store, live)) if r['path'] == waiting)['script_status'] == '不通过'


def test_completed_archive_child_replaces_old_unfinished_archive_placeholder(tmp_path):
    store = Store(tmp_path / 'data')
    zipped, member = str(tmp_path / 'source.zip'), str(tmp_path / 'private' / 'paper.txt')
    store.save({'run_id': 'old', 'status': 'stopped', 'inputs': [{'path': zipped}], 'items': []})
    live = {'run_id': 'retry', 'status': 'running', 'retry_of': 'old', 'inputs': [{'path': zipped}],
            'child_runs': [{'input_source': zipped, 'status': 'completed'}],
            'items': [{'id': 'member', 'source_path': member, 'input_source_path': zipped,
                       'status': 'reject', 'reason_kind': 'screening', 'reason': '正文过短'}]}
    rows = task_rows(task_snapshot(store, live))
    assert [r['path'] for r in rows] == [member]


def test_parallel_progress_exposes_recorded_tokens(tmp_path):
    from test_workbench import paper
    path = tmp_path / 'paper.txt'
    path.write_text(paper(), encoding='utf-8')
    short = tmp_path / 'short.txt'
    short.write_text('short synthetic source', encoding='utf-8')
    proc = processor(tmp_path)
    proc.config = {**proc.config, 'base_url': 'https://example.test/v1', 'text_model': 'fixture-model'}
    proc.agent.key = 'synthetic-key'
    class Opener:
        def open(self, *args, **kwargs):
            return io.BytesIO(json.dumps({'usage': {'prompt_tokens': 1000, 'completion_tokens': 100,
                                                   'total_tokens': 1100},
                                         'choices': [{'finish_reason': 'stop', 'message': {'content': 'invalid'}}]}).encode())
    proc.agent.opener = Opener()
    events = []
    result = proc.run([record(path), record(short)], events.append)
    assert result['billing']['total_tokens'] > 0
    assert any(e.get('billing', {}).get('total_tokens', 0) > 0 for e in events)


def test_completed_paper_billing_reads_the_ledger_once(tmp_path, monkeypatch):
    from billing import Ledger
    original = Ledger.records
    filtered_scans = []
    def records(self, run_id=None, item_id=None):
        if self.path == tmp_path / 'data' / 'ai_usage.jsonl' and run_id is not None:
            filtered_scans.append(run_id)
        return original(self, run_id, item_id)
    monkeypatch.setattr(Ledger, 'records', records)
    paths = [tmp_path / f'{n}.txt' for n in range(4)]
    for path in paths:
        path.write_text('short synthetic source ' + path.name, encoding='utf-8')
    result = processor(tmp_path).run([record(p) for p in paths])
    assert result['processed_sources'] == 4
    assert not filtered_scans, 'The parent must filter all child receipts from one ledger snapshot'


def test_batch_billing_details_include_child_requests(tmp_path):
    from billing import Ledger
    from billing_ui import show_details
    store = Store(tmp_path / 'data')
    store.save({'run_id': 'parent', 'status': 'completed', 'items': [],
                'child_runs': [{'run_id': 'child'}], 'billing': {}})
    ledger = Ledger(store.root)
    for rid, total in [('child', 1100), ('unrelated', 2200)]:
        receipt = ledger.begin('fixture-model', 'text', 'screen', 1, {}, {'run_id': rid})
        ledger.finish(receipt, {'prompt_tokens': total - 100, 'completion_tokens': 100,
                                'total_tokens': total}, 'response')
    captured = []
    app = SimpleNamespace(store=store, billing_run_id='parent', text_window=lambda title, body: captured.append(body))
    show_details(app)
    summary = captured[0].split('批次 parent\n', 1)[1].split('\n', 1)[0]
    assert '1,100 Token' in summary


def test_video_only_archive_is_material_screening_not_extraction_failure(tmp_path):
    path = tmp_path / 'video.zip'
    with zipfile.ZipFile(path, 'w') as stream:
        stream.writestr('lesson.mp4', b'synthetic video')
    before = path.read_bytes()
    result = processor(tmp_path).run([record(path)])
    item = result['items'][0]
    assert item['reason_kind'] == 'screening'
    assert '论文' in item['reason'] and '压缩包存在无法解压' not in item['reason']
    assert item['ai_result']['status'] == 'not_started'
    assert path.read_bytes() == before


def test_nonword_zip_with_docx_extension_has_an_explicit_input_error(tmp_path):
    from agent_client import AgentError
    from auto_pipeline import lightweight
    path = tmp_path / 'drawing.docx'
    with zipfile.ZipFile(path, 'w') as stream:
        stream.writestr('[Content_Types].xml', '<Types/>')
        stream.writestr('drs/e2oDoc.xml', '<drawing/>')
    with pytest.raises(AgentError, match='正文'):
        lightweight(path)


@pytest.mark.skipif(os.name != 'nt', reason='Windows extended path spelling')
def test_ai_resume_accepts_equivalent_extended_windows_checkpoint_path(tmp_path):
    from ai_resume import validate_prepared
    from auto_pipeline import VERSION
    from test_ai_resume import rejected_run, records
    store, _, first = rejected_run(tmp_path)
    saved = records(first)[0]
    saved['prepared_checkpoint'] = {**saved['prepared_checkpoint'],
                                    'path': '\\\\?\\' + saved['prepared_checkpoint']['path']}
    _, checkpoint = validate_prepared(saved, store.root, VERSION)
    assert checkpoint['source_hash'] == first['items'][0]['source_hash']
