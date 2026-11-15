"""Task lifecycle and archive identity regressions from the live audit."""
import io
import copy
import json
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app import retry_sources
from auto_pipeline import AutoProcessor, _ObservedGate
from engine import Store
from script_task import task_snapshot, task_rows


def record(path):
    return {'path': str(path), 'name': path.name}


def processor(store, retry=None, parallel=False):
    cfg = {'auto_upload': False, 'resume': {'enabled': False},
           'concurrency': {'enabled': parallel, 'papers': 2, 'global_llm': 1,
                           'images_per_paper': 1, 'max_retries': 1,
                           'backoff_base_seconds': 0}}
    if retry:
        cfg['retry_of'] = retry
    return AutoProcessor(store, cfg)


def test_report_write_failure_saves_failed_batch_and_remains_retryable(tmp_path):
    class BlockedStore(Store):
        def save(self, manifest):
            super().save(manifest)
            if manifest['status'] == 'running' and not manifest['items']:
                path = self.runs / manifest['run_id'] / 'rejected'
                if not path.exists():
                    path.write_text('synthetic output collision', encoding='utf-8')

    source = tmp_path / 'short.txt'
    source.write_text('short original', encoding='utf-8')
    before = source.read_bytes()
    store = BlockedStore(tmp_path / 'data')
    events = []
    with pytest.raises(FileExistsError):
        processor(store).run([record(source)], events.append)
    saved = store.history()[0]
    assert saved['status'] == 'failed'
    assert saved['finished_at']
    assert saved['items'][0]['script_result']['status'] == 'failed'
    assert [row['path'] for row in retry_sources(saved, scope='script')] == [str(source)]
    assert events[-1]['status'] == 'failed'
    assert source.read_bytes() == before


def test_archive_stop_persists_members_and_retries_each_identity_once(tmp_path):
    source = tmp_path / 'pair.zip'
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('one.txt', 'short unique first')
        archive.writestr('two.txt', 'short unique second')
    before = source.read_bytes()
    store = Store(tmp_path / 'data')
    first = processor(store)

    def stop_after_member(event):
        if event.get('progress', {}).get('stage') == '不通过':
            first.stop.set()

    stopped = first.run([record(source)], stop_after_member)
    snapshot = task_snapshot(store, stopped)
    assert sorted(row['name'] for row in task_rows(snapshot)) == ['one.txt', 'two.txt']
    retries = retry_sources(snapshot, scope='script')
    assert sorted(Path(row['path']).name for row in retries) == ['one.txt', 'two.txt']
    assert all(row.get('input_source_path') == str(source) for row in retries)
    latest = processor(store, stopped['run_id']).run(retries)
    for _ in range(2):
        snapshot = task_snapshot(store, latest)
        assert len(task_rows(snapshot)) == 2
        retries = retry_sources(snapshot, scope='script')
        assert len(retries) == 2
        latest = processor(store, latest['run_id']).run(retries)
    assert source.read_bytes() == before


def test_reextracted_legacy_archive_member_replaces_previous_private_path(tmp_path):
    store = Store(tmp_path / 'data')
    source = str(tmp_path / 'pair.zip')
    old_member = str(tmp_path / 'old' / 'private' / 'archive' / 'source' / 'one.txt')
    new_member = str(tmp_path / 'new' / 'private' / 'archive' / 'source' / 'one.txt')
    old = {'run_id': 'old', 'status': 'completed', 'inputs': [{'path': source}],
           'items': [{'id': 'one', 'source_path': old_member, 'input_source_path': source,
                      'status': 'reject', 'reason_kind': 'technical', 'reason': 'old failure'}]}
    store.save(old)
    current = {'run_id': 'new', 'retry_of': 'old', 'status': 'completed',
               'inputs': [{'path': source}], 'items': [
                   {'id': 'new-one', 'source_path': new_member, 'input_source_path': source,
                    'status': 'reject', 'reason_kind': 'screening', 'reason': 'current screening'}]}
    rows = task_rows(task_snapshot(store, current))
    assert len(rows) == 1
    assert rows[0]['path'] == new_member
    assert 'old failure' in rows[0]['previous_error']


def test_changed_original_archive_does_not_reprocess_its_old_private_members(tmp_path):
    source = tmp_path / 'pair.zip'
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('one.txt', 'short unique original')
    store = Store(tmp_path / 'data')
    result = processor(store).run([record(source)])
    retries = retry_sources(result, scope='script')
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('one.txt', 'changed source document')
    latest = processor(store, result['run_id']).run(retries)
    assert latest['items'][0]['reason_kind'] == 'technical'
    assert '压缩包' in latest['items'][0]['reason']
    assert '修改' in latest['items'][0]['reason']


def test_parallel_archive_child_failure_keeps_all_saved_member_results(tmp_path, monkeypatch):
    source=tmp_path/'pair.zip'
    with zipfile.ZipFile(source,'w') as archive:
        archive.writestr('one.txt','short first member')
        archive.writestr('two.txt','short second member')
    other=tmp_path/'other.txt'
    other.write_text('short nonarchive source',encoding='utf-8')
    before=source.read_bytes()
    refresh=Store.refresh_view

    def fail_worker_final_view(store,run_id,progress=lambda message:None):
        saved=store.load(run_id)
        if 'workers' in store.root.parts and saved.get('discovered_inputs'):
            raise OSError('synthetic final view failure')
        return refresh(store,run_id,progress)

    monkeypatch.setattr(Store,'refresh_view',fail_worker_final_view)
    store=Store(tmp_path/'data')
    result=processor(store,parallel=True).run([record(source),record(other)])
    members=[item for item in result['items'] if item.get('input_source_path')==str(source)]
    assert sorted(Path(item['source_path']).name for item in members)==['one.txt','two.txt']
    assert all(Path(item['path']).is_file() for item in members)
    failed_child=next(child for child in result['child_runs'] if child['input_source']==str(source))
    assert failed_child['status']=='failed' and failed_child['run_id']
    assert store.load(result['run_id'])['status']=='failed'
    assert len(task_rows(task_snapshot(store,result)))==3
    assert source.read_bytes()==before


def test_parallel_archive_partial_failure_keeps_unstarted_member_retryable(tmp_path,monkeypatch):
    source=tmp_path/'bundle.zip'
    with zipfile.ZipFile(source,'w') as archive:
        for name in ('one.txt','two.txt','three.txt'):
            archive.writestr(name,'short unique '+name)
    other=tmp_path/'other.txt'
    other.write_text('short unrelated source',encoding='utf-8')
    save=Store.save
    tripped=False

    def fail_after_second_saved_member(store,manifest):
        nonlocal tripped
        save(store,manifest)
        if (not tripped and 'workers' in store.root.parts and manifest['status']=='running'
                and len(manifest.get('discovered_inputs',[]))==3 and len(manifest['items'])==2):
            tripped=True
            raise OSError('synthetic post-checkpoint failure')

    monkeypatch.setattr(Store,'save',fail_after_second_saved_member)
    store=Store(tmp_path/'data')
    result=processor(store,parallel=True).run([record(source),record(other)])
    assert tripped and result['status']=='failed'
    snapshot=task_snapshot(store,result)
    assert sorted(row['name'] for row in task_rows(snapshot))==['one.txt','other.txt','three.txt','two.txt']
    retries=retry_sources(snapshot,scope='script')
    assert sorted(Path(row['path']).name for row in retries)==['one.txt','other.txt','three.txt','two.txt']
    assert all(row.get('archive_source_hash') for row in retries if Path(row['path']).name!='other.txt')


def test_archive_ai_retry_keeps_member_identity_and_original_archive_guard(tmp_path):
    from ai_resume import retry_records
    from test_stage_resume import CONFIG,KEY,Transport,response,document
    member=tmp_path/'one.docx'
    document(member)
    source=tmp_path/'source.zip'
    with zipfile.ZipFile(source,'w') as archive:
        archive.writestr('one.docx',member.read_bytes())
    before=source.read_bytes()
    store=Store(tmp_path/'data')
    cfg=copy.deepcopy(CONFIG)
    cfg['concurrency']['enabled']=False
    first=AutoProcessor(store,cfg,KEY)
    first.agent.opener=Transport(lambda message: {'decision':'reject','reason':'synthetic privacy retry'}
                                if message['task']=='privacy_text' else response(message['task']))
    original=first.run([record(source)])
    retries=retry_records(original['items'])
    assert len(retries)==1
    assert retries[0]['input_source_path']==str(source)
    assert retries[0]['archive_member_path'] and retries[0]['archive_source_hash']
    cfg['retry_of']=original['run_id']
    resumed=AutoProcessor(store,cfg,KEY)
    resumed.agent.opener=Transport()
    result=resumed.run(retries,operation='retry_ai')
    assert result['items'][0]['status']=='pass'
    assert len(task_rows(task_snapshot(store,result)))==1
    assert source.read_bytes()==before
    with zipfile.ZipFile(source,'w') as archive:
        archive.writestr('changed.txt','changed original archive')
    stale=AutoProcessor(store,cfg,KEY)
    stale.agent.opener=Transport()
    refused=stale.run(retries,operation='retry_ai')
    assert refused['items'][0]['reason_kind']=='technical'
    assert '压缩包已修改' in refused['items'][0]['reason']
    assert not stale.agent.opener.requests


def test_parallel_usage_is_visible_before_first_paper_finishes(tmp_path):
    from test_workbench import paper
    inputs = []
    for name in ('one.txt', 'two.txt'):
        path = tmp_path / name
        path.write_text(paper() + '\n来源 ' + name, encoding='utf-8')
        inputs.append(record(path))
    proc = processor(Store(tmp_path / 'data'), parallel=True)
    proc.config.update(base_url='https://synthetic.test/v1', text_model='synthetic')
    proc.agent.config = dict(proc.config)
    proc.agent.key = 'synthetic-key'

    class Transport:
        def open(self, request, timeout):
            result = {'decision': 'pass', 'reason': 'synthetic',
                      'sensitive_strings': [], 'major': '会计学', 'title': '企业财务管理研究'}
            return io.BytesIO(json.dumps({'choices': [{'finish_reason': 'stop',
                'message': {'content': json.dumps(result)}}],
                'usage': {'prompt_tokens': 100, 'completion_tokens': 20, 'total_tokens': 120}}).encode())

    proc.agent.opener = Transport()
    events = []
    proc.run(inputs, events.append)
    live_usage = [event for event in events if event.get('processed_sources') == 0
                  and event.get('billing', {}).get('total_tokens', 0) > 0]
    assert live_usage, 'Actual in-flight child receipts must be displayed before a whole paper finishes'


def test_parent_stop_cancels_waiting_child_before_inflight_request_returns(tmp_path, monkeypatch):
    from test_workbench import paper
    inputs = []
    for name in ('one.txt', 'two.txt'):
        path = tmp_path / name
        path.write_text(paper() + '\n独立来源 ' + name, encoding='utf-8')
        inputs.append(record(path))
    proc = processor(Store(tmp_path / 'data'), parallel=True)
    proc.config.update(base_url='https://synthetic.test/v1', text_model='synthetic')
    proc.agent.config = dict(proc.config)
    proc.agent.key = 'synthetic-key'
    inflight, waiting, release, cancelled_child = [threading.Event() for _ in range(4)]
    acquire = _ObservedGate.acquire

    def observe_acquire(gate, *args, **kwargs):
        if gate.current:
            waiting.set()
        return acquire(gate, *args, **kwargs)

    monkeypatch.setattr(_ObservedGate, 'acquire', observe_acquire)

    class Transport:
        calls = 0

        def open(self, request, timeout):
            self.calls += 1
            inflight.set()
            assert release.wait(5)
            result = {'decision': 'pass', 'reason': 'synthetic',
                      'sensitive_strings': [], 'major': '会计学', 'title': '企业财务管理研究'}
            return io.BytesIO(json.dumps({'choices': [{'finish_reason': 'stop',
                'message': {'content': json.dumps(result)}}]}).encode())

    transport = Transport()
    proc.agent.opener = transport

    def progress(event):
        if event.get('progress', {}).get('stage') == '单篇处理结束' and proc.stop.is_set():
            cancelled_child.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(proc.run, inputs, progress)
        try:
            assert inflight.wait(4)
            assert waiting.wait(4)
            proc.stop.set()
            assert cancelled_child.wait(2), 'Waiting child must finish cancellation before the other response returns'
        finally:
            release.set()
        result = future.result(timeout=5)
    assert result['status'] == 'stopped'
    assert transport.calls == 1
