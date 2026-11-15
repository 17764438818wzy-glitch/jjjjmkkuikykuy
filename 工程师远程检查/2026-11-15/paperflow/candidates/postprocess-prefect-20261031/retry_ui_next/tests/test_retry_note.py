"""Display notes for one verified manual retry; no live Prefect or model calls."""
import copy
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'generation_next' / 'tests'))
sys.path.insert(0, str(ROOT / 'generation_next'))
sys.path.insert(0, str(ROOT / 'retry_ui_next'))
from test_retry_source import install_retry_records
from stage_runtime import retry_source_snapshot


def settled_view(observer):
    observer.llm_results({}, 'passed')
    with observer._lock:
        pending = list(observer._pending.values())
    for future in pending:
        future.result(timeout=3)
    return observer.llm_results({}, 'passed')


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / 'control.sqlite3'
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper-1', receipt_id='b' * 64,
                    attempt_id='attempt-1', parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    with sqlite3.connect(path) as db:
        db.executescript('''CREATE TABLE tasks(task_id TEXT PRIMARY KEY,prefect_order_model TEXT,prefect_run_id TEXT,
            desired_state TEXT,task_status TEXT,start_generation INTEGER,start_pending INTEGER);
            CREATE TABLE task_deletions(task_id TEXT);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,attempt_id TEXT,
            parent_run_id TEXT,task_run_id TEXT,state TEXT,generation INTEGER,manifest_sha256 TEXT);
            CREATE TABLE paperflow_postprocess_stage_jobs(task_id TEXT,group_id TEXT);
            CREATE TABLE postprocessing_claims(task_id TEXT,group_id TEXT);
            CREATE TABLE postprocessing_results(task_id TEXT,group_id TEXT);''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (identity['task_id'], 'v2', identity['parent_run_id'], 'running', 'running', 1, 0))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (*identity.values(), 'waiting_postprocessing', 1, 'c' * 64))
    install_retry_records(path, identity)
    item = {key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')}
    item['order_note'] = '前处理已交接；文件夹工单后续状态需核对'
    value = {'available': True, 'counts': {'passed': 1},
             'list': {'filter': 'passed', 'items': [item]}}
    class Observer:
        def llm_results(self, *_args, **_kwargs): return copy.deepcopy(value)
    def verify(actual, generation):
        assert actual == identity and generation == 1
        selected = retry_source_snapshot(path, actual, 1)
        return dict(identity=actual, generation=1, source_llm_generation=1, manifest_sha256='c' * 64,
                    source_retry_sha256=selected['sha256'], source_retry_prefect_sha256='d' * 64,
                    raw_complete=True, session_current=True, not_deleted=True)
    return path, identity, Observer(), verify


def test_verified_retry_adds_same_folder_waiting_note(sample):
    from retry_note import RetryNoteObserver
    path, identity, original, verify = sample
    observer = RetryNoteObserver(original, path, verify_upstream=verify)
    try:
        result = settled_view(observer)
        assert result['list']['items'][0]['order_note'] == '人工重试已完成，核验时等待后处理；原失败记录已保留'
        assert result['list']['items'][0]['group_id'] == identity['group_id']
    finally:
        observer.close()


@pytest.mark.parametrize('change', ['claimed', 'paused', 'pausing', 'changed_during_verification', 'unavailable'])
def test_note_is_never_misleading_when_state_or_evidence_changes(sample, change):
    from retry_note import RetryNoteObserver
    path, identity, original, verify = sample
    if change == 'claimed':
        with sqlite3.connect(path) as db:
            db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?)',
                       (identity['task_id'], identity['group_id']))
    if change == 'paused':
        with sqlite3.connect(path) as db:
            db.execute("UPDATE tasks SET desired_state='paused',task_status='paused'")
    if change == 'pausing':
        with sqlite3.connect(path) as db:
            db.execute("UPDATE tasks SET desired_state='paused',task_status='pausing'")
    def changed(actual, generation):
        if change == 'unavailable': raise ValueError('unavailable')
        result = verify(actual, generation)
        if change == 'changed_during_verification':
            with sqlite3.connect(path) as db:
                db.execute('INSERT INTO postprocessing_claims VALUES(?,?)',
                           (actual['task_id'], actual['group_id']))
        return result
    observer = RetryNoteObserver(original, path, verify_upstream=changed)
    try:
        result = settled_view(observer)
        note = result['list']['items'][0]['order_note']
        if change == 'paused':
            assert note == '人工重试已完成，核验时任务已暂停；原失败记录已保留'
        elif change == 'pausing':
            assert note == '人工重试已完成，核验时任务状态正在切换；原失败记录已保留'
        else:
            assert note == '前处理已交接；文件夹工单后续状态需核对'
    finally:
        observer.close()


def test_slow_source_verification_does_not_block_original_list(sample):
    from retry_note import RetryNoteObserver
    path, _, original, verify = sample
    started, release = threading.Event(), threading.Event()
    def slow(actual, generation):
        started.set()
        assert release.wait(3)
        return verify(actual, generation)
    observer = RetryNoteObserver(original, path, verify_upstream=slow)
    try:
        before = time.monotonic()
        result = observer.llm_results({}, 'passed')
        assert time.monotonic() - before < 1
        assert result['list']['items'][0]['order_note'] == '前处理已交接；文件夹工单后续状态需核对'
        assert started.wait(2)
    finally:
        release.set()
        observer.close()


def test_changed_database_discards_previously_verified_waiting_note(sample):
    from retry_note import RetryNoteObserver
    path, identity, original, verify = sample
    observer = RetryNoteObserver(original, path, verify_upstream=verify)
    try:
        assert '等待后处理' in settled_view(observer)['list']['items'][0]['order_note']
        with sqlite3.connect(path) as db:
            db.execute('INSERT INTO postprocessing_claims VALUES(?,?)', (identity['task_id'], identity['group_id']))
        assert observer.llm_results({}, 'passed')['list']['items'][0]['order_note'] == '前处理已交接；文件夹工单后续状态需核对'
    finally:
        observer.close()


def test_database_change_while_constructing_page_discards_all_added_notes(sample, monkeypatch):
    from retry_note import RetryNoteObserver
    path, identity, original, verify = sample
    observer = RetryNoteObserver(original, path, verify_upstream=verify)
    try:
        assert '等待后处理' in settled_view(observer)['list']['items'][0]['order_note']
        cached = observer._cached_note
        def changing(item, revision):
            note = cached(item, revision)
            with sqlite3.connect(path) as db:
                db.execute('INSERT INTO postprocessing_claims VALUES(?,?)', (identity['task_id'], identity['group_id']))
            return note
        monkeypatch.setattr(observer, '_cached_note', changing)
        assert observer.llm_results({}, 'passed')['list']['items'][0]['order_note'] == '前处理已交接；文件夹工单后续状态需核对'
    finally:
        observer.close()


def test_old_completed_notes_do_not_block_observations_on_a_new_page(sample, monkeypatch):
    from retry_note import RetryNoteObserver
    path, identity, _, verify = sample
    items = [{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')}]
    items *= 20
    items = [dict(item, group_id=str(i).zfill(32)) for i, item in enumerate(items)]
    current = [items]
    class Observer:
        def llm_results(self, *_args):
            return {'available': True, 'list': {'filter': 'passed', 'items': copy.deepcopy(current[0])}}
    observer = RetryNoteObserver(Observer(), path, verify_upstream=verify)
    monkeypatch.setattr(observer, '_note', lambda _: ('核验提示', observer._revision(), time.monotonic()))
    try:
        settled_view(observer)
        current[0] = [dict(items[0], group_id='f' * 32)]
        assert settled_view(observer)['list']['items'][0]['order_note'] == '核验提示'
    finally:
        observer.close()


def test_note_survives_the_normal_five_second_page_refresh(sample, monkeypatch):
    import retry_note
    path, _, original, verify = sample
    clock = [100.0]
    monkeypatch.setattr(retry_note, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    observer = retry_note.RetryNoteObserver(original, path, verify_upstream=verify)
    try:
        assert '等待后处理' in settled_view(observer)['list']['items'][0]['order_note']
        clock[0] = 105.2
        assert '等待后处理' in observer.llm_results({}, 'passed')['list']['items'][0]['order_note']
        clock[0] = 116.0
        assert observer.llm_results({}, 'passed')['list']['items'][0]['order_note'] == '前处理已交接；文件夹工单后续状态需核对'
    finally:
        observer.close()


def test_large_page_eventually_observes_items_after_the_first_twenty(sample, monkeypatch):
    from retry_note import RetryNoteObserver
    path, identity, _, verify = sample
    items = [{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')}]
    items = [dict(items[0], group_id=str(i).zfill(32)) for i in range(21)]
    class Observer:
        def llm_results(self, *_args):
            return {'available': True, 'list': {'filter': 'passed', 'items': copy.deepcopy(items)}}
    observer = RetryNoteObserver(Observer(), path, verify_upstream=verify)
    monkeypatch.setattr(observer, '_note', lambda _: ('核验提示', observer._revision(), time.monotonic()))
    try:
        for _ in range(4):
            result = settled_view(observer)
            if result['list']['items'][-1].get('order_note') == '核验提示':
                break
        assert result['list']['items'][-1]['order_note'] == '核验提示'
    finally:
        observer.close()


def test_another_page_does_not_cancel_inflight_or_queued_observations(sample, monkeypatch):
    from retry_note import RetryNoteObserver
    path, identity, _, verify = sample
    seed = {key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')}
    first = [dict(seed, group_id=str(i).zfill(32)) for i in range(20)]
    current = [first]
    class Observer:
        def llm_results(self, *_args):
            return {'available': True, 'list': {'filter': 'passed', 'items': copy.deepcopy(current[0])}}
    observer = RetryNoteObserver(Observer(), path, verify_upstream=verify)
    release, started, count_lock = threading.Event(), threading.Event(), threading.Lock()
    count = [0]
    def slow(_):
        with count_lock:
            count[0] += 1
            if count[0] == 2:
                started.set()
        assert release.wait(3)
        return '核验提示', observer._revision(), time.monotonic()
    monkeypatch.setattr(observer, '_note', slow)
    try:
        observer.llm_results({}, 'passed')
        assert started.wait(2)
        workers = [thread for thread in threading.enumerate() if thread.name.startswith('retry-note-observer-')]
        assert workers and all(thread.daemon for thread in workers)
        current[0] = [dict(seed, group_id='f' * 32)]
        observer.llm_results({}, 'passed')
        with observer._lock:
            assert len(observer._queue) == 19
            assert sum(entry[1][1] == 'f' * 32 for entry in observer._queue) == 1
        release.set()
        for _ in range(3):
            result = settled_view(observer)
            if result['list']['items'][0].get('order_note') == '核验提示':
                break
        assert result['list']['items'][0]['order_note'] == '核验提示'
    finally:
        release.set()
        observer.close()
