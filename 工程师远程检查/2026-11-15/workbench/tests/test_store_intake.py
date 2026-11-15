import hashlib
import time
from pathlib import Path

from paperflow.store import Store


def test_same_content_reuses_task_and_changed_original_creates_version(tmp_path):
    store = Store(tmp_path / 'state.sqlite3')
    original = tmp_path / '论文.docx'
    original.write_bytes(b'first')
    first = store.register_source(original, hashlib.sha256(b'first').hexdigest())
    same = store.register_source(original, first['source_sha256'])
    assert same['task_id'] == first['task_id']
    store.mark_active(first['task_id'])
    original.write_bytes(b'second')
    second = store.register_source(original, hashlib.sha256(b'second').hexdigest())
    assert second['task_id'] != first['task_id']
    assert store.active_for(original) == first['task_id']
    store.mark_active(second['task_id'])
    assert store.active_for(original) == second['task_id']


def test_expired_lease_is_reclaimed_without_repeating_completed_stage(tmp_path):
    store = Store(tmp_path / 'state.sqlite3')
    task = store.register_source(tmp_path / 'a.pdf', 'a' * 64)
    claimed = store.claim(['round1'], 'dead-process', now=10, lease_seconds=5)
    assert claimed['task_id'] == task['task_id']
    assert store.claim(['round1'], 'other', now=12) is None
    store.update(task['task_id'], stage='round2', owner='dead-process', lease_until=15)
    resumed = store.claim(['round2'], 'new-process', now=16)
    assert resumed['stage'] == 'round2'
    assert resumed['owner'] == 'new-process'


def test_repair_cannot_starve_among_new_tasks(tmp_path):
    store = Store(tmp_path / 'state.sqlite3')
    for i in range(7):
        store.register_source(tmp_path / f'{i}.pdf', f'{i:064x}')
    repair = store.all_tasks()[-1]
    store.update(repair['task_id'], stage='repair', kind='repair')
    got = []
    for i in range(3):
        task = store.claim(['round1', 'repair'], f'worker-{i}', now=time.time())
        got.append(task['task_id'])
    assert repair['task_id'] in got


def test_source_alias_deduplication_keeps_both_paths(tmp_path):
    store = Store(tmp_path / 'state.sqlite3')
    a = store.register_source(tmp_path / 'a.pdf', 'b' * 64)
    b = store.register_source(tmp_path / 'copy.pdf', 'b' * 64)
    assert a['task_id'] == b['task_id']
    store.mark_active(a['task_id'])
    assert store.active_for(tmp_path / 'copy.pdf') == a['task_id']
