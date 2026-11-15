from pathlib import Path
import json
import sqlite3
import subprocess
import sys

import pytest

from test_registered_input import registered


def test_only_verified_registered_group_enters_waiting_once(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, receipt, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    first = queue.receive('task-one', item, receipt_root=receipt_root)
    second = queue.receive('task-one', item, receipt_root=receipt_root)

    assert first['state'] == 'waiting'
    assert second['state'] == 'waiting'
    rows = queue.list_waiting()
    assert len(rows) == 1
    assert rows[0]['paper_id'] == receipt['paper_id']
    assert rows[0]['receipt_id'] == receipt['receipt_id']
    assert rows[0]['body']['file_id'] == receipt['inventory']['body']['file_id']
    assert len(rows[0]['attachments']) == 1
    assert rows[0]['attachments'][0]['file_id'] == receipt['inventory']['attachments'][0]['file_id']
    assert Path(rows[0]['body']['path']).is_relative_to(Path(item['delivery_dir']) / 'files')
    assert 'trash' not in rows[0]
    assert 'excluded' not in rows[0]


def test_incomplete_current_observation_is_not_accepted(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, receipt, item = registered
    attachment = next(row for row in receipt['items'] if row['kind'] == 'attachment')
    Path(attachment['path']).write_bytes(b'damaged synthetic delivery copy')
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    with pytest.raises(ValueError, match='核验'):
        queue.receive('task-one', item, receipt_root=receipt_root)
    assert queue.list_waiting() == []
    with sqlite3.connect(queue.path) as db:
        failure = db.execute(
            'SELECT task_id,group_id,paper_id,receipt_id,receipt_path,delivery_dir,'
            'reason_code,reason FROM receive_failures'
        ).fetchone()
    assert failure[:6] == ('task-one', item['group_id'], item['paper_id'], item['receipt_id'],
                          item['receipt_path'], item['delivery_dir'])
    assert failure[6] == 'ValueError'
    assert '核验' in failure[7]


def test_unregistered_or_identity_mismatch_never_enters_waiting(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    for invalid in ({**item, 'registration_status': 'failed'},
                    {**item, 'paper_id': 'PF_wrong'}):
        with pytest.raises(ValueError):
            queue.receive('task-one', invalid, receipt_root=receipt_root)
    assert queue.list_waiting() == []


def test_cross_task_same_paper_is_held_instead_of_double_counted(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'local-queue.sqlite3')
    queue.receive('task-one', item, receipt_root=receipt_root)
    conflict = queue.receive('task-two', item, receipt_root=receipt_root)
    assert conflict['state'] == 'needs_review'
    assert queue.list_waiting() == []
    assert len(queue.list_review()) == 2
    queue.record_scan(complete=True)
    assert queue.snapshot()['waiting_available'] is False
    assert queue.snapshot()['waiting_count'] is None


def test_foreign_sqlite_database_is_never_used_as_local_queue(tmp_path):
    from local_queue import LocalQueue

    foreign = tmp_path / 'other.sqlite3'
    with sqlite3.connect(foreign) as db:
        db.execute('CREATE TABLE original(value TEXT)')
        db.execute("INSERT INTO original VALUES('keep')")
    with pytest.raises(ValueError, match='数据库'):
        LocalQueue(foreign)
    with sqlite3.connect(foreign) as db:
        assert db.execute('SELECT value FROM original').fetchone()[0] == 'keep'
        assert {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")} == {'original'}


def test_old_item_verification_is_not_freshened_by_later_scan_timestamp(registered, tmp_path):
    from local_queue import LocalQueue

    _, receipt_root, _, item = registered
    queue = LocalQueue(tmp_path / 'queue.sqlite3')
    queue.receive('task-one', item, receipt_root=receipt_root)
    with sqlite3.connect(queue.path) as db:
        db.execute("UPDATE groups SET observed_at='2000-01-01T00:00:00+00:00'")
    queue.record_scan(complete=True)
    assert queue.snapshot(max_age_seconds=60)['waiting_available'] is False
    assert queue.snapshot(max_age_seconds=60)['waiting_count'] is None


def test_upstream_trash_is_not_passed_as_a_local_input(tmp_path):
    from local_queue import LocalQueue

    upstream = Path(__file__).resolve().parents[2] / 'first-step-control-20261029'
    script = r'''
import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from paperflow_prefect.group_classification import process_one_group
from paperflow_prefect.first_step_registration import register_delivery
base = Path(sys.argv[2]); library = base / 'library'; group = library / 'person'
group.mkdir(parents=True)
(group / 'paper.txt').write_text('题目：合成论文\n摘要：正文', encoding='utf8')
(group / '任务书.docx').write_text('任务书\n过程材料', encoding='utf8')
class Judge:
    def choose_body(self, rows):
        return {'choice': next(row['file_id'] for row in rows if row['relative'] == 'paper.txt')}
def evidence(path):
    return {'format': 'docx' if path.suffix == '.docx' else 'text',
            'text': path.read_text(encoding='utf8'), 'truncated': False}
delivery = process_one_group(group, library, base / 'output', Judge, evidence, verified_new=True)
receipt = register_delivery(delivery['delivery_dir'], receipt_root=base / 'receipts',
                            history_roots=(), lock_path=base / 'locks' / 'receipt.lock')
item = dict(status='passed', registration_status='registered', delivery_complete=True,
            group_id=receipt['group_id'], paper_id=receipt['paper_id'],
            receipt_id=receipt['receipt_id'], receipt_path=receipt['record_path'],
            delivery_dir=delivery['delivery_dir'])
print(json.dumps({'item': item, 'root': str(base / 'receipts')}, ensure_ascii=False))
'''
    result = subprocess.run(
        [sys.executable, '-B', '-X', 'utf8', '-c', script, str(upstream), str(tmp_path)],
        capture_output=True, text=True, encoding='utf8', timeout=30,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    handoff = json.loads((Path(data['item']['delivery_dir']) / 'handoff.json').read_text(encoding='utf8'))
    assert len(handoff['trash']) == 1
    original_trash_copy = Path(data['item']['delivery_dir']) / 'files' / handoff['trash'][0]['relative']
    assert original_trash_copy.is_file()
    queue = LocalQueue(tmp_path / 'queue.sqlite3')
    accepted = queue.receive('task-one', data['item'], receipt_root=data['root'])
    assert accepted['body']['relative'] == 'paper.txt'
    assert accepted['attachments'] == []
    assert 'trash' not in accepted and 'excluded' not in accepted
    assert original_trash_copy.is_file()
