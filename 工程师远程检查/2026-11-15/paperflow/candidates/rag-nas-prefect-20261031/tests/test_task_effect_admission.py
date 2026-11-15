from contextlib import closing
import sqlite3

import pytest

from effect_journal import EffectJournal
from folder_native_client import FolderNativeClient
from task_effect_admission import task_effect_admission


@pytest.fixture
def control(tmp_path):
    path = tmp_path / 'control.sqlite3'
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('CREATE TABLE tasks(task_id TEXT, desired_state TEXT, start_pending INTEGER, '
                   'start_generation INTEGER, task_status TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT)')
        db.execute("INSERT INTO tasks VALUES('task-1','running',0,5,'complete')")
    return path


class Raw:
    base_url = 'http://synthetic'
    api_key = 'synthetic'
    opener = None

    def __init__(self, journal):
        self.journal = journal
        self.calls = 0

    def upload_bytes(self, *_):
        self.calls += 1
        with closing(self.journal._connect()) as db:
            assert db.execute('SELECT state FROM remote_effects').fetchone()['state'] == 'intent'
        return 'doc-1'


def test_real_task_admission_records_intent_before_sending(tmp_path, control):
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw(journal)
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
        lambda *_: task_effect_admission(control, 'task-1', 5), lambda: True)
    assert client.upload_bytes('test-dataset', 'body', b'data') == 'doc-1'
    with closing(journal._connect()) as db:
        assert db.execute('SELECT state FROM remote_effects').fetchone()['state'] == 'returned'
    assert raw.calls == 1


@pytest.mark.parametrize('update', ["UPDATE tasks SET desired_state='paused'",
                                    'UPDATE tasks SET start_generation=6',
                                    "INSERT INTO task_deletions VALUES('task-1')"])
def test_paused_new_generation_or_deleted_task_does_not_send(tmp_path, control, update):
    with closing(sqlite3.connect(control)) as db, db:
        db.execute(update)
    journal = EffectJournal(tmp_path / 'effects.sqlite3')
    journal.initialize()
    raw = Raw(journal)
    client = FolderNativeClient(raw, journal, {'test-dataset'}, 'pipeline',
        lambda *_: task_effect_admission(control, 'task-1', 5), lambda: True)
    with pytest.raises(InterruptedError):
        client.upload_bytes('test-dataset', 'body', b'data')
    assert raw.calls == 0
    with closing(journal._connect()) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_effects').fetchone()[0] == 0
