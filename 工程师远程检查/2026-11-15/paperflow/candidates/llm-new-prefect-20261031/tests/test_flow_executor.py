"""Orchestrate a real folder grain without executing postprocessing."""
from pathlib import Path
import sys
import threading

import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from flow_executor import execute_folder


class Ledger:
    def __init__(self): self.events = []
    def register(self, identity, verify):
        self.events.append('register')
        verify(identity)
        return {'state':'waiting_llm'}
    def begin(self, identity, generation, verify):
        self.events.append(('begin', generation))
        verify(identity)
    def finish(self, identity, verify, guard):
        self.events.append('finish')
        verify(identity)
        return {'state': 'waiting_postprocessing'}


class Operations:
    def __init__(self, attachments, failure=None):
        self.attachments = attachments
        self.failure = failure
        self.events = []
        self.lock = threading.Lock()
    def body(self):
        with self.lock: self.events.append('body')
        if self.failure == 'body': raise ValueError('synthetic body failure')
    def attachment(self, row):
        with self.lock: self.events.append(row['file_id'])
        if self.failure == row['file_id']: raise ValueError('synthetic attachment failure')


def test_all_components_finish_before_one_folder_enters_waiting_postprocessing():
    identity = {'folder': 'one'}
    manifest = {'attachments': [{'file_id':'a'}, {'file_id':'b'}]}
    ops, ledger, stop = Operations(manifest['attachments']), Ledger(), threading.Event()
    result = execute_folder(identity, 4, ledger, lambda _: manifest, ops, object(), stop)
    assert result['state'] == 'waiting_postprocessing'
    assert set(ops.events) == {'body', 'a', 'b'}
    assert ledger.events == ['register', ('begin', 4), 'finish']


@pytest.mark.parametrize('failure', ['body', 'a'])
def test_any_failed_component_keeps_folder_out_of_waiting_postprocessing(failure):
    manifest = {'attachments': [{'file_id':'a'}, {'file_id':'b'}]}
    ops, ledger, stop = Operations(manifest['attachments'], failure), Ledger(), threading.Event()
    with pytest.raises(ValueError, match='synthetic'):
        execute_folder({'folder':'one'}, 1, ledger, lambda _: manifest, ops, object(), stop)
    assert 'finish' not in ledger.events


def test_pause_before_admission_prevents_body_and_attachment_work():
    manifest = {'attachments': [{'file_id':'a'}]}
    ops, ledger, stop = Operations(manifest['attachments']), Ledger(), threading.Event()
    stop.set()
    with pytest.raises(InterruptedError):
        execute_folder({'folder':'one'}, 1, ledger, lambda _: manifest, ops, object(), stop)
    assert ops.events == [] and ledger.events == []


def test_already_waiting_postprocessing_does_not_replay_any_work():
    manifest = {'attachments': [{'file_id':'a'}]}
    ops, ledger, stop = Operations(manifest['attachments']), Ledger(), threading.Event()
    def registered(identity, verify):
        ledger.events.append('register')
        verify(identity)
        return {'state':'waiting_postprocessing'}
    ledger.register = registered
    result = execute_folder({'folder':'one'}, 1, ledger, lambda _: manifest, ops, object(), stop)
    assert result['state'] == 'waiting_postprocessing'
    assert ops.events == [] and ledger.events == ['register']
