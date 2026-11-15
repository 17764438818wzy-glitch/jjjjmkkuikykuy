from pathlib import Path
import json
import threading

import pytest

from paperflow.config import Config
from paperflow.intake import Intake
from paperflow.service import InstanceLock, Service
from paperflow.store import Store


def test_intake_waits_until_original_is_stable(tmp_path):
    source = tmp_path/'originals'
    source.mkdir()
    original = source/'paper.docx'
    original.write_bytes(b'original')
    config = Config(home=str(tmp_path/'runtime'), source_roots=[str(source)],
                    handoff_root=str(tmp_path/'handoff'), stable_seconds=10)
    store = Store(config.root/'state.sqlite3')
    intake = Intake(config,store)
    intake.scan(now=100)
    assert store.all_tasks() == []
    intake.scan(now=111)
    assert len(store.all_tasks()) == 1


def test_startup_only_shows_status_and_does_not_scan_or_run(tmp_path):
    class Workflow:
        def step(self, task):
            raise AssertionError('constructor must not invoke graph')
        def close(self):
            pass
    config = Config(home=str(tmp_path/'runtime'), handoff_root=str(tmp_path/'handoff'), source_roots=[])
    service = Service(config, workflow_factory=lambda c,s: Workflow())
    assert service.status()['tasks'] == {}
    assert not service.running
    service.close()


def test_two_controllers_cannot_acquire_same_instance_lock(tmp_path):
    path = tmp_path/'worker.lock'
    with InstanceLock(path):
        with pytest.raises(RuntimeError, match='already running'):
            with InstanceLock(path):
                pass


def test_legacy_ready_is_adopted_for_round_three_without_repeating_first_two(tmp_path, package_factory):
    from paperflow.package import sha
    root, ident, catalog = package_factory()
    original=tmp_path/'original.docx'; original.write_bytes(b'original')
    handoff=tmp_path/'handoff'; (handoff/'tasks').mkdir(parents=True)
    task=dict(task_id=ident['task_id'],source_id='source1',source_sha256=sha(original),
              input={'path':str(original)}, output={'result_directory':str(root)})
    (handoff/'tasks'/'task1.json').write_text(json.dumps(task),encoding='utf-8')
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(handoff),source_roots=[])
    store=Store(config.root/'state.sqlite3')
    intake=Intake(config,store)
    intake.import_handoffs()
    from paperflow.graph import Workflow
    flow=Workflow(config,store,catalog=catalog);flow.apply_pending_requests();flow.close()
    adopted=store.all_tasks()[0]
    # 原task和交付原稿哈希不一致，必须拒绝，不能只看 READY 文件名。
    assert adopted['stage']=='parked'


def test_backpressure_still_allows_rag_queue_to_drain(tmp_path):
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),
                  source_roots=[],max_ingest_backlog=1,min_free_gib=0)
    class Workflow:
        def step(self,task):
            return dict(task,stage='verify')
        def close(self): pass
    service=Service(config,workflow_factory=lambda c,s:Workflow())
    a=service.store.register_source(tmp_path/'a.pdf','a'*64)
    b=service.store.register_source(tmp_path/'b.pdf','b'*64)
    service.store.update(a['task_id'],stage='ingest')
    assert service.backpressure() is True
    assert service.allowed_stages('llm') == []
    assert 'ingest' in service.allowed_stages('rag')
    service.close()
