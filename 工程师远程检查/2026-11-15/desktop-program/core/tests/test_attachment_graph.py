import os
from pathlib import Path
import sys
import zipfile

import pytest

from paperflow.config import Config
from paperflow.control import Manager
from paperflow.package import load,save,sha
from paperflow.processing_control import ProcessingCommits
from paperflow.store import Store
from test_winjob import wait_until


@pytest.mark.parametrize('kind',['drawing','software','exclude'])
def test_attachment_finishes_after_round1_with_immutable_original_and_no_rag_action(tmp_path,kind):
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],
             catalog_path=str(tmp_path/'catalog.json'),rag_url='http://127.0.0.1:1')
    c.prepare();save(c.catalog_path,{'majors':[]});c.save(tmp_path/'config.json')
    source=tmp_path/('project.zip' if kind=='software' else 'drawing.dwg')
    if kind=='software':
        with zipfile.ZipFile(source,'w') as z:z.writestr('program/main.py','print("never executed")')
    else:source.write_bytes(b'AC1032\x00\xfforiginal drawing')
    if kind=='exclude':(c.root/'exclude-fixture').touch()
    store=Store(c.root/'state.sqlite3');task=store.register_source(source,sha(source))
    manager=Manager(store,tmp_path/'manager',heartbeat_interval=.2,lease_seconds=10).start()
    ProcessingCommits(manager,c);manager.resume()
    def run():
        ex=manager.launch('graph',task['task_id'],[sys.executable,'-X','utf8','-B',
            str(Path('tests/fixtures/attachment_processing_worker.py').resolve()),str(tmp_path/'config.json'),str(store.path),task['task_id']],
            cwd=Path.cwd(),env={'PYTHONPATH':str(Path.cwd())},total_timeout=60)
        wait_until(lambda:ex.exit_saved,seconds=20)
        return ex
    try:
        run()
        result=load(c.root/'attachment-graph-result.json')
        assert result['stage']==('held' if kind=='exclude' else 'attachment_ready')
        rows=manager.ledger.rows('SELECT * FROM attachment_tasks')
        if kind=='exclude':
            assert not rows and not manager.ledger.rows('SELECT * FROM delivery_packages')
            return
        assert len(rows)==1
        original=Path(rows[0]['package_dir'])/('original'+source.suffix)
        assert original.read_bytes()==source.read_bytes()
        assert not manager.ledger.rows('SELECT * FROM delivery_packages')
        assert not manager.ledger.rows("SELECT * FROM control_actions WHERE kind LIKE 'rag_%'")
        assert (c.root/'model-calls.txt').read_text(encoding='utf-8').splitlines()==['round1']
        run()
        assert len(manager.ledger.rows('SELECT * FROM attachment_tasks'))==1
        assert (c.root/'model-calls.txt').read_text(encoding='utf-8').splitlines()==['round1']
    finally:manager.shutdown()
