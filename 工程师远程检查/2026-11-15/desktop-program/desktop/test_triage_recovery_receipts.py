import importlib.util,json
from pathlib import Path
import pytest


@pytest.fixture
def runner():
    path=Path(__file__).parent/'data/diagnostics/triage-repair-20260927/recover.py'
    spec=importlib.util.spec_from_file_location('repair_recovery_runner',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_archive_receipt_preserves_all_member_results(runner):
    m={'run_id':'new','items':[{'id':'one','status':'pass','paper_id':'p1'},
                              {'id':'two','status':'reject','reason':'bad','source_path':'inner.docx'}]}
    row=runner.result_row('archive','process',m)
    assert len(row['results'])==2 and row['results'][1]['source_path']=='inner.docx'
    assert row['status']=='mixed' and row['new_id'] is None


def test_crash_receipt_remembers_created_run_and_pending_source(runner,tmp_path):
    receipt={'last_run':'previous','items':[],'status':'running'};path=tmp_path/'receipt.json'
    class Processor:
        def run(self,selected,callback,operation):
            callback({'run_id':'created-before-crash'})
            raise RuntimeError('fixture interruption')
    with pytest.raises(RuntimeError):
        runner.execute_one(Processor(),{'path':'source'},'process','old',receipt,path,lambda e:None)
    saved=json.loads(path.read_text(encoding='utf8'))
    assert saved['status']=='failed' and saved['last_run']=='created-before-crash'
    assert saved['active']=={'old_id':'old','run_id':'created-before-crash'}
