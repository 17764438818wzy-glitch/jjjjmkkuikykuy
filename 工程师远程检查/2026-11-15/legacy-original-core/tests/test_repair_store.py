from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

import pytest

from paperflow.config import Config
from paperflow.handoff import PackageHandoff
from paperflow.package import load,save,sha
from paperflow.store import Store


@pytest.fixture
def repair_case(tmp_path,package_factory):
    source=tmp_path/'original.txt';source.write_text('original retained')
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),catalog_path=str(tmp_path/'catalog.json'),
             source_roots=[],rag_url='http://127.0.0.1:1')
    c.prepare();s=Store(c.root/'state.sqlite3');task=s.register_source(source,sha(source))
    material,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
        value=load(material/name);value['source_id']=task['source_id'];save(material/name,value)
    (material/'manifest.json').unlink();(material/'READY.json').unlink()
    profile=load(material/'profile.json');profile['routing_candidates']=[dict(code=code) for code in ('120201','120202','120203')]
    save(material/'profile.json',profile)
    for code in ('120202','120203'):catalog['majors'].append(dict(code=code,paper_dataset_id='body-'+code,image_dataset_id='image-'+code))
    save(c.catalog_path,catalog);h=PackageHandoff(s,c.handoff_root)
    result=h.handoff(task['task_id'],task,material,catalog,mapping_version=sha(c.catalog_path))
    s.update(task['task_id'],stage='handed_off',package_dir=result['package_dir'],manifest_sha256=result['manifest_sha256'])
    from paperflow.repair_store import RepairLedger
    ledger=RepairLedger(s)
    identity={k:task[k] for k in ('task_id','source_id','source_sha256')};identity['manifest_sha256']=result['manifest_sha256']
    return c,s,task,ledger,identity


def report(case,code='120202',problem='missing_chunks',stage='round3_chunk',legacy=0):
    _,_,_,ledger,identity=case
    return ledger._write(lambda db:ledger.request_sql(db,identity,stage,problem,code,dict(detail='specific missing business record'),legacy_count=legacy))


def test_concurrent_reports_merge_and_charge_once(repair_case):
    with ThreadPoolExecutor(6) as pool:
        rows=list(pool.map(lambda i:report(repair_case,'120202' if i%2 else '120203',problem='CHUNK-MISSING' if i%2 else 'missing_chunks'),range(12)))
    assert len({r['repair_request_id'] for r in rows})==1
    ledger=repair_case[3]
    assert ledger.used(repair_case[4])==1
    assert len(ledger.rows('SELECT * FROM repair_reports'))==2
    assert len(ledger.rows('SELECT * FROM correction_attempts'))==1


def test_shared_budget_does_not_reset_for_problem_name_thread_or_revision(repair_case):
    first=report(repair_case)
    second=report(repair_case,problem='privacy',stage='round3_redact')
    third=report(repair_case,problem='missing_assets',stage='round3_assets')
    assert first['state']==second['state']=='queued'
    assert third['state']=='held_budget' and repair_case[3].used(repair_case[4])==2
    assert report(repair_case)['repair_request_id']==first['repair_request_id']


def test_legacy_mirrors_use_max_not_sum_and_exhaustion_is_retained(repair_case):
    c,s,t,ledger,identity=repair_case
    s.update(t['task_id'],correction_count=1,repair_count=2)
    result=report(repair_case,legacy=1)
    assert result['state']=='held_budget' and ledger.used(identity)==2
    assert ledger.rows('SELECT * FROM repair_reports') and not ledger.rows('SELECT * FROM correction_attempts')


def test_same_syntax_attempt_replays_without_refund_or_second_charge(repair_case):
    _,_,_,ledger,identity=repair_case
    reserve=lambda name:ledger._write(lambda db:ledger.reserve_sql(db,identity,name,'syntax',dict(stage='round1'),legacy_count=0))
    assert reserve('same')==reserve('same')==1
    assert report(repair_case)['state']=='queued'
    from paperflow.producer import SyntaxRepairExhausted
    with pytest.raises(SyntaxRepairExhausted):reserve('new')
    assert ledger.used(identity)==2


@pytest.mark.parametrize('problem',['timeout','identity','unknown_write','hash_changed','quality_score'])
def test_non_content_failures_cannot_start_repair(repair_case,problem):
    with pytest.raises(ValueError):report(repair_case,problem=problem)
    assert repair_case[3].used(repair_case[4])==0


def test_repair_run_is_stable_and_same_source_repairs_serialize(repair_case):
    _,_,task,ledger,_=repair_case
    a=report(repair_case);b=report(repair_case,stage='round3_redact',problem='privacy')
    start=lambda rid:ledger._write(lambda db:ledger.start_sql(db,rid))
    first=start(a['repair_request_id'])
    assert start(a['repair_request_id'])==first
    assert first['task_id']==task['task_id'] and first['thread_id']!=task['task_id'] and first['stage']=='repair'
    with pytest.raises(ValueError,match='shared repair'):start(b['repair_request_id'])
    assert first['revision']==2 and ledger.used(repair_case[4])==2
