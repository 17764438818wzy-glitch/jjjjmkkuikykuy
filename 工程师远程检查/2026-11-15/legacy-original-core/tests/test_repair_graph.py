import json
import os
from pathlib import Path
import sqlite3
import sys

import pytest

from paperflow.package import load,save,sha
from paperflow.repair_store import RepairLedger
from test_processing_graph import graph_case,run_worker,model_calls
from test_winjob import wait_until


def request(case,stage='round3_chunk',problem='missing_chunks'):
    c,s,t,m,_=case;ledger=m.processing.repairs.ledger
    # Repair runs leave the original task projection unchanged. A SELECT without
    # ORDER BY can pick a newer hash from the covering index after the first fix.
    identity=dict(t,manifest_sha256=s.get(t['task_id'])['manifest_sha256'])
    return ledger._write(lambda db:ledger.request_sql(db,identity,stage,problem,'120201',dict(detail='specific missing chunk')))


def run_repair(case,rid,mode='normal',wait=True,endpoint=None):
    c,s,t,m,config=case;ledger=m.processing.repairs.ledger
    ledger._write(lambda db:ledger.start_sql(db,rid))
    ex=m.launch('graph',t['task_id'],[sys.executable,'-X','utf8',str(Path('tests/fixtures/processing_worker.py').resolve()),
        str(config),str(s.path),t['task_id'],mode,*([endpoint] if endpoint else [])],cwd=Path.cwd(),
        env={'PF_TEST_REPAIR':rid,'PYTHONPATH':str(Path.cwd())},total_timeout=150,run_id=rid)
    if wait:wait_until(lambda:ex.exit_saved,seconds=90 if endpoint else 30)
    return ex


def checkpoint(c,tid):
    with sqlite3.connect(c.root/'checkpoints/processing.sqlite3') as db:
        return db.execute('SELECT * FROM checkpoints WHERE thread_id=?',(tid,)).fetchall()


@pytest.mark.parametrize('stage,problem,expected',[
    ('round3_chunk','missing_chunks',['repair']),
    ('round3_redact','privacy',['repair','round3_chunk']),
    ('round3_assets','missing_assets',['repair','round3_redact','round3_chunk'])])
def test_new_repair_thread_runs_only_affected_suffix_and_preserves_original(graph_case,stage,problem,expected):
    c,s,t,m,_=graph_case
    run_worker(graph_case);before=checkpoint(c,t['task_id']);original=s.get(t['task_id'])
    old=original['manifest_sha256'];old_bytes=Path(original['package_dir'],'manifest.json').read_bytes()
    req=request(graph_case,stage,problem);rid=req['repair_request_id']
    run_repair(graph_case,rid)
    row=m.processing.repairs.ledger.get(rid)
    assert row['state']=='complete', (c.root/'fixture-events.jsonl').read_text(encoding='utf-8')
    assert model_calls(c)[5:]==expected
    assert checkpoint(c,t['task_id'])==before and s.get(t['task_id'])==original
    assert Path(original['package_dir'],'manifest.json').read_bytes()==old_bytes
    assert checkpoint(c,rid) and json.loads(row['result'])['manifest_sha256']!=old
    assert m.ledger.rows('SELECT generation FROM target_versions')[0]['generation']==2
    assert m.processing.repairs.ledger.used(t)==1
    deps=m.processing.repairs.ledger.rows('SELECT dependency FROM package_stage_dependencies WHERE manifest_sha256=? AND stage=?',
        (json.loads(row['result'])['manifest_sha256'],stage))
    dependency=json.loads(deps[0]['dependency'])
    source_dep=json.loads(m.processing.repairs.ledger.rows('SELECT dependency FROM package_stage_dependencies WHERE manifest_sha256=? AND stage=?',(old,stage))[0]['dependency'])
    assert source_dep['token'] in dependency['lineage'] and rid in dependency['resolved_requests']
    calls=model_calls(c);run_repair(graph_case,rid)
    assert model_calls(c)==calls and len(m.ledger.rows('SELECT * FROM delivery_packages'))==2


@pytest.mark.parametrize('fault',['after-cache-repair','after-seal','after-handoff'])
def test_repair_crash_replay_has_no_extra_model_or_budget(graph_case,fault):
    c,_,t,m,_=graph_case;run_worker(graph_case);rid=request(graph_case)['repair_request_id']
    run_repair(graph_case,rid,fault);run_repair(graph_case,rid,fault)
    assert m.processing.repairs.ledger.get(rid)['state']=='complete', (c.root/'fixture-events.jsonl').read_text(encoding='utf-8')
    assert model_calls(c)[5:]==['repair'] and m.processing.repairs.ledger.used(t)==1
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==2


def test_ordinary_repair_cannot_change_specialty_set(graph_case):
    c,_,_,m,_=graph_case;run_worker(graph_case);rid=request(graph_case)['repair_request_id']
    run_repair(graph_case,rid,'change-specialties')
    assert m.processing.repairs.ledger.get(rid)['state']!='complete'
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1
    assert 'specialty' in (c.root/'fixture-events.jsonl').read_text(encoding='utf-8')


@pytest.mark.parametrize('mode',['syntax-once','after-correction'])
def test_material_and_json_corrections_share_two_slots_even_after_crash(graph_case,mode):
    c,_,t,m,_=graph_case;run_worker(graph_case);rid=request(graph_case)['repair_request_id']
    run_repair(graph_case,rid,mode)
    if mode=='after-correction':run_repair(graph_case,rid,mode)
    ledger=m.processing.repairs.ledger
    assert ledger.get(rid)['state']=='complete',(c.root/'fixture-events.jsonl').read_text(encoding='utf-8')
    assert ledger.used(t)==2 and len(ledger.rows('SELECT * FROM correction_attempts'))==2


def test_exhausted_repair_is_held_and_cannot_be_launched_again(graph_case):
    c,_,t,m,_=graph_case;run_worker(graph_case);rid=request(graph_case)['repair_request_id']
    run_repair(graph_case,rid,'syntax-always')
    assert m.processing.repairs.ledger.get(rid)['state']=='held_budget'
    assert m.processing.repairs.ledger.used(t)==2
    with pytest.raises(ValueError,match='held'):run_repair(graph_case,rid)


def test_content_lineage_does_not_change_with_timing_or_diagnostics(tmp_path):
    from paperflow.phase_cache import content_token
    root=tmp_path/'draft';root.mkdir();save(root/'chunks.json',dict(chunks=[dict(content='bad learning')]))
    a=content_token(root,'round3_chunk');save(root/'processing.json',dict(duration=55));save(root/'review.json',dict(note='unrelated'))
    assert content_token(root,'round3_chunk')==a
    save(root/'chunks.json',dict(chunks=[dict(content='corrected learning')]))
    assert content_token(root,'round3_chunk')!=a


def test_known_bad_cache_is_not_reused_but_earlier_phase_is(graph_case):
    from paperflow.phase_cache import read_cache
    c,_,t,m,_=graph_case;run_worker(graph_case)
    entries={stage:load(next((c.root/'work'/t['task_id']/'phase-cache'/stage).glob('*.json')))
        for stage in ('round3_assets','round3_chunk')}
    request(graph_case)
    for stage,entry in entries.items():
        hit=read_cache(c,entry['input_task'],stage,entry['fingerprint'])
        assert bool(hit)==(stage=='round3_assets')


def test_changed_sealed_source_cannot_be_laundered_into_repair(graph_case):
    c,s,t,m,_=graph_case;run_worker(graph_case);rid=request(graph_case)['repair_request_id']
    package=Path(s.get(t['task_id'])['package_dir']);(package/'article.redacted.md').write_text('tampered',encoding='utf-8')
    run_repair(graph_case,rid)
    assert model_calls(c)[5:]==[]
    assert m.processing.repairs.ledger.get(rid)['state']!='complete'


def test_no_content_change_does_not_clear_defect_with_new_revision(graph_case):
    c,_,_,m,_=graph_case;run_worker(graph_case);rid=request(graph_case)['repair_request_id']
    run_repair(graph_case,rid,'no-content-change')
    assert m.processing.repairs.ledger.get(rid)['state']!='complete'
    assert len(m.ledger.rows('SELECT * FROM delivery_packages'))==1


def test_partial_real_repair_cannot_discard_another_defect_responsibility(graph_case):
    c,_,t,m,_=graph_case;run_worker(graph_case)
    first=request(graph_case)['repair_request_id'];run_repair(graph_case,first)
    current=m.ledger.rows('SELECT manifest_sha256 FROM target_versions')[0]['manifest_sha256']
    second=request(graph_case,problem='body_loss')['repair_request_id']
    origin=m.processing.repairs.ledger.get(first)['source_manifest']
    assert origin!=current and m.processing.repairs.ledger.get(second)['source_manifest']==origin
    run_repair(graph_case,second)
    assert m.processing.repairs.ledger.get(second)['state']=='complete'
    assert m.ledger.rows('SELECT manifest_sha256 FROM target_versions')[0]['manifest_sha256']==current
    assert m.ledger.rows('SELECT outcome FROM repair_targets WHERE repair_request_id=?',(second,))[0]['outcome']=='dependency_pending'
    assert m.processing.repairs.ledger.used(t)==2 and model_calls(c)[5:]==['repair','repair']
