import json
from pathlib import Path
import shutil

import pytest

from paperflow.handoff import PackageHandoff
from paperflow.package import load,save,sha
from paperflow.handoff_store import canonical
from paperflow.phase_cache import CACHE_STAGES
from test_repair_store import repair_case,report


def source_dependencies(case):
    _,_,t,ledger,identity=case
    deps={stage:dict(token=stage+'-source',cache_stage=stage,cache_key=(str(i)*64)) for i,stage in enumerate(CACHE_STAGES)}
    ledger._write(lambda db:[db.execute('INSERT INTO package_stage_dependencies VALUES(?,?,?)',
        (identity['manifest_sha256'],stage,canonical(dep))) for stage,dep in deps.items()])
    return deps


def material(case,tmp_path,name):
    c,_,_,ledger,identity=case;output=tmp_path/name
    shutil.copytree(ledger.registration(identity['manifest_sha256'])['package_dir'],output)
    (output/'manifest.json').unlink();(output/'READY.json').unlink()
    value=load(output/'chunks.json');value['chunks'][-1]['content']+=' repair '+name;save(output/'chunks.json',value)
    return output


@pytest.mark.parametrize('newer',['none','dependent','independent','unknown','frozen'])
def test_shared_handoff_advances_all_or_retains_conflicting_responsibility(repair_case,tmp_path,newer):
    c,s,t,ledger,identity=repair_case;deps=source_dependencies(repair_case)
    rid=report(repair_case)['repair_request_id'];run=ledger._write(lambda db:ledger.start_sql(db,rid))
    h=PackageHandoff(s,c.handoff_root);other=None
    if newer in ('dependent','independent','unknown'):
        changed=material(repair_case,tmp_path,'newer')
        p=load(changed/'profile.json');p['routing_candidates']=[dict(code='120201')];save(changed/'profile.json',p)
        newer_deps=dict(deps)
        if newer=='independent':newer_deps['round3_chunk']=dict(token='<REDACTED_SECRET>',lineage_complete=True,lineage=['new independent decision'])
        if newer=='unknown':newer_deps={}
        other=h.handoff('new-decision',dict(t,revision=3,stage_dependencies=newer_deps),changed,c.catalog,
            mapping_version=sha(c.catalog_path),expected_targets={'120201':run['expected_targets']['120201']})['manifest_sha256']
    if newer=='frozen':
        # Freeze of the old package is not a ban on reading it for a new repair.
        ledger._write(lambda db:db.execute('INSERT INTO package_freezes VALUES(?,?,?)',(identity['manifest_sha256'],'*','old defect')))
    run['stage_dependencies']['round3_chunk']=dict(token='<REDACTED_SECRET>',cache_stage='repair',cache_key='f'*64)
    intent=h.prepare(rid,run,material(repair_case,tmp_path,'repair'),c.catalog,mapping_version=sha(c.catalog_path),
        expected_targets=run['expected_targets'],repair_source=run['repair_source'])
    result=h.resume(rid,intent['input_fingerprint']);assert h.resume(rid,intent['input_fingerprint'])==result
    current=ledger.expectations(t['task_id'],['120201','120202','120203'])
    assert current['120202']['manifest_sha256']==current['120203']['manifest_sha256']==result['manifest_sha256']
    targets={r['specialty_code']:r['outcome'] for r in ledger.rows('SELECT * FROM repair_targets')}
    if other:
        assert current['120201']['manifest_sha256']==other
        assert targets['120201']==('superseded_independent' if newer=='independent' else 'dependency_pending')
    else:assert current['120201']['manifest_sha256']==result['manifest_sha256']
    assert ledger.get(rid)['state']=='complete' and ledger.used(identity)==1
    assert len(ledger.rows('SELECT * FROM repair_targets'))==3


def test_repair_handoff_failure_rolls_back_package_targets_and_result(repair_case,tmp_path):
    c,s,_,ledger,identity=repair_case;rid=report(repair_case)['repair_request_id']
    run=ledger._write(lambda db:ledger.start_sql(db,rid));h=PackageHandoff(s,c.handoff_root)
    intent=h.prepare(rid,dict(run,revision=99),material(repair_case,tmp_path,'bad'),c.catalog,mapping_version=sha(c.catalog_path),
        expected_targets=run['expected_targets'],repair_source=run['repair_source'])
    with pytest.raises(ValueError,match='revision'):h.resume(rid,intent['input_fingerprint'])
    assert len(ledger.rows('SELECT * FROM delivery_packages'))==1
    assert not ledger.rows('SELECT * FROM repair_targets')
    assert ledger.get(rid)['result'] is None and ledger.used(identity)==1
