import json
from pathlib import Path

import pytest

from paperflow.handoff import PackageHandoff
from paperflow.package import load,save,sha
from test_repair_store import repair_case,report
from test_repair_handoff import source_dependencies,material


def publish_repair(case,tmp_path,run,name):
    c,s,_,_,_=case;h=PackageHandoff(s,c.handoff_root)
    return h.handoff(run['thread_id'],run,material(case,tmp_path,name),c.catalog,mapping_version=sha(c.catalog_path),
        expected_targets=run['expected_targets'],repair_source=run['repair_source'])


def test_other_defect_survives_partial_change_to_same_stage(repair_case,tmp_path):
    c,s,t,ledger,identity=repair_case;deps=source_dependencies(repair_case)
    rid=report(repair_case,problem='body_loss')['repair_request_id'];run=ledger._write(lambda db:ledger.start_sql(db,rid))
    # Fix X changed the chunks bytes, but its input still contains unfixed Y.
    deps['round3_chunk']=dict(token='<REDACTED_SECRET>',lineage_complete=True,
        lineage=[deps['round3_chunk']['token'],'different-after-fixing-X'])
    changed=material(repair_case,tmp_path,'partial');p=load(changed/'profile.json');p['routing_candidates']=[dict(code='120201')];save(changed/'profile.json',p)
    h=PackageHandoff(s,c.handoff_root)
    h.handoff('partial-other-fix',dict(t,revision=3,stage_dependencies=deps),changed,c.catalog,mapping_version=sha(c.catalog_path),
        expected_targets={'120201':run['expected_targets']['120201']})
    publish_repair(repair_case,tmp_path,run,'fix-Y')
    target=ledger.rows("SELECT * FROM repair_targets WHERE specialty_code='120201'")[0]
    assert target['outcome']=='dependency_pending'


@pytest.mark.parametrize('late_after_complete',[False,True])
@pytest.mark.parametrize('legacy',[0,1])
def test_new_report_evidence_is_retained_and_forwarded_with_same_budget(repair_case,tmp_path,late_after_complete,legacy):
    c,s,t,ledger,identity=repair_case
    register=lambda code,evidence:ledger._write(lambda db:ledger.request_sql(db,identity,'round3_chunk','missing_chunks',code,evidence,legacy_count=legacy))
    rid=register('120202',dict(missing='X'))['repair_request_id'];run=ledger._write(lambda db:ledger.start_sql(db,rid))
    if late_after_complete:result=publish_repair(repair_case,tmp_path,run,'fixed-X')
    register('120203',dict(missing='Y'));register('120202',dict(missing='Z'))
    assert ledger._write(lambda db:ledger.start_sql(db,rid))==run  # Never mutate an in-use cache key.
    if not late_after_complete:result=publish_repair(repair_case,tmp_path,run,'fixed-X')
    links=ledger.rows('SELECT * FROM repair_report_followups')
    assert len(links)==2 and len({x['followup_request_id'] for x in links})==1
    child_id=links[0]['followup_request_id'];child=ledger.get(child_id)
    assert child['source_manifest']==result['manifest_sha256']
    assert child['state']==('queued' if legacy==0 else 'held_budget') and ledger.used(t)==2
    assert {x['outcome'] for x in ledger.rows('SELECT * FROM repair_targets')}=={'reports_pending'}
    register('120203',dict(missing='Y'));register('120202',dict(missing='Z'))
    assert ledger.used(t)==2 and len(ledger.rows('SELECT * FROM repair_report_followups'))==2
    if legacy==0:
        child_run=ledger._write(lambda db:ledger.start_sql(db,child_id))
        assert child_run['repair_problem']['requirements']==[dict(missing='Y'),dict(missing='Z')]
        publish_repair(repair_case,tmp_path,child_run,'fixed-Y-Z')
        assert all(x['resolved_manifest'] for x in ledger.rows('SELECT * FROM repair_report_followups'))


def test_identical_report_from_late_specialty_needs_no_extra_attempt(repair_case,tmp_path):
    _,_,t,ledger,identity=repair_case;rid=report(repair_case)['repair_request_id']
    run=ledger._write(lambda db:ledger.start_sql(db,rid));report(repair_case,code='120203')
    publish_repair(repair_case,tmp_path,run,'same-defect')
    assert not ledger.rows('SELECT * FROM repair_report_followups') and ledger.used(t)==1
