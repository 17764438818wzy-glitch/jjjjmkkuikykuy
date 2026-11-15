import json
from pathlib import Path

from paperflow.delivery_control import DeliveryCommits
from paperflow.delivery_worker import launch_late_report
from paperflow.package import load, save
from test_processing_graph import graph_case, run_worker, model_calls
from test_repair_graph import run_repair, checkpoint
from test_delivery_flow import flow, IndexedRag, release
from test_winjob import wait_until


def test_late_ipc_reports_complete_graph_repair_and_duplicate_specialty_settles(graph_case, tmp_path):
    config, store, task, manager, _ = graph_case
    codes = ['120201','120202','120203']; catalog = config.catalog
    for code in codes[1:]:
        catalog['majors'].append(dict(code=code,name=code,paper_dataset_id='body-'+code,image_dataset_id='image-'+code))
    save(config.catalog_path, catalog)
    for name, key in [('profile.json','routing_candidates'),('corrections.json','final_routing_candidates')]:
        path = config.root/'fixture-material'/name; data = load(path)
        data[key] = [dict(code=code,name=code) for code in codes]; save(path,data)
    run_worker(graph_case)
    original_checkpoint = checkpoint(config,task['task_id'])
    source = store.get(task['task_id'])['manifest_sha256']
    commits = DeliveryCommits(manager,config)
    case = (manager,config,task,source,commits,None,catalog)
    client = IndexedRag()
    a = flow(case,tmp_path,client); assert a.run()['state']=='active'; release(a)
    old_active = commits.ledger.active(task['source_id'],'120201')
    b = flow(case,tmp_path,client,'120202')
    first = b.report_problem('round3_chunk','missing_chunks',{'missing':'X'})['repair_request_id']; release(b)
    run_repair(graph_case,first)

    def late(code):
        ex = launch_late_report(manager,config,source,code,'round3_chunk','missing_chunks',{'missing':'Y'})
        wait_until(lambda: ex.exit_saved,seconds=20)
        result = load(config.root/'delivery-results'/(ex.execution_id+'.json'))
        assert result['state']=='reported',result
        assert ex.exit_evidence['all_recorded_processes_exited']
        return result

    received = late('120202'); second = received['report']['followup_request_id']
    run_repair(graph_case,second)
    row = commits.repairs.get(second); assert row['state']=='complete'
    final_manifest = json.loads(row['result'])['manifest_sha256']
    before = commits.repairs.rows('SELECT * FROM target_versions')
    repeated = late('120203')
    assert repeated['report']['followup_request_id']==second
    links = commits.repairs.rows('SELECT * FROM repair_report_followups')
    assert len(links)==2 and all(link['resolved_manifest']==final_manifest for link in links)
    assert {r['outcome'] for r in commits.repairs.rows(
        'SELECT * FROM repair_targets WHERE repair_request_id=?',(first,))}=={'followup_registered'}
    assert commits.repairs.rows('SELECT * FROM target_versions')==before
    assert {r['manifest_sha256'] for r in before}=={final_manifest}
    assert commits.ledger.active(task['source_id'],'120201')==old_active
    assert commits.repairs.used(task)==2
    assert model_calls(config)[5:]==['repair','repair']
    assert checkpoint(config,task['task_id'])==original_checkpoint
    save(Path('evidence/late-repair-acceptance.json'),dict(
        scope='synthetic documents; real local graph, Job and IPC; RAG substitute only',
        first=first,second=second,source=source,result=final_manifest,late=received,duplicate=repeated,
        links=links,budget=2,old_active_preserved=True,terminal_checkpoint_preserved=True))
