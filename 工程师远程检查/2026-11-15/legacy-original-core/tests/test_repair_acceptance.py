import json
from pathlib import Path
import threading
from http.server import ThreadingHTTPServer

from paperflow.package import load,save,sha
from test_processing_graph import graph_case,run_worker,model_calls
from test_processing_harness import Handler
from test_repair_graph import request,run_repair,checkpoint
from test_delivery_flow import flow,IndexedRag,release
from test_winjob import wait_until


def test_real_sdk_dsh_tool_repairs_files_and_reuses_terminal_cli(graph_case):
    c,s,t,m,_=graph_case;run_worker(graph_case);original=s.get(t['task_id'])
    rid=request(graph_case)['repair_request_id']
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    server.mode='actual';server.requests_seen=0;server.stop_stream=threading.Event();server.streaming=threading.Event()
    threading.Thread(target=lambda:server.serve_forever(poll_interval=.1),daemon=True).start()
    try:
        ex=run_repair(graph_case,rid,'actual',endpoint=f'http://127.0.0.1:{server.server_port}/v1')
        row=m.processing.repairs.ledger.get(rid)
        assert row['state']=='complete',(c.root/'fixture-events.jsonl').read_text(encoding='utf-8')
        result=json.loads(row['result'])
        assert sha(Path(original['package_dir'])/'chunks.json')!=sha(Path(result['package_dir'])/'chunks.json')
        assert sha(Path(original['package_dir'])/'article.redacted.md')==sha(Path(result['package_dir'])/'article.redacted.md')
        assert [json.loads(x)['stage'] for x in (c.root/'actual-tools.jsonl').read_text().splitlines()]==['repair']
        assert server.requests_seen==2 and ex.exit_evidence['all_recorded_processes_exited']
        from paperflow.repair_control import launch_repair
        replay=launch_repair(m,c,rid,total_timeout=30);wait_until(lambda:replay.exit_saved,seconds=15)
        assert server.requests_seen==2 and replay.exit_evidence['all_recorded_processes_exited']
        save(Path('evidence/repair-harness.json'),dict(endpoint='loopback SSE, synthetic material, installed SDK and dsh.cmd',
            requests=server.requests_seen,repair_request_id=rid,source=original['manifest_sha256'],result=result,
            tools=[json.loads(x) for x in (c.root/'actual-tools.jsonl').read_text().splitlines()],exit=ex.exit_evidence))
    finally:server.stop_stream.set();server.shutdown();server.server_close()


def test_pause_and_manager_restart_resume_same_repair_without_budget_reset(graph_case):
    c,s,t,m,config=graph_case;run_worker(graph_case);before=checkpoint(c,t['task_id'])
    rid=request(graph_case)['repair_request_id'];ex=run_repair(graph_case,rid,'pause-repair',wait=False)
    wait_until(lambda:(c.root/'model-entered').exists(),seconds=20);m.pause('repair pause')
    wait_until(lambda:ex.exit_saved,seconds=13)
    assert not list((c.root/'work'/t['task_id']/'phase-cache'/'repair').glob('*.json'))
    old_epoch=m.epoch;m.shutdown()
    from paperflow.control import Manager
    from paperflow.processing_control import ProcessingCommits
    new=Manager(s,m.root).start();ProcessingCommits(new,c)
    try:
        assert new.stopped.is_set() and new.epoch!=old_epoch
        new.resume();case=c,s,t,new,config;run_repair(case,rid)
        assert new.processing.repairs.ledger.get(rid)['state']=='complete'
        assert new.processing.repairs.ledger.used(t)==1 and checkpoint(c,t['task_id'])==before
        assert model_calls(c)[5:]==['repair','repair']
    finally:new.shutdown()


def test_b_c_shared_report_updates_successful_a_then_separate_redelivery(graph_case,tmp_path):
    c,s,t,m,config=graph_case
    codes=['120201','120202','120203'];catalog=c.catalog
    for code in codes[1:]:catalog['majors'].append(dict(code=code,name=code,paper_dataset_id='paper-'+code,image_dataset_id='image-'+code))
    save(c.catalog_path,catalog)
    for name,key in [('profile.json','routing_candidates'),('corrections.json','final_routing_candidates')]:
        path=c.root/'fixture-material'/name;data=load(path);data[key]=[dict(code=code,name=code) for code in codes];save(path,data)
    run_worker(graph_case);d=s.get(t['task_id'])['manifest_sha256']
    from paperflow.delivery_control import DeliveryCommits
    dc=DeliveryCommits(m,c);case=(m,c,t,d,dc,None,catalog);client=IndexedRag()
    a=flow(case,tmp_path,client);assert a.run()['state']=='active';release(a)
    old_active=dc.ledger.active(t['source_id'],codes[0]);old_docs=dict(client.docs)
    requests=[]
    for code in codes[1:]:
        unit=flow(case,tmp_path,client,code)
        requests.append(unit.report_problem('round3_chunk','missing_chunks',dict(detail='same shared missing method chunk')))
        release(unit)
    rid=requests[0]['repair_request_id'];assert requests[1]['repair_request_id']==rid
    run_repair(graph_case,rid);row=m.processing.repairs.ledger.get(rid)
    assert row['state']=='complete'
    digest=json.loads(row['result'])['manifest_sha256']
    assert {x['manifest_sha256'] for x in m.ledger.rows('SELECT * FROM target_versions')}=={digest}
    assert dc.ledger.active(t['source_id'],codes[0])==old_active and client.docs==old_docs
    upgrade=flow((m,c,t,digest,dc,None,catalog),tmp_path,client)
    assert upgrade.run()['state']=='active'
    assert dc.ledger.active(t['source_id'],codes[0])['identity']['manifest_sha256']==digest
    assert dc.repairs.used(t)==1 and model_calls(c)[5:]==['repair']
    assert [x['outcome'] for x in dc.repairs.rows('SELECT * FROM repair_targets ORDER BY specialty_code')]==['advanced']*3
    save(Path('evidence/repair-shared-flow.json'),dict(source=d,result=digest,repair_request_id=rid,
        reports=dc.repairs.rows('SELECT * FROM repair_reports'),targets=dc.repairs.rows('SELECT * FROM repair_targets'),
        active_a=dc.ledger.active(t['source_id'],codes[0]),budget=dc.repairs.used(t),
        remote='IndexedRag in-memory fault boundary; no LAN or production writes'))
