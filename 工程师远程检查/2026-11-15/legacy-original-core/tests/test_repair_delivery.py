import pytest
import json
from pathlib import Path
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import flow,IndexedRag
from test_delivery_state import owner,begin
from test_winjob import wait_until
from paperflow.package import save,load


def test_delivery_reports_share_request_and_technical_failure_never_repairs(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case;client=IndexedRag()
    a=flow(delivery_case,tmp_path,client);a.run()
    b=flow(delivery_case,tmp_path,client,'120202')
    client.fail=lambda *args:(_ for _ in ()).throw(ConnectionError('network only'))
    with pytest.raises(ConnectionError):b.run()
    assert not commits.repairs.rows('SELECT * FROM repair_requests')
    first=b.report_problem('round3_chunk','missing_chunks',dict(chunk='method-1',detail='missing required learning chunk'))
    second=a.report_problem('round3_chunk','CHUNK-MISSING',dict(chunk='method-1',detail='same content defect'))
    assert first['repair_request_id']==second['repair_request_id']
    assert len(commits.repairs.rows('SELECT * FROM repair_reports'))==2
    assert commits.repairs.used(t)==1
    assert commits.ledger.active(t['source_id'],'120201')['identity']['manifest_sha256']==d


def test_report_sql_wait_and_final_pause_cannot_charge(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case;ex,identity=owner(delivery_case,tmp_path)
    begin(m,ex,identity,operation='repair_report')
    descriptor=commits.stage(ex.execution_id,dict(identity=identity,stage='round3_chunk',problem_type='missing_chunks',evidence=dict(chunk='c1')))
    with m.store.db() as db,ThreadPoolExecutor(1) as pool:
        db.execute('BEGIN IMMEDIATE');pending=pool.submit(commits.handle,ex.execution_id,'repair_report',descriptor)
        time.sleep(.06);assert not pending.done();m.pause('blocked repair report');db.commit()
        with pytest.raises(ValueError):pending.result(timeout=5)
    assert not commits.repairs.rows('SELECT * FROM repair_requests') and commits.repairs.used(t)==0


def test_worker_reports_through_real_ipc_without_model_or_rag_calls(delivery_case,tmp_path):
    from paperflow.delivery_store import target_key
    from paperflow.handoff_store import canonical
    m,c,t,d,commits,*_=delivery_case
    identity=commits.ledger.context(d,'120202')['identity'];key=target_key(identity)
    m.change_admission_state('unit:'+canonical(['target',key]),'report',dict(expected_generation=identity['generation']))
    config=tmp_path/'report-config.json';c.save(config);packet=tmp_path/'report-identity.json';save(packet,identity)
    output=tmp_path/'report-result.json'
    worker='''
import sys
from pathlib import Path
from paperflow.config import Config
from paperflow.package import load,save
from paperflow.control_ipc import WorkerClient
from paperflow.delivery_control import DeliveryActions
c=WorkerClient.from_environment()
try:
 a=DeliveryActions(c,Config.load(sys.argv[1]),load(sys.argv[2]))
 r=a.commit('repair_report',dict(stage='round3_chunk',problem_type='missing_chunks',evidence=dict(chunk='c1')))
 save(Path(sys.argv[3]),r)
finally:c.close()
'''
    ex=m.launch('delivery',key,[sys.executable,'-c',worker,str(config),str(packet),str(output)],cwd=Path.cwd(),
        env={'PYTHONPATH':str(Path.cwd())})
    wait_until(lambda:ex.exit_saved,seconds=15)
    assert load(output)['state']=='queued' and commits.repairs.used(t)==1
    assert len(commits.repairs.rows('SELECT * FROM repair_reports'))==1
    assert not m.ledger.rows("SELECT * FROM control_actions WHERE kind IN ('llm','rag_write')")
