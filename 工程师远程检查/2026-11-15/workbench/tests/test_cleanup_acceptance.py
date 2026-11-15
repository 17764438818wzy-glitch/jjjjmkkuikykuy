import json
from pathlib import Path
import threading
import time

import pytest

from paperflow.cleanup_control import CleanupCommits
from paperflow.cleanup_worker import launch_cleanup
from paperflow.cleanup_paths import track_artifact
from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import HandoffLedger
from paperflow.package import save,sha
from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag,flow,advance,release
from test_cleanup_flow import DeletableRag
from test_scheduling_acceptance import RagHandler,start_server
from test_winjob import wait_until


class CleanupHandler(RagHandler):
    def do_DELETE(self):self.dispatch('DELETE')


def execute(m,c,item):
    ex=launch_cleanup(m,c,item['item_id'])
    wait_until(lambda:ex.exit_saved,seconds=25)
    row=m.cleanup.ledger.item(item['item_id'])
    assert row['state']=='deleted',dict(row,execution_result=m.ledger.rows('SELECT * FROM control_execution_results WHERE execution_id=?',(ex.execution_id,)))
    return ex


def test_real_http_job_shared_versions_local_cleanup_and_orphan(delivery_case,tmp_path,monkeypatch):
    m,c,t,old,commits,material,catalog=delivery_case
    rag=IndexedRag()
    a=flow(delivery_case,tmp_path,rag);a.run();release(a)
    b=flow(delivery_case,tmp_path,rag,'120202');b.run();release(b)
    newcase=advance(delivery_case)
    a=flow(newcase,tmp_path,rag);a.run();release(a)
    control=CleanupCommits(m,c,isolation_root=tmp_path);ledger=control.ledger
    ledger.discover_remote();first=ledger.items()[0]
    with pytest.raises(ValueError,match='retention'):ledger._write(lambda db:ledger.begin_sql(db,first['item_id']))
    server,thread=start_server(CleanupHandler)
    server.rag=DeletableRag(rag);server.uploads={};server.http=[];server.fail_b=False
    server.inflight=server.peak=0;server.lock=threading.Lock()
    server.identity=dict(deployment='synthetic-stage7-http',version='fixture-v1')
    thread.start();c.rag_url=f'http://127.0.0.1:{server.server_port}'
    proof=tmp_path/'deployment.json';save(proof,dict(passed=True,endpoint=c.rag_url,identity=server.identity))
    c.delivery_acceptance_report=str(proof)
    actual_launch=m.launch
    def launch(kind,key,argv,**kwargs):
        argv=list(argv)
        if 'paperflow.cleanup_worker' in argv:
            i=argv.index('-m');argv[i:i+2]=[str(Path('tests/fixtures/cleanup_worker.py').resolve())]
        return actual_launch(kind,key,argv,**kwargs)
    monkeypatch.setattr(m,'launch',launch)
    original_hash=sha(t['source_path']);executions=[]
    try:
        with m.store.db() as db:db.execute('UPDATE cleanup_items SET eligible_at=0 WHERE item_id=?',(first['item_id'],))
        executions.append(execute(m,c,first))
        assert commits.ledger.active(t['source_id'],'120202')['identity']['manifest_sha256']==old
        ledger.close_run(t['task_id'],t['task_id'],'complete')
        with pytest.raises(ValueError,match='retained|reference'):ledger.plan_package(old)
        # B independently switches; old remote B and shared package can now retire.
        b=flow(newcase,tmp_path,server.rag,'120202');b.run();release(b)
        ledger.discover_remote()
        second=next(r for r in ledger.items() if r['specialty_code']=='120202')
        with m.store.db() as db:db.execute('UPDATE cleanup_items SET eligible_at=0 WHERE item_id=?',(second['item_id'],))
        executions.append(execute(m,c,second))
        package=ledger.plan_package(old);old_path=json.loads(package['resource'])['path']
        executions.append(execute(m,c,package));assert not Path(old_path).exists()
        assert Path(commits.ledger.registration(newcase[3])['package_dir']).exists()
        # A genuinely unregistered orphan requires explicit abandonment and its own configured clock.
        handoff=PackageHandoff(m.store,c.handoff_root)
        intent=handoff.prepare('orphan-fixture',dict(t,revision=9),material,catalog,mapping_version='orphan-fixture')
        orphan=handoff.publish('orphan-fixture',intent['input_fingerprint'])
        ledger.close_run('orphan-fixture',t['task_id'],'abandoned',reason='isolated acceptance')
        c.orphan_package_retention_seconds=0
        item=ledger.plan_publication('orphan-fixture',intent['input_fingerprint'])
        executions.append(execute(m,c,item));assert not orphan.exists()
        assert sha(t['source_path'])==original_hash
        deleted=[r for r in server.http if r[0]=='DELETE']
        assert len(deleted)==2 and server.peak==1
        evidence=dict(test='real HTTP/native client/AF_PIPE/Windows Job/SQLite/local files',
            deployment=server.identity,endpoint=c.rag_url,synthetic=True,seven_day_boundary_rejected=True,
            http_deletes=deleted,shared_old_kept_while_b_active=True,original_hash_unchanged=True,
            current_package_exists=True,local_orphan_deleted=True,
            all_trees_exited=all(ex.exit_saved for ex in executions),items=[{k:r[k] for k in ('item_id','kind','category','state','receipt')} for r in ledger.items()])
        save(Path('evidence/cleanup-acceptance.json'),evidence)
    finally:server.shutdown();thread.join(2);server.server_close()
