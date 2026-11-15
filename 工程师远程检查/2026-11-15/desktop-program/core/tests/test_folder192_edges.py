"""Large explicit selection transport and original control/serial boundaries."""
import json
from pathlib import Path
import sys
import time

import pytest

from paperflow.control import Manager
from paperflow.package import save
from paperflow.scheduling_policy import ActionDeferred
from paperflow.store import Store
from test_workbench import web_case,login,command,select
from test_managed_entry import entry_case
from test_scheduling_resources import worker
from test_winjob import wait_until


def test_selection_body_over_64k_reaches_exact_lookup_not_old_transport_cap(web_case):
    w,client,_=web_case;headers=login(client)
    response=command(client,headers,'/api/start',inputs=[f'{n:032x}' for n in range(2200)],request_id='large-transport')
    assert response.status_code==400 and '所选文件不存在' in response.json()['detail']
    assert not w.store.all_tasks() and not w.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


def test_chunked_selection_transport_is_bounded_before_registering(web_case,monkeypatch):
    import webworkbench.api as module
    w,client,_=web_case;headers=login(client)
    monkeypatch.setattr(module,'MAX_SELECTION_REQUEST',100)
    response=client.post('/api/start',headers=headers,content=(block for block in (b'{"inputs":',b' '*200,b'[]}')))
    assert response.status_code==413 and not w.store.all_tasks()


def test_explicit_new_web_deployment_allows_over_ten_but_keeps_exact_hash_scope(entry_case):
    from fastapi.testclient import TestClient
    from paperflow.workbench import Workbench
    from test_workbench_release import deployment_case
    from webworkbench.api import create_app
    from webworkbench.auth import create_admin
    root,old,c,manifest=deployment_case(entry_case)
    item=json.loads(manifest.read_text());item['maximum_selected']=None;save(manifest,item)
    admin=root/'admin.json';create_admin(admin,'admin','isolated-workbench-password')
    w=Workbench(c,root,legacy_root=old,deployment=manifest)
    with TestClient(create_app(w,admin)) as client:
        headers=login(client);ids=[select(client,headers,name=f'{n}.txt',data=f'isolated {n}'.encode()) for n in range(12)]
        # Keep source scope and durable batch real; no network/model worker.
        original=w.manager.resume;w.manager.resume=lambda **kw:None
        try:
            response=command(client,headers,'/api/start',inputs=ids,request_id='explicit-folder')
            assert response.status_code==200,response.text
            scope=json.loads(Path(c.production_pilot).read_text())
            assert len(scope['sources'])==12 and all(set(row)=={'path','sha256'} for row in scope['sources'])
            assert len(w.scheduler.service.selected_tasks)==12 and not w.manager.status()['admission_open']
            assert not w.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')
        finally:w.manager.resume=original


@pytest.mark.parametrize('kind',['local','rag_read','rag_write'])
def test_new_mode_still_serializes_action_lane_without_restart(tmp_path,kind):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',resource_limits={'llm':192,'local':1,'rag':1}).start()
    m.wait_resources=True;m.resume()
    try:
        first,a=worker(m,tmp_path,'a',kind);second,b=worker(m,tmp_path,'b',kind)
        (a/'go').touch();wait_until(lambda:(a/'performed').exists());(b/'go').touch()
        time.sleep(.3);assert not (b/'performed').exists() and not second.exit_saved
        lane='rag' if kind.startswith('rag') else 'local';assert m.status()['resources'][lane]['used']==1
        (a/'release').touch();wait_until(lambda:(b/'performed').exists())
        assert m.status()['resources'][lane]['used']==1
        (b/'release').touch();wait_until(lambda:first.exit_saved and second.exit_saved)
        assert m.status()['resources'][lane]['used']==0
    finally:m.shutdown()
