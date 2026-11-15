"""Folder intake uses the existing queue and action admission, never auto-starts."""
import json
from pathlib import Path
import sqlite3
import time

import pytest

from paperflow.package import sha
from paperflow.scheduling_store import selection_filter
from test_workbench import web_case, login, command
from test_managed_entry import entry_case


def upload(client, headers, name, content, relative):
    return client.post('/api/inputs/upload',params={'name':name,'relative_path':relative},content=content,headers=headers)


def test_nested_folder_metadata_same_names_and_dedup_without_start(web_case):
    w,client,_=web_case;headers=login(client)
    a=upload(client,headers,'正文.txt',b'paper a','选稿/甲/正文.txt')
    b=upload(client,headers,'正文.txt',b'paper b','选稿/乙/正文.txt')
    duplicate=upload(client,headers,'copy.txt',b'paper a','选稿/丙/copy.txt')
    assert a.status_code==b.status_code==duplicate.status_code==200
    assert a.json()['id']!=b.json()['id'] and duplicate.json()['id']==a.json()['id']
    rows=client.get('/api/inputs').json()
    assert {r['relative_path'] for r in rows}=={'选稿/甲/正文.txt','选稿/乙/正文.txt'}
    assert len(rows)==2 and w.control_state()['state']=='paused'
    assert not w.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')


@pytest.mark.parametrize('relative',['../x.txt','root/../x.txt','/root/x.txt','C:/root/x.txt','root\\x.txt','root//x.txt','root/./x.txt','root/different.txt'])
def test_folder_path_is_metadata_and_cannot_escape_owned_upload(web_case,relative):
    w,client,_=web_case;headers=login(client)
    response=upload(client,headers,'x.txt',b'never accepted',relative)
    assert response.status_code==400
    assert client.get('/api/inputs').json()==[]


def test_more_than_256_inputs_are_visible_and_one_batch_is_exact(web_case):
    w,client,_=web_case;headers=login(client)
    ids=[]
    for n in range(270):
        p=w.root/'uploads'/str(n)/'paper.txt';p.parent.mkdir(parents=True);p.write_text(str(n))
        ids.append(w.record_upload('paper.txt',p,relative_path=f'全部/组{n//90}/第{n}.txt'.replace(f'第{n}.txt','paper.txt'))['id'])
    rows=client.get('/api/inputs').json()
    assert len(rows)==270 and {r['id'] for r in rows}==set(ids)
    # Keep registration and scope real; prevent synthetic sources from invoking LLM.
    resumed=[]
    original=w.manager.resume
    w.manager.resume=lambda **kw:resumed.append(kw)
    try:
        response=command(client,headers,'/api/start',inputs=ids,request_id='folder-batch')
        assert response.status_code==200,response.text
        batch=w.scheduler.ledger.rows('SELECT * FROM web_batches')[0]
        assert json.loads(batch['selection'])==ids and len(json.loads(batch['task_ids']))==270
        assert len(resumed)==1 and w.manager.stopped.is_set()
        assert not w.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')
        assert client.get('/api/status').json()['settings']['selected_limit'] is None
    finally:w.manager.resume=original


def test_large_selection_filter_keeps_exact_scope_without_sql_parameter_explosion():
    ids=[str(n) for n in range(9000)]
    condition,args=selection_filter(ids)
    assert len(args)<=4
    with sqlite3.connect(':memory:') as db:
        # Exercise the actual JSON1 parameter contract rather than textual SQL matching.
        assert db.execute('SELECT COUNT(*) FROM json_each(?)',(args[0],)).fetchone()[0]==len(ids)
    assert 'json_each' in condition


def test_scheduler_192_policy_preserves_separate_serial_lanes(entry_case):
    from paperflow.control import Manager
    from paperflow.scheduler import Scheduler
    from paperflow.store import Store
    root,old,_,c=entry_case
    c.article_workers=192;c.llm_workers=192;c.local_workers=1;c.rag_workers=1;c.max_live_units=194
    c.prepare();Path(c.handoff_root).mkdir(parents=True)
    m=Manager(Store(c.root/'state.sqlite3'),root/'manager').start()
    try:
        Scheduler(m,c,isolation_root=root)
        assert m.unit_limits=={'graph':192,'delivery':1,'cleanup':1}
        assert m.resource_limits=={'llm':192,'local':1,'rag':1}
        assert m.peer_limit>=194 and m.wait_resources
        assert m.status()['state']=='paused'
    finally:m.shutdown()
