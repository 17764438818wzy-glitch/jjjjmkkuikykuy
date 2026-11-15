import io
import json
import threading
import time

import pytest

from test_workbench import web_case, login
from test_managed_entry import entry_case


def test_host_memory_uses_available_not_free_and_rejects_partial_sample(tmp_path):
    from paperflow.remote_gateway import host_memory
    path=tmp_path/'meminfo'
    path.write_text('MemTotal: 16384 kB\nMemFree: 100 kB\nMemAvailable: 6144 kB\n')
    assert host_memory(path)==dict(total_bytes=16384*1024,used_bytes=10240*1024,available_bytes=6144*1024)
    path.write_text('MemTotal: 16384 kB\nMemFree: 100 kB\n')
    assert host_memory(path) is None


def test_health_includes_host_memory_without_business_operations(tmp_path,monkeypatch):
    import paperflow.remote_gateway as module
    sample=dict(total_bytes=16384,used_bytes=10240,available_bytes=6144)
    monkeypatch.setattr(module,'host_memory',lambda:sample)
    gateway=module.Gateway(tmp_path/'jobs',dict(image='image',deployment_id='deployment',version='version'))
    monkeypatch.setattr(gateway,'inspect',lambda *_:pytest.fail('health must not access Docker'))
    output=io.BytesIO()
    gateway.handle(dict(op='health',namespace='0'*64,execution_id='0'*32,epoch='0'*32,job_id='0'*32,
                        image='image',deployment_id='deployment'),io.BytesIO(),output)
    assert json.loads(output.getvalue())['host_memory']==sample
    assert list((tmp_path/'jobs').iterdir())==[]


def test_server_memory_caches_and_clears_values_after_connection_failure(monkeypatch):
    import paperflow.workbench_resources as module
    settings=dict(host='server',image='image',deployment_id='deployment')
    reader=module.ServerMemory(settings)
    clock=[100];calls=[]
    monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    def probe(settings,header,**kwargs):
        calls.append((header,kwargs))
        if len(calls)>1:raise ConnectionError('offline')
        return dict(ready=True,image='image',host_memory=dict(total_bytes=16384,used_bytes=10240,available_bytes=6144))
    monkeypatch.setattr(module,'request',probe)
    assert reader.read()['state']=='available'
    assert reader.read()['total_bytes']==16384 and len(calls)==1
    assert calls[0][0]['op']=='health' and calls[0][1]['timeout']<=4
    clock[0]+=6
    failed=reader.read()
    assert failed['state']=='unavailable' and failed['total_bytes'] is None and failed['used_bytes'] is None
    assert reader.read()==failed and len(calls)==2


@pytest.mark.parametrize('sample',[None,{},dict(total_bytes=True,used_bytes=0,available_bytes=1),
    dict(total_bytes=10,used_bytes=11,available_bytes=-1),dict(total_bytes=10,used_bytes=7,available_bytes=7)])
def test_server_memory_does_not_report_invalid_values_as_real_memory(monkeypatch,sample):
    import paperflow.workbench_resources as module
    monkeypatch.setattr(module,'request',lambda *_args,**_kwargs:dict(ready=True,image='image',host_memory=sample))
    value=module.ServerMemory(dict(host='server',image='image',deployment_id='deployment')).read()
    assert value['state']=='unavailable' and value['total_bytes'] is None


def test_server_memory_endpoint_is_authenticated_and_slow_read_cannot_block_pause(web_case,monkeypatch):
    import paperflow.workbench_resources as module
    workbench,client,_=web_case
    assert client.get('/api/resources/server').status_code==401
    headers=login(client)
    epoch=client.get('/api/control').json()['manager']['epoch']
    workbench.server_memory=module.ServerMemory(dict(host='server',image='image',deployment_id='deployment'))
    entered=threading.Event();release=threading.Event();responses=[]
    def blocked(*args,**kwargs):
        entered.set();assert release.wait(5)
        raise ConnectionError('offline')
    monkeypatch.setattr(module,'request',blocked)
    thread=threading.Thread(target=lambda:responses.append(client.get('/api/resources/server')))
    thread.start()
    try:
        assert entered.wait(2)
        before=time.monotonic()
        assert client.get('/api/resources/server').status_code==503
        response=client.post('/api/pause',headers=headers,json={'epoch':epoch})
        assert response.status_code==200 and time.monotonic()-before<1
        assert response.json()['manager']['admission_open'] is False
    finally:
        release.set();thread.join(3)
    assert not thread.is_alive() and responses[0].json()['state']=='unavailable'
    assert workbench.store.all_tasks()==[]
    assert workbench.scheduler.ledger.rows('SELECT * FROM scheduler_attempts')==[]
