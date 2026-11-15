import json
from pathlib import Path

import pytest

from model_activity import ModelActivityReadError, ModelActivityReader


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding='utf8')


def running(root, *, large=False):
    work = root/'runs/current/private/paper'
    request = work/'model/request-state.json'
    put(request, dict(status='request_in_flight', client_http_requests=1, http_started_at=101))
    manifest = dict(run_id='current', status='running', active_items=[dict(preextract_work_dir=str(work))])
    if large:manifest['items']=[{'historical_result':'x'*(17*1024*1024)}]
    put(root/'runs/current/manifest.json', manifest)
    put(root/'private/model_activity/owners/current.json',
        dict(schema='model-activity-owner/1',run_id='current',pid=123,create_time=1))
    return request


def test_large_stopped_history_does_not_hide_live_count(tmp_path):
    running(tmp_path)
    put(tmp_path/'runs/earlier/manifest.json', dict(run_id='earlier', status='stopped',
        items=[{'historical_result':'x'*(17*1024*1024)}]))
    reader=ModelActivityReader(tmp_path,process_is_same=lambda pid,created:True)
    value=reader.read('current')
    assert (value['total'],value['current'],value['background'])==(1,1,0)
    assert all(set(projection[1])=={'run_id','status','active_items'} for projection in reader._manifests.values())
    assert reader.read('current')['total']==1


def test_large_running_batch_retains_owner_and_request_checks(tmp_path):
    running(tmp_path,large=True)
    value=ModelActivityReader(tmp_path,process_is_same=lambda pid,created:True).read('current')
    assert value['total']==1 and value['unconfirmed']==0
    dead=ModelActivityReader(tmp_path,process_is_same=lambda pid,created:False).read('current')
    assert dead['total']==0 and dead['unconfirmed']==1


@pytest.mark.parametrize('kind',['owner','request'])
def test_small_status_and_owner_records_keep_their_size_limit(tmp_path,kind):
    request=running(tmp_path)
    path=request if kind=='request' else tmp_path/'private/model_activity/owners/current.json'
    value=json.loads(path.read_text());value['unexpected_padding']='x'*(17*1024*1024)
    put(path,value)
    snapshot=ModelActivityReader(tmp_path,process_is_same=lambda pid,created:True).read('current')
    assert snapshot['partial'] and not snapshot['current_complete']


def test_json_read_stops_at_limit_plus_one_if_file_grows(tmp_path,monkeypatch):
    path=tmp_path/'state.json'
    put(path,{'status':'request_in_flight'})
    reader=ModelActivityReader(tmp_path)
    original=Path.open
    reads=[]
    class GrowingFile:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def read(self,limit):
            reads.append(limit)
            return b'x'*limit
    def opened(self,*args,**kwargs):
        return GrowingFile() if self==path else original(self,*args,**kwargs)
    monkeypatch.setattr(Path,'open',opened)
    with pytest.raises(ModelActivityReadError):reader._json(path,max_bytes=1024)
    assert reads==[1025]


def test_oversized_manifest_is_still_rejected_before_reading(tmp_path,monkeypatch):
    path=tmp_path/'runs/current/manifest.json';put(path,{'run_id':'current','status':'running'})
    original=Path.stat
    def oversized(self,*args,**kwargs):
        if self==path:
            class Stat:
                st_size=128*1024*1024+1
                st_mtime_ns=1
            return Stat()
        return original(self,*args,**kwargs)
    monkeypatch.setattr(Path,'stat',oversized)
    with pytest.raises(ModelActivityReadError):ModelActivityReader(tmp_path)._manifest(path)
