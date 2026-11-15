import io
import json
import uuid
from contextlib import nullcontext
import os

import pytest

from paperflow.remote_files import file_map,manifest,write_archive
from paperflow.remote_gateway import Gateway


def header():return dict(namespace='a'*64,execution_id='b'*32,epoch='c'*32,job_id='d'*32,image='sha256:'+'1'*64)


def test_closed_execution_rejects_delayed_upload_and_start(tmp_path,monkeypatch):
    gateway=Gateway(tmp_path/'server',{'image':'sha256:'+'1'*64})
    # OS flock is exercised by the real Linux gateway acceptance, not this unit test.
    if os.name=='nt':gateway.locked=lambda *args,**kwargs:nullcontext()
    monkeypatch.setattr(gateway,'inventory',lambda _h:[])
    h=header();proof=gateway.reap_execution(h)
    assert proof['all_exited'] and proof['execution_closed']
    src=tmp_path/'source';src.mkdir();(src/'x').write_bytes(b'x')
    files=file_map(src);h['files']=manifest(files);stream=io.BytesIO();write_archive(stream,files);stream.seek(0)
    with pytest.raises(ValueError,match='closed'):gateway.prepare(h,stream)
    with pytest.raises(ValueError,match='closed'):gateway.run(h,io.BytesIO(),io.BytesIO())


@pytest.mark.parametrize('field,value',[('execution_id','../escape'),('namespace','a'*64+'/x'),('epoch','not-an-epoch'),('job_id','/absolute')])
def test_gateway_identity_cannot_select_other_paths(tmp_path,field,value):
    gateway=Gateway(tmp_path,{'image':'sha256:'+'1'*64});h=header();h[field]=value
    with pytest.raises(ValueError):gateway.validate(h)


def test_unknown_docker_result_does_not_count_as_absent(tmp_path,monkeypatch):
    gateway=Gateway(tmp_path,{'image':'sha256:'+'1'*64})
    if os.name=='nt':gateway.locked=lambda *args,**kwargs:nullcontext()
    import subprocess
    monkeypatch.setattr(subprocess,'run',lambda *a,**k:subprocess.CompletedProcess(a[0],1,'','Cannot connect to Docker daemon'))
    with pytest.raises(ConnectionError):gateway.inspect('owned')
    monkeypatch.setattr(subprocess,'run',lambda *a,**k:subprocess.CompletedProcess(a[0],1,'','error: no such object: owned'))
    assert gateway.inspect('owned') is None


def test_wrong_deployment_rejected_before_creation(tmp_path):
    gateway=Gateway(tmp_path,dict(image='sha256:'+'1'*64,deployment_id='trusted'))
    h=header();h.update(image='wrong-image',deployment_id='trusted')
    with pytest.raises(ValueError):gateway.validate(h)
    h.update(image='sha256:'+'1'*64,deployment_id='other-server')
    with pytest.raises(ValueError):gateway.validate(h)


def test_create_in_progress_cannot_be_proved_exited(tmp_path,monkeypatch):
    gateway=Gateway(tmp_path,{'image':'sha256:'+'1'*64})
    if os.name=='nt':gateway.locked=lambda *args,**kwargs:nullcontext()
    h=header();target=gateway.validate(h)/h['job_id'];target.mkdir(parents=True)
    (target/'intent.json').write_text(json.dumps({k:h[k] for k in ('namespace','execution_id','epoch','job_id')}))
    (target/'creation.json').write_text(json.dumps({'state':'creating'}))
    monkeypatch.setattr(gateway,'inspect',lambda _name:None)
    with pytest.raises(ConnectionError):gateway.reap_execution(h)
    assert (gateway.validate(h)/'closed.json').exists()
    with pytest.raises(ValueError):gateway.check_open(h)


def test_unchanged_sync_preserves_native_sdk_file_observation(tmp_path,monkeypatch):
    gateway=Gateway(tmp_path/'server',{'image':'sha256:'+'1'*64})
    if os.name=='nt':
        gateway.locked=lambda *args,**kwargs:nullcontext()
        monkeypatch.setattr(os,'chown',lambda *_:None,raising=False)
    h=header();output=gateway.validate(h)/h['job_id']/'data/output';output.mkdir(parents=True)
    target=output/'result.txt';target.write_text('FIRST');os.utime(target,(1000,1000))
    before=target.stat()
    source=tmp_path/'pc';source.mkdir();(source/'result.txt').write_text('FIRST');(source/'failure.json').write_text('new diagnostic')
    files=file_map(source);h['files']=manifest(files);stream=io.BytesIO();write_archive(stream,files);stream.seek(0)
    gateway.update(h,stream)
    assert target.stat().st_mtime_ns==before.st_mtime_ns
    assert target.stat().st_ino==before.st_ino
    assert (output/'failure.json').read_text()=='new diagnostic'
