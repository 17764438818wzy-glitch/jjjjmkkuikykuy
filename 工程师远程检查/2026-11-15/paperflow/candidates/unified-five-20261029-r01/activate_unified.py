"""Apply a reviewed source set to the idle, existing runner with exact backups."""
import hashlib
import json
from pathlib import Path
import shutil
import time
from urllib.request import Request,urlopen

ROOT=Path(__file__).resolve().parent
def read(path):return json.loads(Path(path).read_text(encoding='utf8'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf8')
def api(path,body=None,method=None):
    req=Request('http://192.168.1.120:4200/api'+path,data=json.dumps(body).encode() if body is not None else None,
                headers={'Content-Type':'application/json'},method=method or ('POST' if body is not None else 'GET'))
    with urlopen(req,timeout=20) as response:
        data=response.read();return json.loads(data) if data else None

def main():
    baseline=read(ROOT/'evidence/baseline-main-manifest.json');main=Path(baseline['source'])
    for name,digest in baseline['files'].items():
        if not (main/name).is_file() or sha(main/name)!=digest:raise ValueError('main source drift: '+name)
    import psutil
    record=read(main/'runtime/runner-process.json')
    if psutil.pid_exists(record['pid']):
        process=psutil.Process(record['pid'])
        if abs(process.create_time()-record['create_time'])<0.01:raise RuntimeError('stop the idle old runner before activation')
    runs=api('/flow_runs/filter',{'flow_runs':{'state':{'type':{'any_':['RUNNING','PENDING','CANCELLING','SCHEDULED']}}},
         'deployments':{'id':{'any_':['3c968368-58fc-40a0-9966-0ba2fde8c92c']}},'limit':100})
    limits=api('/v2/concurrency_limits/filter',{'limit':100,'offset':0})
    if runs or any(r['active_slots'] for r in limits if r['name'].startswith('paperflow-connected-')):
        raise RuntimeError('existing execution or resource occupancy; no activation')
    config=read(ROOT/'evidence/production-config.json')
    target=main/'runtime/backups'/('unified-five-'+str(time.time_ns()));target.mkdir(parents=True)
    save(target/'deployment-before.json',api('/deployments/3c968368-58fc-40a0-9966-0ba2fde8c92c'))
    save(target/'limits-before.json',limits)
    sources={p.relative_to(ROOT):p for directory in ('paperflow_prefect','tests') for p in (ROOT/directory).rglob('*.py')}
    sources[Path('manage.py')]=ROOT/'manage.py';sources[Path('config.json')]=ROOT/'evidence/production-config.json'
    changes=[]
    for relative,source in sources.items():
        destination=main/relative
        if destination.is_file() and sha(destination)==sha(source):continue
        if destination.exists() and relative.as_posix() not in baseline['files']:
            raise ValueError('unreviewed newly present main file: '+str(relative))
        old=sha(destination) if destination.exists() else None
        if old:
            previous=target/'files'/relative;previous.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(destination,previous)
        changes.append({'path':str(relative),'before':old,'after':sha(source)})
    save(target/'change-manifest.json',changes)
    for row in changes:
        destination=main/row['path'];destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(sources[Path(row['path'])],destination)
        if sha(destination)!=row['after']:raise IOError('copied source hash mismatch')
    for key in ('library_root','data_root','receipt_root','classification_root','nas_destination'):
        Path(config[key]).mkdir(parents=True,exist_ok=True)
    rag=next(r for r in limits if r['name']=='paperflow-connected-rag')
    if rag['limit'] not in (4,20):raise ValueError('unexpected prior RAG limit')
    api('/v2/concurrency_limits/'+rag['id'],{'limit':20,'active':True},method='PATCH')
    after=api('/v2/concurrency_limits/paperflow-connected-rag')
    if after['limit']!=20 or not after['active']:raise RuntimeError('remote quota update not confirmed')
    report=dict(status='applied_awaiting_runner_verification',backup=str(target),main=str(main),changes=changes,
                rag_limit=after['limit'],nas_limit=next(r['limit'] for r in limits if r['name']=='paperflow-connected-nas'))
    save(ROOT/'evidence/activation.json',report)
    print(json.dumps({k:report[k] for k in ('status','backup','rag_limit','nas_limit')},ensure_ascii=False))

if __name__=='__main__':main()
