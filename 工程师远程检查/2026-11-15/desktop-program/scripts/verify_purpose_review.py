"""Explicit isolated purpose acceptance; never launches a production processor."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[1]
UPDATE=ROOT/'updates/purpose-review-20261022'
sys.dont_write_bytecode=True
sys.path[:0]=[str(UPDATE/'desktop'),str(ROOT/'desktop'),str(ROOT/'core')]


def sha(path):
    result=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for data in iter(lambda:stream.read(1024*1024),b''):result.update(data)
    return result.hexdigest()


def cases():
    original=ROOT.parents[1]/'work'
    inventory=json.loads((original/'night_material_inventory.json').read_text(encoding='utf-8'))['rows']
    selected={
        'AUTO_d1d5e32f9b484f30bf51':'drawing',
        'AUTO_e9cb39ec745540c7aa4b':'drawing',
        'AUTO_036c72e0a5af42aa8eb0':'delete',
        'AUTO_d01a7d205e974e058f3c':'delete',
        'AUTO_b4083cfce841422db7d1':'delete',
        'AUTO_8031159c67b244a7adb3':'delete',
    }
    result=[]
    for row in inventory:
        if row['id'] not in selected:continue
        record=dict(row.get('record') or {},path=row.get('accessible_source_path') or row['source_path'])
        record.setdefault('name',row['name'])
        result.append(dict(id=row['id'],record=record,expected=selected[row['id']]))
    continuation=json.loads((original/'continuation_inspection.json').read_text(encoding='utf-8'))['rows']
    for row in continuation:
        if any(n in row['source_path'] for n in ('何雨佳_BS.docx','刘姚辉.docx')):
            result.append(dict(id=Path(row['source_path']).name,record=row['record'],expected='body'))
    extra=ROOT/'_verification/purpose-review-20261022/real-extra-cases.json'
    if extra.exists():result.extend(json.loads(extra.read_text(encoding='utf-8')))
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--live',action='store_true')
    parser.add_argument('--limit',type=int,default=1)
    parser.add_argument('--case',default='')
    parser.add_argument('--workers',type=int,default=2,choices=(1,2))
    args=parser.parse_args()
    if not args.live:raise SystemExit('真实请求仅在显式 --live 时执行')
    if not 1<=args.limit<=10:raise SystemExit('隔离验收上限为10份')
    home=ROOT/'_verification/purpose-review-20261022/live'
    home.mkdir(parents=True,exist_ok=True)
    config=json.loads((ROOT/'config/purpose-review/settings.json').read_text(encoding='utf-8'))
    config['cache_root']=str(home/'cache')
    processor=SimpleNamespace(config={'purpose_review':config},stop=threading.Event(),
        store=SimpleNamespace(root=home/'isolated-data'),_resume_root=home/'isolated-data',
        agent=SimpleNamespace(opener=None,llm_gate=threading.BoundedSemaphore(2),
            ledger=None,billing_context={},on_usage=None))
    from material_roles import classify_document
    from purpose_review import scope
    rows=cases()
    if args.case:rows=[r for r in rows if args.case in r['id']]
    def inspect(case):
        path=Path(case['record']['path']);before=sha(path);start=time.monotonic()
        role=classify_document(case['record'],processor=processor,work=home/'work')
        assert path.is_file() and sha(path)==before,'原文件发生变化'
        good=(role['kind']=='drawing' and role['action']=='associated') if case['expected']=='drawing' else (
            role['kind']=='document' and role['action']=='review') if case['expected']=='body' else (
            role['action']=='pending') if case['expected']=='mixed' else role['action']=='delete'
        row=dict(id=case['id'],source_path=str(path),sha256=before,expected=case['expected'],
                 passed=good,seconds=round(time.monotonic()-start,3),role=role)
        print(json.dumps(dict(id=case['id'],passed=good,kind=role['kind'],action=role['action'],
                              reason=role['reason'],seconds=row['seconds']),ensure_ascii=False),flush=True)
        return row
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        result=list(pool.map(inspect,rows[:args.limit]))
    report=home/('acceptance-'+(args.case or 'first'+str(args.limit)).replace('/','_')+'.json')
    report.write_text(json.dumps(dict(model=config['model'],reasoning_effort=config['reasoning_effort'],
        production_tasks_started=False,originals_unchanged=True,rows=result),ensure_ascii=False,indent=2),encoding='utf-8')
    return 0 if result and all(r['passed'] for r in result) else 1


if __name__=='__main__':raise SystemExit(main())
