"""Inspect existing alternate manuscripts; writes only scratch evidence."""
import concurrent.futures as cf,json,pathlib,shutil,time
from collections import defaultdict
import organize_library as o
import review_pending_library as p

def attempt(r):
    try:return p.extract(r)
    except Exception as e:return {'sha256':r['sha256'],'error':str(e)}

def main():
    ds=json.loads((o.WORK/'decisions.json').read_text('utf-8'))
    chosen=[d for d in ds if d['body'] and d['alternates']]
    tasks={d['task'] for d in chosen};rs=[r for r in o.get_records() if r['task'] in tasks and r['role']=='candidate']
    unique={r['sha256']:r for r in rs}
    old=json.loads((p.PHASE/'extracted.json').read_text('utf-8'));cache={e['sha256']:e for e in old}
    ep=p.PHASE/'extended-extracted.json'
    if ep.exists():cache.update({e['sha256']:e for e in json.loads(ep.read_text('utf-8'))})
    jobs=[]
    for r in unique.values():
        if r['format'] not in {'.doc','.rtf'}:continue
        if r['sha256'].startswith(('7723f3eb030a','010d6474c6aa','59b75db38fab')):continue  # confirmed process handbooks; full-text conversion stalls
        copy=p.PHASE/'office-input'/(r['sha256']+r['format'])
        if not copy.exists():shutil.copyfile(r['local_path'],copy)
        if o.digest(copy)!=r['sha256']:raise RuntimeError('Copy hash mismatch')
        if not (p.PHASE/'office-text'/(r['sha256']+'.txt')).exists():jobs.append(copy)
    print(json.dumps({'groups':len(chosen),'documents':len(unique),'office_conversions':len(jobs)}),flush=True)
    step=1 if len(jobs)<10 else 24
    shards=[(100+i,jobs[j:j+step]) for i,j in enumerate(range(0,len(jobs),step))]
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        futures=[ex.submit(p.convert_shard,s) for s in shards]
        for future in cf.as_completed(futures):
            try:print(json.dumps({'converted':len(future.result())}),flush=True)
            except Exception as e:print(json.dumps({'conversion_note':str(e)[:160]}),flush=True)
    missing=[r for sha,r in unique.items() if sha not in cache or cache[sha].get('error')]
    with cf.ProcessPoolExecutor(max_workers=4) as ex:
        for i,e in enumerate(ex.map(attempt,missing,chunksize=4),1):
            cache[e['sha256']]=e
            if i%200==0:print(json.dumps({'extracted':i,'total':len(missing)}),flush=True)
    o.dump(ep,[cache[sha] for sha in unique]);groups=[];bytask=defaultdict(list)
    for r in rs:bytask[r['task']].append(r)
    for i,d in enumerate(chosen,1):
        cs=list({r['sha256']:r for r in bytask[d['task']]}.values());sel=cache[d['body']['sha256']];pairs=[]
        for r in cs:
            e=cache[r['sha256']]
            if r['sha256']!=d['body']['sha256'] and not sel.get('error') and not e.get('error'):pairs.append(p.compare(sel,e))
        groups.append(dict(d,candidates=[dict(r,extraction=cache[r['sha256']]) for r in cs],pairs=pairs))
        if i%100==0:print(json.dumps({'compared_groups':i,'total':len(chosen)}),flush=True)
    o.dump(p.PHASE/'extended-groups.json',groups)
    print(json.dumps({'complete':True,'errors':[e for sha,e in cache.items() if sha in unique and e.get('error')]}),flush=True)

if __name__=='__main__':main()
