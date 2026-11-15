import concurrent.futures as cf
import hashlib,json,os,pathlib,time
from organize_library import WORK,SOURCE,OUTPUT,digest,dump,get_records

def verify_hash(item):
    path,sha=item
    try:return None if digest(path)==sha else {'path':str(path),'error':'SHA256 mismatch'}
    except Exception as e:return {'path':str(path),'error':str(e)}

if __name__=='__main__':
    baseline=json.loads((WORK/'source-inventory.json').read_text('utf-8'))
    records=get_records();originals=[r for r in records if 'source_mtime_ns' in r]
    source_expected={r['source']:r for r in originals}
    source_baseline={f['path']:f for f in baseline['files']}
    current={}
    for parent,dirs,names in os.walk(SOURCE,followlinks=False):
        for name in names:
            p=pathlib.Path(parent)/name;s=p.stat()
            current[p.relative_to(SOURCE).as_posix()]={'size':s.st_size,'mtime_ns':s.st_mtime_ns}
    errors=[]
    if set(source_expected)!=set(source_baseline):errors.append({'error':'Source inspection coverage mismatch'})
    missing=sorted(set(source_baseline)-set(current));added=sorted(set(current)-set(source_baseline))
    changed=[p for p in set(current)&set(source_baseline) if any(current[p][k]!=source_baseline[p][k] for k in ('size','mtime_ns'))]
    with (WORK/'materialization.jsonl').open(encoding='utf-8') as f:results=[json.loads(line) for line in f if line.strip()]
    expected={};metadata_paths=set();actions=set()
    for r in results:
        actions.update(a['source'] for a in r['actions'])
        if r.get('folder_path'):metadata_paths.add(r['folder_path']+'/资料清单.json')
        for o in r['outputs']:
            if o['path'] in expected and expected[o['path']]!=o['sha256']:errors.append({'error':'Conflicting destination hashes','path':o['path']})
            expected[o['path']]=o['sha256']
        if r['body']:
            bodydir=OUTPUT/r['folder_path']/'正文'
            files=list(bodydir.iterdir()) if bodydir.exists() else []
            if len(files)!=1:errors.append({'error':'Body folder does not contain exactly one manuscript','task':r['task']})
            elif expected.get(files[0].relative_to(OUTPUT).as_posix())!=r['body']['sha256']:errors.append({'error':'Body identity mismatch','task':r['task']})
    if actions!=set(source_baseline):errors.append({'error':'Source disposition coverage mismatch','missing':sorted(set(source_baseline)-actions)[:30],'extra':sorted(actions-set(source_baseline))[:30]})
    actual={}
    for parent,dirs,names in os.walk(OUTPUT,followlinks=False):
        for name in names:
            p=pathlib.Path(parent)/name;rel=p.relative_to(OUTPUT).as_posix()
            if rel.startswith('_整理记录/') or rel in metadata_paths or rel=='整理说明.txt':continue
            actual[rel]=p.stat().st_size
    missing_outputs=sorted(set(expected)-set(actual));extra_outputs=sorted(set(actual)-set(expected))
    checks=[(SOURCE/rel,r['sha256']) for rel,r in source_expected.items()]+[(OUTPUT/rel,sha) for rel,sha in expected.items()]
    start=time.time()
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for i,error in enumerate(ex.map(verify_hash,checks),1):
            if error:errors.append(error)
            if i%2500==0:print(json.dumps({'hashes_checked':i,'total':len(checks),'seconds':round(time.time()-start)}),flush=True)
    report={'source_files':len(source_baseline),'source_bytes':sum(f['size'] for f in baseline['files']),'source_missing':missing,'source_added':added,'source_metadata_changed':changed,'source_disposition_coverage':len(actions),'output_data_files':len(expected),'output_missing':missing_outputs,'output_unrecorded':extra_outputs,'hash_checks':len(checks),'errors':errors,'passed':not any((missing,added,changed,missing_outputs,extra_outputs,errors))}
    dump(WORK/'verification.json',report);dump(OUTPUT/'_整理记录'/'完整性核验.json',report)
    summary=json.loads((WORK/'summary.json').read_text('utf-8'));summary['verification']='passed' if report['passed'] else 'failed';summary['unique_output_files']=len(expected)
    dump(WORK/'summary.json',summary);dump(OUTPUT/'_整理记录'/'整理统计.json',summary)
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True)
    if not report['passed']:raise SystemExit(2)
