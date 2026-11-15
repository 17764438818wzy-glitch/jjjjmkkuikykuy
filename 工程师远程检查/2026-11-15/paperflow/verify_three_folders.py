"""Verify every original and every final copy before replacing the organized view."""
import concurrent.futures as cf,json,os,pathlib,time
import organize_library as o
from prepare_three_folder_plan import PHASE
from materialize_three_folders import STAGE,OLD

def check(item):
    path,sha=item
    try:
        if o.digest(path)!=sha:return {'path':str(path),'error':'SHA256 mismatch'}
    except Exception as e:return {'path':str(path),'error':str(e)}

def main():
    baseline=json.loads((o.WORK/'source-inventory.json').read_text('utf-8'))
    source_baseline={f['path']:f for f in baseline['files']};records=o.get_records()
    originals={r['source']:r for r in records if 'source_mtime_ns' in r};errors=[]
    if set(source_baseline)!=set(originals):errors.append({'error':'Original baseline coverage mismatch'})
    current={}
    for parent,dirs,names in os.walk(o.SOURCE,followlinks=False):
        for name in names:
            path=pathlib.Path(parent)/name;s=path.stat();current[path.relative_to(o.SOURCE).as_posix()]={'size':s.st_size,'mtime_ns':s.st_mtime_ns}
    missing=sorted(set(source_baseline)-set(current));added=sorted(set(current)-set(source_baseline))
    changed=[rel for rel in set(current)&set(source_baseline) if any(current[rel][k]!=source_baseline[rel][k] for k in ('size','mtime_ns'))]
    archives={a['source']:a for a in json.loads((o.WORK/'archive-inventory.json').read_text('utf-8'))}
    bytask={}
    for r in records:
        if r['role']=='archive':bytask.setdefault(r['task'],set()).add(r.get('container_key',r['source']))
    expected={};folders=set();schools=set();source_disposition=set();body_count=0;groups=0;member_count=0
    with (PHASE/'materialization.jsonl').open(encoding='utf-8') as log:
        for line in log:
            r=json.loads(line);groups+=1;folder=r['folder_path'];folders.add(folder);schools.add(r['school']);root=STAGE/folder
            kids=list(root.iterdir())
            if {x.name for x in kids}!={'正文','附件','垃圾材料'} or not all(x.is_dir() for x in kids):errors.append({'error':'Person root must contain exactly three folders','folder':folder})
            bodyfiles=list((root/'正文').iterdir());body_count+=bool(r['body'])
            if len(bodyfiles)!=int(bool(r['body'])):errors.append({'error':'Body manuscript count mismatch','folder':folder})
            for out in r['outputs']:
                rel=out['path']
                if rel in expected and expected[rel]!=out['sha256']:errors.append({'error':'Conflicting output hashes','path':rel})
                expected[rel]=out['sha256']
                if pathlib.PurePosixPath(rel).parts[:2]!=tuple(pathlib.PurePosixPath(folder).parts) or pathlib.PurePosixPath(rel).parts[2]!=out['category']:errors.append({'error':'Output category path mismatch','path':rel})
            if r['body'] and len(bodyfiles)==1 and expected.get(bodyfiles[0].relative_to(STAGE).as_posix())!=r['body']['sha256']:errors.append({'error':'Selected body identity mismatch','folder':folder})
            source_disposition.update(a['source'] for a in r['actions'])
            archive_expected={(key,m['index']) for key in bytask.get(r['task'],[]) for m in archives[key]['members'] if not m['directory']}
            coverage={(c['container'],c['index']) for c in r['archive_coverage']};member_count+=len(coverage)
            if archive_expected!=coverage:errors.append({'error':'Incomplete archive member coverage','folder':folder})
            for c in r['archive_coverage']:
                if c.get('output') and c['output'] not in expected:errors.append({'error':'Archive member points at missing output','folder':folder,'member':c['index']})
    if source_disposition!=set(source_baseline):errors.append({'error':'Original disposition mismatch','missing':sorted(set(source_baseline)-source_disposition)[:20]})
    if {x.name for x in STAGE.iterdir()}!=schools or not all(x.is_dir() for x in STAGE.iterdir()):errors.append({'error':'Unexpected output root content'})
    actual_folders={x.relative_to(STAGE).as_posix() for school in STAGE.iterdir() for x in school.iterdir()}
    if actual_folders!=folders:errors.append({'error':'School/person directory coverage mismatch'})
    actual={}
    for parent,dirs,names in os.walk(STAGE,followlinks=False):
        for name in names:
            path=pathlib.Path(parent)/name;rel=path.relative_to(STAGE).as_posix();actual[rel]=path.stat().st_size
            if path.stat().st_nlink!=1:errors.append({'error':'Output is hardlinked','path':rel})
    missing_out=sorted(set(expected)-set(actual));extra_out=sorted(set(actual)-set(expected))
    checks=[(o.SOURCE/rel,r['sha256']) for rel,r in originals.items()]+[(STAGE/rel,sha) for rel,sha in expected.items()];start=time.time()
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for i,e in enumerate(ex.map(check,checks),1):
            if e:errors.append(e)
            if i%2500==0:print(json.dumps({'hashes_checked':i,'total':len(checks),'seconds':round(time.time()-start)}),flush=True)
    report={'groups':groups,'strict_three_folders':not errors,'body_ready':body_count,'source_files':len(source_baseline),'source_bytes':sum(x['size'] for x in source_baseline.values()),'source_missing':missing,'source_added':added,'source_metadata_changed':changed,'source_disposition_coverage':len(source_disposition),'archive_members_covered':member_count,'output_data_files':len(expected),'output_bytes':sum(actual.values()),'output_missing':missing_out,'output_unrecorded':extra_out,'hash_checks':len(checks),'errors':errors,'passed':not any((missing,added,changed,missing_out,extra_out,errors))}
    o.dump(PHASE/'verification.json',report);print(json.dumps(report,ensure_ascii=False),flush=True)
    if not report['passed']:raise SystemExit(2)

if __name__=='__main__':main()
