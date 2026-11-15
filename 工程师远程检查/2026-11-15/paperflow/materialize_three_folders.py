"""Rebuild a separate, verified copy with exactly three folders per person."""
import concurrent.futures as cf,hashlib,json,pathlib,shutil,time
from collections import Counter,defaultdict
import organize_library as o
from prepare_three_folder_plan import PHASE

OLD=o.OUTPUT
STAGE=PHASE/'整理结果'
ARCHIVES={}

def package_key(source,members):
    return (source,tuple(sorted((m['name'].replace('\\','/'),m['size'],(m.get('crc') or '').lower()) for m in members)))

def materialize(item):
    decision,records,previous=item
    root=STAGE/decision['school']/decision['folder']
    if any(pathlib.Path(decision[k]).name!=decision[k] or decision[k] in {'.','..',''} for k in ('school','folder')):raise ValueError('Invalid person path components')
    resolved_root=root.resolve();resolved_stage=STAGE.resolve();resolved_source=o.SOURCE.resolve()
    if not resolved_root.is_relative_to(resolved_stage) or resolved_root.is_relative_to(resolved_source):
        raise ValueError('Person directory escapes staging root: '+json.dumps({'root':str(resolved_root),'stage':str(resolved_stage),'source':str(resolved_source)},ensure_ascii=False))
    for category in ('正文','附件','垃圾材料'):(root/category).mkdir(parents=True,exist_ok=True)
    routes=decision['routes'];outputs=[];actions=[];copied={};coverage=[]
    def route(r):
        return routes.get(r['sha256'],{'category':'垃圾材料' if r['role']=='exclude' else '附件','reason':r['reason']})
    def identity(r):return {k:r[k] for k in ('source','member','sha256','container_key','member_index') if k in r}
    def copy(r,category=None,reason=None):
        c=category or route(r)['category'];sha=r['sha256']
        if sha in copied:
            out=copied[sha];actions.append(dict(identity(r),category=out['category'],reason='相同字节已有副本',output=out['path']))
            return out
        name=r['name']
        if r.get('format') in {'.doc','.docx','.pdf','.rtf'}:name=pathlib.Path(name).stem+r['format']
        if c=='正文':name='论文'+r.get('format',pathlib.Path(name).suffix)
        dest=o.unique_path(root/c,name,sha)
        out=o.copy_verified(pathlib.Path(r['local_path']),dest,sha);out.update(category=c,source=r['source'],member=r.get('member'))
        outputs.append(out);copied[sha]=out
        actions.append(dict(identity(r),category=c,reason=reason or route(r)['reason'],output=out['path']))
        return out
    body=decision.get('body')
    if body:
        selected=next(r for r in records if r['sha256']==body['sha256'] and r['source']==body['source'] and r.get('member')==body.get('member'))
        copy(selected,'正文',decision['selection_reason'])
    ars={r.get('container_key',r['source']):r for r in records if r['role']=='archive'}
    children=defaultdict(dict)
    for ar in ars.values():
        if ar.get('parent_container'):children[ar['parent_container']][ar['parent_member_index']]=ar
    docs=defaultdict(dict)
    for r in records:
        if r['kind'] in {'member','nested_member'}:docs[r.get('container_key',r['source'])][r['member_index']]=r
    pure_cache={}
    def mcategory(key,m):
        idx=m['index']
        if idx in docs[key]:return route(docs[key][idx])['category']
        if idx in children[key]:return pure(children[key][idx].get('container_key',children[key][idx]['source']))
        return '垃圾材料' if o.special(m['name']) or o.non_document_exclusion(m['name']) else '附件'
    def pure(key):
        if key not in pure_cache:
            cats={mcategory(key,m) for m in ARCHIVES[key]['members'] if not m['directory']}
            pure_cache[key]=next(iter(cats)) if len(cats)==1 and next(iter(cats)) in {'附件','垃圾材料'} else ('附件' if not cats else 'mixed')
        return pure_cache[key]
    generated=defaultdict(list)
    for x in previous.get('outputs',[]):
        if x.get('generated'):generated[package_key(x['source'],x['members'])].append(x)
    def package(ar,key,members,category):
        if not members:return None
        matches=generated.get(package_key(ar['source'],members),[])
        reusable=matches[0] if category=='附件' and len(matches)==1 and not ar.get('nested') and not children[key] else None
        name=o.safe_name(pathlib.Path(ar['name']).stem,75)+'_'+category+'.zip'
        dest=root/category/name
        if reusable:
            dest=o.unique_path(root/category,name,reusable['sha256'])
            out=o.copy_verified(OLD/reusable['path'],dest,reusable['sha256'])
            out.update(generated=True,members=reusable['members'],reused_verified_package=True)
        else:
            # The container suffix is deterministic, including after interruption.
            dest=root/category/(pathlib.Path(name).stem+'__'+hashlib.sha256(key.encode()).hexdigest()[:10]+'.zip')
            cache=PHASE/'package-cache'/(hashlib.sha256(str(dest).encode()).hexdigest()+'.json')
            signature=hashlib.sha256(json.dumps(package_key(ar['source'],members),ensure_ascii=False).encode()).hexdigest()
            if dest.exists():
                if not cache.exists():raise RuntimeError('Existing package has no completion receipt: '+str(dest))
                receipt=json.loads(cache.read_text('utf-8'));out=receipt['output']
                if receipt['member_signature']!=signature or o.digest(dest)!=out['sha256']:raise RuntimeError('Existing package receipt mismatch')
            else:
                out=o.filtered_package(pathlib.Path(ar['local_path']),members,dest,ARCHIVES[key])
                o.dump(cache,{'member_signature':signature,'output':out})
        out.update(category=category,source=ar['source'],container_key=key);outputs.append(out)
        return out
    def represent_whole(key,out):
        ar=ars[key]
        for m in ARCHIVES[key]['members']:
            if m['directory']:continue
            coverage.append({'container':key,'index':m['index'],'name':m['name'],'output':out['path'],'mode':'inside_original_archive'})
            if m['index'] in docs[key]:
                r=docs[key][m['index']];actions.append(dict(identity(r),category=out['category'],reason='完整附件或过程材料压缩包中的文件',output=out['path']))
            if m['index'] in children[key]:
                child=children[key][m['index']];represent_whole(child.get('container_key',child['source']),out)
    def archive(key):
        ar=ars[key];cat=pure(key)
        if cat!='mixed':
            out=copy(ar,cat,'包内文件均属于'+cat+'，保留原压缩包')
            represent_whole(key,out);return
        a=ARCHIVES[key];members=[m for m in a['members'] if not m['directory']]
        keep=[];trash=[];separate=[];nested=[]
        for m in members:
            idx=m['index']
            if idx in children[key]:nested.append(m);continue
            if idx in docs[key]:
                r=docs[key][idx]
                # Preserve and reuse the already verified attachment package where possible.
                if r['role']=='attachment' and route(r)['category']=='附件':keep.append(m)
                else:separate.append(m)
            elif mcategory(key,m)=='垃圾材料':trash.append(m)
            else:keep.append(m)
        for ms,c in ((keep,'附件'),(trash,'垃圾材料')):
            out=package(ar,key,ms,c)
            if out:
                for m in ms:coverage.append({'container':key,'index':m['index'],'name':m['name'],'output':out['path'],'mode':'filtered_archive'})
        for m in separate:
            out=copy(docs[key][m['index']]);coverage.append({'container':key,'index':m['index'],'name':m['name'],'output':out['path'],'mode':'file_or_exact_copy'})
        for m in nested:
            child=children[key][m['index']];ck=child.get('container_key',child['source']);archive(ck)
            coverage.append({'container':key,'index':m['index'],'name':m['name'],'child_container':ck,'mode':'nested_archive_processed'})
        actions.append(dict(identity(ar),category='已分流',reason='混合压缩包所有文件已分别进入三个目录；原包留在原始总论文中',container_key=key))
    for r in records:
        if r['kind'] not in {'member','nested_member'} and r['role']!='archive':copy(r)
    for key,ar in ars.items():
        if not ar.get('parent_container'):archive(key)
    expected={(k,m['index']) for k in ars for m in ARCHIVES[k]['members'] if not m['directory']}
    actual={(c['container'],c['index']) for c in coverage}
    if expected!=actual:raise RuntimeError('Archive coverage mismatch '+decision['task'])
    result={k:v for k,v in decision.items() if k!='routes'}
    result.update(folder_path=root.relative_to(STAGE).as_posix(),outputs=outputs,actions=actions,archive_coverage=coverage,notes=[])
    return result

def main():
    global ARCHIVES
    plan=PHASE/'all-decisions.json';plan_sha=o.digest(plan)
    stamp=PHASE/'stage-plan-sha256.txt'
    if stamp.exists() and stamp.read_text()!=plan_sha:raise RuntimeError('Plan changed after stage creation')
    if not stamp.exists():stamp.write_text(plan_sha)
    if shutil.disk_usage(PHASE).free<130*2**30:raise RuntimeError('Insufficient free space reserve')
    if STAGE.resolve().is_relative_to(o.SOURCE.resolve()):raise RuntimeError('Stage cannot be under originals')
    o.OUTPUT=STAGE;STAGE.mkdir(exist_ok=True)
    ARCHIVES={a['source']:a for a in json.loads((o.WORK/'archive-inventory.json').read_text('utf-8'))}
    ds=json.loads(plan.read_text('utf-8'));groups=defaultdict(list)
    for r in o.get_records():groups[r['task']].append(r)
    previous={r['task']:r for r in (json.loads(line) for line in (o.WORK/'materialization.jsonl').open(encoding='utf-8'))}
    results=[];start=time.time();logpath=PHASE/'materialization.jsonl'
    with logpath.open('w',encoding='utf-8') as log,cf.ThreadPoolExecutor(max_workers=6) as ex:
        for i,r in enumerate(ex.map(materialize,[(d,groups[d['task']],previous[d['task']]) for d in ds]),1):
            results.append(r);log.write(json.dumps(r,ensure_ascii=False)+'\n');log.flush()
            if i%500==0:print(json.dumps({'organized':i,'total':len(ds),'seconds':round(time.time()-start)}),flush=True)
    counts=Counter(x['category'] for r in results for x in r['outputs'])
    summary={'groups':len(results),'body_ready':sum(bool(r['body']) for r in results),'without_body':sum(not r['body'] for r in results),'outputs':dict(counts),'output_bytes':sum(x['size'] for r in results for x in r['outputs']),'stage':str(STAGE),'final_output':str(OLD),'verification':'pending'}
    o.dump(PHASE/'summary.json',summary);print(json.dumps(summary,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
