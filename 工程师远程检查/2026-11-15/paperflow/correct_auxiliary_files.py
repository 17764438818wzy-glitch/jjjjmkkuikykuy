"""Replace only newly organized copies affected by explicit non-Word process files."""
import concurrent.futures as cf
import json,pathlib,shutil,time
from collections import defaultdict
import organize_library as o


def inside(path,root):
    if not path.resolve().is_relative_to(root.resolve()):raise ValueError('Path escaped intended root: '+str(path))
    if path.resolve().is_relative_to(o.SOURCE.resolve()):raise ValueError('Source is read-only')
    return path


if __name__=='__main__':
    final=o.OUTPUT
    if not (o.WORK/'summary.json').exists():raise RuntimeError('Full copy must finish before corrections')
    with (o.WORK/'materialization.jsonl').open(encoding='utf-8') as f:old=[json.loads(line) for line in f if line.strip()]
    records=o.get_records();groups=defaultdict(list);affected=set();reclassified=[]
    for r in records:
        why=o.non_document_exclusion(r['name'])
        if why and r['role']=='attachment':
            reclassified.append({'source':r['source'],'member':r.get('member'),'old_role':r['role'],'reason':why})
            r['role']='exclude';r['reason']=why;affected.add(r['task'])
        groups[r['task']].append(r)
    o.ARCHIVES={a['source']:a for a in json.loads((o.WORK/'archive-inventory.json').read_text('utf-8'))}
    for a in o.ARCHIVES.values():
        if any(o.non_document_exclusion(m['name']) for m in a.get('members',[]) if not m['directory']):
            affected.add(o.task_of(a.get('root_source',a['source'])))
    for r in old:
        if r['notes']:affected.add(r['task'])
    inventory_changes=o.WORK/'archive-directory-corrections.json'
    if inventory_changes.exists():
        for a in json.loads(inventory_changes.read_text('utf-8')):
            affected.add(o.task_of(a.get('root_source',a['source'])))
    decisions=json.loads((o.WORK/'decisions.json').read_text('utf-8'))
    chosen=[d for d in decisions if d['task'] in affected]
    assert len(chosen)==len(affected)
    stage=o.WORK/'非正文附件修正';backup=o.WORK/'修正前副本'
    if stage.exists() or backup.exists():raise FileExistsError('Correction state already exists; inspect before resuming')
    o.OUTPUT=stage;new=[]
    print(json.dumps({'affected_groups':len(chosen),'reclassified_originals':len(reclassified),'staging':str(stage)},ensure_ascii=False),flush=True)
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        for i,r in enumerate(ex.map(o.materialize_task,[(d,groups[d['task']]) for d in chosen]),1):
            for item in r['outputs']:
                if o.digest(stage/item['path'])!=item['sha256']:raise RuntimeError('Staging readback mismatch')
                if item.get('generated'):
                    if any(o.non_document_exclusion(m['name']) for m in item['members']):raise RuntimeError('Excluded process member remains in attachment package')
            new.append(r)
            if i%50==0:print(json.dumps({'corrected_staged':i,'total':len(chosen)}),flush=True)
    o.dump(o.WORK/'auxiliary-correction-staged.json',{'reclassified':reclassified,'results':new})
    old_by={r['task']:r for r in old}
    promotions=[]
    with (o.WORK/'correction-promotions.jsonl').open('x',encoding='utf-8') as journal:
        for r in new:
            previous=old_by[r['task']]
            if previous.get('folder_path')!=r.get('folder_path'):raise RuntimeError('Unexpected classification folder change')
            if not r.get('folder_path'):continue
            relative=pathlib.PurePosixPath(r['folder_path'])
            current=inside(final/relative,final);staged=inside(stage/relative,stage);saved=inside(backup/relative,backup)
            if not current.is_dir() or not staged.is_dir() or saved.exists():raise RuntimeError('Unexpected promotion paths')
            saved.parent.mkdir(parents=True,exist_ok=True)
            current.rename(saved)
            try:staged.rename(current)
            except Exception:
                saved.rename(current);raise
            promotion={'task':r['task'],'previous_copy':str(saved),'updated_copy':str(current)}
            journal.write(json.dumps(promotion,ensure_ascii=False)+'\n');journal.flush();promotions.append(promotion)
    new_by={r['task']:r for r in new};results=[new_by.get(r['task'],r) for r in old]
    shutil.copyfile(o.WORK/'materialization.jsonl',o.WORK/'materialization.before-auxiliary-correction.jsonl')
    for target,rows in [(o.WORK/'inspection.jsonl',records),(o.WORK/'materialization.jsonl',results)]:
        temporary=target.with_name(target.name+'.corrected')
        with temporary.open('x',encoding='utf-8') as f:
            for row in rows:f.write(json.dumps(row,ensure_ascii=False)+'\n')
        temporary.replace(target)
    shutil.copyfile(o.WORK/'materialization.jsonl',final/'_整理记录'/'整理映射.jsonl')
    shutil.copyfile(o.WORK/'archive-inventory.json',final/'_整理记录'/'压缩包内部清单.json')
    summary=json.loads((o.WORK/'summary.json').read_text('utf-8'))
    summary.update(outputs=sum(len(r['outputs']) for r in results),output_bytes=sum(x['size'] for r in results for x in r['outputs']),notes=sum(len(r['notes']) for r in results),auxiliary_corrected_groups=len(new),verification='pending')
    o.dump(o.WORK/'summary.json',summary);o.dump(final/'_整理记录'/'整理统计.json',summary)
    report={'affected_groups':len(new),'reclassified':reclassified,'promotions':promotions,'source_written':False,'all_staged_outputs_hash_verified':True}
    o.dump(o.WORK/'auxiliary-correction.json',report);o.dump(final/'_整理记录'/'非正文附件修正.json',report)
    print(json.dumps({'corrected_groups':len(new),'notes':summary['notes'],'complete':True},ensure_ascii=False),flush=True)
