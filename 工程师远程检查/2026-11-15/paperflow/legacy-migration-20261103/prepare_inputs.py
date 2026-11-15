"""Export verified sealed inputs once; preserve source folder evidence and original bytes."""
import collections
import copy
import hashlib
import json
import ntpath
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).parent
sys.path.insert(0,r'F:\论文预处理系统\程序\updates\unified-prefect-20261029-material-r02\core')
sys.path.insert(0,str(ROOT.parent/'article-repository'))
from paperflow.package import verify_delivery_identity,inside
from export_existing import source_key,catalog_profile,source_time


def context(payload,detail):
    raw=payload.get('source_path') or detail.get('input_source_path')
    if not raw:return None
    folder=ntpath.dirname(raw);key=source_key(folder)
    member=detail.get('archive_member_path')
    if member:key+='|archive-folder:'+ntpath.dirname(member).replace('\\','/').casefold()
    return {'source_key':key,'folder_name':ntpath.basename(folder) or folder,'original_path':raw,
            'original_name':ntpath.basename(member or raw)}


def main():
    snapshot=json.loads((ROOT/'source-inventory.json').read_text())
    majors=json.loads((ROOT.parent/'article-repository/majors.json').read_text())['majors']
    known={m[k] for m in majors for k in ('paper_dataset_id','image_dataset_id')}
    documents=[d for d in snapshot['documents'] if d['kb_id'] in known]
    groups=collections.defaultdict(list)
    for doc in documents:
        meta=doc['meta'];key=tuple(meta.get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'))
        groups[key].append(doc)
    old_records=json.loads((ROOT.parent/'article-repository/legacy-records.json').read_text())
    existing={r['source_key']:r for r in old_records}
    index=collections.defaultdict(dict);task_index=collections.defaultdict(dict);hash_index=collections.defaultdict(dict)
    databases=[Path(p) for p in (r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite',
        r'F:\论文预处理系统\工作间\新项目-20261027-7958216247\runtime\query.sqlite',
        r'F:\论文预处理系统\工作间\新稿小测-20261028\runtime\query.sqlite',
        r'F:\论文预处理系统\看板新运行数据-20261027\query.sqlite')]
    for db in databases:
        if not db.exists():continue
        with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
            tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table in ('assets','papers'):
                if table not in tables:continue
                for payload_raw,detail_raw in conn.execute('SELECT payload,detail FROM '+table):
                    payload=json.loads(payload_raw);detail=json.loads(detail_raw)
                    item=context(payload,detail)
                    key=(detail.get('source_hash'),detail.get('package_hash'))
                    task=detail.get('task') or {}
                    if item and all(key):index[key][item['source_key']]=item
                    if item and task.get('task_id') and task.get('source_sha256'):
                        task_index[(task['task_id'],task['source_sha256'])][item['source_key']]=item
                    if item and detail.get('source_hash'):hash_index[detail['source_hash']][item['source_key']]=item
    matches=json.loads((ROOT.parent/'legacy-package-matches.json').read_text())['matched_packages']
    packages=[p for p in matches if (p['task'],p['source_sha256'],p['manifest_sha256']) in groups]
    objects={};plans=[];stats=collections.Counter();run_cache={};rejected=[]
    with (ROOT/'packages.jsonl').open('wb') as stream:
        for pos,item in enumerate(packages):
            key=(item['task'],item['source_sha256'],item['manifest_sha256']);package=Path(item['path'])
            try:
                manifest,digest,_,_=verify_delivery_identity(package,{'task_id':item['task'],'source_sha256':item['source_sha256']})
                if digest!=item['manifest_sha256']:raise ValueError('saved manifest changed')
                profile,tree,assets,chunks=[json.loads((package/name).read_text(encoding='utf-8-sig')) for name in ('profile.json','tree.json','assets.json','chunks.json')]
                image_rows=assets.get('items',[]) if isinstance(assets,dict) else assets
                for asset in image_rows:
                    if not asset.get('file') or not asset.get('sha256'):continue
                    file=inside(package,asset['file'])
                    if file.is_file():objects.setdefault(asset['sha256'],str(file))
                sources=dict(index.get((item['source_sha256'],item['manifest_sha256']),{}))
                if not sources:sources.update(task_index.get((item['task'],item['source_sha256']),{}))
                run_manifest=package.parents[1]/'manifest.json'
                if not sources and run_manifest.exists():
                    path=str(run_manifest)
                    if path not in run_cache:
                        raw=json.loads(run_manifest.read_text(encoding='utf-8-sig'));mapped=collections.defaultdict(dict)
                        for row in raw.get('items',[]):
                            if not isinstance(row,dict):continue
                            source=context(row,row)
                            if source and row.get('source_hash'):mapped[row['source_hash']][source['source_key']]=source
                        run_cache[path]=mapped
                    sources.update(run_cache[path].get(item['source_sha256'],{}))
                material_path=package.parent/'material.json'
                if not sources and material_path.exists():
                    source=json.loads(material_path.read_text(encoding='utf-8-sig')).get('source') or {}
                    if source.get('sha256')==item['source_sha256'] and source.get('file'):
                        candidate=context({'source_path':source['file']},{})
                        sources[candidate['source_key']]=candidate
                # This resolves a path, never the row ID: known distinct folders stay distinct.
                unique=hash_index.get(item['source_sha256'],{})
                if not sources and len(unique)==1:
                    sources.update(unique);stats['unique_catalog_source_resolution']+=1
                if not sources:
                    unresolved='legacy-unresolved:'+snapshot['tenant']+':'+item['task']+':'+item['manifest_sha256']
                    sources[unresolved]={'source_key':unresolved,'folder_name':'原始文件夹未记录','original_name':'论文原文','original_path':''}
                    stats['unresolved_source_folders']+=1
                records=[]
                for source in sources.values():
                    record=copy.deepcopy(existing.get(source['source_key'],{}))
                    record.update(source)
                    record.update(title=(profile.get('article') or {}).get('title') or record.get('title') or source['folder_name'],
                                  profile=catalog_profile(profile),source_sha256=item['source_sha256'],source='legacy_ragflow_migration')
                    record.setdefault('inventory_complete',False);record.setdefault('files',[])
                    if not any(f.get('sha256')==item['source_sha256'] for f in record['files']):
                        record['files'].insert(0,{'id':'legacy-body-'+item['source_sha256'][:20],'name':source['original_name'],
                            'kind':'原文','sha256':item['source_sha256'],'bytes':None,'relative':'objects/'+item['source_sha256'][:2]+'/'+item['source_sha256'],'availability':'unknown'})
                    records.append(record)
                payload={'identity':list(key),'original':(package/'article.redacted.md').read_text(encoding='utf-8-sig'),
                         'profile':profile,'tree':tree,'assets':image_rows,'chunks':chunks.get('chunks',[]),
                         'catalog_records':records,'package_path':str(package)}
                raw=(json.dumps(payload,ensure_ascii=False,separators=(',',':'))+'\n').encode()
                offset=stream.tell();stream.write(raw)
                plans.append({'identity':list(key),'offset':offset,'length':len(raw),'document_ids':[d['id'] for d in groups[key]]})
                stats['verified_packages']+=1;stats['source_contexts']+=len(records)
            except (OSError,ValueError,KeyError,TypeError) as exc:
                stats['rejected_'+type(exc).__name__]+=1
                rejected.append({'identity':list(key),'error':type(exc).__name__,'message':str(exc)[:800]})
            if (pos+1)%500==0:print(json.dumps({'prepared':pos+1,'total':len(packages),'verified':stats['verified_packages']}),flush=True)
    (ROOT/'package-index.json').write_text(json.dumps({'plans':plans,'stats':dict(stats),'rejected':rejected},ensure_ascii=False),encoding='utf-8')
    (ROOT/'image-sources.json').write_text(json.dumps(objects,ensure_ascii=False),encoding='utf-8')
    stats['unique_images']=len(objects);stats['input_bytes']=(ROOT/'packages.jsonl').stat().st_size
    print(json.dumps(dict(stats),ensure_ascii=False),flush=True)


if __name__=='__main__':main()
