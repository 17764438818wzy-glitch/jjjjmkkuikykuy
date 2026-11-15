"""Read existing catalogs and completed delivery files; never call old RAGFlow."""
import collections
import hashlib
import json
import ntpath
from datetime import datetime,timezone
from pathlib import Path,PureWindowsPath
import sqlite3
import sys

sys.path.insert(0,r'F:\论文预处理系统\看板')

ROOT=Path(__file__).parent
OLD=Path(r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite')
ARCHIVE=Path(r'F:\论文预处理系统\旧预处理工程\ragflow_upstream\data\private\original_archive\archive.sqlite')
NAS=PureWindowsPath(r'\\zwjy-nas\paper_ai\论文原件库')

def source_key(path):
    return 'folder:'+ntpath.normcase(ntpath.normpath(str(path))).replace('\\','/')

def relative(path):
    try: return PureWindowsPath(path).relative_to(NAS).as_posix()
    except ValueError: return ''

def inventory_complete(archives):
    return bool(archives) and all(row.get('status')=='verified' for row in archives)

def profile_from_package(item):
    root=Path(item['path'])
    raw=(root/'manifest.json').read_bytes()
    digest=hashlib.sha256(raw).hexdigest()
    manifest=json.loads(raw.decode('utf-8-sig'))
    ready=json.loads((root/'READY.json').read_text(encoding='utf-8-sig'))
    if digest!=item['manifest_sha256'] or ready.get('manifest_sha256')!=digest: raise ValueError('seal mismatch')
    for key,expected in [('source_sha256',item['source_sha256']),('task_id',item['task'])]:
        if manifest.get(key)!=expected or ready.get(key)!=expected: raise ValueError('profile identity mismatch')
    entry=next((v if isinstance(v,dict) else {'path':v,'sha256':manifest.get('file_hashes',{}).get(v)}
                for v in manifest['files'] if (v.get('path') if isinstance(v,dict) else v)=='profile.json'),None)
    profile=(root/'profile.json').read_bytes()
    if not entry or hashlib.sha256(profile).hexdigest()!=entry['sha256']: raise ValueError('profile bytes changed')
    value=json.loads(profile.decode('utf-8-sig'))
    if value.get('source_sha256',item['source_sha256'])!=item['source_sha256']: raise ValueError('profile source mismatch')
    return value

def catalog_profile(profile):
    from article_repository.profiles import display
    fields=display(profile)
    # The repository stores its selected display fields; sealed originals stay intact.
    return {'summary':fields['summary'],'keywords':fields['keywords'],
        'applicable_audience':fields['audience'],'applicable_levels':fields['level'],
        'research':{'object':fields['object'],'overall_path':fields['path']},
        'learning_units':[{'method':fields['methods'],'learn':fields['learn']}]}

def source_time(value):
    if value is None: return None
    if isinstance(value,(int,float)): return datetime.fromtimestamp(value,timezone.utc).isoformat()
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    return (stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp).astimezone(timezone.utc).isoformat()


def record_from_legacy_package(item):
    root=Path(item['path'])
    receipt=json.loads((root.parent/'delivery-receipt.json').read_text(encoding='utf-8-sig'))
    if (receipt.get('status')!='complete' or receipt.get('stage')!='verified'
            or receipt.get('manifest_sha256')!=item['manifest_sha256']
            or receipt.get('task',{}).get('task_id')!=item['task']
            or receipt.get('task',{}).get('source_sha256')!=item['source_sha256']
            or not receipt.get('targets')
            or not all(t.get('verified') and t.get('retrieval_verified') for t in receipt['targets'].values())):
        raise ValueError('historical publication completion is not confirmed')
    source=json.loads((root.parent/'material.json').read_text(encoding='utf-8-sig')).get('source') or {}
    candidates=[]
    for path in (root.parents[2]/'revisions').rglob('inventory.json'):
        inventory=json.loads(path.read_text(encoding='utf-8-sig'))
        body=inventory.get('body') or {}
        if (inventory.get('schema')=='prefect-paper/1' and body.get('sha256')==item['source_sha256']
                and source_key(body.get('path'))==source_key(source.get('file'))):
            candidates.append((path,inventory))
    if not candidates or len({source_key(p['person_dir']) for _,p in candidates})!=1:
        raise ValueError('historical source folder is not confirmed')
    path,paper=candidates[0]
    archive_path=path.parent/'archive'/'nas-receipt.json'
    archive=json.loads(archive_path.read_text(encoding='utf-8-sig')) if archive_path.exists() else {}
    if archive.get('paper_id')!=paper['paper_id'] or archive.get('revision')!=paper['revision']: archive={}
    profile=profile_from_package(item)
    files=[]
    for row in paper['archive_files']:
        digest=row['sha256']
        known=next((f for f in archive.get('files',[]) if f.get('sha256')==digest and f.get('bytes')==row['bytes']),None)
        location=relative(PureWindowsPath(archive.get('root',''))/'objects'/digest[:2]/digest) if known else ''
        files.append({'id':row['file_id'],'name':PureWindowsPath(row['relative']).name,
            'kind':'原文' if row['file_id']==paper['body']['file_id'] else '附件',
            'sha256':digest,'bytes':row['bytes'],'relative':location,'availability':'unknown'})
    return {'source_key':source_key(paper['person_dir']),'folder_name':ntpath.basename(paper['person_dir']),
        'title':(profile.get('article') or {}).get('title') or ntpath.basename(paper['person_dir']),
        'source':'legacy_saved_packages','profile':catalog_profile(profile),'majors':sorted(receipt['targets']),
        'files':files,'inventory_complete':True,'qdrant':{},'migration':'not_migrated',
        'source_updated_at':source_time(receipt.get('last_remote_verified_at')),'legacy_asset_ids':[]}

def export_old():
    from dashboard.profile import ProfileReader
    conn=sqlite3.connect(OLD.as_uri()+'?mode=ro',uri=True)
    conn.row_factory=sqlite3.Row
    archive=sqlite3.connect(ARCHIVE.as_uri()+'?mode=ro',uri=True); archive.row_factory=sqlite3.Row
    # Direct read avoids any constructor that initializes or writes the old archive.
    archived={r['asset_id']:json.loads(r['payload']) for r in archive.execute('SELECT * FROM archives')}
    assets=conn.execute('SELECT * FROM assets ORDER BY last_observed_at DESC').fetchall()
    package_path=ROOT.parent/'legacy-package-matches.json'
    packages=json.loads(package_path.read_text(encoding='utf-8-sig'))['matched_packages'] if package_path.exists() else []
    package_index={(r['source_sha256'],r['manifest_sha256']):r for r in packages}
    profile_reader=ProfileReader(r'F:\论文预处理系统\旧预处理工程\ragflow_upstream')
    grouped={}
    missing_profile=0
    for saved in assets:
        aid=saved['asset_id']; base=json.loads(saved['payload']); detail=json.loads(saved['detail'])
        routes=[dict(r) for r in conn.execute('SELECT * FROM asset_routes WHERE asset_id=?',(aid,))]
        row={**base,'detail':detail}
        profile=profile_reader.observe(row)
        matched=package_index.get((detail.get('source_hash'),detail.get('package_hash')))
        if matched:
            try: profile={'status':'available',**profile_from_package(matched)}
            except (OSError,ValueError,KeyError): pass
        if profile.get('status')!='available': missing_profile+=1
        sightings=conn.execute('SELECT p.payload,p.detail FROM asset_sightings s JOIN papers p ON p.view_id=s.view_id AND p.row_id=s.row_id WHERE s.asset_id=?',(aid,)).fetchall()
        candidates=[(base,detail)]+[(json.loads(s['payload']),json.loads(s['detail'])) for s in sightings]
        for payload,source in candidates:
            if not payload.get('verified'): continue
            raw=payload.get('source_path') or source.get('input_source_path')
            member=source.get('archive_member_path')
            if not raw: continue
            folder=ntpath.dirname(raw)
            key=source_key(folder)
            if member: key+='|archive-folder:'+ntpath.dirname(member).replace('\\','/').casefold()
            record=grouped.setdefault(key,{'source_key':key,'folder_name':ntpath.basename(folder) or folder,
                'title':base.get('title') or base.get('name') or ntpath.basename(folder),
                'source':'legacy_catalog','profile':catalog_profile(profile) if profile.get('status')=='available' else {},
                'majors':[],'files':[],'inventory_complete':False,'qdrant':{},'migration':'not_migrated','legacy_asset_ids':[]})
            record['majors']=sorted(set(record['majors'])|{r['code'] for r in routes})
            if aid not in record['legacy_asset_ids']: record['legacy_asset_ids'].append(aid)
            saved_archive=archived.get(aid) or {}
            versions=[saved_archive.get('previous_verified') or {},saved_archive]
            for version in versions:
                if version.get('status') not in ('verified','partial'): continue
                for f in version.get('files',[]):
                    digest=f.get('sha256')
                    if not digest or any(r['id']==f['id'] for r in record['files']): continue
                    record['files'].append({'id':f['id'],'name':f['name'],'kind':f.get('kind','附件'),
                        'relative':'objects/'+digest[:2]+'/'+digest,'sha256':digest,'bytes':f.get('bytes'),
                        'availability':'unknown'})
    for record in grouped.values():
        record['inventory_complete']=inventory_complete([archived.get(aid,{}) for aid in record['legacy_asset_ids']])
    extras=[]; skipped=collections.Counter()
    for item in packages:
        try: extras.append(record_from_legacy_package(item))
        except (OSError,ValueError,KeyError,TypeError): skipped['unresolved_saved_package']+=1
    before=len(grouped)
    for record in sorted(extras,key=lambda r:r.get('source_updated_at') or '',reverse=True):
        grouped.setdefault(record['source_key'],record)
    archive.close(); conn.close()
    return list(grouped.values()),{'asset_records':len(assets),'folders':len(grouped),'missing_profiles':missing_profile,
        'extra_historical_folders':len(grouped)-before,'saved_package_skips':dict(skipped)}

def main():
    records,stats=export_old()
    (ROOT/'legacy-records.json').write_text(json.dumps(records,ensure_ascii=False),encoding='utf8')
    stats['folders_with_files']=sum(bool(r['files']) for r in records)
    print(json.dumps(stats,ensure_ascii=False))

if __name__=='__main__': main()
