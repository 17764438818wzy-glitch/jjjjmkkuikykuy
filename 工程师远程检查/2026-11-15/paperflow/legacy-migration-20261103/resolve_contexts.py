import copy,json,ntpath,collections
from pathlib import Path
ROOT=Path(__file__).parent
plans=json.loads((ROOT/'package-index.json').read_text())['plans']
updates={};stats=collections.Counter();samples=[]
def folder_key(path):return 'folder:'+ntpath.normcase(ntpath.normpath(path)).replace('\\','/')
with (ROOT/'packages.jsonl').open('rb') as stream:
 for plan in plans:
  stream.seek(plan['offset']);payload=json.loads(stream.read(plan['length']))
  if payload['catalog_records'][0]['folder_name']!='原始文件夹未记录':continue
  package=Path(payload['package_path']);material=package.parent/'material.json';source=None;paper=None
  if material.exists():
   src=json.loads(material.read_text(encoding='utf-8-sig')).get('source') or {}
   if (src.get('source_sha256') or src.get('sha256'))==payload['identity'][1] and src.get('file'):source=src['file']
   revisions=package.parents[2]/'revisions'
   if source and revisions.exists():
    found=[]
    for path in revisions.rglob('inventory.json'):
     value=json.loads(path.read_text(encoding='utf-8-sig'))
     body=value.get('body') or {}
     if body.get('sha256')==payload['identity'][1] and folder_key(body.get('path',''))==folder_key(source):found.append(value)
    if found and len({folder_key(v['person_dir']) for v in found})==1:paper=found[0]
  if source:
   record=copy.deepcopy(payload['catalog_records'][0]);folder=paper['person_dir'] if paper else ntpath.dirname(source)
   record.update(source_key=folder_key(folder),folder_name=ntpath.basename(folder),original_path=source,original_name=ntpath.basename(source))
   for file in record['files']:
    if file.get('sha256')==payload['identity'][1]:file['name']=ntpath.basename(source)
   updates[json.dumps(payload['identity'],separators=(',',':'))]=[record];stats['resolved']+=1
  else:
   stats['still_unresolved']+=1
   if len(samples)<5:
    samples.append({'package':str(package),'parents':[{'path':str(p),'files':[q.name for q in p.iterdir() if q.is_file()][:12]} for p in list(package.parents)[:3]]})
(ROOT/'context-overrides.json').write_text(json.dumps(updates,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'stats':dict(stats),'samples':samples},ensure_ascii=False)[:7000])
