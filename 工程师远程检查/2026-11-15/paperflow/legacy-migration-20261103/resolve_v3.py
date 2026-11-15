import copy,hashlib,json,ntpath,collections
from pathlib import Path
ROOT=Path(__file__).parent
pending=json.loads((ROOT/'unresolved-contexts.json').read_text())
updates=json.loads((ROOT/'context-overrides.json').read_text())
plans={tuple(p['identity']):p for p in json.loads((ROOT/'package-index.json').read_text())['plans']}
targets={p['identity'][1] for p in pending};matches=collections.defaultdict(dict);hashed=0
for root in (Path(r'C:\Users\Administrator\Desktop\保定理工音乐学'),Path(r'C:\Users\Administrator\Desktop\保定理工体育体育教育')):
 if not root.exists():continue
 for file in root.rglob('*'):
  if not file.is_file() or file.suffix.lower() not in ('.pdf','.doc','.docx'):continue
  with file.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
  hashed+=1
  if sha in targets:
   folder=file.parent;key='folder:'+ntpath.normcase(ntpath.normpath(str(folder))).replace('\\','/')
   matches[sha][key]=file
unresolved=[];resolved=0
with (ROOT/'packages.jsonl').open('rb') as stream:
 for item in pending:
  identity=item['identity'];contexts=matches.get(identity[1],{})
  if not contexts:unresolved.append(item);continue
  p=plans[tuple(identity)];stream.seek(p['offset']);payload=json.loads(stream.read(p['length']));records=[]
  for key,file in contexts.items():
   record=copy.deepcopy(payload['catalog_records'][0]);record.update(source_key=key,folder_name=file.parent.name,original_path=str(file),original_name=file.name)
   for f in record['files']:
    if f.get('sha256')==identity[1]:f.update(name=file.name,bytes=file.stat().st_size)
   records.append(record)
  updates[json.dumps(identity,separators=(',',':'))]=records;resolved+=1
(ROOT/'context-overrides.json').write_text(json.dumps(updates,ensure_ascii=False),encoding='utf-8')
(ROOT/'unresolved-contexts.json').write_text(json.dumps(unresolved,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'hashed_originals':hashed,'resolved_groups':resolved,'remaining':len(unresolved)}))
