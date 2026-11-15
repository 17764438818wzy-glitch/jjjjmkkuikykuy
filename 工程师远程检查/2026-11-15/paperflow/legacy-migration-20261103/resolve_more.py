import copy,json,sqlite3,ntpath,collections
from pathlib import Path
ROOT=Path(__file__).parent
plans=json.loads((ROOT/'package-index.json').read_text())['plans']
updates=json.loads((ROOT/'context-overrides.json').read_text())
catalog=json.loads((ROOT/'catalog-contexts.json').read_text())
hashes=collections.defaultdict(dict)
for row in catalog:
 if row.get('sha') and row['source_key'].startswith('folder:'):hashes[row['sha']][row['source_key']]=row
database=Path(r'F:\论文预处理系统\预处理核心与工作台\deployments\workbench-8089-20261018\runtime\state\state.sqlite3')
with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as conn:
 tasks={r[0]:json.loads(r[1]) for r in conn.execute('SELECT task_id,data FROM tasks')}
stats=collections.Counter();unresolved=[]
with (ROOT/'packages.jsonl').open('rb') as stream:
 for plan in plans:
  identity=plan['identity'];key=json.dumps(identity,separators=(',',':'))
  if key in updates:continue
  stream.seek(plan['offset']);payload=json.loads(stream.read(plan['length']))
  if payload['catalog_records'][0]['folder_name']!='原始文件夹未记录':continue
  record=copy.deepcopy(payload['catalog_records'][0]);candidates=hashes.get(identity[1],{})
  if len(candidates)==1:
   context=next(iter(candidates.values()))
   record.update(source_key=context['source_key'],folder_name=context['folder_name'],files=context['files'])
   updates[key]=[record];stats['unique_catalog_context']+=1;continue
  task=tasks.get(identity[0]) or {}
  if task.get('source_sha256')==identity[1] and task.get('source_path'):
   source=task['source_path'];folder=ntpath.dirname(source)
   record.update(source_key='folder:'+ntpath.normcase(ntpath.normpath(folder)).replace('\\','/'),folder_name=ntpath.basename(folder),
                 original_path=source,original_name=ntpath.basename(source))
   for file in record['files']:
    if file.get('sha256')==identity[1]:file['name']=ntpath.basename(source)
   updates[key]=[record];stats['task_source_context']+=1;continue
  unresolved.append({'identity':identity,'package':payload['package_path']})
(ROOT/'context-overrides.json').write_text(json.dumps(updates,ensure_ascii=False),encoding='utf-8')
(ROOT/'unresolved-contexts.json').write_text(json.dumps(unresolved,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'resolved':dict(stats),'remaining':len(unresolved),'samples':unresolved[:3]},ensure_ascii=False))
