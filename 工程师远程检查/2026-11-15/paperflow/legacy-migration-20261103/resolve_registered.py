import copy,json,ntpath,collections
from pathlib import Path
import httpx
ROOT=Path(__file__).parent
pending=json.loads((ROOT/'unresolved-contexts.json').read_text());updates=json.loads((ROOT/'context-overrides.json').read_text())
plans={tuple(p['identity']):p for p in json.loads((ROOT/'package-index.json').read_text())['plans']}
group_to_item={Path(i['package']).parents[3].name:i for i in pending}
parents=sorted({Path(i['package']).parents[4].name for i in pending})
# publication/attempt/body/fingerprint/postprocessing/group/task: the exact group is parent[4].
group_to_item={Path(i['package']).parents[4].name:i for i in pending}
parents=sorted({Path(i['package']).parents[5].name for i in pending})
found={};scanned=0
with httpx.Client(base_url='http://192.168.1.120:4200/api',trust_env=False,timeout=30) as client:
 for offset in range(0,6000,200):
  response=client.post('/flow_runs/filter',json={'flows':{'name':{'any_':['paperflow-v3-paper']}},
   'flow_runs':{'parent_flow_run_id':{'any_':parents}},'limit':200,'offset':offset,'sort':'EXPECTED_START_TIME_DESC'})
  response.raise_for_status();runs=response.json();scanned+=len(runs)
  for run in runs:
   reg=run.get('parameters',{}).get('registered') or {};group=reg.get('group_id');paper=reg.get('paper') or {}
   if group in group_to_item and (paper.get('body') or {}).get('sha256')==group_to_item[group]['identity'][1]:found[group]=reg
  if len(found)==len(group_to_item) or len(runs)<200:break
with (ROOT/'packages.jsonl').open('rb') as stream:
 for group,reg in found.items():
  item=group_to_item[group];plan=plans[tuple(item['identity'])];stream.seek(plan['offset']);payload=json.loads(stream.read(plan['length']))
  paper=reg['paper'];record=copy.deepcopy(payload['catalog_records'][0]);folder=paper['person_dir']
  record.update(source_key='folder:'+ntpath.normcase(ntpath.normpath(folder)).replace('\\','/'),folder_name=ntpath.basename(folder),
    original_path=paper['body']['path'],original_name=ntpath.basename(paper['body']['path']))
  record['files']=[{'id':f['file_id'],'name':ntpath.basename(f['relative']),'sha256':f['sha256'],'bytes':f['bytes'],
      'relative':'Prefect五步/统一五步正式/'+reg['task_id']+'/'+group+'/'+f['relative'].replace('\\','/'),
      'availability':'unknown','kind':'原文' if f['file_id']==paper['body']['file_id'] else '附件'} for f in paper['archive_files']]
  updates[json.dumps(item['identity'],separators=(',',':'))]=[record]
(ROOT/'context-overrides.json').write_text(json.dumps(updates,ensure_ascii=False),encoding='utf-8')
(ROOT/'registered-contexts.json').write_text(json.dumps(found,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'scanned_registered_runs':scanned,'resolved':len(found),'remaining':len(group_to_item)-len(found)}))
