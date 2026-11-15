"""Find exact SHA originals in the two known intake roots; do not infer folder identity."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).parent
pending=json.loads((ROOT/'empty-original-recovery.json').read_text())['unresolved']
articles={r['id']:r for r in json.loads((ROOT/'final-missing-files.json').read_text())['articles']}
by_sha={r['source_sha256']:[] for r in pending if r.get('source_sha256')}
scanned=0
for root in (Path(r'C:\Users\Administrator\Desktop\保定理工音乐学'),Path(r'C:\Users\Administrator\Desktop\保定理工体育体育教育')):
 if not root.exists():continue
 for path in root.rglob('*'):
  if not path.is_file() or path.suffix.casefold() not in ('.pdf','.doc','.docx','.md'):continue
  with path.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
  scanned+=1
  if sha in by_sha:by_sha[sha].append(path)
patches=[];objects={};unresolved=[]
for row in pending:
 sha=row.get('source_sha256');paths=by_sha.get(sha,[])
 if not paths:unresolved.append(row);continue
 path=paths[0];objects[sha]={'path':str(path),'bytes':path.stat().st_size}
 patches.append({'article_id':row['id'],'source_key':articles[row['id']]['source_key'],'files':[
  {'id':'legacy-body-'+sha[:20],'kind':'原文','name':path.name,'sha256':sha,'bytes':path.stat().st_size,
   'relative':'objects/'+sha[:2]+'/'+sha,'availability':'unknown'}]})
(ROOT/'empty-file-plan.json').write_text(json.dumps({'patches':patches,'local_objects':objects,'unresolved':unresolved},ensure_ascii=False),encoding='utf-8')
print(json.dumps({'files_scanned':scanned,'matched_articles':len(patches),'unresolved_articles':len(unresolved)}))
