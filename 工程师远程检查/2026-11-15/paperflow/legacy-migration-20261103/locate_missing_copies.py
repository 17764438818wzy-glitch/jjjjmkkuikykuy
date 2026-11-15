"""Find byte-identical original copies without changing the recorded source folder identity."""
import collections,hashlib,json,re,sys
from pathlib import Path
ROOT=Path(__file__).parent
OUTPUT=ROOT/(sys.argv[1] if len(sys.argv)>1 else 'remaining-file-plan.json')
assert OUTPUT.resolve().parent==ROOT.resolve()
plan=json.loads(OUTPUT.read_text());pending=plan['unresolved']
by_name=collections.defaultdict(list);by_number=collections.defaultdict(list)
numbers={token for r in pending for token in re.findall(r'\d{9,}',r.get('name') or '')}
scanned=0
for root in (Path(r'C:\Users\Administrator\Desktop\总论文'),Path(r'F:\总论文_整理后_三文件夹版_20261028'),Path(r'C:\Users\Administrator\Desktop\保定理工音乐学'),Path(r'C:\Users\Administrator\Desktop\保定理工体育体育教育')):
 if not root.exists():continue
 for path in root.rglob('*'):
  if not path.is_file():continue
  scanned+=1;by_name[path.name.casefold()].append(path)
  for number in set(re.findall(r'\d{9,}',str(path))):
   if number in numbers:by_number[number].append(path)
hashes={};patches={p['article_id']:p for p in plan['patches']};unresolved=[];found=0
for item in pending:
 name=item.get('name') or '';candidates=by_name.get(name.casefold(),[])+by_name.get(re.sub(r'^\d{3}_','',name).casefold(),[])
 for number in re.findall(r'\d{9,}',name):candidates+=by_number.get(number,[])
 matched=None
 for path in dict.fromkeys(candidates):
  if str(path) not in hashes:
   with path.open('rb') as stream:hashes[str(path)]=hashlib.file_digest(stream,'sha256').hexdigest()
  if hashes[str(path)]==item['sha256']:matched=path;break
 if not matched:unresolved.append(item);continue
 sha=item['sha256'];plan['local_objects'][sha]={'path':str(matched),'bytes':matched.stat().st_size}
 patch=patches.setdefault(item['article_id'],{'article_id':item['article_id'],'source_key':item['source_key'],'files':[]})
 if not any(f.get('sha256')==sha for f in patch['files']):patch['files'].append({'id':'legacy-body-'+sha[:20],
  'name':name,'sha256':sha,'bytes':matched.stat().st_size,'relative':'objects/'+sha[:2]+'/'+sha,'kind':'原文','availability':'unknown'})
 found+=1
plan.update(patches=list(patches.values()),unresolved=unresolved)
OUTPUT.write_text(json.dumps(plan,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'scanned_files':scanned,'hashed_candidates':len(hashes),'additional_originals_found':found,'remaining':len(unresolved)}))
