"""Resolve remaining downloadable originals against recorded paths and known workspace aliases."""
import hashlib,json,ntpath,os,sys
from pathlib import Path
ROOT=Path(__file__).parent
OUTPUT=ROOT/(sys.argv[1] if len(sys.argv)>1 else 'remaining-file-plan.json')
assert OUTPUT.resolve().parent==ROOT.resolve()
sys.path.insert(0,str(ROOT.parent/'article-repository'))
from article_repository.files import Files
rows=json.loads((ROOT/'current-missing-files.json').read_text())['articles']
files=Files(r'\\192.168.1.67\paper_ai\论文原件库',username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
patches=[];local_objects={};unresolved=[]
aliases=[('E:\\rag mid\\',r'F:\论文预处理系统\预处理核心与工作台'+'\\'),('E:\\uv\\',r'F:\论文预处理系统\旧预处理工程'+'\\')]
for article in rows:
 base=article['base'];changed=[]
 for file in base.get('files',[]):
  if file.get('availability')=='available' or not file.get('sha256'):continue
  sha=file['sha256'];row={**file,'relative':'objects/'+sha[:2]+'/'+sha}
  try:
   path=files.path(row);info=files.stat(path)
   with files.open(path,'rb') as stream:
    if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('NAS object hash mismatch')
   row.update(bytes=info.st_size,availability='available');changed.append(row);continue
  except (OSError,ValueError):pass
  candidates=[]
  original=base.get('original_path')
  if original:candidates.append(original)
  if article['source_key'].startswith('folder:') and '|archive-folder:' not in article['source_key']:
   candidates.append(ntpath.join(article['source_key'][7:],file.get('name') or ''))
  for candidate in list(candidates):
   for old,new in aliases:
    if candidate.casefold().startswith(old.casefold()):candidates.append(new+candidate[len(old):])
  found=None
  for candidate in dict.fromkeys(candidates):
   path=Path(candidate)
   if not path.is_file():continue
   with path.open('rb') as stream:actual=hashlib.file_digest(stream,'sha256').hexdigest()
   if actual==sha:found=path;break
  if found:local_objects.setdefault(sha,{'path':str(found),'bytes':found.stat().st_size});row['bytes']=found.stat().st_size;changed.append(row)
  else:unresolved.append({'article_id':article['id'],'name':file.get('name'),'sha256':sha,'source_key':article['source_key']})
 if changed:patches.append({'article_id':article['id'],'source_key':article['source_key'],'files':changed})
OUTPUT.write_text(json.dumps({'patches':patches,'local_objects':local_objects,'unresolved':unresolved},ensure_ascii=False),encoding='utf-8')
print(json.dumps({'articles':len(patches),'files':sum(len(p['files']) for p in patches),'local_objects':len(local_objects),'unresolved':len(unresolved)}))
