"""Fill missing article files from verified local originals, preserving NAS content addresses."""
import hashlib,json,os,sqlite3,sys,uuid
from pathlib import Path,PureWindowsPath
import smbclient
ROOT=Path(__file__).parent
sys.path.insert(0,str(ROOT.parent/'article-repository'))
from article_repository.files import Files
articles=json.loads((ROOT/'repository-missing-files.json').read_text())['articles']
plan=json.loads((ROOT/'file-link-plan.json').read_text());patches={p['article_id']:p for p in plan['patches']}
db=Path(r'F:\论文预处理系统\看板运行数据\runtime\query.sqlite')
with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
 assets={aid:(json.loads(p),json.loads(d)) for aid,p,d in conn.execute('SELECT asset_id,payload,detail FROM assets')}
files=Files(r'\\192.168.1.67\paper_ai\论文原件库',username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
copied=0;unresolved=[]
def publish(source,sha):
 global copied
 row={'relative':'objects/'+sha[:2]+'/'+sha};target=files.path(row)
 try:
  with files.open(target,'rb') as stream:
   if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('Existing NAS object hash mismatch')
  return row['relative']
 except FileNotFoundError:pass
 smbclient.makedirs(str(PureWindowsPath(target).parent),exist_ok=True)
 temp=target+'.migration-'+uuid.uuid4().hex
 try:
  with source.open('rb') as incoming,files.open(temp,'wb') as output:
   while chunk:=incoming.read(256*1024):output.write(chunk)
  with files.open(temp,'rb') as stream:
   if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('NAS readback hash mismatch')
  try:smbclient.rename(temp,target)
  except FileExistsError:
   with files.open(target,'rb') as stream:
    if hashlib.file_digest(stream,'sha256').hexdigest()!=sha:raise ValueError('Concurrent NAS object hash mismatch')
  copied+=1
 finally:
  try:smbclient.remove(temp)
  except OSError as exc:
   if exc.errno not in (2,20):raise
 return row['relative']
for article in articles:
 base=article['base'];source=next((assets[a] for a in base.get('legacy_asset_ids',[]) if a in assets),None)
 if not source:continue
 payload,detail=source;original=Path(payload.get('source_path') or detail.get('input_source_path') or '')
 if not original.is_file():unresolved.append(article['id']);continue
 expected=detail.get('source_hash') or (detail.get('task') or {}).get('source_sha256')
 with original.open('rb') as stream:actual=hashlib.file_digest(stream,'sha256').hexdigest()
 if not expected or actual!=expected:raise ValueError('Local original differs from saved article identity')
 if detail.get('archive_member_path'):raise ValueError('Archive-member source needs explicit member handling')
 patch=patches.setdefault(article['id'],{'article_id':article['id'],'source_key':article['source_key'],'files':[]})
 known={f.get('sha256') for f in patch['files']}
 for path in original.parent.rglob('*'):
  if not path.is_file() or path.name.startswith('~$'):continue
  with path.open('rb') as stream:sha=hashlib.file_digest(stream,'sha256').hexdigest()
  relative=publish(path,sha)
  if sha not in known:
   patch['files'].append({'id':'legacy-body-'+sha[:20] if path==original else 'legacy-file-'+sha[:20],
    'name':str(path.relative_to(original.parent)),'sha256':sha,'bytes':path.stat().st_size,'relative':relative,
    'kind':'原文' if path==original else '附件','availability':'available'});known.add(sha)
 patch['inventory_complete']=True
(ROOT/'file-link-plan.json').write_text(json.dumps({'patches':list(patches.values()),'unresolved_article_ids':unresolved},ensure_ascii=False),encoding='utf-8')
print(json.dumps({'linked_articles':len(patches),'files':sum(len(p['files']) for p in patches.values()),'new_NAS_objects':copied,'unresolved_articles':len(unresolved)}))
