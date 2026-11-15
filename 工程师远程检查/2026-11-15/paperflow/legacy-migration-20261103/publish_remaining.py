import json,os,sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
ROOT=Path(__file__).parent
sys.path.insert(0,str(ROOT.parent/'article-repository'))
from article_repository.files import Files
from nas_transfer import publish_file
OUTPUT=ROOT/(sys.argv[1] if len(sys.argv)>1 else 'remaining-file-plan.json')
assert OUTPUT.resolve().parent==ROOT.resolve()
plan=json.loads(OUTPUT.read_text())
files=Files(r'\\192.168.1.67\paper_ai\论文原件库',username=os.environ['ARTICLE_NAS_USER'],password=os.environ['ARTICLE_NAS_PASSWORD'])
files.stat(files.root)
needed={file['sha256'] for patch in plan['patches'] for file in patch['files'] if file.get('availability')!='available'}
objects={sha:row for sha,row in plan['local_objects'].items() if sha in needed}
with ThreadPoolExecutor(max_workers=2) as pool:
 copied=sum(pool.map(lambda item:publish_file(item[1]['path'],item[0],files),objects.items()))
for patch in plan['patches']:
 for file in patch['files']:
  if file['sha256'] in objects:file['availability']='available'
OUTPUT.write_text(json.dumps(plan,ensure_ascii=False),encoding='utf-8')
print(json.dumps({'new_NAS_objects':copied,'linked_articles':len(plan['patches']),'files':sum(len(p['files']) for p in plan['patches'])}))
