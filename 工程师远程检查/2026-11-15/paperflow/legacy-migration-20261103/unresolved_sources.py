import json,collections
from pathlib import Path
root=Path(__file__).parent;plans=json.loads((root/'package-index.json').read_text())['plans']
stats=collections.Counter();samples=[]
with (root/'packages.jsonl').open('rb') as stream:
 for plan in plans:
  stream.seek(plan['offset']);value=json.loads(stream.read(plan['length']))
  if value['catalog_records'][0]['folder_name']!='原始文件夹未记录':continue
  path=Path(value['package_path']);run=path.parents[1]
  stats['run_manifest_exists']+=(run/'manifest.json').exists()
  stats['material_exists']+=(path.parent/'material.json').exists()
  for tag in ('single-request','private','packages','publication'):
   if tag in path.parts:stats[tag]+=1
  if len(samples)<4:
   sample={'path':str(path),'identity':value['identity'],'nearby':[p.name for p in path.parent.iterdir() if p.is_file()][:20]}
   if (run/'manifest.json').exists():
    manifest=json.loads((run/'manifest.json').read_text(encoding='utf-8-sig'))
    sample['manifest_keys']=list(manifest);sample['first_item']=(manifest.get('items') or [])[:1]
   if (path.parent/'material.json').exists():
    material=json.loads((path.parent/'material.json').read_text(encoding='utf-8-sig'));sample['material_keys']=list(material);sample['source']=material.get('source')
   samples.append(sample)
print(json.dumps({'stats':dict(stats),'samples':samples},ensure_ascii=False)[:10000])
