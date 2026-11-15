import subprocess
program='''
import collections,copy,hashlib,json,os
from pathlib import Path
from qdrant_client import QdrantClient,models
from paperflow_qdrant.library import stable_id
from haystack_integrations.document_stores.qdrant.converters import convert_id
from article_repository.catalog import Catalog
from migrate import Source
root=Path('/input');snapshot=json.loads((root/'source-inventory.json').read_text());plans=json.loads((root/'package-index.json').read_text())['plans']
majors=json.loads((root/'majors.json').read_text())['majors'];datasets={m[k] for m in majors for k in ('paper_dataset_id','image_dataset_id')}
known={tuple(p['identity']) for p in plans};groups=collections.defaultdict(list)
for d in snapshot['documents']:
 identity=tuple(d['meta'].get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'))
 if d['kb_id'] in datasets and all(identity) and identity not in known:groups[identity].append(d)
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'],timeout=120);cat=Catalog(os.environ['ARTICLE_DATABASE_URL'])
payloads={};missing=[];pairs={identity[1:]:identity for identity in groups};stats=collections.Counter();sample=None
source=Source(snapshot,json.loads((root/'storage-inventory.json').read_text()))
candidates=collections.defaultdict(list);offset=None
while True:
 page,offset=client.scroll('paperflow_v1',scroll_filter=models.Filter(must=[models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest'))]),
  limit=256,offset=offset,with_vectors=False,with_payload=models.PayloadSelectorInclude(include=['meta.document.dataset_meta_fields','meta.document.meta_fields.repository_article_id','meta.document.meta_fields.paperflow_role']))
 for p in page:
  stats['manifests']+=1
  doc=p.payload['meta']['document']
  if doc.get('meta_fields',{}).get('repository_article_id'):stats['migration_copy']+=1;continue
  if sample is None:sample={'meta':doc.get('meta_fields'),'owners':[{'keys':list(owner),'identity':[owner.get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256')]} for owner in doc.get('dataset_meta_fields',{}).values()][:1]}
  for owner in doc.get('dataset_meta_fields',{}).values():
   pair=tuple(owner.get(k) for k in ('paperflow_source_sha256','paperflow_manifest_sha256'))
   if pair in pairs:stats['matching_pair']+=1;candidates[pairs[pair]].append(p.id);break
 if offset is None:break
for identity,docs in groups.items():
 found=client.retrieve('paperflow_v1',ids=candidates[identity],with_vectors=False) if candidates[identity] else []
 accepted=[]
 for point in found:
  folder=point.payload['meta']['folder_id'];doc=point.payload['meta']['document']
  if not client.retrieve('paperflow_v1',ids=[convert_id(stable_id(folder,'complete'))],with_payload=False):stats['not_complete']+=1;continue
  with cat.connect() as conn:rows=conn.execute("SELECT base FROM article WHERE base->'qdrant'->'folder_ids' ? %s",(folder,)).fetchall()
  if not rows:stats['catalog_row_missing']+=1;continue
  meta=doc['meta_fields'];owner=next(iter(doc.get('dataset_meta_fields',{}).values()),{});meta={**meta,**owner}
  original=doc.get('original');expected=meta.get('paperflow_original_sha256')
  if not original or (expected and hashlib.sha256(original.encode()).hexdigest()!=expected):stats['original_invalid']+=1;continue
  profile=json.loads(meta.get('two_pass_profile_json') or '{}');tree=json.loads(meta.get('two_pass_tree_json') or '{}')
  if not profile or not tree:stats['no_profile_or_tree']+=1;continue
  old=next(d for d in source.metadata(docs) if d['meta'].get('two_pass_profile_json'))['meta']
  assert profile==json.loads(old['two_pass_profile_json']) and tree==json.loads(old['two_pass_tree_json']),'Native packet metadata differs from legacy packet'
  records=[copy.deepcopy(r['base']) for r in rows]
  payload={'identity':list(identity),'original':original,'profile':profile,'tree':tree,
   'assets':json.loads(meta.get('rag_mid_assets_json') or '[]'),'chunks':doc['records'],'catalog_records':records,'restored_from_native_folder':folder}
  accepted.append(payload)
 if accepted:
  variants={hashlib.sha256(p['original'].encode()).hexdigest() for p in accepted}
  if len(variants)!=1:raise ValueError('Native full-text copies disagree')
  payloads[json.dumps(identity,separators=(',',':'))]=accepted[0]
 else:missing.append(list(identity))
print(json.dumps({'payloads':payloads,'missing_identities':missing,'unsealed_identity_groups':len(groups),'stats':dict(stats),'sample':sample},ensure_ascii=False))
'''
command=['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/input:/input:ro','--entrypoint','python','paperflow-legacy-migration:20261103','-']
result=subprocess.run(command,input=program,capture_output=True,text=True,timeout=180)
assert result.returncode==0,result.stderr[-2000:]
print(result.stdout)
