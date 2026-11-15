import subprocess
program='''
import json,os
from pathlib import Path
from migrate import Source
from qdrant_client import QdrantClient,models
root=Path('/input');snapshot=json.loads((root/'source-inventory.json').read_text());source=Source(snapshot,json.loads((root/'storage-inventory.json').read_text()))
index=json.loads((root/'package-index.json').read_text());identity=index['rejected'][0]['identity']
docs=[d for d in snapshot['documents'] if [d['meta'].get(k) for k in ('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256')]==identity]
doc=next(d for d in source.metadata(docs) if d['meta'].get('two_pass_profile_json'))
profile=json.loads(doc['meta']['two_pass_profile_json']);tree=json.loads(doc['meta']['two_pass_tree_json'])
client=QdrantClient(url=os.environ['QDRANT_URL'],api_key=os.environ['QDRANT_API_KEY'])
conditions=[models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
 models.FieldCondition(key='meta.document.dataset_meta_fields.'+doc['kb_id']+'.paperflow_source_sha256',match=models.MatchValue(value=identity[1])),
 models.FieldCondition(key='meta.document.dataset_meta_fields.'+doc['kb_id']+'.paperflow_manifest_sha256',match=models.MatchValue(value=identity[2]))]
found=client.scroll('paperflow_v1',scroll_filter=models.Filter(must=conditions),limit=10,with_vectors=False)[0]
print(json.dumps({'identity':identity,'native_candidates':[{'folder':r.payload['meta']['folder_id'],'original_chars':len(r.payload['meta']['document'].get('original') or ''),
 'source_doc':r.payload['meta']['document']['id']} for r in found],'meta_keys':list(doc['meta']),
 'unit_sample':profile.get('learning_units',[])[:1],'tree_keys':list(tree),'tree_sample':str(tree)[:2000]},ensure_ascii=False))
'''
result=subprocess.run(['docker','exec','-i','paperflow-legacy-migration-20261103','python','-'],input=program,capture_output=True,text=True,timeout=120)
assert not result.returncode,result.stderr[-1500:]
print(result.stdout)
