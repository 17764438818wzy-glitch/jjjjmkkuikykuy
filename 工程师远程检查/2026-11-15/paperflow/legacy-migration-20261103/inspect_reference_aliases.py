import subprocess
program=r'''
import json,os
from types import SimpleNamespace
from pathlib import Path
from haystack.utils import Secret
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from qdrant_client import models
from paperflow_qdrant.library import Library
from repair_attachments import current_map
store=QdrantDocumentStore(url=os.environ['QDRANT_URL'],api_key=Secret.from_env_var('QDRANT_API_KEY'),index='paperflow_v1',embedding_dim=1024,progress_bar=False,timeout=120)
migration=SimpleNamespace(library=Library(store,None));aliases=current_map(migration)
points=store._client.scroll('paperflow_v1',limit=256,with_vectors=False,scroll_filter=models.Filter(must=[
 models.FieldCondition(key='meta.kind',match=models.MatchValue(value='manifest')),
 models.FieldCondition(key='meta.active',match=models.MatchValue(value=True)),
 models.FieldCondition(key='meta.document.meta_fields.paperflow_role',match=models.MatchAny(any=['attachment-card','attachment-reference','attachment-primary','attachment-note']))]))[0]
rows=[]
for point in points:
 doc=point.payload['meta']['document']
 for dataset in doc['dataset_ids']:
  fields={**doc['meta_fields'],**doc['dataset_meta_fields'][dataset]}
  if not fields.get('legacy_primary_document_id'):continue
  key=fields.get('primary_dataset_id','')+'/'+fields['legacy_primary_document_id']
  if aliases.get(key)==fields.get('primary_document_id') and aliases.get(key):continue
  rows.append({'folder_id':point.payload['meta']['folder_id'],'legacy_source_documents':fields['legacy_document_ids'],
   'primary_key':key,'current_primary_target':fields.get('primary_document_id'),'now_unique_target':aliases.get(key)})
value={'pending_reference_memberships':len(rows),'now_unique':sum(bool(r['now_unique_target']) for r in rows),'rows':rows}
Path('/state/reference-alias-audit.json').write_text(json.dumps(value));print(json.dumps(value))
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/state:/state','--entrypoint','python','paperflow-legacy-migration:20261103','-'],input=program,capture_output=True,text=True,timeout=300)
assert result.returncode==0,result.stderr[-2500:]
print(result.stdout)
