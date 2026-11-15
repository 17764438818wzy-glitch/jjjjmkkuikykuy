import subprocess
program='''
import json
from pathlib import Path
from migrate import Source
root=Path('/input');snapshot=json.loads((root/'source-inventory.json').read_text());source=Source(snapshot,json.loads((root/'storage-inventory.json').read_text()))
for did in ('00cde98cce2211f18aa88f6d4dd599d1','0061557ad3ee11f18de1d7504e6560be'):
 doc=next(d for d in snapshot['documents'] if d['id']==did);full=source.metadata([doc])[0];rows=source.chunks(full,vectors=False)
 print(json.dumps({'id':did,'metadata':full['meta'],'chunks':[{'id':r['id'],'text':r.get('content_with_weight')} for r in rows[:2]]},ensure_ascii=False))
'''
result=subprocess.run(['docker','run','--rm','-i','--network','host','--env-file','/opt/paperflow-legacy-migration-20261103/.migration.env',
 '-v','/opt/paperflow-legacy-migration-20261103/input:/input:ro','--entrypoint','python','paperflow-legacy-migration:20261103','-'],input=program,capture_output=True,text=True,timeout=120)
assert not result.returncode,result.stderr[-2000:]
print(result.stdout)
