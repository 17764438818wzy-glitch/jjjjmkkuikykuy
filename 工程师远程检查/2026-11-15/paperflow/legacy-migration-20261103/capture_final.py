"""Capture compact owned acceptance evidence; never print environment variables."""
import json,subprocess
from pathlib import Path
root=Path('/opt/paperflow-legacy-migration-20261103')
names=('final-coverage.json','endpoint-validation.json','attachment-endpoint-validation.json','acceptance-result.json',
 'reference-repair-result.json','attachment-repair-result.json','source-reconciliation.json')
value={name:json.loads((root/'state'/name).read_text()) for name in names if (root/'state'/name).exists()}
value['runners']={}
for name in ('paperflow-legacy-migration-acceptance','paperflow-legacy-migration-20261103','paperflow-legacy-attachment-repair-20261103','paperflow-legacy-reference-repair-20261103'):
 result=subprocess.run(['docker','inspect',name],capture_output=True,text=True)
 if result.returncode:continue
 info=json.loads(result.stdout)[0]
 value['runners'][name]={'state':info['State']['Status'],'exit':info['State']['ExitCode'],'image':info['Image']}
print(json.dumps(value,ensure_ascii=False))
