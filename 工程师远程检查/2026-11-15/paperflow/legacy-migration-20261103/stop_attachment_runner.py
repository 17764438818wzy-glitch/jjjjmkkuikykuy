"""Stop only this migration's attachment correction before loading its reviewed fix."""
import json,subprocess
name='paperflow-legacy-attachment-repair-20261103'
info=json.loads(subprocess.check_output(['docker','inspect',name]))[0]
if info['State']['Running']:
 subprocess.run(['docker','stop','--time','30',name],check=True,capture_output=True,timeout=45)
print(json.dumps({'container':name,'state':json.loads(subprocess.check_output(['docker','inspect',name]))[0]['State']['Status']}))
