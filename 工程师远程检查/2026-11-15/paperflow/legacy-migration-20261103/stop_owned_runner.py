import json,subprocess,os
name='paperflow-legacy-migration-20261103'
info=json.loads(subprocess.check_output(['docker','inspect',name]))[0]
assert info['Config']['Image']=='paperflow-legacy-migration:20261103'
assert any(m.get('Source')=='/opt/paperflow-legacy-migration-20261103/state' for m in info['Mounts'])
subprocess.run(['docker','stop','--time','30',name],check=True,capture_output=True)
print(json.dumps({'owned_migration_stopped':name,'host_cpu_count':os.cpu_count(),'checkpoints_preserved':True}))
