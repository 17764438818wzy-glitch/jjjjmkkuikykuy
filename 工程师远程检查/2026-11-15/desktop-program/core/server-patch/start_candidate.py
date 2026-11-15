"""Run on the LAN host; never changes/restarts the original container."""
import hashlib
import json
from pathlib import Path
import subprocess

root=Path(__file__).resolve().parent
name='pf-stage4-api-20261017'
def docker(*args,**kw):
    return subprocess.run(['sudo','-n','docker',*args],check=True,capture_output=True,**kw).stdout
original=json.loads(docker('inspect','ragflow-server-ragflow-1'))[0]
try:
    docker('inspect',name)
except subprocess.CalledProcessError:pass
else:raise RuntimeError('candidate already exists; inspect before reuse')
private=root/'service_conf.yaml'
docker('cp','ragflow-server-ragflow-1:/ragflow/conf/service_conf.yaml',str(private))
subprocess.run(['sudo','-n','chmod','600',str(private)],check=True)
networks=list(original['NetworkSettings']['Networks'])
args=['create','--name',name,'--label','paperflow.stage=4','--memory','2g','--memory-swap','2g',
      '--cpus','1','--pids-limit','256','--network',networks[0],
      '-p','127.0.0.1:18088:9380','--env-file','/dev/stdin',
      '-e','PYTHONPATH=/ragflow','-e','LITELLM_LOCAL_MODEL_COST_MAP=True',
      '-v',str(root)+':/candidate:ro',
      '-v',str(private)+':/ragflow/conf/service_conf.yaml:ro',
      '-v',str(root/'chunk_api.py')+':/ragflow/api/apps/restful_apis/chunk_api.py:ro',
      '--workdir','/ragflow','--entrypoint','/ragflow/.venv/bin/python']
for host in original['HostConfig'].get('ExtraHosts') or []:args.extend(['--add-host',host])
for address in original['HostConfig'].get('Dns') or []:args.extend(['--dns',address])
args.extend([original['Image'],'/candidate/serve_candidate.py'])
docker(*args,input=('\n'.join(original['Config']['Env'])+'\n').encode())
for network in networks[1:]:docker('network','connect',network,name)
docker('start',name)
print(json.dumps(dict(name=name,image=original['Image'],original_started=original['State']['StartedAt'],
    networks=networks,port='127.0.0.1:18088',patch_sha256=hashlib.sha256((root/'chunk_api.py').read_bytes()).hexdigest())))
