"""Run the reviewed remote reader and capture its JSON without logging article content."""
import json
import os
from pathlib import Path
import sys
import paramiko

source=Path(sys.argv[1]).resolve()
target=Path(sys.argv[2]).resolve()
assert source.parent==Path(__file__).resolve().parent and target.parent==source.parent
client=paramiko.SSHClient();client.load_system_host_keys()
client.connect('192.168.1.120',username='wzy',password=os.environ['RAG_DIAG_SSH_PASSWORD'],look_for_keys=False,allow_agent=False,timeout=15)
stdin,stdout,stderr=client.exec_command("sudo -S -p '' python3 -",timeout=900)
stdin.write(os.environ['RAG_DIAG_SSH_PASSWORD']+'\n'+source.read_text(encoding='utf-8'))
stdin.flush();stdin.channel.shutdown_write()
raw=stdout.read();error=stderr.read();code=stdout.channel.recv_exit_status();client.close()
if code:
    print(error.decode('utf-8','replace')[-2000:]);raise SystemExit(code)
value=json.loads(raw)
target.write_bytes(raw)
print(json.dumps({'capture':str(target),'bytes':len(raw),'items':len(value)},ensure_ascii=False))
