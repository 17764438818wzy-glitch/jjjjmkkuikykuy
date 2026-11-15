"""Subprocess probe used only by task-scoped tests; denies external network."""
import asyncio
import json
import os
from pathlib import Path
import socket
import sys
import time
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]

def deny_external():
    original=socket.socket.connect; original_ex=socket.socket.connect_ex
    def check(address):
        if isinstance(address,tuple) and address[0] not in ('127.0.0.1','::1','localhost'):
            raise PermissionError('test denied non-loopback connection')
    def connect(sock,address): check(address); return original(sock,address)
    def connect_ex(sock,address): check(address); return original_ex(sock,address)
    socket.socket.connect=connect; socket.socket.connect_ex=connect_ex

def main(path):
    deny_external()
    from paperflow_prefect.records import read_json,write_json
    from paperflow_prefect.pipeline_runtime import artifact
    from paperflow_prefect.pipeline_contracts import StageEnvelope,StageResult
    from paperflow_prefect.stage_process import process_identity
    import psutil
    path=Path(path); value=read_json(path); who=process_identity(psutil.Process())
    write_json(str(path)+'.started.json',who)
    while not Path(str(path)+'.go.json').exists(): time.sleep(.02)
    if value.get('sleep'):
        while True: time.sleep(.1)
    if value.get('spawn_child'):
        import subprocess
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)'],creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        write_json(path.parent/'grandchild.json',dict(pid=child.pid))
        while True: time.sleep(.1)
    envelope=StageEnvelope.from_dict(value['envelope'])
    proof=path.parent/'proof.json'; write_json(proof,dict(executed=True))
    result=StageResult(envelope.operation_key,'complete',(artifact(proof),),())
    output=path.parent/'result.json'; write_json(output,result.to_dict())
    write_json(str(path)+'.done.json',dict(result_ref=artifact(output).to_dict()))

if __name__=='__main__': main(sys.argv[1])
