"""Supervised native synchronous stages; envelope capsule is the only CLI input."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import psutil
from .pipeline_contracts import StageEnvelope,StageResult,ArtifactRef
from .pipeline_runtime import current_context,artifact,verify_ref,load_stage_context
from .model_transport import disk
from .records import write_json,read_json

OPERATIONS={'intake':'receive','prepare':'prepare','package':'package','rag_body':'publish_body',
            'rag_attachment':'publish_attachment','archive':'archive','cleanup':'cleanup','finish':'finish'}
RESOURCES={'prepare':'local','package':'package','rag_body':'rag','rag_attachment':'rag','archive':'nas'}
REMOTE={'rag_body','rag_attachment','archive','cleanup'}


def process_identity(process):
    return dict(pid=process.pid,process_create_time=process.create_time())


def owned_tree(pid):
    try:
        parent=psutil.Process(pid)
        return [process_identity(p) for p in [parent,*parent.children(recursive=True)]]
    except psutil.NoSuchProcess: return []


def stop_tree(identities):
    alive=[]
    for row in reversed(identities):
        try:
            p=psutil.Process(row['pid'])
            if abs(p.create_time()-row['process_create_time'])<.001: p.terminate(); alive.append(p)
        except psutil.NoSuchProcess: pass
    _,alive=psutil.wait_procs(alive,timeout=1)
    for p in alive:
        try: p.kill()
        except psutil.NoSuchProcess: pass
    _,alive=psutil.wait_procs(alive,timeout=1)
    return [process_identity(p) for p in alive]


async def supervise(command,capsule_path,record_path,*,env=None,timeout=3600):
    """Private process primitive, also usable by synthetic loopback integration probes."""
    spawning=asyncio.create_task(asyncio.create_subprocess_exec(*command,str(capsule_path),env=env,
        stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL,
        creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)))
    try: process=await asyncio.shield(spawning)
    except asyncio.CancelledError:
        while not spawning.done():
            try: await asyncio.shield(spawning)
            except asyncio.CancelledError: continue
        process=spawning.result()
        identities=await disk(owned_tree,process.pid)
        survivors=await disk(stop_tree,identities)
        await disk(write_json,record_path,dict(schema='pipeline-stage-process/1',processes=identities,
            status='termination_unconfirmed' if survivors else 'terminated',survivors=survivors,
            error_type='CancelledError',finished_at=time.time()))
        await asyncio.wait_for(process.wait(),2)
        raise
    identities=[]; record=dict(schema='pipeline-stage-process/1',status='starting',started_at=time.time())
    wait=asyncio.create_task(process.wait()); deadline=time.monotonic()+timeout
    try:
        identities=await disk(owned_tree,process.pid)
        record.update(launcher=identities[0] if identities else {'pid':process.pid},processes=identities)
        await disk(write_json,record_path,record)
        while not wait.done():
            current=await disk(owned_tree,process.pid)
            merged={(r['pid'],r['process_create_time']):r for r in identities+current}; identities=list(merged.values())
            started=Path(str(capsule_path)+'.started.json')
            if started.exists() and not Path(str(capsule_path)+'.go.json').exists():
                child=await disk(read_json,started)
                if child not in identities: raise ValueError('child PID identity is not owned by launcher')
                record.update(status='running',child=child,processes=identities)
                await disk(write_json,record_path,record)
                await disk(write_json,str(capsule_path)+'.go.json',dict(child=child))
            if time.monotonic()>=deadline: raise TimeoutError('stage process exceeded deadline')
            await asyncio.wait({wait},timeout=.05)
        code=wait.result()
        record.update(status='exited',exit_code=code,processes=identities,finished_at=time.time())
        await disk(write_json,record_path,record)
        if code: raise RuntimeError('stage child failed; inspect private receipt')
        done=await disk(read_json,str(capsule_path)+'.done.json')
        result_ref=ArtifactRef.from_dict(done['result_ref']); await disk(verify_ref,result_ref)
        return StageResult.from_dict(await disk(read_json,result_ref.path))
    except BaseException as exc:
        identities=list({(r['pid'],r['process_create_time']):r for r in identities+await disk(owned_tree,process.pid)}.values())
        survivors=await disk(stop_tree,identities)
        record.update(status='termination_unconfirmed' if survivors else 'terminated',processes=identities,
                      survivors=survivors,error_type=type(exc).__name__,finished_at=time.time())
        await disk(write_json,record_path,record)
        try: await asyncio.wait_for(asyncio.shield(wait),2)
        except (TimeoutError,asyncio.CancelledError): pass
        raise


async def run_stage_process(envelope: StageEnvelope,operation: str) -> StageResult:
    ctx=current_context(envelope)
    if operation not in OPERATIONS or operation!=envelope.stage.value: raise ValueError('unsupported stage operation')
    root=ctx.ledger.root/'processes'/envelope.operation_key/uuid.uuid4().hex
    capsule=root/'envelope.json'; record=root/'process.json'
    await disk(write_json,capsule,dict(schema='pipeline-process-envelope/1',envelope=envelope.to_dict(),
        config_ref=dict(path=str(ctx.config_path),sha256=ctx.config_sha256),operation=operation))
    command=[ctx.settings.values['pipeline'].get('python',sys.executable),'-m','paperflow_prefect.stage_process']
    env=dict(os.environ); env['PYTHONPATH']=str(Path(__file__).resolve().parents[1])
    try:
        if operation in RESOURCES:
            async with ctx.scope(RESOURCES[operation]):
                await disk(ctx.fence)
                result=await supervise(command,capsule,record,env=env,
                    timeout=ctx.settings.values['pipeline'].get('stage_timeout',3600))
        else:
            await disk(ctx.fence); result=await supervise(command,capsule,record,env=env)
        if result.operation_key!=envelope.operation_key: raise ValueError('child result identity mismatch')
        for ref in result.evidence_refs: await disk(verify_ref,ref)
        return result
    except BaseException:
        if operation in REMOTE and record.exists():
            ctx.ledger.commit_result(envelope,StageResult(envelope.operation_key,'unknown',(await disk(artifact,record),),()))
        raise


def child_main(path):
    path=Path(path).resolve(); capsule=read_json(path)
    envelope=StageEnvelope.from_dict(capsule['envelope']); config_ref=ArtifactRef.from_dict(capsule['config_ref'])
    verify_ref(config_ref)
    os.environ['PAPERFLOW_PIPELINE_CONFIG_SHA256']=config_ref.sha256
    who=process_identity(psutil.Process()); write_json(str(path)+'.started.json',who)
    deadline=time.monotonic()+15
    while not Path(str(path)+'.go.json').exists():
        if time.monotonic()>deadline: raise TimeoutError('parent handshake missing')
        time.sleep(.02)
    if read_json(str(path)+'.go.json')['child']!=who: raise ValueError('parent child binding mismatch')
    ctx=load_stage_context(Path(config_ref.path),envelope); ctx.fence()
    operation=capsule['operation']
    if operation not in OPERATIONS or operation!=envelope.stage.value: raise ValueError('invalid operation')
    if operation=='intake': result=ctx.backend.receive(envelope.receipt_ref.path)
    elif operation=='rag_attachment': result=ctx.backend.publish_attachment(ctx.paper,ctx.row)
    else: result=getattr(ctx.backend,OPERATIONS[operation])(ctx.paper)
    state='complete' if operation in ('intake','prepare','package') else 'verified'
    if operation not in ('intake','prepare','package') and result.get('status') not in ('verified','complete'):
        state='needs_attention'
    business=path.parent/'business-receipt.json'; write_json(business,result)
    evidence={str(business): artifact(business)}
    def collect(value,key=''):
        if isinstance(value,dict):
            for name,item in value.items(): collect(item,name)
        elif isinstance(value,list):
            for item in value: collect(item,key)
        elif isinstance(value,str):
            try:
                candidate=Path(value)
                if candidate.is_absolute() and candidate.is_file():
                    evidence[str(candidate.resolve())]=artifact(candidate)
                elif key=='package_path' and candidate.is_absolute() and candidate.is_dir():
                    for member in candidate.rglob('*'):
                        if member.is_file(): evidence[str(member.resolve())]=artifact(member)
            except (ValueError,OSError):
                # Free prose is not a path. Declared artifacts below remain mandatory.
                if key in ('material','receipt','package_path'): raise
    collect(result)
    if operation=='package':
        prepared=ctx.backend.prepared_path(ctx.paper)
        evidence[str(prepared)]=artifact(prepared)
    output=StageResult(envelope.operation_key,state,tuple(evidence.values()),())
    result_path=path.parent/'result.json'; write_json(result_path,output.to_dict())
    write_json(str(path)+'.done.json',dict(result_ref=artifact(result_path).to_dict()))


if __name__=='__main__': child_main(sys.argv[1])
