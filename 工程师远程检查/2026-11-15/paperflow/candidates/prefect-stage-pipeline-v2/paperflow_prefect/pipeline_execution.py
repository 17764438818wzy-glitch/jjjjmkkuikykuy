"""Stage execution owns only the selected stage and its cancellable resources."""
import asyncio
from dataclasses import replace
from pathlib import Path
from filelock import FileLock
from .pipeline_contracts import StageEnvelope,StageResult,operation_key
from .pipeline_runtime import (load_stage_context,configured_path,active_context,current_context,
    task_owner,artifact,verify_ref)
from .model_transport import disk,save_request,send_model
from .records import Records,read_json,write_json


def _advance(generator,response):
    try: return False,generator.send(response)
    except StopIteration as done: return True,done.value


def _connection(ctx):
    reference=ctx.settings.values['pipeline'].get('model_connection_ref')
    if reference:
        from .pipeline_contracts import ArtifactRef
        ref=ArtifactRef.from_dict(reference); verify_ref(ref); return read_json(ref.path)
    from .legacy import connection
    return connection(ctx.settings)


def _make_steps(ctx):
    from .request_admission import require_model_history,bind_work
    backend,paper,row=ctx.backend,ctx.paper,ctx.row
    attachment=ctx.envelope.stage.value=='attachment'
    work=backend.attachment_work(paper,row) if attachment else backend.body_work(paper)
    stage='identify' if attachment else 'profile'
    cached=Records(work).cached(stage)
    if attachment and cached is not None:
        from .attachments import verify_cached_description
        verify_cached_description(row,paper['paper_id'],work,cached)
        backend.attachment_card(paper,row,cached)
        return work,None,cached
    if not attachment and cached is not None:
        return work,backend.profile_steps(paper,ctx.connection),None
    backend.confirm_history(paper,row); require_model_history(paper,row,work)
    bind_work(backend.root,paper,row,'attachment' if attachment else 'body',work)
    if attachment:
        from .pipeline_attachment import attachment_steps
        generator=attachment_steps(row,paper['paper_id'],work,ctx.connection,None)
    else:
        generator=backend.profile_steps(paper,ctx.connection)
    return work,generator,None


def _complete_model(ctx,work,result):
    if ctx.envelope.stage.value=='attachment':
        Records(work).complete('identify',result,[work/'description.json'])
        card=ctx.backend.attachment_card(ctx.paper,ctx.row,result)
        paths=[work/'description.json',work/'evidence.json',Path(card['path'])]
    else:
        paths=[Path(result[k]) for k in ('candidate','validation','bundle')]
        retained=Records(work).cached('profile')
        if retained is None: Records(work).complete('profile',result,paths)
        elif retained != result: raise ValueError('verified native profile result changed')
        paths.append(work/'material.json')
    from .profile_recovery import selected_model
    for directory in {work/'model',selected_model(work)}:
        paths.extend(path for path in directory.rglob('*') if path.is_file() and path.suffix in ('.json','.jsonl','.txt'))
    paths=list(dict.fromkeys(paths))
    return StageResult(ctx.envelope.operation_key,'passed',tuple(artifact(p) for p in paths),())


async def _models(ctx):
    ctx.connection=await disk(_connection,ctx)
    work=await disk(ctx.backend.attachment_work,ctx.paper,ctx.row) if ctx.envelope.stage.value=='attachment' else await disk(ctx.backend.body_work,ctx.paper)
    # Native lock stays with the stage across all yielded requests, never a paper lifecycle lock.
    lock=FileLock(str(work/('attachment-identification.lock' if ctx.envelope.stage.value=='attachment' else 'profile-recovery.lock')),
                  timeout=0,thread_local=False)
    await disk(lock.acquire)
    response=None; generator=None
    try:
        async with ctx.scope('local'):
            work,generator,cached=await disk(_make_steps,ctx)
        if generator is None: return await disk(_complete_model,ctx,work,cached)
        while True:
            async with ctx.scope('local'):
                done,value=await disk(_advance,generator,response)
            if done: return await disk(_complete_model,ctx,work,value)
            body,connection,directory=value
            ctx.connection=connection
            envelope=ctx.envelope
            # Native part and synthesis IDs remain independent durable operations.
            relative=Path(directory).resolve().relative_to((work/'model').resolve())
            if relative.parts:
                part_id=relative.parts[0]
                inputs=dict(paper_id=envelope.paper_id,source_sha256=ctx.row['sha256'],stage=envelope.stage.value,
                    item_id=envelope.item_id,part_id=part_id,input_refs=envelope.input_refs,contract_version=envelope.contract_version)
                child=replace(envelope,part_id=part_id,operation_key=operation_key(inputs))
                child_ctx=replace(ctx,envelope=child)
                token=active_context.set(child_ctx)
                try:
                    with ctx.ledger.claim(child,ctx.owner) as claimed:
                        if not claimed['execute']:
                            raise ValueError('native partition journal missing a ledger-completed response')
                        request=await save_request(body,directory,child)
                        response_ref=await send_model(request,child)
                        ctx.ledger.commit_result(child,StageResult(child.operation_key,'complete',(response_ref,),()))
                finally: active_context.reset(token)
            else:
                request=await save_request(body,directory,envelope)
                await send_model(request,envelope)
            response=await disk((Path(directory)/'response.txt').read_text,'utf8')
    finally:
        if generator is not None: await disk(generator.close)
        await disk(lock.release)


async def execute_stage(envelope: StageEnvelope) -> StageResult:
    if isinstance(envelope,dict): envelope=StageEnvelope.from_dict(envelope)
    ctx=await disk(load_stage_context,configured_path(),envelope)
    ctx.owner=task_owner(ctx); token=active_context.set(ctx)
    try:
        with ctx.ledger.claim(envelope,ctx.owner) as claim:
            if not claim['execute']: return StageResult.from_dict(claim['result'])
            await disk(ctx.fence)
            if envelope.stage.value in ('profile','attachment','rag_attachment') and envelope.item_id is None and envelope.stage.value!='profile':
                if not ctx.paper['attachments']:
                    path=ctx.backend.paper_root(ctx.paper)/('pipeline-empty-'+envelope.stage.value+'.json')
                    await disk(write_json,path,dict(reason='empty_inventory',paper_id=envelope.paper_id))
                    result=StageResult(envelope.operation_key,'skipped',(await disk(artifact,path),),())
                else: raise ValueError('attachment item required')
            elif envelope.stage.value in ('profile','attachment'):
                result=await _models(ctx)
            else:
                from .stage_process import run_stage_process
                result=await run_stage_process(envelope,envelope.stage.value)
            ctx.ledger.commit_result(envelope,result)
            return result
    finally: active_context.reset(token)
