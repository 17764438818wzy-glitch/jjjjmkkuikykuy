"""Real native preparation/build/parse; HTTP fake is loopback and source is generated."""
import asyncio
from contextlib import asynccontextmanager
import io
import json
import os
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
from pipeline_execution_probe import deny_external
from paperflow_prefect.records import write_json,read_json,sha_file
from paperflow_prefect.pipeline_contracts import StageEnvelope,operation_key
from paperflow_prefect.pipeline_runtime import artifact

async def run(root,partition=False,crash=False,adoption=False,recovery=False,shared=False):
    deny_external()
    from paperflow_prefect.settings import Settings
    from paperflow_prefect.legacy import bind
    values=dict(Settings.load().values,library_root=str(root/'library'),data_root=str(root/'data'),
        nas_destination=str(root/'nas'),history_roots=[],attachment_receipt_roots=[],rag_history_roots=[],
        local_cleanup_enabled=False,prefect_api_url='http://127.0.0.1:9/api')
    (root/'data').mkdir(); (root/'library').mkdir()
    write_json(root/'model-config.json',read_json(values['model_config'])); values['model_config']=str(root/'model-config.json')
    bind(Settings(values))
    from docx import Document
    document=Document(); document.add_heading('1 排程研究',1)
    for text in ('本研究比较两种排程方案，目标是减少作业等待时间。',
        '材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录各作业等待时间，比较平均等待时间。',
        '比较得到两组等待时间。本例仅说明比较过程，新问题需要重新采集作业数据。'): document.add_paragraph(text)
    if partition:
        document.add_heading('2 复验排程',1)
        document.add_paragraph('材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录各作业等待时间，比较平均等待时间。')
    raw=io.BytesIO(); document.save(raw)
    from test_receipt_h21 import handoff
    from paperflow_prefect.receipts import ReceiptStore
    from paperflow_prefect.intake_handoff import receive_handoff
    h=handoff(root/'library',root/'library/person',body=raw.getvalue(),attachments=[('附件/code.txt',b'print(1)')])
    write_json(root/'handoff.json',h)
    receipt=receive_handoff(root/'handoff.json',ReceiptStore(root/'receipts'))
    paper=receipt['inventory']; received=artifact(Path(receipt['record_path']))
    generation={'global':0,'batch':0,'runtime':0}; write_json(root/'control.json',dict(allowed=True,generation=generation))
    calls=[]; sessions=[]
    async def server(reader,writer):
        sessions.append(writer)
        try:
            header=await reader.readuntil(b'\r\n\r\n'); n=int(next(x.split(b':')[1] for x in header.split(b'\r\n') if x.lower().startswith(b'content-length:')))
            body=json.loads(await reader.readexactly(n)); calls.append(body)
            if '只简单识别附件' in body['messages'][0]['content']:
                candidate=dict(kind='Python代码',summary='输出一个数字。',evidence_refs=['text-preview'],limitations=[])
            else:
                from fixture_profile import _candidate
                from preextract_images import restore_request_images
                work=next((root/'data').glob('papers/*/body/*/material.json')).parent
                original=read_json(work/'material.json')
                if partition and '本次仅统合已完成' in body['messages'][0]['content']:
                    assembled=read_json(work/'model/parts-candidate.json')
                    candidate=dict(profile={k:v for k,v in assembled['profile'].items() if k!='learning_units'},
                        unit_links=[dict(id=u['id'],upstream_unit_ids=u.get('upstream_unit_ids',[])) for u in assembled['profile']['learning_units']],
                        section_links=[],gaps=[],redaction_plan=[],record_kind=assembled['record_kind'])
                elif partition:
                    from preextract_partition_run import _request_hash
                    part=next(p for p in (work/'model').glob('part-*/planned-request.json') if _request_hash(read_json(p))==_request_hash(body))
                    bundle=restore_request_images(read_json(part.parent/'material.json'),part.parent,body,original_bundle=original)
                    candidate=_candidate(dict(template=Path(values['template_path'])),bundle)
                else:
                    bundle=restore_request_images(original,work,body)
                    candidate=_candidate(dict(template=Path(values['template_path'])),bundle)
            reply=json.dumps(dict(choices=[dict(message=dict(content=json.dumps(candidate,ensure_ascii=False)),finish_reason='stop')],usage=dict(total_tokens=5)),ensure_ascii=False).encode()
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '+str(len(reply)).encode()+b'\r\n\r\n'+reply); await writer.drain()
        finally: writer.close()
    srv=await asyncio.start_server(server,'127.0.0.1',0)
    connection=dict(model='test',base_url=f'http://127.0.0.1:{srv.sockets[0].getsockname()[1]}/v1',api_key='<REDACTED_SECRET>',
        max_tokens=10000,max_output_tokens=10000,context_window=1000000,supports_images=True,max_images=600,
        request_layout='legacy',max_image_bytes=24*1024*1024,max_single_image_bytes=6*1024*1024,max_request_bytes=48*1024*1024)
    write_json(root/'connection.json',connection)
    values['pipeline']=dict(instance_id='fixture',ledger_root=str(root/'ledger'),control_path=str(root/'control.json'),
        binding_refs=[artifact(Path(values[k])).to_dict() for k in ('model_config','runtime_config','template_path','catalog_path','source_manifest')],
        model_connection_ref=artifact(root/'connection.json').to_dict(),prefect_api_url='http://127.0.0.1:9/api',python=sys.executable)
    config=root/'config.json'; write_json(config,values)
    os.environ.update(PAPERFLOW_PIPELINE_CONFIG=str(config),PAPERFLOW_PIPELINE_CONFIG_SHA256=sha_file(config),
        PREFECT_API_URL='http://127.0.0.1:9/api',PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false')
    import paperflow_prefect.pipeline_runtime as runtime
    from paperflow_prefect.pipeline_execution import execute_stage
    active=[]; transitions=[]
    @asynccontextmanager
    async def scope(name):
        active.append(name); transitions.append(('enter',name))
        if name in ('body-http','attachment-http'): assert 'local' not in active
        try: yield
        finally: active.remove(name); transitions.append(('exit',name))
    runtime.native_scope=scope
    def env(stage,item=None):
        row=paper['body'] if item is None else paper['attachments'][0]
        args=dict(paper_id=paper['paper_id'],source_sha256=row['sha256'],stage=stage,item_id=item,part_id=None,input_refs=[],contract_version='native-v1')
        return StageEnvelope('fixture',paper['paper_id'],received,operation_key(args),stage,item,None,(),'native-v1',generation,0,'normal')
    try:
        prepare=await execute_stage(env('prepare'))
        assert any(Path(r.path).name=='material.json' for r in prepare.evidence_refs), 'prepare cache must bind actual material bytes'
        attachment=await execute_stage(env('attachment',paper['attachments'][0]['file_id']))
        if partition:
            import copy
            from preextract_contract import load_template
            from preextract_request import build_request
            from preextract_partition import _span,_coverage
            from preextract_partition_run import create_plan
            work=next((root/'data').glob('papers/*/body/*/material.json')).parent
            original=read_json(work/'material.json'); template,_=load_template(values['template_path']); catalog=read_json(values['catalog_path'])
            parts=[]
            for index,section in enumerate(original['sections'],1):
                subset=copy.deepcopy(original); subset['sections']=[section]
                subset['blocks']=[b for b in original['blocks'] if b['section_id']==section['id']]
                subset['assets']=[]; ranges=[_span(b) for b in subset['blocks']]
                subset['partition_scope']=dict(primary_ranges=ranges,primary_asset_ids=[])
                body,capacity=build_request(subset,template,catalog,connection)
                parts.append(dict(id=f'part-{index:03d}',ranges=ranges,asset_ids=[],bundle=subset,body=body,capacity=capacity))
            assert len(parts)==2
            create_plan(dict(parts=parts,coverage=_coverage(original,parts)),original,original,work,template,connection)
        write_json(root/'profile-envelope.json',env('profile').to_dict())
        if crash:
            import paperflow_prefect.pipeline_execution as execution
            original_advance=execution._advance
            def crash_before_parse(generator,response):
                if response is not None: os._exit(23)
                return original_advance(generator,response)
            execution._advance=crash_before_parse
        profile=await execute_stage(env('profile'))
        again=await execute_stage(env('profile'))
        expected=4 if partition else 2
        assert len(calls)==expected and profile==again
        if partition:
            state=read_json(work/'model/partition-state.json')
            assert state['status']=='assembled' and len(state['requests'])==3
            reservations=read_json(next((root/'ledger/papers').glob('*.json')))['reservations']
            assert {r['part_id'] for r in reservations if r['part_id']}=={'part-001','part-002','global-01-001'}
        if shared:
            from pipeline_cache_adoption_probe import check_shared
            work=next((root/'data').glob('papers/*/body/*/material.json')).parent
            check_shared(root,values,config,dict(receipt_path=received.path,inventory=paper),work,connection,calls)
            return
        if adoption:
            from pipeline_cache_adoption_probe import check
            work=next((root/'data').glob('papers/*/body/*/material.json')).parent
            await check(root,values,config,env('profile'),work,calls,partition=partition,recovery=recovery)
            return
        # Reproduce the concurrently introduced native attachment recovery format.
        import shutil
        from paperflow_prefect.attachment_recovery import PARENT_FILES,INTENT,ATTEMPT
        from paperflow_prefect.profile_recovery import selected_model,_read_complete
        attachment_work=next((root/'data').glob('papers/*/attachments/*/*/description.json')).parent
        attachment_row=paper['attachments'][0]
        request_sha=read_json(attachment_work/'model/request-state.json')['request_sha256']
        authority=root/'legacy-authorization.json'
        item=dict(kind='attachment',paper_id=paper['paper_id'],work=str(attachment_work),model=str(attachment_work/'model'),
            source_sha256=attachment_row['sha256'],source=attachment_row,request_id='fixture-original',parent_request_sha256=request_sha,
            protected_hashes={str(attachment_work/name):sha_file(attachment_work/name) if (attachment_work/name).is_file() else None for name in PARENT_FILES})
        write_json(authority,dict(schema='desktop108-retry54-authorized-scope/1',max_additional_attempts_per_failed_request=1,requests=[item]))
        intent=dict(schema='attachment-engineering-attempt/1',root_work=str(attachment_work),attempt=1,max_additional_attempts=1,
            paper_id=paper['paper_id'],source=attachment_row,parent_request_sha256=request_sha,request_id='fixture-original',
            authorization_path=str(authority),authorization_sha256=sha_file(authority))
        child=attachment_work/'revisions'/ATTEMPT; write_json(attachment_work/INTENT,intent); write_json(child/'revision.json',intent)
        shutil.copytree(attachment_work/'model',child/'model')
        assert selected_model(attachment_work)==child/'model'
        chosen,reply=_read_complete(attachment_work)
        assert chosen==child/'model' and reply==(child/'model/response.txt').read_text(encoding='utf8')
        from paperflow_prefect.pipeline_ledger import EvidenceChanged
        for name,target in [('source',Path(paper['body']['path'])),('config',config),
                            ('receipt',Path(received.path)),('model_config',root/'model-config.json'),
                            ('raw_response',next((root/'data').glob('papers/*/body/*/model/**/response.txt')))]:
            saved=target.read_bytes(); target.write_bytes(b'changed')
            try:
                try: await execute_stage(env('profile'))
                except EvidenceChanged: pass
                else: raise AssertionError(name+' mismatch accepted')
            finally: target.write_bytes(saved)
        assert len(calls)==expected
        write_json(root/'native-result.json',dict(prepare=prepare.state,attachment=attachment.state,profile=profile.state,
            loopback_http_calls=len(calls),no_batches=True,transitions=transitions,source_config_mismatch_calls=0))
    finally:
        srv.close(); await srv.wait_closed()
        for writer in sessions: writer.close()

async def offline(root):
    deny_external()
    import paperflow_prefect.pipeline_runtime as runtime
    from paperflow_prefect.pipeline_execution import _models
    from paperflow_prefect.pipeline_contracts import StageEnvelope
    config=root/'config.json'
    os.environ.update(PAPERFLOW_PIPELINE_CONFIG=str(config),PAPERFLOW_PIPELINE_CONFIG_SHA256=sha_file(config),PREFECT_API_URL='http://127.0.0.1:9/api',PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false')
    env=StageEnvelope.from_dict(read_json(root/'profile-envelope.json'))
    ctx=runtime.load_stage_context(config,env)
    assert ctx.ledger.read_operation(env.operation_key)['state']=='running'
    @asynccontextmanager
    async def scope(name):
        assert name=='local', 'offline parsing tried to acquire HTTP'
        yield
    ctx.scope=scope
    token=runtime.active_context.set(ctx)
    try:
        result=await _models(ctx)
        assert result.state=='passed'
        # A fresh claim classifies the crashed reserved send unknown; offline
        # parse does not silently clear the original operation or resend.
        import psutil
        owner=dict(instance_id='offline',pid=os.getpid(),process_create_time=psutil.Process().create_time(),task_run_id='offline')
        from paperflow_prefect.pipeline_ledger import OutcomeUnknown
        try:
            with ctx.ledger.claim(env,owner): raise AssertionError('unknown allowed')
        except OutcomeUnknown: pass
        assert ctx.ledger.read_operation(env.operation_key)['state']=='unknown'
        write_json(root/'offline-result.json',dict(parsed=True,added_http_calls=0,state='unknown'))
    finally: runtime.active_context.reset(token)

if __name__=='__main__':
    mode=sys.argv[2] if len(sys.argv)>2 else ''
    root=Path(sys.argv[1]).resolve()
    asyncio.run(offline(root) if mode=='offline' else run(root,mode in ('partition','adoption-partition'),mode=='crash',mode.startswith('adoption'),mode=='adoption-recovery',mode=='shared-body'))
