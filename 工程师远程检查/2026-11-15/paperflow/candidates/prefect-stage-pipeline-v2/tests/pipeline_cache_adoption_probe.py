"""Read-only first adoption checks; every case uses a new empty pipeline ledger."""
from pathlib import Path
import shutil
from paperflow_prefect.records import read_json,write_json,sha_file

async def check(root,values,config,envelope,work,calls,*,partition=False,recovery=False):
    import os
    from paperflow_prefect.pipeline_execution import execute_stage
    from paperflow_prefect.profile_recovery import selected_model,PARENT_FILES,INTENT,ATTEMPT
    if recovery:
        completed={name:(work/'model'/name).read_bytes() for name in ('request.json','request-state.json','response.txt','response-events.jsonl')}
        state=read_json(work/'model/request-state.json'); state.update(status='request_failed_or_uncertain',http_status=503,error_type='HTTPError')
        write_json(work/'model/request-state.json',state)
        from preextract_pipeline import create_revision
        source=Path(read_json(work/'material.json')['source']['file'])
        hashes={name:sha_file(work/name) if (work/name).is_file() else None for name in PARENT_FILES}
        write_json(work/INTENT,dict(schema='profile-engineering-attempt/1',root_work=str(work),attempt=1,max_additional_attempts=1,
            source=str(source),source_sha256=sha_file(source),parent_request_sha256=state['request_sha256'],parent_hashes=hashes))
        child=create_revision(source,work,ATTEMPT,mode='rerun',reason='synthetic already-authorized recovery',recover_failed=True)
        (child/'model').mkdir()
        for name,blob in completed.items(): (child/'model'/name).write_bytes(blob)
        assert selected_model(work)==child/'model'
    model=work/'model/part-001' if partition else selected_model(work)
    material=work/'material.json'
    changes=[('raw_response',model/'response.txt',lambda blob:b'changed response'),
        ('request',model/'request.json',lambda blob:b'{}'),
        ('journal',model/'response-events.jsonl',lambda blob:b'data: {"error":{"code":"bad"}}\n\n'),
        ('missing_journal',model/'response-events.jsonl',lambda blob:None),
        ('state',model/'request-state.json',lambda blob:b'{}'),
        ('material',material,lambda blob:mutated_material(material))]
    if partition:
        changes += [('synthesis_response',work/'model/global-01-001/response.txt',lambda blob:b'changed synthesis'),
                    ('partition_plan',work/'model/partition-plan.json',lambda blob:b'{}')]
    if recovery:
        changes += [('recovery_parent',work/'model/request-state.json',lambda blob:b'{}'),
                    ('recovery_revision',model.parent/'revision.json',lambda blob:b'{}')]
    failures=[]; before_calls=len(calls)
    for index,(name,path,mutate) in enumerate(changes+[('valid',None,None)]):
        values['pipeline']['ledger_root']=str(root/('adoption-ledger-'+str(index)))
        write_json(config,values); os.environ['PAPERFLOW_PIPELINE_CONFIG_SHA256']=sha_file(config)
        baseline={str(p):sha_file(p) for p in work.rglob('*') if p.is_file() and p.suffix!='.lock'}
        original=path.read_bytes() if path else None
        if path:
            changed=mutate(original)
            if changed is None: path.unlink()
            else: path.write_bytes(changed)
        raised=None
        try:
            result=await execute_stage(envelope)
        except (ValueError,RuntimeError,OSError,KeyError,TypeError) as exc: raised=type(exc).__name__
        finally:
            if path: path.write_bytes(original)
        if name!='valid' and raised is None: failures.append(name+' adopted changed/unavailable evidence')
        if name=='valid' and raised is not None: failures.append('valid evidence rejected '+raised)
        after={str(p):sha_file(p) for p in work.rglob('*') if p.is_file() and p.suffix!='.lock'}
        if after!=baseline: failures.append(name+' rewrote native evidence')
    assert len(calls)==before_calls, 'cache adoption sent HTTP'
    assert not failures, failures
    write_json(root/'adoption-result.json',dict(checked=[name for name,_,_ in changes],valid=True,added_http_calls=0,preserved_native_bytes=True))


def mutated_material(path):
    import json
    material=read_json(path)
    material['blocks'][0]['text']+=' changed after original request'
    return json.dumps(material,ensure_ascii=False).encode('utf8')

def check_shared(root,values,config,paper,work,connection,calls):
    import io,json
    from unittest.mock import patch
    from paperflow_prefect import body
    from paperflow_prefect.records import Records
    from paperflow_prefect.unified import UnifiedBackend
    from paperflow_prefect.settings import Settings
    expected=Records(work).cached('profile')
    protected={str(p):sha_file(p) for p in work.rglob('*') if p.is_file() and p.suffix!='.lock'}
    def forbidden(*args,**kwargs): raise AssertionError('cached synchronous API tried to send')
    assert body.extract_profile(work,Path(values['template_path']),read_json(values['catalog_path']),connection,forbidden)==expected
    backend=UnifiedBackend(Settings(values)); backend.receive(paper['receipt_path'])
    with patch('paperflow_prefect.legacy.connection',return_value=connection):
        assert backend.profile(paper['inventory'])==expected
    assert protected=={str(p):sha_file(p) for p in work.rglob('*') if p.is_file() and p.suffix!='.lock'}
    original=(work/'model/response.txt').read_bytes(); (work/'model/response.txt').write_bytes(b'changed')
    try:
        for operation in (lambda:body.extract_profile(work,Path(values['template_path']),backend.catalog,connection,forbidden),
                          lambda:backend.profile(paper['inventory'])):
            with patch('paperflow_prefect.legacy.connection',return_value=connection):
                try: operation()
                except RuntimeError: pass
                else: raise AssertionError('shared cached safety guard skipped')
    finally: (work/'model/response.txt').write_bytes(original)
    # The public sync API must still build and validate a brand-new request.
    fresh=root/'sync-work'; fresh.mkdir(); shutil.copyfile(work/'material.json',fresh/'material.json')
    from preextract_request import request_once
    class Response(io.BytesIO): headers={'Content-Type':'application/json'}
    added=[]
    def invoke(request,conn,directory):
        added.append(str(directory))
        raw=json.dumps(dict(choices=[dict(message=dict(content=original.decode('utf8')),finish_reason='stop')])).encode()
        class Opener:
            def open(self,*args,**kwargs): return Response(raw)
        return request_once(request,conn,directory,opener=Opener())
    result=body.extract_profile(fresh,Path(values['template_path']),backend.catalog,connection,invoke)
    assert read_json(result['candidate'])==read_json(expected['candidate']) and len(added)==1
    assert protected=={str(p):sha_file(p) for p in work.rglob('*') if p.is_file() and p.suffix!='.lock'}
    write_json(root/'shared-result.json',dict(cached_calls=0,new_injected_requests=1,original_bytes_preserved=True))
