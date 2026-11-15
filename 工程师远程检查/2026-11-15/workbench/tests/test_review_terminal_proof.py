"""Review-only fault reproduction using archived server code and local adapters."""
import ast
import asyncio
import copy
import datetime
import json
import re
import sys
import tarfile
import threading
import types
from pathlib import Path
from types import SimpleNamespace as NS

from paperflow.delivery_control import DeliveryActions
from test_candidate_creation import Vector
from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_reconciliation import unknown


def test_http_success_with_pending_backend_write_is_not_terminal_proof(delivery_case,tmp_path,monkeypatch):
    client,workflow,action=unknown(delivery_case,tmp_path)
    (dataset_id,document_id),document=next(iter(client.docs.items()))
    chunk=client.chunks[document_id][0]
    with tarfile.open('evidence/deployed-source.tar.gz') as archive:
        member=next(m for m in archive.getmembers() if m.name.endswith('rag/utils/es_conn.py'))
        server_source=archive.extractfile(member).read().decode('utf-8')
    module=ast.parse(server_source)
    insert=next(n for cls in module.body if isinstance(cls,ast.ClassDef)
                for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='insert')
    class ConnectionTimeout(Exception):pass
    pending=[]
    def bulk(**kwargs):
        # A timed-out client request is still queued at its downstream backend.
        pending.extend(copy.deepcopy(kwargs['operations'][1::2]))
        raise ConnectionTimeout('client timeout does not cancel queued backend work')
    namespace=dict(copy=copy,re=re,ATTEMPT_TIME=2,ConnectionTimeout=ConnectionTimeout,
                   time=NS(sleep=lambda seconds:None))
    exec(compile(ast.Module(body=[insert],type_ignores=[]),'<archived-es-insert>','exec'),namespace)
    adapter=NS(es=NS(bulk=bulk),logger=NS(exception=lambda *a:None,warning=lambda *a:None),
               _connect=lambda:None)
    insert_results=[]
    def actual_insert(rows,index,kb):
        result=namespace['insert'](adapter,rows,index,kb)
        insert_results.append(result)
        return result

    # Run the actual candidate API body; substitute external DB/model/backend only.
    api=ast.parse(Path('server-patch/chunk_api.py').read_text(encoding='utf-8'))
    add=next(n for n in api.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='add_chunk')
    add.decorator_list=[]
    async def body():return {'content':chunk['content']}
    nlp=types.ModuleType('rag.nlp')
    nlp.rag_tokenizer=NS(tokenize=lambda value:value,fine_grained_tokenize=lambda value:value)
    nlp.search=NS(index_name=lambda tenant:'index_'+tenant)
    monkeypatch.setitem(sys.modules,'rag',types.ModuleType('rag'))
    monkeypatch.setitem(sys.modules,'rag.nlp',nlp)
    api_namespace=dict(KnowledgebaseService=NS(accessible=lambda **kwargs:True),
        _get_dataset_tenant_id=lambda ds:'tenant',
        DocumentService=NS(query=lambda **kwargs:[NS(id=document_id,kb_id=dataset_id,
            name=document['name'],status=document['status'])],get_embd_id=lambda did:'embed',
            increment_chunk_num=lambda *args:None),
        get_request_json=body,is_content_empty=lambda value:not value,
        get_error_data_result=lambda *args,**kwargs:dict(error=kwargs),datetime=datetime,
        xxhash=NS(xxh64=lambda data:NS(hexdigest=lambda:chunk['id'])),
        resolve_model_config=lambda *args:{},LLMType=NS(EMBEDDING=NS(value='embedding')),
        TenantLLMService=NS(model_instance=lambda config:object()),
        _encode_with_request_user=lambda *args:([Vector([1.,0.]),Vector([0.,1.])],2),
        settings=NS(docStoreConn=NS(insert=actual_insert)),Chunk=lambda **kwargs:kwargs,
        get_result=lambda **kwargs:dict(code=0,**kwargs))
    exec(compile(ast.Module(body=[add],type_ignores=[]),'<candidate-add-chunk>','exec'),api_namespace)
    response=asyncio.run(api_namespace['add_chunk']('tenant',dataset_id,document_id))
    assert response['code']==0 and insert_results==[[]] and len(pending)==2
    assert all(row['available_int']==0 for row in pending)

    # Capture the real production wrapper's fact, rather than inventing terminal=True.
    facts=[]
    class CaptureTransport:
        execution_id=action['execution_id']
        def perform(self,kind,resource,invoke,**kwargs):
            facts.append(invoke(threading.Event()))
    actions=DeliveryActions(CaptureTransport(),workflow.config,workflow.identity)
    actions.run('rag_write',{'operation':'http','write_operation':'chunk_add'},
                lambda cancel:response['data'])
    fact=facts[0]
    workflow.actions.m.record_result(action['execution_id'],action['action_id'],action['epoch'],
                                    'complete',fact)
    assert workflow.actions.m.delivery.boundary is None
    result=workflow.run()
    summary=dict(backend_attempts=len(pending),api_code=response['code'],
                 insert_return=insert_results[0],terminal_fact=fact,
                 state_before_late_write=result['state'],
                 unresolved_remaining=len(workflow.ledger.unresolved(workflow.identity['source_id'],workflow.code)),
                 available_before_late_write=chunk.get('available'))
    # A queued old insertion completes after the successful readbacks and activation.
    chunk['available']=bool(pending[-1]['available_int'])
    summary['available_after_late_write']=chunk['available']
    Path('evidence/review-terminal-result.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    assert result['state']=='blocked', summary
