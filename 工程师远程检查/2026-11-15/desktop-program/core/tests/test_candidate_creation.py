"""Execute the deployed add_chunk body; replace only external DB/model/storage."""
import ast
import asyncio
import datetime
import sys
import types
from pathlib import Path
from types import SimpleNamespace as NS

import pytest


class Vector(list):
    def __mul__(self, factor):return Vector(v*factor for v in self)
    __rmul__=__mul__
    def __add__(self, other):return Vector(a+b for a,b in zip(self,other))
    def tolist(self):return list(self)


@pytest.mark.parametrize('status,requested,expected',[
    ('0',None,0),('0',True,0),('1',False,0),('1',None,1),('1',True,1),('unknown',None,0)])
@pytest.mark.parametrize('image',[False,True])
def test_created_index_record_respects_disabled_document(monkeypatch,status,requested,expected,image):
    module=ast.parse(Path('server-patch/chunk_api.py').read_text(encoding='utf-8'))
    node=next(n for n in module.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='add_chunk')
    node.decorator_list=[]
    request={'content':'candidate evidence'}
    if requested is not None:request['available']=requested
    if image:request['image_base64']='synthetic image'
    async def body():return request
    nlp=types.ModuleType('rag.nlp')
    nlp.rag_tokenizer=NS(tokenize=lambda s:s,fine_grained_tokenize=lambda s:s)
    nlp.search=NS(index_name=lambda tenant:'index_'+tenant)
    monkeypatch.setitem(sys.modules,'rag',types.ModuleType('rag'))
    monkeypatch.setitem(sys.modules,'rag.nlp',nlp)
    inserted=[]
    namespace=dict(KnowledgebaseService=NS(accessible=lambda **k:True),
        _get_dataset_tenant_id=lambda ds:'tenant',
        DocumentService=NS(query=lambda **k:[NS(id='doc',kb_id='ds',name='candidate',status=status)],
            get_embd_id=lambda did:'embed',increment_chunk_num=lambda *a:None),
        get_request_json=body,is_content_empty=lambda s:not s,
        get_error_data_result=lambda *a,**k:dict(error=k),datetime=datetime,
        xxhash=NS(xxh64=lambda b:NS(hexdigest=lambda:'chunk-id')),
        _decode_chunk_image_base64=lambda value:(b'image',None),
        _store_chunk_image_or_error=lambda *a:None,
        resolve_model_config=lambda *a:{},LLMType=NS(EMBEDDING=NS(value='embedding')),
        TenantLLMService=NS(model_instance=lambda c:object()),
        _encode_with_request_user=lambda *a:([Vector([1.,0.]),Vector([0.,1.])],2),
        settings=NS(docStoreConn=NS(insert=lambda rows,*a:inserted.extend(rows))),
        Chunk=lambda **k:k,get_result=lambda **k:k)
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<deployed-add_chunk>','exec'),namespace)
    result=asyncio.run(namespace['add_chunk']('tenant','ds','doc'))
    assert result['data']['chunk']['id']=='chunk-id'
    assert len(inserted)==1
    assert inserted[0].get('available_int')==expected
    assert inserted[0]['q_2_vec']==[.1,.9]
    if image:assert inserted[0]['img_id']=='ds-chunk-id'
