"""Test-only API host: current RAGFlow app, no startup migrations or workers."""
import hashlib
import json
import os
from pathlib import Path
import re


def allowed_request(method,path,body,datasets):
    if method=='POST' and path=='/api/v1/retrieval':
        ids=body.get('dataset_ids') if isinstance(body,dict) else None
        return bool(isinstance(ids,list) and ids and all(isinstance(x,str) and x in datasets for x in ids))
    if method=='GET' and path.startswith('/api/v1/documents/images/'):
        image_id=path.removeprefix('/api/v1/documents/images/')
        return any(image_id.startswith(ds+'-') and '/' not in image_id for ds in datasets)
    match=re.fullmatch(r'/api/v1/datasets/([^/]+)/documents(?:/([^/]+))?(?:/chunks(?:/([^/]+))?)?',path)
    if not match or match[1] not in datasets or match[2] in ('parse','upload_info'):
        return False
    return method in ('GET','POST','PATCH','DELETE')


def main():
    os.environ.setdefault('LITELLM_LOCAL_MODEL_COST_MAP','True')
    from quart import request,jsonify
    from api.apps import app
    identity=json.loads(Path('/candidate/scope.json').read_text())
    datasets=set(identity['dataset_ids'])
    assert datasets and all(re.fullmatch('[0-9a-f]{32}',ds) for ds in datasets)
    fingerprint=hashlib.sha256(Path('/ragflow/api/apps/restful_apis/chunk_api.py').read_bytes()).hexdigest()
    async def scope_guard():
        if request.path=='/pf-stage4/identity' and request.method=='GET':
            return jsonify(dict(candidate=True,chunk_api_sha256=fingerprint,**identity))
        match=re.fullmatch(r'/pf-stage4/index/([0-9a-f]{32})/([0-9a-f]+)',request.path)
        if match and request.method=='GET' and match[1] in datasets:
            # Test-only, read-only evidence from the deployed index connection.
            from api.db.services.knowledgebase_service import KnowledgebaseService
            from common import settings
            from rag.nlp import search
            found,kb=KnowledgebaseService.get_by_id(match[1])
            if not found:return jsonify(dict(error='dataset missing')),404
            row=settings.docStoreConn.get(match[2],search.index_name(kb.tenant_id),[match[1]])
            if not row or match[1] not in row.get('kb_id',[]):return jsonify(dict(error='chunk missing')),404
            vectors={k:dict(dimensions=len(v),nonzero=any(float(x)!=0 for x in v))
                     for k,v in row.items() if re.fullmatch(r'q_\d+_vec',k) and isinstance(v,list)}
            return jsonify(dict(dataset_id=match[1],chunk_id=match[2],document_id=row.get('doc_id'),
                available_int=row.get('available_int'),vectors=vectors,image_id=row.get('img_id'),
                content_sha256=hashlib.sha256(row['content_with_weight'].encode()).hexdigest()))
        body=await request.get_json(silent=True) if request.is_json else None
        if not allowed_request(request.method,request.path,body,datasets):
            return jsonify(dict(code=403,message='outside stage4 test datasets')),403
    app.before_request_funcs.setdefault(None,[]).insert(0,scope_guard)
    # Importing the existing app initializes connections. Do not call run_server:
    # it initializes production tables/data and starts unrelated background work.
    app.run(host='0.0.0.0',port=9380,debug=False,use_reloader=False)


if __name__=='__main__':main()
