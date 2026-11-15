"""Batch ingestion and the read protocol used by LibreChat's existing tools."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import tempfile

import anyio
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response


def create_app(library, object_root, *, token):
    if not token:
        raise ValueError('an API token is required')
    objects = Path(object_root)
    objects.mkdir(parents=True, exist_ok=True)
    app = FastAPI(title='Paperflow Qdrant Library')

    def auth(request: Request):
        if not hmac.compare_digest(request.headers.get('Authorization', ''), 'Bearer '+token):
            raise HTTPException(401, 'invalid API credential')

    def result(data): return {'code': 0, 'data': data}

    @app.get('/health')
    def health(): return {'status':'ok','backend':'qdrant','points':library.store.count_documents()}

    @app.get('/paperflow/status',dependencies=[Depends(auth)])
    def status(): return health()

    @app.post('/paperflow/folders', dependencies=[Depends(auth)])
    def ingest(batch: dict):
        try:
            return library.ingest(batch)
        except (ValueError, KeyError, TypeError) as exc:
            raise HTTPException(409, str(exc)) from exc

    def object_path(digest):
        if not re.fullmatch('[a-f0-9]{64}', digest):
            raise HTTPException(400, 'invalid object digest')
        return objects / digest[:2] / digest

    @app.put('/paperflow/objects/{digest}', dependencies=[Depends(auth)])
    async def put_object(digest: str, request: Request):
        path = object_path(digest)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.upload-')
        hashed = hashlib.sha256()
        try:
            with os.fdopen(fd, 'wb') as stream:
                async for chunk in request.stream():
                    hashed.update(chunk)
                    await anyio.to_thread.run_sync(stream.write,chunk)
                await anyio.to_thread.run_sync(stream.flush)
                await anyio.to_thread.run_sync(os.fsync,stream.fileno())
            if hashed.hexdigest() != digest:
                raise HTTPException(400, 'object content hash mismatch')
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary): os.unlink(temporary)
        return {'id':digest,'bytes':path.stat().st_size,'sha256':digest}

    @app.get('/paperflow/objects/{digest}', dependencies=[Depends(auth)])
    def get_object(digest: str):
        path = object_path(digest)
        if not path.is_file(): raise HTTPException(404, 'object not found')
        import filetype
        return FileResponse(path,media_type=filetype.guess_mime(str(path)) or 'application/octet-stream')

    @app.get('/api/v1/documents/images/{image_id}', dependencies=[Depends(auth)])
    def get_image(image_id: str):
        if re.fullmatch('[a-f0-9]{64}',image_id) and object_path(image_id).is_file():
            return get_object(image_id)
        raise HTTPException(404, 'image not found')

    @app.get('/api/v1/datasets/{dataset}/documents', dependencies=[Depends(auth)])
    def documents(dataset: str, id: str | None=None, name: str | None=None,
                  keywords: str | None=None, page: int=1, page_size: int=100):
        if page < 1 or not 1 <= page_size <= 1000: raise HTTPException(400, 'invalid pagination')
        rows = ([library.document(dataset,id)] if id else library.documents(dataset,name=name))
        rows = [d for d in rows if d is not None and (not name or d['name']==name)
                and (not keywords or keywords in d['name'])]
        if id and rows: return result({'docs':rows,'total':len(rows)})
        return result({'docs':rows[(page-1)*page_size:page*page_size],'total':len(rows)})

    @app.get('/api/v1/datasets/{dataset}/documents/{document}', dependencies=[Depends(auth)])
    def original(dataset: str, document: str):
        if library.document(dataset,document):
            return Response(library.original(dataset,document),media_type='text/plain; charset=utf-8')
        raise HTTPException(404, 'document not found')

    @app.get('/api/v1/datasets/{dataset}/documents/{document}/chunks', dependencies=[Depends(auth)])
    def chunks(dataset: str, document: str, page: int=1, page_size: int=100, chunk_ids: str | None=None):
        if page<1 or not 1<=page_size<=1000: raise HTTPException(400,'invalid pagination')
        if library.document(dataset,document):
            rows=library.chunks(dataset,document,ids=chunk_ids.split(',') if chunk_ids else None)
            return result({'chunks':rows[(page-1)*page_size:page*page_size],'total':len(rows)})
        raise HTTPException(404, 'document not found')

    @app.get('/api/v1/datasets/{dataset}/documents/{document}/structure/graph', dependencies=[Depends(auth)])
    def structure(dataset: str, document: str):
        doc=library.document(dataset,document)
        if doc is None:
            raise HTTPException(404, 'document not found')
        tree=json.loads(doc['meta_fields'].get('two_pass_tree_json','{}'))
        rows=library.chunks(dataset,document)
        nodes=tree.get('nodes',[])
        names={n['id']:str(n.get('title') or n['id'])+' ['+n['id']+']' for n in nodes}
        entities=[{'name':names[n['id']],'type':'title','source_chunk_ids':[
            r['id'] for r in rows if n['id'] in r['section_ids']]} for n in nodes]
        relations=[{'from':names[n['parent_id']],'to':names[n['id']],'type':'contains'}
                   for n in nodes if n.get('parent_id') in names]
        return result({'templates':[{'kind':'page_index','template_id':'paperflow_tree',
            'template_name':'论文原始结构','entities':entities,'relations':relations}],
            'total_entities':len(entities),'returned_entities':len(entities),
            'total_relations':len(relations),'returned_relations':len(relations)})

    @app.post('/api/v1/retrieval', dependencies=[Depends(auth)])
    def retrieval(body: dict):
        datasets=body.get('dataset_ids',[])
        limit=int(body.get('page_size',8))
        page=int(body.get('page',1))
        if page<1 or not 1<=limit<=1000: raise HTTPException(400,'invalid pagination')
        count=min(page*limit,1000)
        question=body.get('question','')
        doc_ids=body.get('document_ids')
        rows=library.search(question,datasets,document_ids=doc_ids,limit=count)
        return result({'chunks':rows[(page-1)*limit:page*limit],'total':len(rows)})

    return app
