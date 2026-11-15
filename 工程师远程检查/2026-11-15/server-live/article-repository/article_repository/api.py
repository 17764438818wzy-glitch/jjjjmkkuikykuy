"""LAN article UI API. No login or processing controls."""
from pathlib import Path
import re
from typing import Literal

from fastapi import FastAPI,HTTPException,Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel,ConfigDict
from psycopg.errors import LockNotAvailable
from starlette.background import BackgroundTask
from .profiles import display,details,reference,file_summary
from .ratings import apply_rating_reference


class Edit(BaseModel):
    model_config=ConfigDict(extra='forbid')
    revision:int
    changes:dict


class RatingWrite(BaseModel):
    model_config=ConfigDict(extra='forbid')
    operation_id:str
    run_id:str
    request_id:str
    revision:int
    research_type:str
    source_files:list[dict]
    rubric_version:str
    rubric_sha256:str
    model:str
    rating:dict
    evidence_basis:Literal['original','profile']='original'
    profile_sha256:str|None=None
    profile_snapshot:dict|None=None


class ClassificationWrite(BaseModel):
    model_config=ConfigDict(extra='forbid')
    operation_id:str
    request_id:str
    run_id:str
    revision:int
    profile_sha256:str
    research_type:str
    model:str


class RatingClaim(ClassificationWrite):
    stage:Literal['rating','classification']
    rubric_sha256:str
    request_sha256:str


class ClaimTerminal(BaseModel):
    model_config=ConfigDict(extra='forbid')
    run_id:str
    request_id:str
    operation_id:str
    native_state:dict


class ClaimBegin(BaseModel):
    model_config=ConfigDict(extra='forbid')
    run_id:str
    request_id:str
    operation_id:str
    request_sha256:str
    model:str


def create_app(catalog,files,sync,*,site=None,lifespan=None):
    catalog.profile_resolver=getattr(sync,'read_profile',None)
    app=FastAPI(title='文章仓库',lifespan=lifespan,docs_url=None,redoc_url=None)

    @app.exception_handler(LockNotAvailable)
    async def busy_sync(request,error):
        from fastapi.responses import JSONResponse
        return JSONResponse({'detail':'此文章正在同步，请稍后刷新'},status_code=409)

    @app.exception_handler(LookupError)
    async def missing(request,error):
        from fastapi.responses import JSONResponse
        return JSONResponse({'detail':str(error)},status_code=404)

    @app.exception_handler(ValueError)
    async def conflict(request,error):
        from fastapi.responses import JSONResponse
        return JSONResponse({'detail':str(error)},status_code=409)

    @app.get('/api/library/health')
    def health(): return {'status':'ok','articles':catalog.search(page_size=1)['total']}

    @app.get('/api/library/majors')
    def majors(): return catalog.majors()

    @app.get('/api/library/statistics')
    def statistics(): return catalog.statistics()

    @app.get('/api/library/years')
    def years(): return catalog.years()

    @app.get('/api/library/articles')
    def search(keyword:str=Query('',max_length=200),major:str='',
               level:Literal['','correspondence','bachelor','key_bachelor','unclassified']='',
               file_condition:Literal['','original','attachment','both','unverified']='',
               attachment_ext:str=Query('',pattern=r'^[a-zA-Z0-9]{0,12}$'),
               research_type:Literal['','engineering_system','experimental_testing','survey_case_applied','theoretical_modeling','textual_normative','creative_design','unclassified']='',
               year:str=Query('',pattern=r'^(?:|unclassified|(?:19|20)[0-9]{2})$'),
               page:int=Query(1,ge=1,le=100000),page_size:int=Query(30,ge=1,le=100)):
        return catalog.search(keyword=keyword,major=major,level=level,file_condition=file_condition,
                              attachment_ext=attachment_ext,research_type=research_type,year=year,page=page,page_size=page_size)

    @app.get('/api/library/articles/{aid}')
    def detail(aid:str):
        value=catalog.detail(aid)
        if value['profile']['legacy_summary'] and hasattr(sync,'read_profile'):
            try:
                source=sync.read_profile(value)
                if source:
                    manual={k:value['fields'][k] for k in value['manual_fields']}
                    value['fields']=display(source)|manual
                    value['reference']=reference(source,manual)
                    value['profile']=details(source,manual)
                    catalog.observe_rating_profile(aid,value['revision'],source)
                    value['rating']=catalog.rating(aid)['rating']
            except Exception:
                value['profile_notice']='完整画像暂不可读取，当前展示目录已保存内容。'
        value['files']=files.observe(value['files'])
        catalog.observe_files(aid,value['files'])
        value['file_summary']=file_summary(value['files'],value['inventory_complete'])
        for file in value['files']:
            file.pop('relative',None)
            file['url']=f'/api/library/articles/{aid}/files/{file["id"]}'
        return apply_rating_reference(value)

    @app.get('/api/library/rating-discovery')
    def rating_discovery(after:int=Query(0,ge=0),limit:int=Query(100,ge=1,le=100)):
        return catalog.rating_discovery(after=after,limit=limit)

    @app.get('/api/library/rating-discovery/bootstrap')
    def rating_bootstrap(since:str='',cursor:str=Query('',max_length=3000),limit:int=Query(100,ge=1,le=100)):
        return catalog.rating_bootstrap(since,cursor=cursor,limit=limit)

    @app.post('/api/library/articles/{aid}/research-type')
    def classify(aid:str,body:ClassificationWrite):
        return catalog.classify(aid,body.model_dump())

    @app.get('/api/library/articles/{aid}/research-type')
    def classification(aid:str,operation_id:str=Query(...,min_length=1,max_length=200)):
        return catalog.classification(aid,operation_id)

    @app.post('/api/library/articles/{aid}/rating-claim')
    def rating_claim(aid:str,body:RatingClaim):
        return catalog.claim_rating(aid,body.model_dump())

    @app.post('/api/library/articles/{aid}/rating-claim/terminal')
    def claim_terminal(aid:str,body:ClaimTerminal):
        return catalog.finish_rating_claim(aid,body.model_dump())

    @app.post('/api/library/articles/{aid}/rating-claim/begin')
    def claim_begin(aid:str,body:ClaimBegin):
        return catalog.begin_rating_claim(aid,body.model_dump())

    @app.post('/api/library/articles/{aid}/rating')
    def rate(aid:str,body:RatingWrite):
        return catalog.rate(aid,body.model_dump(exclude_unset=True))

    @app.get('/api/library/articles/{aid}/rating-input')
    def rating_input(aid:str):
        return catalog.rating_input(aid)

    @app.get('/api/library/articles/{aid}/rating')
    def rating(aid:str,operation_id:str=Query('',max_length=200)):
        return catalog.rating(aid,operation_id)

    def synchronize(aid):
        return catalog.synchronize(aid,sync)

    @app.patch('/api/library/articles/{aid}')
    def edit(aid:str,body:Edit):
        catalog.edit(aid,body.revision,body.changes)
        return synchronize(aid)

    @app.post('/api/library/articles/{aid}/sync')
    def resync(aid:str): return synchronize(aid)

    @app.get('/api/library/articles/{aid}/files/{file_id}')
    def download(aid:str,file_id:str):
        item=catalog.detail(aid)
        row=next((f for f in item['files'] if f['id']==file_id),None)
        if not row: raise LookupError('未找到原文或附件')
        try: path=files.download(row)
        except FileNotFoundError: raise HTTPException(404,'原件当前不可用')
        except OSError: raise HTTPException(503,'NAS 暂不可读取，请稍后重试')
        return FileResponse(path,filename=row.get('name') or file_id,media_type='application/octet-stream',
                            background=BackgroundTask(path.unlink,missing_ok=True))

    @app.get('/api/library/articles/{aid}/bundle')
    def bundle(aid:str):
        item=catalog.detail(aid)
        try: path=files.bundle(item)
        except OSError: raise HTTPException(503,'NAS 暂不可读取，请稍后重试')
        name=re.sub(r'[<>:"/\\|?*\x00-\x1f]','_',item['title'])[:140] or '论文'
        return FileResponse(path,filename=name+'.zip',media_type='application/zip',
                            background=BackgroundTask(path.unlink,missing_ok=True))

    if site: app.mount('/',StaticFiles(directory=site,html=True),name='article-repository')
    return app
