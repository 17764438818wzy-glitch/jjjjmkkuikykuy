"""Reuse the 8089 session/security/static surface with managed-only routes."""
from contextlib import asynccontextmanager
import hmac
import json
from pathlib import Path
import sqlite3
import uuid
import anyio

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.concurrency import run_in_threadpool

from paperflow.workbench import ControlConflict, MAX_UPLOAD, MAX_SELECTION_REQUEST
from .auth import Sessions

STATIC = Path(__file__).with_name('static')
COOKIE = 'paperflow_workbench_session'


def create_app(workbench, admin_path, *, allowed_hosts=None):
    sessions = Sessions(workbench.root / 'web-sessions.sqlite3', admin_path)

    @asynccontextmanager
    async def lifespan(app):
        workbench.open()
        try:
            yield
        finally:
            workbench.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts or ['localhost','127.0.0.1','[::1]','testserver'])

    @app.middleware('http')
    async def security(request: Request, next_handler):
        if request.method not in {'GET','HEAD','OPTIONS'}:
            origin = request.headers.get('origin')
            if origin and origin.rstrip('/') != str(request.base_url).rstrip('/'):
                return JSONResponse({'detail':'请求来源不匹配'}, status_code=403)
            maximum = MAX_UPLOAD if request.url.path == '/api/inputs/upload' else MAX_SELECTION_REQUEST if request.url.path == '/api/start' else 65536
            length = request.headers.get('content-length','')
            if length.isdigit() and int(length) > maximum:
                return JSONResponse({'detail':'请求过大'}, status_code=413)
        if request.url.path.startswith('/api/') and request.url.path != '/api/login':
            session = await run_in_threadpool(sessions.get,request.cookies.get(COOKIE))
            if not session:
                return JSONResponse({'detail':'请先登录'}, status_code=401)
            if request.method not in {'GET','HEAD','OPTIONS'}:
                if not hmac.compare_digest(request.headers.get('x-csrf-token',''),session['csrf']):
                    return JSONResponse({'detail':'会话校验失败，请刷新页面'}, status_code=403)
            request.state.session = session
        response = await next_handler(request)
        response.headers.update({'X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY',
            'Referrer-Policy':'same-origin','Cache-Control':'no-store',
            'Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"})
        return response

    @app.exception_handler(ControlConflict)
    async def conflict(request, exc):
        return JSONResponse({'detail':str(exc)}, status_code=409)

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse({'detail':str(exc)}, status_code=400)

    @app.exception_handler(sqlite3.OperationalError)
    async def database_busy(request, exc):
        return JSONResponse({'detail':'台账暂不可读，状态未知；暂停仍可提交'}, status_code=503)

    @app.exception_handler(RuntimeError)
    async def unavailable(request, exc):
        return JSONResponse({'detail':str(exc)}, status_code=503)

    @app.post('/api/login')
    async def login(request: Request):
        value = await request.json()
        if not isinstance(value,dict):
            raise ValueError('无效登录请求')
        token, csrf = await run_in_threadpool(sessions.login,value.get('username'),value.get('password'),request.client.host)
        response = JSONResponse({'csrf':csrf})
        response.set_cookie(COOKIE,token,httponly=True,samesite='strict',secure=request.url.scheme=='https',max_age=43200)
        return response

    @app.get('/api/session')
    def session(request: Request):
        return request.state.session

    @app.post('/api/logout')
    def logout(request: Request):
        sessions.logout(request.cookies.get(COOKIE))
        response = JSONResponse({'ok':True})
        response.delete_cookie(COOKIE)
        return response

    @app.get('/healthz')
    def health():
        control=workbench.control_state()
        return {'ok':True,'mode':'operator' if workbench.deployment else 'isolated',
                'state':control['state'],'admission_open':control['admission_open']}

    @app.get('/api/status')
    def status(batch_id: str | None = None):
        return workbench.status(batch_id)

    @app.get('/api/control')
    def control():
        return workbench.dashboard_state()

    @app.get('/api/completed')
    def completed(cursor: str | None = None, query: str = '', limit: int = 50):
        from paperflow.workbench_completed import completed
        return completed(workbench,cursor=cursor,query=query,limit=limit)

    @app.get('/api/completed/{task_id}')
    def completed_article(task_id: str):
        from paperflow.workbench_completed import completed_detail
        value=completed_detail(workbench,task_id)
        if value is None:raise HTTPException(404,'未找到已确认完成的记录；状态可能已变化，请刷新')
        return value

    @app.get('/api/inputs')
    def inputs():
        return workbench.inputs()

    @app.get('/api/nas/roots')
    def nas_roots():
        return workbench.nas.roots()

    @app.get('/api/resources/server')
    async def server_memory():
        with workbench.server_memory.reserve():
            return await run_in_threadpool(workbench.server_memory.read)

    @app.get('/api/nas/browse')
    async def nas_browse(root: str, path: str = '', offset: int = 0, limit: int = 100):
        with workbench.nas.reserve():
            return await run_in_threadpool(workbench.nas.browse,root,path,offset,limit)

    @app.post('/api/inputs/nas')
    async def nas_import(value: dict):
        with workbench.nas.reserve():
            result=await run_in_threadpool(workbench.nas.import_file,value.get('root'),value.get('path'),value.get('version'))
            return await run_in_threadpool(workbench.intake.record_preparation,value.get('request_id'),value.get('item_id'),
                result,(value.get('path')or'').rsplit('/',1)[-1],value.get('path'))

    @app.post('/api/inputs/upload')
    async def upload(request: Request, name: str, relative_path: str = '', request_id: str | None=None,item_id: str | None=None):
        workbench.check_name(name)
        relative_path = workbench.check_relative_path(name,relative_path)
        folder = workbench.root / 'uploads' / uuid.uuid4().hex
        await run_in_threadpool(folder.mkdir,parents=True)
        path = folder / name
        count = 0
        accepted = False
        try:
            async with await anyio.open_file(path,'xb') as stream:
                async for block in request.stream():
                    count += len(block)
                    if count > MAX_UPLOAD:
                        raise HTTPException(413,'单个文件不能超过 2 GiB')
                    await stream.write(block)
            if not count:
                raise ValueError('不能选择空文件')
            row = await run_in_threadpool(workbench.record_upload,name,path,relative_path=relative_path)
            accepted = True
            await run_in_threadpool(workbench.intake.record_preparation,request_id,item_id,row,name,relative_path)
            return {k:row[k] for k in ('id','name','bytes','created','relative_path')}
        except BaseException:
            if not accepted:
                path.unlink(missing_ok=True)
                if folder.exists() and not any(folder.iterdir()):
                    folder.rmdir()
            raise

    @app.post('/api/start')
    async def start(request: Request):
        body=bytearray()
        async for block in request.stream():
            if len(body)+len(block)>MAX_SELECTION_REQUEST:
                raise HTTPException(413,'所选文件清单超过请求传输上限，请分次选择')
            body.extend(block)
        try:value=json.loads(body)
        except (ValueError,UnicodeDecodeError):raise ValueError('无效选稿请求') from None
        if not isinstance(value,dict):raise ValueError('无效选稿请求')
        return await run_in_threadpool(workbench.start,value.get('inputs'),value.get('request_id'),value.get('epoch'),value.get('generation'),manifest=value.get('manifest'),first_item=value.get('first_item'))

    @app.get('/api/intake/{batch_id}')
    def intake_listing(batch_id: str,offset: int=0,limit: int=20,grouped: bool=False):
        return workbench.intake.listing(batch_id,offset,limit,grouped)

    @app.post('/api/intake/append')
    async def intake_append(value: dict):
        return await run_in_threadpool(workbench.intake.append,value.get('batch_id'),value.get('items'),value.get('epoch'),value.get('generation'))

    @app.post('/api/pause')
    def pause(value: dict):
        return workbench.pause(value.get('epoch'))

    @app.post('/api/continue')
    def resume(value: dict):
        return workbench.resume(value.get('epoch'),value.get('generation'),value.get('batch_id'))

    @app.get('/')
    def index():
        return FileResponse(STATIC / 'index.html')

    app.mount('/static', StaticFiles(directory=STATIC), name='static')
    return app
