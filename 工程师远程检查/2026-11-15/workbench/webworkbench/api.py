"""Reuse the 8089 session/security/static surface with managed-only routes."""
from contextlib import asynccontextmanager
import hmac
from pathlib import Path
import sqlite3
import uuid
import anyio

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.concurrency import run_in_threadpool

from paperflow.workbench import ControlConflict, MAX_UPLOAD
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
            maximum = MAX_UPLOAD if request.url.path == '/api/inputs/upload' else 65536
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
        return {'manager':workbench.control_state()}

    @app.get('/api/inputs')
    def inputs():
        return workbench.inputs()

    @app.post('/api/inputs/upload')
    async def upload(request: Request, name: str):
        workbench.check_name(name)
        folder = workbench.root / 'uploads' / uuid.uuid4().hex
        await run_in_threadpool(folder.mkdir,parents=True)
        path = folder / name
        count = 0
        try:
            async with await anyio.open_file(path,'xb') as stream:
                async for block in request.stream():
                    count += len(block)
                    if count > MAX_UPLOAD:
                        raise HTTPException(413,'单个文件不能超过 2 GiB')
                    await stream.write(block)
            if not count:
                raise ValueError('不能选择空文件')
            row = await run_in_threadpool(workbench.record_upload,name,path)
            return {k:row[k] for k in ('id','name','bytes','created')}
        except BaseException:
            path.unlink(missing_ok=True)
            if folder.exists() and not any(folder.iterdir()):
                folder.rmdir()
            raise

    @app.post('/api/start')
    def start(value: dict):
        return workbench.start(value.get('inputs'),value.get('request_id'),value.get('epoch'),value.get('generation'))

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
