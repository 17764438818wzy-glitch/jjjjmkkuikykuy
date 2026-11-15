"""Upload and task control routes for the independent first step."""
from fastapi import HTTPException, Query, Request
from pydantic import BaseModel, Field, StrictInt
from starlette.concurrency import run_in_threadpool
from starlette.responses import StreamingResponse
import httpx
import logging
import re
import uuid

from .first_step_control import ControlError, FirstStepControl


class UploadRequest(BaseModel):
    root_name: str


class TaskRequest(BaseModel):
    name: str
    request_id: str | None = None


class SaveRequest(BaseModel):
    name: str | None = None


class StartRequest(BaseModel):
    expected_revision: int


class ManifestFile(BaseModel):
    relative_path: str
    size: int


class ManifestRequest(BaseModel):
    files: list[ManifestFile]


class SealRequest(BaseModel):
    expected_files: StrictInt = Field(gt=0)


def install_control_routes(app, output_dir, data_dir, *, scheduler=None,
                           tus_base_url='http://127.0.0.1:11080/files/', tus_backend_url=None):
    control = FirstStepControl(output_dir, data_dir, scheduler=scheduler, tus_base_url=tus_base_url,
                               tus_backend_url=tus_backend_url)

    def call(method, *args):
        try:
            return method(*args)
        except ControlError as exc:
            raise HTTPException(status_code=exc.status, detail=str(exc)) from None

    if tus_backend_url is not None:
        async def forward_tus(request: Request, tus_id: str = ''):
            if tus_id and not re.fullmatch(r'[0-9a-f]{32}', tus_id):
                raise HTTPException(status_code=404, detail='上传编号无效')
            if request.method != 'OPTIONS':
                await run_in_threadpool(call, control.tus.authorize, request.method, tus_id, request.headers)
            permitted = {'tus-resumable', 'upload-length', 'upload-metadata', 'upload-offset',
                         'content-type', 'content-length', 'x-request-id', 'x-task-id',
                         'x-folder-upload-id', 'x-writer-id'}
            headers = {key: value for key, value in request.headers.items() if key.lower() in permitted}
            if request.method == 'POST':
                # One ID per incoming POST; tusd may retry its hook with this same ID.
                headers['x-gateway-invocation'] = uuid.uuid4().hex
            upstream = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0), trust_env=False)
            try:
                body = request.stream() if request.method in {'POST', 'PATCH'} else None
                outgoing = upstream.build_request(request.method, control.tus_backend_url + tus_id,
                                                  headers=headers, content=body)
                response = await upstream.send(outgoing, stream=True)
            except httpx.HTTPError:
                await upstream.aclose()
                raise HTTPException(status_code=503, detail='上传服务暂时不可用') from None
            if request.method == 'POST' and response.status_code >= 400:
                try:
                    await run_in_threadpool(control.tus.release_failed_create, headers['x-gateway-invocation'])
                except Exception:
                    logging.getLogger(__name__).warning('上传创建失败后的占位核查未完成')
            response_headers = {key: value for key, value in response.headers.items()
                                if key.lower() not in {'connection', 'transfer-encoding', 'server', 'date'}}
            location = response.headers.get('location')
            if location:
                location_id = location.rstrip('/').rsplit('/', 1)[-1]
                if not re.fullmatch(r'[0-9a-f]{32}', location_id):
                    await response.aclose()
                    await upstream.aclose()
                    raise HTTPException(status_code=502, detail='上传服务返回的地址无效')
                response_headers['location'] = control.tus_base_url + location_id
            async def stream():
                try:
                    async for chunk in response.aiter_raw():
                        yield chunk
                finally:
                    await response.aclose()
                    await upstream.aclose()
            return StreamingResponse(stream(), status_code=response.status_code, headers=response_headers)

        app.add_api_route('/tus/files/', forward_tus, methods=['OPTIONS', 'POST'])
        app.add_api_route('/tus/files/{tus_id}', forward_tus, methods=['OPTIONS', 'HEAD', 'PATCH'])

    @app.post('/api/v1/first-step/tasks')
    def create_task(body: TaskRequest):
        return call(control.create_task, body.name, body.request_id)

    @app.get('/api/v1/first-step/tasks/by-request/{request_id}')
    def task_by_request(request_id: str):
        return call(control.task_by_request, request_id)

    @app.get('/api/v1/first-step/tasks')
    def list_tasks(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=200)):
        return call(control.list_tasks, page, page_size)

    @app.post('/api/v1/first-step/tasks/{task_id}/save')
    def save_task(task_id: str, body: SaveRequest):
        return call(control.save_task, task_id, body.name)

    @app.post('/api/v1/first-step/tasks/{task_id}/uploads')
    def create_upload(task_id: str, body: UploadRequest):
        return call(control.create_upload, task_id, body.root_name)

    @app.post('/api/v1/first-step/uploads/{upload_id}/manifest')
    def manifest(upload_id: str, body: ManifestRequest):
        return call(control.tus.manifest, upload_id, [item.model_dump() for item in body.files])

    @app.post('/api/v1/first-step/uploads/{upload_id}/seal')
    def seal(upload_id: str, body: SealRequest):
        return call(control.tus.seal, upload_id, body.expected_files)

    @app.get('/api/v1/first-step/uploads/{upload_id}')
    def upload_state(upload_id: str, page: int = Query(1, ge=1),
                     page_size: int = Query(50, ge=1, le=200)):
        return call(control.tus.read, upload_id, page, page_size)

    @app.get('/api/v1/first-step/uploads/{upload_id}/resume-info')
    def resume_info(upload_id: str, relative_path: str):
        return call(control.tus.resume_info, upload_id, relative_path)

    @app.post('/api/v1/first-step/internal/tusd-hooks')
    def tusd_hook(body: dict):
        return call(control.tus.hook, body)

    @app.post('/api/v1/first-step/uploads/{upload_id}/finish')
    def finish_upload(upload_id: str):
        return call(control.tus.finish, upload_id)

    @app.post('/api/v1/first-step/tasks/{task_id}/start')
    def start(task_id: str, body: StartRequest):
        return call(control.start, task_id, body.expected_revision)

    @app.post('/api/v1/first-step/tasks/{task_id}/pause')
    def pause(task_id: str):
        return call(control.pause, task_id)

    return control
