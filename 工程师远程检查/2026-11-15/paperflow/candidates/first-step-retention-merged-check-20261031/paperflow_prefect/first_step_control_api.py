"""Upload and task control routes for the independent first step."""
from fastapi import HTTPException, Query, Request
from pydantic import BaseModel, Field, StrictBool, StrictInt
from starlette.concurrency import run_in_threadpool
from starlette.responses import StreamingResponse
import httpx
import logging
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from typing import Literal
from filelock import Timeout as FileLockTimeout

from .first_step_control import ControlError, FirstStepControl


class UploadRequest(BaseModel):
    root_name: str


class LocalSourceRequest(BaseModel):
    path: str


class TaskRequest(BaseModel):
    name: str
    request_id: str | None = None


class SaveRequest(BaseModel):
    name: str | None = None


class StartRequest(BaseModel):
    expected_revision: int


class ReserveLocalRetention(BaseModel):
    group_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    paper_id: str = Field(min_length=1, max_length=128)
    receipt_id: str = Field(pattern=r'^[0-9a-f]{64}$')
    consumer_id: Literal['local-intake']
    request_id: uuid.UUID


class LocalRetentionIdentity(BaseModel):
    consumer_id: Literal['local-intake']
    receipt_id: str = Field(pattern=r'^[0-9a-f]{64}$')


class ReleaseLocalRetention(LocalRetentionIdentity):
    reason: Literal['processed', 'cancelled']
    confirm_not_in_use: StrictBool


class ManifestFile(BaseModel):
    relative_path: str
    size: int


class ManifestRequest(BaseModel):
    files: list[ManifestFile]


class SealRequest(BaseModel):
    expected_files: StrictInt = Field(gt=0)


def browse_local_folder():
    """Open the operating system's local folder chooser without reading files."""
    executable = sys.executable
    if os.name == 'nt' and executable.lower().endswith('pythonw.exe'):
        executable = executable[:-11] + 'python.exe'
    script = ("import tkinter as tk; from tkinter import filedialog; "
              "root=tk.Tk(); root.withdraw(); root.attributes('-topmost', True); "
              "path=filedialog.askdirectory(parent=root, title='选择本机资料文件夹', mustexist=True); "
              "print(path, flush=True); root.destroy()")
    kwargs = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
    try:
        result = subprocess.run([executable, '-X', 'utf8', '-c', script], capture_output=True,
                                text=True, encoding='utf-8', timeout=600, check=True, **kwargs)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ControlError('无法打开本机文件夹选择窗口，请直接输入路径', 500) from exc
    return {'path': result.stdout.strip() or None}


def install_control_routes(app, output_dir, data_dir, *, scheduler=None,
                           tus_base_url='http://127.0.0.1:11080/files/', tus_backend_url=None,
                           receipt_root=None, local_intake_queue_db=None):
    control = FirstStepControl(output_dir, data_dir, scheduler=scheduler, tus_base_url=tus_base_url,
                               tus_backend_url=tus_backend_url, receipt_root=receipt_root,
                               local_intake_queue_db=local_intake_queue_db)

    def call(method, *args, **kwargs):
        try:
            return method(*args, **kwargs)
        except ControlError as exc:
            raise HTTPException(status_code=exc.status, detail=str(exc)) from None

    def call_retention(method, *args, **kwargs):
        try:
            return call(method, *args, **kwargs)
        except (sqlite3.Error, OSError, FileLockTimeout):
            raise HTTPException(status_code=503, detail='接收引用或任务状态暂时不可核验') from None

    if tus_backend_url is not None:
        async def forward_tus(request: Request, tus_id: str = ''):
            if tus_id and not re.fullmatch(r'[0-9a-f]{32}', tus_id):
                raise HTTPException(status_code=404, detail='上传编号无效')
            task_id = request.headers.get('x-task-id')
            transfer = False
            upstream = None
            try:
                if request.method != 'OPTIONS':
                    task_id = await run_in_threadpool(call, control.begin_upload_transfer, task_id,
                                                      request.headers.get('x-folder-upload-id'))
                    transfer = True
                    await run_in_threadpool(call, control.tus.authorize, request.method, tus_id, request.headers)
                permitted = {'tus-resumable', 'upload-length', 'upload-metadata', 'upload-offset',
                             'content-type', 'content-length', 'x-request-id', 'x-task-id',
                             'x-folder-upload-id', 'x-writer-id'}
                headers = {key: value for key, value in request.headers.items() if key.lower() in permitted}
                if request.method == 'POST':
                # One ID per incoming POST; tusd may retry its hook with this same ID.
                    headers['x-gateway-invocation'] = uuid.uuid4().hex
                upstream = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0), trust_env=False)
                body = request.stream() if request.method in {'POST', 'PATCH'} else None
                outgoing = upstream.build_request(request.method, control.tus_backend_url + tus_id,
                                                  headers=headers, content=body)
                response = await upstream.send(outgoing, stream=True)
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
                        raise HTTPException(status_code=502, detail='上传服务返回的地址无效')
                    response_headers['location'] = control.tus_base_url + location_id
                async def stream():
                    try:
                        async for chunk in response.aiter_raw():
                            yield chunk
                    finally:
                        await response.aclose()
                        await upstream.aclose()
                        if transfer:
                            control.end_upload_transfer(task_id)
                return StreamingResponse(stream(), status_code=response.status_code, headers=response_headers)
            except BaseException as exc:
                if upstream is not None:
                    await upstream.aclose()
                if transfer:
                    control.end_upload_transfer(task_id)
                if isinstance(exc, httpx.HTTPError):
                    raise HTTPException(status_code=503, detail='上传服务暂时不可用') from None
                raise

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

    @app.post('/api/v1/first-step/tasks/{task_id}/local-source')
    def select_local_source(task_id: str, body: LocalSourceRequest):
        return call(control.select_local_source, task_id, body.path)

    @app.post('/api/v1/first-step/local-folder-dialog')
    def local_folder_dialog():
        return call(browse_local_folder)

    @app.delete('/api/v1/first-step/tasks/{task_id}')
    def delete_task(task_id: str):
        from .first_step_delete import delete_task as remove_task
        return call_retention(remove_task, control, task_id)

    @app.post('/api/v1/first-step/tasks/{task_id}/local-retentions')
    def reserve_local_retention(task_id: str, body: ReserveLocalRetention):
        from .first_step_retention import reserve
        return call_retention(reserve, control, task_id, body.group_id, body.paper_id,
                              body.receipt_id, body.consumer_id, str(body.request_id))

    @app.get('/api/v1/first-step/tasks/{task_id}/local-retentions')
    def list_local_retentions(task_id: str, page: int = Query(1, ge=1),
                              page_size: int = Query(200, ge=1, le=200)):
        from .first_step_retention import list_retentions
        return call_retention(list_retentions, control, task_id, page, page_size)

    @app.post('/api/v1/first-step/tasks/{task_id}/local-retentions/{retention_id}/confirm')
    def confirm_local_retention(task_id: str, retention_id: uuid.UUID, body: LocalRetentionIdentity):
        from .first_step_retention import change_state
        return call_retention(change_state, control, task_id, str(retention_id), body.consumer_id,
                              body.receipt_id, action='confirm')

    @app.post('/api/v1/first-step/tasks/{task_id}/local-retentions/{retention_id}/release')
    def release_local_retention(task_id: str, retention_id: uuid.UUID, body: ReleaseLocalRetention):
        from .first_step_retention import change_state
        if body.confirm_not_in_use is not True:
            raise HTTPException(status_code=409, detail='必须确认资料已不再使用')
        return call_retention(change_state, control, task_id, str(retention_id), body.consumer_id,
                              body.receipt_id, action='release', reason=body.reason)

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
