"""Isolated first-step control, summary, and task-bound tus gateway."""
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from paperflow_prefect.settings import Settings


app = FastAPI(title='论文第一步统计', version='1.0')
app.add_middleware(CORSMiddleware, allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
                   allow_methods=['GET', 'POST', 'DELETE', 'HEAD', 'PATCH', 'OPTIONS'], allow_headers=['*'],
                   expose_headers=['Location', 'Upload-Offset', 'Upload-Length', 'Upload-Metadata',
                                   'Tus-Resumable', 'Tus-Version', 'Tus-Extension', 'Tus-Max-Size'])
settings = Settings.load()
if settings.values.get('first_step_order_model') == 'v2':
    from paperflow_prefect_v2.group_task_summary import install_group_summary_routes
    from paperflow_prefect_v2.first_step_control_api import install_control_routes
    from paperflow_prefect_v2.first_step_scheduler import PrefectScheduler
else:
    from paperflow_prefect.group_task_summary import install_group_summary_routes
    from paperflow_prefect.first_step_control_api import install_control_routes
    from paperflow_prefect.first_step_scheduler import PrefectScheduler
control = install_control_routes(app, settings.path('classification_root'), settings.path('data_root'),
    scheduler=PrefectScheduler(settings['prefect_api_url']),
    receipt_root=settings.path('receipt_root'),
    local_intake_queue_db=(os.environ.get('PAPERFLOW_LOCAL_INTAKE_DB')
                           or settings.values.get('local_intake_queue_db')),
    tus_base_url=os.environ.get('PAPERFLOW_TUS_PUBLIC_URL', 'http://127.0.0.1:14203/tus/files/'),
    tus_backend_url=os.environ.get('PAPERFLOW_TUS_BACKEND_URL', 'http://127.0.0.1:11080/files/'))
install_group_summary_routes(app, settings.path('classification_root'), refresh_task=control.refresh_task)


@app.get('/health')
def health():
    return {'status': 'ok', 'scope': 'first-step-summary'}
