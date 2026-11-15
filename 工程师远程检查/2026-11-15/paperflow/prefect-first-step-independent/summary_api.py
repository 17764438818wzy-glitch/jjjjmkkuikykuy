"""Read-only API for the independent first-step task summaries."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from paperflow_prefect.group_task_summary import install_group_summary_routes
from paperflow_prefect.settings import Settings


app = FastAPI(title='论文第一步统计', version='1.0')
app.add_middleware(CORSMiddleware, allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
                   allow_methods=['GET'], allow_headers=['*'])
install_group_summary_routes(app, Settings.load().path('classification_root'))


@app.get('/health')
def health():
    return {'status': 'ok', 'scope': 'first-step-summary'}
