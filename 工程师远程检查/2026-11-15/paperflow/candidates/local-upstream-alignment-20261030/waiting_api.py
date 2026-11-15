"""Frontend-facing waiting-count API; constructing it starts no intake work."""

from datetime import datetime, timezone
import sqlite3

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


def create_app(queue, *, max_age_seconds=60):
    app = FastAPI(title='本地接收等待统计', version='1.0')
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
        allow_methods=['GET'], allow_headers=['*'])

    @app.get('/api/v1/local-intake/preprocessing/summary')
    def waiting_summary(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        try:
            return queue.snapshot(max_age_seconds=max_age_seconds)
        except (OSError, sqlite3.Error):
            return JSONResponse(
                status_code=503,
                headers={'Cache-Control': 'no-store'},
                content={'scope': 'all_tasks', 'waiting_count': None,
                         'waiting_available': False,
                         'as_of': datetime.now(timezone.utc).isoformat(),
                         'upstream_scan_completed_at': None,
                         'oldest_verified_at': None, 'needs_review_count': None,
                         'preprocessing_handoff_connected': False,
                         'error': 'local_state_unavailable'})

    return app
