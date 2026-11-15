"""Frontend-facing waiting-count API; constructing it starts no intake work."""

from datetime import datetime, timezone
import sqlite3

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
def create_app(queue, *, max_age_seconds=60, enable_claims=False, ticket_observer=None):
    app = FastAPI(title='本地接收等待统计', version='1.0')
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
        allow_methods=['GET'], allow_headers=['*'])

    @app.get('/api/v1/local-intake/preprocessing/summary')
    def waiting_summary(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        try:
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            snapshot['prefect_ticket_summary'] = (
                ticket_observer.summary(snapshot) if ticket_observer is not None else
                {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}})
            return snapshot
        except (OSError, sqlite3.Error):
            return JSONResponse(
                status_code=503,
                headers={'Cache-Control': 'no-store'},
                content={'scope': 'all_tasks', 'waiting_count': None,
                         'waiting_available': False,
                         'as_of': datetime.now(timezone.utc).isoformat(),
                         'upstream_scan_completed_at': None,
                         'upstream_scan_status': 'unavailable',
                         'receiver_status': 'unknown', 'receiver_heartbeat_at': None,
                         'oldest_verified_at': None, 'needs_review_count': None,
                         'preprocessing_handoff_connected': False,
                         'error': 'local_state_unavailable'})

    @app.get('/api/v1/local-intake/llm/waiting')
    def llm_waiting(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        try:
            return {'available': True, **queue.list_waiting_llm()}
        except (OSError, sqlite3.Error, ValueError):
            return JSONResponse(status_code=503, headers={'Cache-Control': 'no-store'},
                                content={'available': False, 'waiting_count': None, 'items': []})

    return app
