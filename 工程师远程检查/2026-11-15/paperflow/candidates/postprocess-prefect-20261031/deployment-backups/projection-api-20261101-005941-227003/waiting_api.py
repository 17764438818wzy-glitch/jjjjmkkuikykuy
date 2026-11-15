"""Frontend-facing waiting-count API; constructing it starts no intake work."""

from datetime import datetime, timezone
import sqlite3
from threading import Lock, Thread
import time
from typing import Literal

from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
def create_app(queue, *, max_age_seconds=60, enable_claims=False, ticket_observer=None):
    app = FastAPI(title='本地接收等待统计', version='1.0')
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
        allow_methods=['GET'], allow_headers=['*'])
    llm_cache = {'key': None, 'at': 0.0, 'value': None, 'inflight': False}
    llm_lock = Lock()

    def llm_snapshot_key(snapshot):
        return (snapshot.get('receiver_status'), snapshot.get('receiver_session_id'),
                snapshot.get('scan_source_instance'), snapshot.get('waiting_available'),
                snapshot.get('upstream_scan_verified'), snapshot.get('waiting_llm_total'))

    def llm_summary_nonblocking(snapshot):
        """Keep Prefect latency from blanking unrelated preprocessing cards."""
        if ticket_observer is None or snapshot.get('receiver_status') != 'running':
            return None
        key = llm_snapshot_key(snapshot)
        started = time.monotonic()
        with llm_lock:
            age = started - llm_cache['at'] if llm_cache['key'] == key else None
            cached = llm_cache['value'] if age is not None and age <= 8 else None
            launch = (age is None or age >= 4) and not llm_cache['inflight']
            if launch:
                llm_cache['inflight'] = True
        if launch:
            def refresh():
                value = None
                try:
                    from llm_queue_summary import summarize_llm
                    value = summarize_llm(queue, ticket_observer.index, ticket_observer, snapshot)
                    current = queue.snapshot(max_age_seconds=max_age_seconds)
                    if llm_snapshot_key(current) != key:
                        value = None
                except Exception:
                    value = None
                finally:
                    with llm_lock:
                        llm_cache.update(key=key, at=time.monotonic(), value=value, inflight=False)
            Thread(target=refresh, name='prefect-llm-summary', daemon=True).start()
        return cached

    @app.get('/api/v1/local-intake/preprocessing/summary')
    def waiting_summary(response: Response, result_filter: Literal['passed', 'failed', 'unrecognized'] | None = None,
                        result_page: int = Query(1, ge=1, le=1000000),
                        result_page_size: int = Query(20, ge=1, le=100),
                        llm_result_filter: Literal['passed', 'failed', 'unrecognized'] | None = None,
                        llm_result_page: int = Query(1, ge=1, le=1000000),
                        llm_result_page_size: int = Query(20, ge=1, le=100)):
        response.headers['Cache-Control'] = 'no-store'
        try:
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            snapshot['prefect_ticket_summary'] = (
                ticket_observer.summary(snapshot) if ticket_observer is not None else
                {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}})
            results = (ticket_observer.preprocessing_results(snapshot, result_filter, result_page, result_page_size)
                       if ticket_observer is not None else {'available': False, 'counts': {}})
            snapshot['preprocessing_results'] = results
            # An unavailable LLM results projection must not break the existing
            # preprocessing counts or other queue cards.
            try:
                snapshot['llm_results'] = (
                    ticket_observer.llm_results(snapshot, llm_result_filter, llm_result_page, llm_result_page_size)
                    if ticket_observer is not None else {'available': False, 'counts': {}})
            except Exception:
                snapshot['llm_results'] = {'available': False, 'counts': {}, 'message': 'LLM 结果暂不可用'}
            ticket_summary = snapshot['prefect_ticket_summary']
            if ticket_summary.get('available'):
                ticket_summary['counts'] = dict(ticket_summary.get('counts', {}))
                # The live snapshot checks which local preprocessing attempts
                # are actually active. Do not discard it merely because an
                # older cached ticket snapshot has a different count.
                in_progress = snapshot.get('in_progress_count')
                if type(in_progress) is int and in_progress >= 0:
                    ticket_summary['counts']['preprocessing_in_progress'] = in_progress
                    ticket_summary['preprocessing_in_progress_as_of'] = snapshot.get('as_of')
                else:
                    ticket_summary['counts'].pop('preprocessing_in_progress', None)
                # Passed preprocessing is cumulative, not the current LLM
                # waiting queue. Derive LLM phases from the folder ledger.
                names = ('waiting_llm', 'llm_body_in_progress', 'llm_attachment_in_progress',
                         'waiting_postprocessing')
                try:
                    llm = llm_summary_nonblocking(snapshot)
                    if llm is None:
                        raise ValueError('LLM 工单快照暂不可用')
                    for name in names:
                        value = llm.get(name)
                        if type(value) is int and value >= 0:
                            ticket_summary['counts'][name] = value
                        else:
                            ticket_summary['counts'].pop(name, None)
                    ticket_summary['waiting_llm_as_of'] = llm.get('as_of')
                    ticket_summary['waiting_llm_partial'] = llm['waiting_llm_partial']
                    ticket_summary['llm_processing_as_of'] = llm.get('as_of')
                    ticket_summary['llm_body_in_progress_partial'] = llm['llm_body_in_progress_partial']
                    ticket_summary['llm_attachment_in_progress_partial'] = llm['llm_attachment_in_progress_partial']
                except Exception:
                    for name in names:
                        ticket_summary['counts'].pop(name, None)
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
            if ticket_observer is None:
                raise ValueError('文件夹工单观察器不可用')
            from llm_queue_summary import summarize_llm
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            llm = summarize_llm(queue, ticket_observer.index, ticket_observer, snapshot)
            return {'available': llm['waiting_llm'] is not None, 'waiting_count': llm['waiting_llm'],
                    'unavailable_count': llm['unavailable_count'], 'items': llm['waiting_items'],
                    'partial': llm['waiting_llm_partial']}
        except Exception:
            return JSONResponse(status_code=503, headers={'Cache-Control': 'no-store'},
                                content={'available': False, 'waiting_count': None, 'items': []})

    return app
