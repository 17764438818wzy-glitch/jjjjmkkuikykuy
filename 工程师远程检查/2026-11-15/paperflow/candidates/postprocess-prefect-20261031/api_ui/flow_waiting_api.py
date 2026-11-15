"""Isolated waiting API candidate; constructing starts no intake or server.

Copied from llm-new-prefect-20261031/flow_waiting_api.py. stage_observer is the
selected FolderStageStore with identity/activity readers already configured.
stage_snapshot_databases must explicitly list ALL SQLite sources read by queue,
ticket_observer and stage_observer, including their shared control database and
the actual LocalQueue.path, which must also match ticket_observer.queue.path.
Missing/invalid queue.path is never inferred from
the supplied list. Until that deployment contract is verified,
omit this list and stage cards stay unknown. Read-only data_version guards detect
commits during cross-source observation. This is not a deployed endpoint.
"""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Literal

from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
STAGE_KEYS = ('waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload')
DIAGNOSTIC_KEYS = ('postprocessing_held', 'postprocessing_unresolved')
POSTPROCESSING_KEYS = STAGE_KEYS + DIAGNOSTIC_KEYS


class _SourceGuard:
    """Hold read-only connections without a snapshot transaction or writer lock."""
    def __init__(self, databases, stage_observer, ticket_observer, queue):
        self.sources = []
        self.ready = False
        try:
            if (stage_observer is None or ticket_observer is None
                    or not callable(getattr(stage_observer, 'summary', None))
                    or not isinstance(databases, (list, tuple)) or not databases):
                return
            control = Path(ticket_observer.index).resolve(strict=True)
            if Path(stage_observer.database).resolve(strict=True) != control:
                return
            queue_path = Path(queue.path)
            if not queue_path.is_absolute() or not queue_path.is_file():
                return
            queue_path = queue_path.resolve(strict=True)
            observer_queue_path = Path(ticket_observer.queue.path)
            if (not observer_queue_path.is_absolute() or not observer_queue_path.is_file()
                    or observer_queue_path.resolve(strict=True) != queue_path):
                return
            paths = [Path(p).resolve(strict=True) for p in databases]
            if len(set(paths)) != len(paths) or control not in paths or queue_path not in paths:
                return
            for path in paths:
                db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)
                self.sources.append((path, db, None, None))
                stat = path.stat()
                version = db.execute('PRAGMA data_version').fetchone()[0]
                self.sources[-1] = (path, db, version, (stat.st_dev, stat.st_ino))
            self.ready = True
        except (OSError, sqlite3.Error, ValueError, TypeError, AttributeError):
            self.ready = False

    def unchanged(self):
        if not self.ready:
            return False
        try:
            return all(db.execute('PRAGMA data_version').fetchone()[0] == version
                       and (path.stat().st_dev, path.stat().st_ino) == file_identity
                       for path, db, version, file_identity in self.sources)
        except (OSError, sqlite3.Error):
            return False

    def close(self):
        for _, db, _, _ in self.sources:
            db.close()


def _stage_counts(llm, stage_observer, guard):
    unknown = dict.fromkeys(POSTPROCESSING_KEYS)
    try:
        total = llm.get('waiting_postprocessing')
        if (not guard.unchanged() or type(total) is not int or total < 0
                or llm.get('waiting_llm_partial') is not False or llm.get('unavailable_count') != 0):
            return unknown
        stage = stage_observer.summary()
        keys = ('waiting_postprocessing', 'postprocessing', 'waiting_rag', 'held', 'unresolved')
        if not isinstance(stage, dict) or set(stage) != set(keys):
            return unknown
        if any(type(stage[key]) is not int or stage[key] < 0 for key in keys if key != 'postprocessing'):
            return unknown
        processing = stage['postprocessing']
        if processing is not None and (type(processing) is not int or processing < 0):
            return unknown
        known_total = sum(value for value in stage.values() if value is not None)
        if (known_total > total or (processing is not None and known_total != total)
                or not guard.unchanged()):
            return unknown
        return dict(waiting_postprocessing=stage['waiting_postprocessing'],
                    postprocessing_in_progress=processing, waiting_rag_upload=stage['waiting_rag'],
                    postprocessing_held=stage['held'], postprocessing_unresolved=stage['unresolved'])
    except Exception:
        return unknown


def create_app(queue, *, max_age_seconds=60, enable_claims=False, ticket_observer=None,
               stage_observer=None, stage_snapshot_databases=None):
    app = FastAPI(title='本地接收等待统计', version='1.0')
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r'^https?://(localhost|127\.0\.0\.1)(:\d+)?$',
        allow_methods=['GET'], allow_headers=['*'])

    @app.get('/api/v1/local-intake/preprocessing/summary')
    def waiting_summary(response: Response, result_filter: Literal['passed', 'failed', 'unrecognized'] | None = None,
                        result_page: int = Query(1, ge=1, le=1000000),
                        result_page_size: int = Query(20, ge=1, le=100)):
        response.headers['Cache-Control'] = 'no-store'
        guard = _SourceGuard(stage_snapshot_databases, stage_observer, ticket_observer, queue)
        try:
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            snapshot['prefect_ticket_summary'] = (
                ticket_observer.summary(snapshot) if ticket_observer is not None else
                {'source': 'prefect', 'scope': 'all_tasks', 'available': False, 'counts': {}})
            results = (ticket_observer.preprocessing_results(snapshot, result_filter, result_page, result_page_size)
                       if ticket_observer is not None else {'available': False, 'counts': {}})
            snapshot['preprocessing_results'] = results
            ticket_summary = snapshot['prefect_ticket_summary']
            ticket_summary['counts'] = dict(ticket_summary.get('counts', {}))
            # Replace cached counters, including cumulative LLM completion.
            # Only the mutually exclusive folder stages supply these cards.
            ticket_summary['counts'].update(dict.fromkeys(POSTPROCESSING_KEYS))
            ticket_summary.pop('postprocessing_basis', None)
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
                from flow_queue_summary import summarize_llm
                names = ('waiting_llm', 'llm_body_in_progress', 'llm_attachment_in_progress')
                try:
                    llm = summarize_llm(queue, ticket_observer.index, ticket_observer, snapshot)
                    for name in names:
                        value = llm.get(name)
                        if type(value) is int and value >= 0:
                            ticket_summary['counts'][name] = value
                        else:
                            ticket_summary['counts'].pop(name, None)
                    ticket_summary['waiting_llm_as_of'] = snapshot.get('as_of')
                    ticket_summary['waiting_llm_partial'] = llm['waiting_llm_partial']
                    stage_counts = _stage_counts(llm, stage_observer, guard)
                    ticket_summary['counts'].update(stage_counts)
                    if any(value is not None for value in stage_counts.values()):
                        ticket_summary['postprocessing_basis'] = 'folder-stage-receipts/1'
                    ticket_summary['postprocessing_as_of'] = snapshot.get('as_of')
                except (OSError, sqlite3.Error, ValueError):
                    for name in names:
                        ticket_summary['counts'].pop(name, None)
            return snapshot
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, AttributeError):
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
        finally:
            guard.close()

    @app.get('/api/v1/local-intake/llm/waiting')
    def llm_waiting(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        try:
            if ticket_observer is None:
                raise ValueError('文件夹工单观察器不可用')
            from flow_queue_summary import summarize_llm
            snapshot = queue.snapshot(max_age_seconds=max_age_seconds)
            llm = summarize_llm(queue, ticket_observer.index, ticket_observer, snapshot)
            return {'available': llm['waiting_llm'] is not None, 'waiting_count': llm['waiting_llm'],
                    'unavailable_count': llm['unavailable_count'], 'items': llm['waiting_items'],
                    'partial': llm['waiting_llm_partial']}
        except (OSError, sqlite3.Error, ValueError):
            return JSONResponse(status_code=503, headers={'Cache-Control': 'no-store'},
                                content={'available': False, 'waiting_count': None, 'items': []})

    return app
