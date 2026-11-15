"""Observe stage-four admission and actual client calls; never schedule work."""
from contextlib import contextmanager
from contextvars import ContextVar
import copy
import os
from pathlib import Path
import threading
import time

from .records import write_json

_current = ContextVar('paperflow_rag_observation', default=None)


class RagProgress:
    def __init__(self, path, limit):
        self.path, self.limit = Path(path), limit
        self.rows = {}
        self.lock = threading.RLock()

    def observation(self, paper, stage):
        key = paper['paper_id'] + ':' + paper['revision']
        with self.lock:
            self.rows[key] = dict(paper_id=paper['paper_id'], revision=paper['revision'],
                stage=stage, phase='waiting_local', occupied=False, in_flight=False,
                paper_verified=False, owner_pid=os.getpid(), updated_at=time.time())
        return Observation(self, key)

    def snapshot(self):
        with self.lock:
            rows = copy.deepcopy(list(self.rows.values()))
            return dict(schema='rag-progress/1', observed_at=time.time(), limit=self.limit,
                counts=dict(waiting=sum(r['phase'] in {'waiting_local', 'waiting_global'} for r in rows),
                    occupied=sum(r['occupied'] for r in rows),
                    uploading=sum(r['in_flight'] and r['phase'] == 'uploading' for r in rows),
                    waiting_verification=sum(r['phase'] == 'waiting_verification' for r in rows),
                    verifying=sum(r['in_flight'] and r['phase'] in {'readback', 'activating', 'retrieval'} for r in rows),
                    verified_papers=sum(r['paper_verified'] for r in rows),
                    unknown=sum(r['phase'] == 'unknown' for r in rows)), papers=rows)


class Observation:
    def __init__(self, progress, key): self.progress, self.key = progress, key

    def update(self, phase, **fields):
        with self.progress.lock:
            row = self.progress.rows[self.key]
            row.update(phase=phase, updated_at=time.time(), **fields)
            if phase == 'ready': row['acquired_at'] = row['updated_at']
            if fields.get('occupied') is False and row.get('acquired_at'):
                row['released_at'] = row['updated_at']
            write_json(self.progress.path, self.progress.snapshot())

    @contextmanager
    def bind(self):
        token = _current.set(self)
        try: yield
        finally: _current.reset(token)

    def call(self, phase, operation):
        self.update(phase, in_flight=True, request_started_at=time.time())
        try:
            result = operation()
        except BaseException as exc:
            self.update('unknown', in_flight=False, error_type=type(exc).__name__,
                        request_finished_at=time.time())
            raise
        self.update('waiting_verification', in_flight=False, request_finished_at=time.time())
        return result


class ObservedClient:
    def __init__(self, client, observation): self.client, self.observation = client, observation
    def __getattr__(self, name): return getattr(self.client, name)

    def upload_bytes(self, *args, **kwargs):
        return self.observation.call('uploading', lambda: self.client.upload_bytes(*args, **kwargs))

    def download_bytes(self, dataset, document):
        from paperflow.rag import RagPublisher
        return self.observation.call('readback', lambda: RagPublisher(self.client)._download(dataset, document))

    def download_image(self, image_id):
        from paperflow.rag import RagPublisher
        return self.observation.call('readback', lambda: RagPublisher(self.client).download_image(image_id))

    def request(self, method, endpoint, *args, **kwargs):
        phase = ('retrieval' if endpoint == '/retrieval' else
                 'readback' if method == 'GET' else
                 'activating' if method == 'PATCH' else 'uploading')
        return self.observation.call(phase, lambda: self.client.request(method, endpoint, *args, **kwargs))


def observe_client(client):
    observation = _current.get()
    return ObservedClient(client, observation) if observation is not None else client
