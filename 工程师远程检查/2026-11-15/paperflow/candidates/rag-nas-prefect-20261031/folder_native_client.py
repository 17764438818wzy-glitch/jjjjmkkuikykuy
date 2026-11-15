"""Isolated body transport with task admission and recorded native requests."""
import hashlib
import time
from urllib.parse import quote

from effect_journal import EffectUnknown
from paperflow_prefect.records import identity


class NativeNotReady(RuntimeError):
    """No native request was sent because the executor is temporarily unavailable."""


def observe_native_request(request, config, native_requests, native_observations):
    def observed(method, endpoint, body=None, *args, **kwargs):
        if method == 'POST' and endpoint == '/documents/ingest':
            started = time.time_ns()
            outcome = 'error'
            try:
                result = request(method, endpoint, body, *args, **kwargs)
                outcome = 'returned'
                return result
            finally:
                native_requests.append({'document_id': body['doc_ids'][0], 'started_at_ns': started,
                                        'ended_at_ns': time.time_ns(), 'outcome': outcome})
        result = request(method, endpoint, body, *args, **kwargs)
        if method == 'GET' and '/documents?id=' in endpoint:
            rows = result.get('docs', []) if isinstance(result, dict) else result
            for row in rows:
                if row.get('pipeline_id') == config.native_pipeline_id:
                    native_observations.append({'at_ns': time.time_ns(), 'document_id': row['id'],
                                                'run': str(row.get('run'))})
        return result
    return observed


class FolderNativeClient:
    def __init__(self, raw, journal, targets, pipeline, admit, require_runtime):
        self.raw, self.journal, self.targets = raw, journal, set(targets)
        self.pipeline, self.admit, self.require_runtime = pipeline, admit, require_runtime
        self.base_url, self.api_key, self.opener = raw.base_url, raw.api_key, raw.opener

    def _send(self, action, send, *, reuse_returned=False):
        digest = identity(action)
        return self.journal.perform(digest, digest, send, self.admit(digest, digest),
                                    reuse_returned=reuse_returned)

    def request(self, method, endpoint, body=None):
        if method == 'GET':
            return self.raw.request(method, endpoint, body)
        if method == 'POST' and endpoint == '/retrieval' and set(body['dataset_ids']) <= self.targets:
            return self.raw.request(method, endpoint, body)
        parts = endpoint.split('/')
        if (method == 'POST' and len(parts) == 6 and parts[1] == 'datasets'
                and parts[2] in self.targets and parts[3] == 'documents' and parts[5] == 'chunks'):
            action = {'method': method, 'endpoint': endpoint, 'body': body}
            return self._send(action, lambda: self.raw.request(method, endpoint, body))
        if method != 'PATCH' or len(parts) < 4 or parts[1] != 'datasets' or parts[2] not in self.targets:
            raise ValueError('write is outside the selected test datasets')
        action = {'method': method, 'endpoint': endpoint, 'body': body}
        return self._send(action, lambda: self.raw.request(method, endpoint, body), reuse_returned=True)

    def upload_bytes(self, dataset, name, content):
        if dataset not in self.targets:
            raise ValueError('upload target is not a test dataset')
        action = {'upload_dataset': dataset, 'name': name, 'sha256': hashlib.sha256(content).hexdigest()}
        return self._send(action, lambda: self.raw.upload_bytes(dataset, name, content))

    def _ingest_identity(self, dataset_id, document_id, pipeline_id):
        body = {'doc_ids': [document_id], 'run': '1', 'delete': False}
        return (identity({'service': self.base_url.rstrip('/'), 'native_document': document_id}),
                identity({'dataset_id': dataset_id, 'pipeline_id': pipeline_id, 'body': body}))

    def ingest_state(self, dataset_id, document_id, pipeline_id):
        return self.journal.state(*self._ingest_identity(dataset_id, document_id, pipeline_id))

    def ingest(self, dataset_id, document_id, pipeline_id, *, timeout=600):
        if dataset_id not in self.targets or pipeline_id != self.pipeline:
            raise ValueError('native ingestion target differs from the test')
        endpoint = (f'/datasets/{quote(dataset_id, safe="")}/documents?id='
                    f'{quote(document_id, safe="")}&page=1&page_size=100')
        body = {'doc_ids': [document_id], 'run': '1', 'delete': False}
        effect_id, request_sha = self._ingest_identity(dataset_id, document_id, pipeline_id)
        state = self.journal.state(effect_id, request_sha)
        if state == 'intent':
            raise EffectUnknown('native submission outcome requires review')
        if state is None:
            data = self.raw.request('GET', endpoint)
            rows = data.get('docs', []) if isinstance(data, dict) else data
            if (len(rows) != 1 or rows[0].get('id') != document_id or rows[0].get('pipeline_id') != pipeline_id
                    or str(rows[0].get('status')) != '0' or str(rows[0].get('run')) not in ('0', 'UNSTART')):
                raise EffectUnknown('native document is not confirmed unstarted')
            observation = self.require_runtime()
            if observation.get('ready') is not True:
                raise NativeNotReady('native executor or memory admission is unavailable')
            deadline = time.monotonic() + observation['ready_valid_until'] - time.time()
            self.journal.perform(effect_id, request_sha, lambda: self.raw.request('POST', '/documents/ingest', body),
                                 self.admit(effect_id, request_sha), reuse_returned=False,
                                 not_after=deadline)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = self.raw.request('GET', endpoint)
            except InterruptedError as exc:
                raise EffectUnknown('native observation interrupted after submission') from exc
            rows = data.get('docs', []) if isinstance(data, dict) else data
            if (len(rows) != 1 or rows[0].get('id') != document_id or rows[0].get('pipeline_id') != pipeline_id
                    or str(rows[0].get('status')) != '0'):
                raise EffectUnknown('native document is not uniquely readable')
            doc = rows[0]
            run = str(doc.get('run'))
            if run in ('3', 'DONE') and float(doc.get('progress', 0)) >= 1:
                return {'document_id': document_id, 'pipeline_id': pipeline_id, 'run': run,
                        'progress': doc['progress']}
            if run in ('FAIL', '4', '-1', '2', 'CANCEL') or float(doc.get('progress', 0)) < 0:
                raise RuntimeError('native processing failed; preserve the original submission')
            time.sleep(1)
        raise EffectUnknown('native processing has not reached a verified terminal state')
