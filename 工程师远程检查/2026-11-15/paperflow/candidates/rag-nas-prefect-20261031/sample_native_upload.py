"""One manual isolated native upload, reusing the existing publisher.

This is a test, not a scheduler or a Prefect entry point. Existing postprocess
records are used as-is. A recorded attempt is never automatically repeated.
"""
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import quote

import live_rag_selftest as app


class TestClient:
    """Limit writes to the selected test datasets and record each request."""
    def __init__(self, raw, journal, targets, pipeline):
        self.raw = raw
        self.journal = journal
        self.targets = set(targets)
        self.pipeline = pipeline
        self.base_url, self.api_key, self.opener = raw.base_url, raw.api_key, raw.opener

    def _send(self, action, send):
        digest = app._sha(app._json(action))
        return self.journal.perform(digest, digest, send, app.admit(digest, digest), reuse_returned=False)

    def request(self, method, endpoint, body=None):
        if method == 'GET':
            return self.raw.request(method, endpoint, body)
        if method == 'POST' and endpoint == '/retrieval':
            if not set(body['dataset_ids']) <= self.targets:
                raise ValueError('retrieval must be limited to the test datasets')
            return self.raw.request(method, endpoint, body)
        parts = endpoint.split('/')
        if method != 'PATCH' or len(parts) < 4 or parts[1] != 'datasets' or parts[2] not in self.targets:
            raise ValueError('write is outside the selected test datasets')
        return self._send({'method': method, 'endpoint': endpoint, 'body': body},
                          lambda: self.raw.request(method, endpoint, body))

    def upload_bytes(self, dataset, name, content):
        if dataset not in self.targets:
            raise ValueError('upload target is not a test dataset')
        return self._send({'upload_dataset': dataset, 'name': name,
                           'sha256': hashlib.sha256(content).hexdigest()},
                          lambda: self.raw.upload_bytes(dataset, name, content))

    def ingest(self, dataset_id, document_id, pipeline_id, *, timeout=600):
        if dataset_id not in self.targets or pipeline_id != self.pipeline:
            raise ValueError('native ingestion target differs from the test')
        body = {'doc_ids': [document_id], 'run': '1', 'delete': False}
        self._send({'ingest_dataset': dataset_id, 'pipeline': pipeline_id, 'body': body},
                   lambda: self.raw.request('POST', '/documents/ingest', body))
        deadline = time.monotonic() + timeout
        endpoint = (f'/datasets/{quote(dataset_id, safe="")}/documents?id='
                    f'{quote(document_id, safe="")}&page=1&page_size=100')
        while time.monotonic() < deadline:
            data = self.raw.request('GET', endpoint)
            rows = data.get('docs', []) if isinstance(data, dict) else data
            if len(rows) != 1:
                raise ValueError('native test document is not uniquely readable')
            doc = rows[0]
            run = str(doc.get('run'))
            if run in ('3', 'DONE') and float(doc.get('progress', 0)) >= 1:
                print('native target complete:', dataset_id, flush=True)
                return {'document_id': document_id, 'pipeline_id': pipeline_id, 'run': run,
                        'progress': doc['progress']}
            if run in ('FAIL', '4', '-1', '2', 'CANCEL') or float(doc.get('progress', 0)) < 0:
                raise RuntimeError('native test failed; retain its request record')
            time.sleep(1)
        raise TimeoutError('native test outcome is not yet known')


def main():
    case = Path(__file__).resolve().parent/'live-tests/batch-309-fc240b92da874cca9a05f9630aeb073e'
    if (case/'rag-upload-attempt.json').exists():
        raise ValueError('this sample already has an upload attempt to review')
    job = '701b5996-27fb-5b1e-954c-198b69004d57'
    packet_path = case/('server-inspection-' + job)
    evidence = json.loads((case/('server-inspection-' + job + '.evidence.json')).read_text(encoding='utf8'))
    prepared = json.loads((case/('server-inspection-' + job + '.native-admission.json')).read_text(encoding='utf8'))
    copied = json.loads((case/'desktop-sample-copy-receipt.json').read_text(encoding='utf8'))
    datasets = json.loads((case/'rag-datasets-complete.json').read_text(encoding='utf8'))
    catalog_bytes = (case.parents[3]/'prefect-paperflow/catalog.json').read_bytes()
    if hashlib.sha256(catalog_bytes).hexdigest() != evidence['catalog_sha256']:
        raise ValueError('specialty catalog differs from the recorded postprocess catalog')
    catalog = json.loads(catalog_bytes.decode('utf-8-sig'))
    task = prepared['task']
    packet = app.validate_package(packet_path, task, catalog, strict_seal=True)
    if (packet['manifest_sha256'] != evidence['manifest_sha256']
            or copied['sha256'] != task['source_sha256'] or not copied['copy_verified']
            or prepared['body_result_sha256'] != evidence['body_result_sha256']):
        raise ValueError('postprocess, sample and copied-file identities do not match')
    with Path(copied['test_copy']).open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != copied['sha256']:
            raise ValueError('desktop test copy changed')
    raw = app.RagflowClient(app.settings['rag_url'], app.load_key(app.settings.path('settings_root'), 'ragflow'))
    config = app.load_native_batch_config(Path(r'E:\uv\ragflow_upstream\data'), raw)
    check = app.NativeDeliveryPublisher(None, lambda _: None, config)
    if not app._qualified(prepared, packet, check) or check.records(packet)['images']:
        raise ValueError('sample is not eligible for this body-only native test')
    targets = datasets['targets']
    observed = {row['id']: row for row in raw.datasets()}
    for route in packet['routes']:
        code = route['code']
        ds = targets[code]['paper_dataset_id']
        receipt = json.loads((case/('rag-dataset-' + code + '.json')).read_text(encoding='utf8'))
        actual = observed.get(ds, {})
        if (receipt['dataset_id'] != ds or actual.get('name') != receipt['name']
                or actual.get('permission') != 'me' or actual.get('document_count') != 0
                or actual.get('chunk_count') != 0):
            raise ValueError('selected test dataset is not the recorded empty test dataset')
        route['paper_dataset_id'] = ds
    readiness = json.loads((case/'runtime-readiness.json').read_text(encoding='utf8'))
    if readiness.get('ready') is not True or time.time() >= readiness.get('ready_valid_until', 0):
        raise ValueError('native executor observation is unavailable or expired')
    app.save(case/'rag-upload-attempt.json', {'job_id': job, 'manifest_sha256': packet['manifest_sha256'],
                                             'targets': targets, 'test_only': True})
    journal = app.EffectJournal(case/'rag-upload-effects.sqlite3')
    journal.initialize()
    client = TestClient(raw, journal, [value['paper_dataset_id'] for value in targets.values()],
                        config.native_pipeline_id)
    receipts = {}
    pub = app.NativeDeliveryPublisher(client, lambda value: app.save(case/'rag-upload-receipts.json', value), config)
    pub.publish(task, packet, receipts)
    pub.verify(task, packet, receipts)
    pub.set_enabled(receipts, True)
    pub.verify_retrieval(task, packet, receipts)
    app.save(case/'rag-upload-complete.json', {'job_id': job, 'manifest_sha256': packet['manifest_sha256'],
        'group_count': 1, 'target_count': len(receipts), 'record_count': len(check.records(packet)['body']),
        'receipts_verified': True, 'retrieval_verified': True, 'test_only': True})
    print('isolated group RAG upload and readback complete:', len(receipts), 'test datasets', flush=True)


if __name__ == '__main__':
    main()
