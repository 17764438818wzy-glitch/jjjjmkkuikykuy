"""Journal the RAG effects used by the reusable publishers.

This is an internal transport adapter, not a queue, API endpoint, or scheduler.
The admission factory must be provided by the new task-control integration;
constructing this client does not connect it to that integration.
"""
import hashlib
import json
import re
from contextlib import nullcontext
from urllib.parse import quote
from urllib.request import Request

from filelock import FileLock
from effect_journal import EffectNeedsReadback


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':')).encode('utf8')


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _effect_discriminator(method, endpoint, body):
    path = endpoint.split('?', 1)[0]
    if method == 'PATCH' and isinstance(body, dict) and body:
        is_doc = re.fullmatch(r'/datasets/[^/]+/documents/[^/]+', path) is not None
        is_chunk = re.fullmatch(r'/datasets/[^/]+/documents/[^/]+/chunks/[^/]+', path) is not None
        allowed = {'meta_fields', 'enabled', 'name', 'pipeline_id'} if is_doc else ({'available'} if is_chunk else set())
        if not set(body) <= allowed:
            raise ValueError('unsupported RAG document update')
        return {'fields': sorted(body), 'enabled': body.get('enabled'), 'available': body.get('available')}
    if method == 'POST' and re.fullmatch(r'/datasets/[^/]+/documents/[^/]+/chunks', path):
        content = body.get('content') if isinstance(body, dict) else None
        marker = content.split('\n', 1)[0] if isinstance(content, str) else ''
        if not ((marker.startswith('[PAPERFLOW]') and marker.endswith('[/PAPERFLOW]'))
                or (marker.startswith('<!-- PFA ') and marker.endswith(' -->'))):
            raise ValueError('RAG chunk has no stable sealed record identity')
        return {'record_marker': marker}
    if method == 'POST' and path == '/documents/ingest' and isinstance(body, dict):
        ids = body.get('doc_ids')
        if (not isinstance(ids, list) or not ids or not all(isinstance(i, str) and i for i in ids)
                or body.get('run') != '1' or body.get('delete') is not False):
            raise ValueError('native ingest has no document identity')
        return {'document_ids': sorted(ids)}
    if method == 'POST' and path == '/files' and isinstance(body, dict):
        parent, name = body.get('parent_id'), body.get('name')
        if (not isinstance(parent, str) or not parent or not isinstance(name, str) or not name
                or body.get('type') != 'folder'):
            raise ValueError('remote directory has no stable identity')
        return {'parent': parent, 'name': name}
    if method == 'POST' and path == '/files/link-to-datasets' and isinstance(body, dict):
        files, datasets = body.get('file_ids'), body.get('kb_ids')
        if (not isinstance(files, list) or len(files) != 1 or not isinstance(datasets, list) or len(datasets) != 1
                or not all(isinstance(i, str) and i for i in files + datasets)):
            raise ValueError('remote file link requires one file and one dataset')
        return {'files': sorted(files), 'datasets': sorted(datasets)}
    raise ValueError('remote write is not part of the new RAG transport contract')


def _canonical_write_path(endpoint):
    # The journal identity and transport path must describe the same resource.
    # One existing attachment operation needs this exact mode parameter.
    # Its queryless or alternate forms are not interchangeable journal keys.
    if endpoint == '/files/link-to-datasets?mode=add':
        return endpoint
    if endpoint == '/files/link-to-datasets':
        raise ValueError('attachment linking requires exact add mode')
    # Otherwise fail closed on queries, encodings, dot segments, duplicate
    # slashes, fragments and backslashes.
    if (not re.fullmatch(r'/(?:[A-Za-z0-9_~.-]+/)*[A-Za-z0-9_~.-]+', endpoint)
            or any(part in {'.', '..'} for part in endpoint.split('/'))):
        raise ValueError('remote write endpoint is not canonical')
    return endpoint


class GuardedRagClient:
    def __init__(self, client, journal, delivery_scope, admission_factory, observation_factory, target_authorizer,
                 completion_verifier=None):
        if not isinstance(delivery_scope, str) or not re.fullmatch(r'[0-9a-f]{64}', delivery_scope):
            raise ValueError('delivery scope must be a verified stable identity')
        if not all(callable(value) for value in (admission_factory, observation_factory, target_authorizer)):
            raise ValueError('new task admission, observation and target authorization are required')
        if completion_verifier is not None and not callable(completion_verifier):
            raise ValueError('completion verifier must be callable')
        self._client = client
        self.journal = journal
        self._scope = delivery_scope
        self._admission = admission_factory
        self._observation = observation_factory
        self._authorize = target_authorizer
        self._completion_verifier = completion_verifier
        self.base_url = client.base_url.rstrip('/')

    def _identity(self, operation, request, *, shared=False):
        scope = 'shared-resource' if shared else self._scope
        effect_id = _sha(_json({'service': self.base_url, 'scope': scope, 'operation': operation}))
        request_sha = _sha(_json(request))
        return effect_id, request_sha

    def _document_fence(self, document):
        directory = self.journal.path.parent / 'document-fences'
        directory.mkdir(parents=True, exist_ok=True)
        key = _sha(_json({'service': self.base_url, 'document_id': document}))
        return key, FileLock(str(directory / (key + '.lock')), timeout=10)

    def _send(self, operation, request, send, *, shared=True, reuse_returned=True, before_intent=None,
              not_after=None):
        # The authorizer receives independent copies and cannot alter the bytes
        # that were identified for the actual send.
        if self._authorize(json.loads(_json(operation)), json.loads(_json(request))) is not True:
            raise PermissionError('remote target does not belong to the sealed handoff')
        effect_id, request_sha = self._identity(operation, request, shared=shared)
        endpoint = operation.get('endpoint', '')
        match = re.fullmatch(r'/datasets/[^/]+/documents/([^/]+)(?:/chunks(?:/[^/]+)?)?', endpoint)
        document = match.group(1) if match else None
        native = endpoint == '/documents/ingest'
        if native:
            document = request['binding']['document_id']
        fence = nullcontext()
        document_key = None
        if document:
            document_key, fence = self._document_fence(document)
        # All candidate writers of a document, including native ingest, hold
        # the same process-shared fence through preflight, send and receipt.
        # This is not a fence for legacy or external writers that ignore it.
        with fence:
            if document_key and not native:
                self.journal.assert_document_available(document_key)
            result = self.journal.perform(effect_id, request_sha, send, self._admission(effect_id, request_sha),
                                          reuse_returned=reuse_returned, before_intent=before_intent,
                                          reserve_document=document_key,
                                          reservation_kind=('native' if native else 'sync') if document_key else None,
                                          not_after=not_after)
            if document_key and not native:
                if operation.get('method') == 'POST' and endpoint.endswith('/chunks'):
                    chunk = result.get('chunk', result) if isinstance(result, dict) else None
                    if not isinstance(chunk, dict) or not isinstance(chunk.get('id'), str) or not chunk['id']:
                        raise EffectNeedsReadback('chunk response has no verifiable identity')
                if self._completion_verifier is None:
                    raise EffectNeedsReadback('document effect requires verified readback and a durable receipt')
                with self._observation() as token:
                    token.start_observation()
                    verified = self._completion_verifier(json.loads(_json(operation)), json.loads(_json(request)),
                                                         json.loads(_json(result)))
                if verified is not True:
                    raise EffectNeedsReadback('document effect readback or receipt is incomplete')
                self.journal.release_synchronous_document_if_owned(document_key, effect_id, request_sha)
            return result

    def effect_state(self, method, endpoint, body):
        method = method.upper()
        _canonical_write_path(endpoint)
        body = json.loads(_json(body))
        discriminator = _effect_discriminator(method, endpoint, body)
        operation = {'method': method, 'endpoint': endpoint, 'identity': discriminator}
        request = {'method': method, 'endpoint': endpoint, 'body': body}
        effect_id, request_sha = self._identity(operation, request, shared=True)
        return self.journal.state(effect_id, request_sha)

    @staticmethod
    def _native_effect(body, binding):
        body = json.loads(_json(body))
        binding = json.loads(_json(binding))
        discriminator = _effect_discriminator('POST', '/documents/ingest', body)
        if (len(discriminator['document_ids']) != 1 or not isinstance(binding, dict)
                or binding.get('document_id') != discriminator['document_ids'][0]
                or not all(isinstance(binding.get(key), str) and binding[key]
                           for key in ('dataset_id', 'document_id', 'pipeline_id', 'paperflow_task',
                                       'paperflow_source_sha256', 'paperflow_manifest_sha256',
                                       'paperflow_revision', 'paperflow_specialty', 'paperflow_role'))):
            raise ValueError('native submission binding is incomplete')
        operation = {'method': 'POST', 'endpoint': '/documents/ingest', 'identity': discriminator}
        request = {'method': 'POST', 'endpoint': '/documents/ingest', 'body': body, 'binding': binding}
        return operation, request

    def _native_effect_state(self, body, binding):
        operation, request = self._native_effect(body, binding)
        effect_id, request_sha = self._identity(operation, request, shared=True)
        return self.journal.state(effect_id, request_sha)

    def _submit_native(self, body, binding, fresh_proof, *, not_after=None):
        operation, request = self._native_effect(body, binding)
        frozen_body, frozen_binding = request['body'], request['binding']
        return self._send(operation, request,
                          lambda: self._client.request('POST', '/documents/ingest', frozen_body),
                          shared=True, before_intent=lambda: fresh_proof(json.loads(_json(frozen_binding))),
                          not_after=not_after)

    def request(self, method, endpoint, body=None, *args, **kwargs):
        method = method.upper()
        if not isinstance(endpoint, str) or not endpoint.startswith('/'):
            raise ValueError('RAG endpoint must be a relative absolute path')
        if args or kwargs:
            raise ValueError('untracked request transport options are not accepted')
        # Freeze the exact JSON payload before any admission wait.
        body = json.loads(_json(body))
        if method == 'GET' or (method == 'POST' and endpoint == '/retrieval'):
            with self._observation() as token:
                token.start_observation()
                return self._client.request(method, endpoint, body)
        _canonical_write_path(endpoint)
        if method == 'POST' and endpoint == '/documents/ingest':
            raise ValueError('native submission requires the verified native adapter')
        discriminator = _effect_discriminator(method, endpoint, body)
        operation = {'method': method, 'endpoint': endpoint, 'identity': discriminator}
        request = {'method': method, 'endpoint': endpoint, 'body': body}
        return self._send(operation, request, lambda: self._client.request(method, endpoint, body),
                          shared=True, reuse_returned=method != 'PATCH')

    def _upload(self, kind, parent, name, content, send):
        if (not isinstance(parent, str) or not parent or not isinstance(name, str) or not name
                or not isinstance(content, bytes)):
            raise ValueError('upload has no stable target, name or bytes')
        operation = {'kind': kind, 'parent': parent, 'name': name}
        request = {**operation, 'content_sha256': _sha(content), 'content_bytes': len(content)}
        return self._send(operation, request, send)

    def upload_bytes(self, dataset, name, content):
        return self._upload('dataset-document', dataset, name, content,
                            lambda: self._client.upload_bytes(dataset, name, content))

    def upload_file(self, parent, name, content):
        return self._upload('file', parent, name, content,
                            lambda: self._client.upload_file(parent, name, content))

    def download_file(self, file_id):
        with self._observation() as token:
            token.start_observation()
            return self._client.download_file(file_id)

    def _download(self, endpoint, *, image=False):
        with self._observation() as token:
            token.start_observation()
            request = Request(self.base_url + '/api/v1' + endpoint,
                              headers={'Authorization': 'Bearer ' + self._client.api_key})
            with self._client.opener.open(request, timeout=30) as response:
                if image and not response.headers.get('Content-Type', '').lower().startswith('image/'):
                    raise ValueError('remote image did not return image bytes')
                return response.read()

    def download_bytes(self, dataset, document):
        if callable(getattr(self._client, 'download_bytes', None)):
            with self._observation() as token:
                token.start_observation()
                return self._client.download_bytes(dataset, document)
        return self._download('/datasets/' + quote(dataset, safe='') + '/documents/' + quote(document, safe=''))

    def download_image(self, image_id):
        if callable(getattr(self._client, 'download_image', None)):
            with self._observation() as token:
                token.start_observation()
                return self._client.download_image(image_id)
        return self._download('/documents/images/' + quote(image_id, safe=''), image=True)
