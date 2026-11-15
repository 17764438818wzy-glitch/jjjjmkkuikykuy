"""Parent admission and server-recorded identities for RAG operations."""

import copy
import hashlib
import json

from ragflow_client import RagflowClient, RagflowHTTPError


def definite_rejection(error):
    return isinstance(error, RagflowHTTPError) and error.status_code in {400, 401, 403, 404, 405, 413, 415, 422}


class TaskPaused(InterruptedError):
    pass


class ServerEffects:
    """Use the caller's Prefect receipt writer; never replay an unknown write."""

    def __init__(self, identity, records, save_event, *, retry_failed=False):
        self.scope = {key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'task_run_id')}
        self.save_event = save_event
        self.retry_failed = retry_failed
        self.last_failed = None
        self.last_not_sent = None
        self.states = {}
        for row in reversed(records):
            data = row.get('data') or {}
            if data.get('scope') != self.scope or row.get('kind') not in {
                    'rag_effect_intent', 'rag_effect_not_sent', 'rag_effect_returned', 'rag_effect_failed'}:
                continue
            self.states[data['effect_id']] = {'status': row['kind'], 'result': data.get('result')}

    def send(self, action, check_running, send):
        self.last_failed = None
        self.last_not_sent = None
        encoded = json.dumps({'scope': self.scope, 'action': action}, sort_keys=True,
                             ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf8')
        effect_id = hashlib.sha256(encoded).hexdigest()
        previous = self.states.get(effect_id) or {}
        if previous.get('status') == 'rag_effect_intent':
            raise ValueError('RAG request outcome unknown; reconciliation required')
        if previous.get('status') == 'rag_effect_failed' and not self.retry_failed:
            raise ValueError('RAG request requires a user-selected retry')
        if check_running() is not True:
            self.last_not_sent = effect_id
            raise TaskPaused('parent task is paused')
        if previous.get('status') == 'rag_effect_returned':
            return copy.deepcopy(previous['result'])

        def save(status, result=None):
            data = {'scope': self.scope, 'effect_id': effect_id, 'action': action, 'result': result}
            if not self.save_event(effect_id + '-' + status.removeprefix('rag_effect_'), status, data):
                raise ValueError('server RAG effect receipt was not saved')
            self.states[effect_id] = {'status': status, 'result': copy.deepcopy(result)}

        save('rag_effect_intent')
        if check_running() is not True:
            save('rag_effect_not_sent')
            self.last_not_sent = effect_id
            raise TaskPaused('parent task is paused')
        try:
            result = send()
        except RagflowHTTPError as exc:
            if definite_rejection(exc):
                save('rag_effect_failed', {'status_code': exc.status_code})
                self.last_failed = effect_id
            raise
        save('rag_effect_returned', result)
        return result


class TaskCheckedClient:
    def __init__(self, client, check_running, effects):
        self.client = client
        self.check_running = check_running
        self.effects = effects

    def __getattr__(self, name):
        return getattr(self.client, name)

    def _check(self):
        if self.check_running() is not True:
            raise TaskPaused('parent task is paused')

    def request(self, method, endpoint, body=None, *args, **kwargs):
        if method == 'GET' and isinstance(self.client, RagflowClient):
            native = copy.copy(self.client)
            send_once = native._request_once

            def checked_once(request, timeout):
                self._check()
                return send_once(request, timeout)

            native._request_once = checked_once
            return native.request(method, endpoint, body, *args, **kwargs)
        if method == 'GET' or (method == 'POST' and endpoint.split('?', 1)[0] == '/retrieval'):
            self._check()
            return self.client.request(method, endpoint, body, *args, **kwargs)
        payload = json.dumps(body, sort_keys=True, ensure_ascii=False, allow_nan=False,
                             separators=(',', ':')).encode('utf8')
        action = {'method': method, 'endpoint': endpoint, 'body_sha256': hashlib.sha256(payload).hexdigest()}
        return self.effects.send(action, self.check_running,
                                 lambda: self.client.request(method, endpoint, body, *args, **kwargs))

    def upload_bytes(self, dataset, name, data):
        action = {'upload_dataset': dataset, 'name': name, 'sha256': hashlib.sha256(data).hexdigest()}
        return self.effects.send(action, self.check_running, lambda: self.client.upload_bytes(dataset, name, data))

    def upload_file(self, parent, name, data):
        action = {'upload_parent': parent, 'name': name, 'sha256': hashlib.sha256(data).hexdigest()}
        return self.effects.send(action, self.check_running, lambda: self.client.upload_file(parent, name, data))

    def download_bytes(self, dataset, document):
        from paperflow.rag import RagPublisher

        self._check()
        return RagPublisher(self.client)._download(dataset, document)

    def download_file(self, file_id):
        self._check()
        return self.client.download_file(file_id)

    def download_image(self, image_id):
        from paperflow.rag import RagPublisher

        self._check()
        return RagPublisher(self.client).download_image(image_id)
