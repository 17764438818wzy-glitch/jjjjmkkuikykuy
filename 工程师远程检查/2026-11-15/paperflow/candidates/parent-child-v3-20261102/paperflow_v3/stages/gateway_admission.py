"""Verify one gateway refusal without treating it as a model response/retry."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import ProxyHandler

ENDPOINT = 'http://192.168.1.120:8081/v1/chat/completions'
HEADER = 'x-workbuddy-admission'
MARKER = 'not-dispatched-v1'
CODE = 'workbuddy_local_capacity_not_sent'
KIND = 'llm_gateway_not_dispatched'
PROOF_FILE = 'gateway-admission.json'
RESPONSE_FILE = 'gateway-rejection.json'


class GatewayNotDispatched(Exception):
    """One HTTP exchange completed, with proof that no model was dispatched."""


class GatewayCapacityWait(Exception):
    """The original child may re-enter admission after releasing its stage."""
    def __init__(self, seconds):
        self.seconds = float(seconds)
        super().__init__('模型本地容量繁忙，等待空位')


def endpoint(connection):
    value = connection['base_url'].rstrip('/')
    return value if value.endswith('/chat/completions') else value + '/chat/completions'


def binding(identity, component_id, request_id, kind):
    return {'identity': dict(identity), 'component_id': component_id,
            'request_id': request_id, 'kind': kind}


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('网关证明JSON字段重复')
        result[key] = value
    return result


def _json(raw):
    return json.loads(raw, object_pairs_hook=_unique_object)


def _pairs(headers):
    if headers is None:
        return []
    if isinstance(headers, list):
        return [(str(key).lower(), str(value)) for key, value in headers]
    return [(str(key).lower(), str(value)) for key, value in headers.items()]


def _values(headers, name):
    return [value for key, value in _pairs(headers) if key == name]


def _signal_headers(headers):
    content = _values(headers, 'content-type')
    return (_values(headers, HEADER) == [MARKER] and len(content) == 1
            and content[0].split(';', 1)[0].strip().lower() == 'application/json')


def _signal(url, status, headers, raw):
    if url != ENDPOINT or status != 429 or not _signal_headers(headers):
        return False
    try:
        data = _json(raw)
    except (UnicodeError, ValueError, TypeError):
        return False
    return isinstance(data, dict) and isinstance(data.get('error'), dict) and data['error'].get('code') == CODE


def _exclusive(path, raw):
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


class GatewayOpener:
    """A single native HTTP attempt. Waiting belongs outside the model lease."""
    def __init__(self, *, connection, model_dir, request_binding, stop, delegate=None):
        self.endpoint = endpoint(connection)
        self.model = Path(model_dir)
        self.binding = request_binding
        self.used = False
        if delegate is None:
            from preextract_request import NoRedirect
            from stop_http import cancellable_opener
            # Proof is accepted only for this direct, fixed LAN endpoint.
            handlers = (ProxyHandler({}), NoRedirect()) if self.endpoint == ENDPOINT else (NoRedirect(),)
            delegate = cancellable_opener(stop, *handlers)
        self.delegate = delegate

    def open(self, request, timeout=None):
        if self.used:
            raise RuntimeError('同一模型请求不能再次调用HTTP')
        self.used = True
        try:
            return self.delegate.open(request, timeout=timeout)
        except HTTPError as error:
            if (self.endpoint != ENDPOINT or request.full_url != ENDPOINT
                    or request.get_method() != 'POST' or error.geturl() != ENDPOINT
                    or error.code != 429 or not _signal_headers(error.headers)):
                raise
            raw = error.read()
            if not _signal(error.geturl(), error.code, error.headers, raw):
                # The native journal still receives every original error byte.
                raise HTTPError(error.geturl(), error.code, error.reason,
                                error.headers, io.BytesIO(raw)) from error
            selected = [[key, value] for key, value in _pairs(error.headers)
                        if key in {HEADER, 'content-type', 'retry-after'}]
            proof = {'schema': 'paperflow-gateway-admission/1', 'endpoint': self.endpoint,
                'status': 429, 'headers': selected, 'binding': self.binding,
                'request_sha256': _sha(request.data), 'response_sha256': _sha(raw)}
            _exclusive(self.model / RESPONSE_FILE, raw)
            _exclusive(self.model / PROOF_FILE,
                       json.dumps(proof, ensure_ascii=False, sort_keys=True).encode('utf8'))
            # Do not let the native HTTPError handler apply upstream rate-limit
            # backoff to a locally rejected request. Its one-shot journal stays.
            raise GatewayNotDispatched('网关确认未派发模型') from None


def verify(model_dir, expected_binding=None):
    """Read back immutable proof plus the unchanged native one-call journal."""
    model = Path(model_dir).resolve()
    proof_raw = (model / PROOF_FILE).read_bytes()
    proof = _json(proof_raw)
    raw = (model / RESPONSE_FILE).read_bytes()
    state_raw = (model / 'request-state.json').read_bytes()
    state = _json(state_raw)
    body = _json((model / 'request.json').read_bytes())
    canonical = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode('utf8')
    events = model / 'response-events.jsonl'
    if (proof.get('schema') != 'paperflow-gateway-admission/1'
            or not _signal(proof.get('endpoint'), proof.get('status'), proof.get('headers', []), raw)
            or proof.get('response_sha256') != _sha(raw)
            or proof.get('request_sha256') != _sha(canonical)
            or state.get('request_sha256') != _sha(canonical)
            or state.get('status') != 'request_failed_or_uncertain'
            or state.get('error_type') != 'GatewayNotDispatched'
            or state.get('http_invoked') is not True or state.get('client_http_requests') != 1
            or (model / 'response.txt').exists() or (events.exists() and events.stat().st_size)
            or (model / 'http-error.txt').exists()
            or expected_binding is not None and proof.get('binding') != expected_binding):
        raise ValueError('网关未派发证明或原请求身份不一致')
    return {'gateway_binding': proof['binding'], 'endpoint_sha256': _sha(ENDPOINT.encode()),
        'request_sha256': proof['request_sha256'], 'http_status': 429,
        'gateway_proof_sha256': _sha(proof_raw), 'native_state_sha256': _sha(state_raw),
        'error_response_sha256': proof['response_sha256'], 'model_dir': str(model)}


def verify_receipt(data, identity=None):
    if not has_proof(data):
        raise ValueError('网关等待回执证据缺失')
    expected = data['gateway_binding']
    if (expected.get('component_id') != data.get('component_id')
            or expected.get('request_id') != data.get('request_id')
            or expected.get('kind') != data.get('kind')
            or identity is not None and expected.get('identity') != identity):
        raise ValueError('网关等待回执不属于原工单')
    proof = verify(data['model_dir'], expected)
    if any(data.get(key) != value for key, value in proof.items()):
        raise ValueError('网关等待原始证据发生变化')
    return proof


def has_proof(data):
    """Validate stored proof metadata for display, never authorize another POST.

    Runtime re-entry always uses verify_receipt and reads back original bytes.
    Read-only snapshots remain useful after successfully archived work is cleaned.
    """
    expected = data.get('gateway_binding')
    if not isinstance(expected, dict) or not isinstance(expected.get('identity'), dict):
        return False
    identity = expected['identity']
    if (any(not isinstance(identity.get(key), str) or not identity[key].strip() for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id'))
            or identity['attempt_id'] != identity['task_run_id'] or identity['parent_run_id'] != identity['task_id']):
        return False
    if (data.get('http_status') != 429 or data.get('kind') not in {'body', 'attachment'}
            or any(expected.get(key) != data.get(key) for key in ('component_id', 'request_id', 'kind'))
            or data.get('endpoint_sha256') != _sha(ENDPOINT.encode())
            or not isinstance(data.get('model_dir'), str) or not data['model_dir']):
        return False
    return all(isinstance(data.get(key), str) and len(data[key]) == 64
        and all(char in '0123456789abcdef' for char in data[key]) for key in (
            'request_sha256', 'gateway_proof_sha256', 'native_state_sha256', 'error_response_sha256'))


def retry_delay(data, prior_waits=0):
    proof = _json((Path(data['model_dir']) / PROOF_FILE).read_bytes())
    hint = _values(proof['headers'], 'retry-after')
    seconds = min(30.0, 1.5 * 2 ** min(prior_waits, 5))
    if len(hint) == 1:
        try:
            value = hint[0].strip()
            parsed = float(value) if value.isascii() and value.replace('.', '', 1).isdigit() else (
                parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            seconds = max(1.5, min(30.0, parsed))
        except (ValueError, TypeError, OverflowError):
            pass
    # Stable per-request jitter avoids all waiting folders re-entering together.
    jitter = .9 + int(data['request_sha256'][:2], 16) / 2550
    return max(1.5, min(30.0, seconds * jitter))
