"""A user-authorized replacement of exact old requests, without rewriting history.

This is a view over the existing manual_retry receipt. It is not a scheduler or
an automatic retry policy. New unknown requests are never covered by old grants.
"""
import hashlib
import json
import copy
from uuid import UUID

FIELD = 'user_model_rerun'
MARKER = '_user_authorized_model_replacement'
MODEL_KINDS = frozenset({'llm_request_intent', 'llm_request_started', 'llm_request_unknown',
    'llm_request_not_sent', 'llm_request_failed', 'llm_response_saved', 'llm_gateway_not_dispatched'})
IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def digest(value):
    # Same wire identity as paperflow_prefect.records.identity.
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()


def _require(value, message):
    if not value:
        raise ValueError('Explicit rerun authorization: ' + message)


def _sha(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def authorization(records, identity=None):
    manual = next((row for row in records if row.get('kind') == 'manual_retry' and FIELD in row.get('data', {})), None)
    if not manual:
        return None
    data, auth = manual['data'], manual['data'][FIELD]
    _require(isinstance(auth, dict) and auth.get('schema') == 'paperflow-user-rerun/1', 'unknown schema')
    bound = auth.get('identity')
    _require(isinstance(bound, dict) and all(isinstance(bound.get(key), str) and bound[key] for key in IDENTITY), 'missing identity')
    _require(bound['attempt_id'] == bound['task_run_id'] and bound['parent_run_id'] == bound['task_id'], 'native identity mismatch')
    _require(manual.get('task_id') == bound['task_id'] and manual.get('folder_id') == bound['group_id']
        and data.get('identity') == bound, 'receipt binding mismatch')
    _require(identity is None or bound == identity, 'another child or paper')
    _require(data.get('stage') == 'llm' and data.get('state') in {'authorized', 'running', 'scheduled'}
        and data.get('retry_id') == auth.get('retry_id'), 'different manual retry')
    for value in (bound['task_id'], bound['task_run_id'], auth['retry_id'], auth.get('original_failed_state_id')):
        try:
            UUID(value)
        except (ValueError, TypeError, AttributeError):
            raise ValueError('Explicit rerun authorization: invalid native UUID') from None
    prepared = next((row['data'] for row in records if row.get('kind') == 'preprocessing_complete'), None)
    _require(prepared is not None and all(prepared.get(key) == bound[key]
        for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')), 'preprocessing identity mismatch')
    requests = auth.get('requests')
    _require(isinstance(requests, list) and requests, 'no frozen requests')
    keys = set()
    for item in requests:
        _require(isinstance(item, dict) and item.get('component_id') and item.get('request_id')
            and item.get('kind') in {'body', 'attachment'} and _sha(item.get('request_sha256'))
            and isinstance(item.get('model_dir'), str) and item['model_dir']
            and isinstance(item.get('retry_work_root'), str) and item['retry_work_root'], 'invalid request identity')
        key = (item['component_id'], item['request_id'])
        _require(key not in keys, 'duplicate request'); keys.add(key)
        files = item.get('journal_files')
        _require(isinstance(files, dict) and {'request.json', 'request-state.json'} <= files.keys()
            and set(files) <= {'request.json', 'request-state.json', 'response.txt', 'response-events.jsonl',
                              'http-error.txt', 'gateway-not-dispatched.json'}
            and all(_sha(value) for value in files.values()), 'invalid journal inventory')
        refs = item.get('records')
        _require(isinstance(refs, list) and refs, 'missing original records')
        for ref in refs:
            _require(ref.get('event_id') and ref.get('event_kind') in MODEL_KINDS
                and _sha(ref.get('data_sha256')) and _sha(ref.get('snapshot_data_sha256')), 'invalid original record')
            try:
                UUID(ref.get('source_artifact_id'))
            except (ValueError, TypeError, AttributeError):
                raise ValueError('Explicit rerun authorization: invalid original artifact') from None
    return manual, auth


def _marker(manual, auth, item):
    return {'kind': MARKER, 'event_id': auth['retry_id'] + ':' + item['component_id'] + ':' + item['request_id'],
        'task_id': manual['task_id'], 'folder_id': manual['folder_id'],
        'recorded_at': manual.get('recorded_at'),
        'data': {**item, 'retry_id': auth['retry_id'], 'identity': auth['identity'],
                 'authorization_sha256': digest(auth)}}


def active_model_records(records, identity=None):
    selected = authorization(records, identity)
    if selected is None:
        return records
    manual, auth = selected
    expected_markers = [_marker(manual, auth, item) for item in auth['requests']]
    existing = [row for row in records if row.get('kind') == MARKER]
    if existing:
        _require(existing == expected_markers, 'changed current-attempt view')
        old_keys = {(item['component_id'], item['request_id']) for item in auth['requests']}
        _require(not any(row.get('kind') in MODEL_KINDS and
            (row.get('data', {}).get('component_id'), row.get('data', {}).get('request_id')) in old_keys
            for row in records), 'original record reinserted in current-attempt view')
        return records
    by_request = {(item['component_id'], item['request_id']): item for item in auth['requests']}
    expected = {(ref['source_artifact_id'], ref['event_id'], ref['event_kind'], ref['data_sha256'])
        for item in auth['requests'] for ref in item['records']}
    seen = set()
    active = []
    for row in records:
        data = row.get('data') or {}
        key = (data.get('component_id'), data.get('request_id'))
        if row.get('kind') in MODEL_KINDS and key in by_request:
            item = by_request[key]
            _require(row.get('task_id') == manual['task_id'] and row.get('folder_id') == manual['folder_id']
                and all(data.get(field) == item[field] for field in ('component_id', 'request_id', 'kind', 'request_sha256')),
                'old request changed or belongs to another folder')
            witness = (row.get('_native_artifact_id'), row.get('event_id'), row['kind'], digest(data))
            _require(witness in expected, 'original record changed or late outcome arrived')
            seen.add(witness)
        else:
            active.append(row)
    _require(seen == expected, 'original record missing')
    return [*active, *expected_markers]


def replacement_for(records, component_id, retry_id=None, request_id=None):
    records = active_model_records(records)
    manual = next((row.get('data') or {} for row in records if row.get('kind') == 'manual_retry'), {})
    markers = [row['data'] for row in records if row.get('kind') == MARKER
        and row['data']['component_id'] == component_id
        and (request_id is None or row['data']['request_id'] == request_id)]
    if not markers:
        return None
    selected = markers[0]
    if manual.get('retry_id') != selected['retry_id']:
        return None  # Historical acknowledgement never grants another send.
    _require(retry_id is None or selected['retry_id'] == retry_id, 'retry identity changed')
    return selected


def active_snapshot_rows(rows):
    """Apply the same exact native authorization to derived display evidence."""
    originals = []
    for row in sorted(rows, key=lambda row: (row.data or {}).get('recorded_at') or '', reverse=True):
        data = row.data or {}
        if data.get('event_kind'):
            originals.append({'kind': data['event_kind'], 'data': data.get('event_data') or {},
                'event_id': data['event_id'], 'task_id': data['task_id'], 'folder_id': data['folder_id']})
    selected = authorization(originals)
    if selected is None:
        return rows
    manual, auth = selected
    witnesses = {ref['source_artifact_id']: (ref, item) for item in auth['requests'] for ref in item['records']}
    result = []
    for row in rows:
        data = row.data or {}; key = str(data.get('source_artifact_id'))
        if key in witnesses:
            ref, item = witnesses[key]; event = data.get('event_data') or {}
            _require(data.get('task_id') == manual['task_id'] and data.get('folder_id') == manual['folder_id']
                and data.get('event_id') == ref['event_id'] and data.get('event_kind') == ref['event_kind']
                and digest(event) == ref['snapshot_data_sha256']
                and all(event.get(field) == item[field] for field in ('component_id', 'request_id', 'kind', 'request_sha256')),
                'snapshot original identity changed')
        else:
            if data.get('event_kind') == 'manual_retry' and FIELD in (data.get('event_data') or {}):
                # A partial display projection must never become a send grant.
                # Keep retry/stage status, having checked its original binding
                # above, but do not re-interpret partial evidence as raw history.
                row = copy.copy(row)
                event = {key: value for key, value in data['event_data'].items() if key != FIELD}
                row.data = {**data, 'event_data': event}
            result.append(row)
    return result
