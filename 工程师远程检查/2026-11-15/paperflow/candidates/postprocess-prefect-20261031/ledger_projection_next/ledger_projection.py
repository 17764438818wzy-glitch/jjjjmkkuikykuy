"""Read-only historical projection. Never dispatch or verify artifact files."""
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import sqlite3
from uuid import UUID, uuid5

BASIS = 'postprocessing-registered-ledger/1'
FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
HEX = re.compile(r'[0-9a-f]{64}')
GROUP = re.compile(r'[0-9a-f]{32}')
ATTEMPT = re.compile(r'[A-Za-z0-9_.:-]{1,128}')
DELIVERY_ID = re.compile(r'[A-Za-z0-9_-]{1,128}')
JOB_NAMESPACE = UUID('5e9b7732-458e-50cb-8916-6b53007c204f')
ACTIVE_JOBS = {'reserved', 'submitting', 'submitted', 'executing'}
UNKNOWN_JOBS = {'submission_unknown', 'execution_unknown', 'identity_unknown', 'delivery_unknown',
                'stopped_before_submit', 'cancel_fenced_reserved', 'cancel_fenced_submitting',
                'cancel_fenced_submitted'}
FAILURES = {'invalid_handoff', 'missing_source', 'incomplete_response', 'body_format', 'body_validation',
            'attachment_format', 'attachment_validation', 'packaging', 'resource_unavailable'}
UNRESOLVED_CODES = {'processing', 'paused', 'outcome_unknown', 'worker_lost', 'evidence_unavailable'}
STAGE_COUNT_KEYS = ('waiting_postprocessing', 'postprocessing_in_progress',
                    'postprocessing_registered_inflight', 'postprocessing_registered_executing',
                    'waiting_rag_upload', 'postprocessing_held', 'postprocessing_unresolved')
TABLES = ('tasks', 'task_deletions', 'llm_folder_queue', 'llm_request_admissions',
          'paperflow_postprocess_stage_jobs', 'postprocessing_claims', 'postprocessing_results',
          'postprocessing_receipts')
REQUIRED_COLUMNS = {
    'tasks': {'task_id', 'name', 'prefect_order_model', 'prefect_run_id', 'desired_state',
              'start_pending', 'task_status', 'start_generation'},
    'task_deletions': {'task_id'},
    'llm_folder_queue': set(FIELDS) | {'manifest_sha256', 'state', 'generation'},
    'llm_request_admissions': {'task_id', 'task_run_id', 'component_id', 'kind', 'state', 'receipt_id', 'generation',
                               'request_sha256', 'response_sha256', 'source_sha256'},
    'paperflow_postprocess_stage_jobs': set(FIELDS) | {'job_id', 'folder_run_id', 'task_key', 'generation',
        'source_llm_generation', 'manifest_sha256', 'state'},
    'postprocessing_claims': set(FIELDS) | {'manifest_sha256', 'generation', 'claim_id', 'state'},
    'postprocessing_results': set(FIELDS) | {'manifest_sha256', 'generation', 'result_id', 'state', 'code',
        'claim_id', 'stage_receipt_sha256', 'proof_path', 'proof_sha256', 'created_at_ns'},
    'postprocessing_receipts': {'claim_id', 'receipt_json', 'receipt_sha256'},
}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _hex(value):
    return isinstance(value, str) and HEX.fullmatch(value) is not None


def _pure_path(value):
    """Parse absolute Windows or POSIX syntax without consulting the filesystem."""
    if not isinstance(value, str) or not value or '\x00' in value:
        return None
    windows = PureWindowsPath(value)
    if windows.is_absolute():
        return windows
    posix = PurePosixPath(value)
    return posix if posix.is_absolute() else None


def _absolute(value):
    return _pure_path(value) is not None


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False


def _identity(row):
    return (isinstance(row, dict) and all(isinstance(row.get(k), str) and row[k] for k in FIELDS)
            and all(_uuid(row[k]) for k in ('task_id', 'parent_run_id', 'task_run_id'))
            and GROUP.fullmatch(row['group_id']) is not None and _hex(row['receipt_id'])
            and ATTEMPT.fullmatch(row['attempt_id']) is not None)


def _matches(a, b):
    return (all(a.get(k) == b.get(k) for k in FIELDS)
            and a.get('manifest_sha256') == b.get('manifest_sha256'))


def _unique(rows, key):
    values = {}
    for row in rows:
        identity = key(row)
        if identity in values:
            raise ValueError('duplicate ledger identity')
        values[identity] = row
    return values


def _receipt_attachments_ok(identity, admitted, attachments, artifacts, component_id_fn):
    """Bind every registered attachment to the upstream's component identity."""
    rows = [row for row in admitted if row.get('kind') == 'attachment']
    if len(rows) != len(attachments):
        return False
    if not rows:
        return True
    if not callable(component_id_fn):
        return False
    expected = {row.get('component_id'): row for row in rows}
    if len(expected) != len(rows) or not all(_hex(key) for key in expected):
        return False
    seen_ids, seen_components = set(), set()
    try:
        for delivery in attachments:
            if (not isinstance(delivery, dict) or delivery.get('paper_id') != identity['paper_id']
                    or not isinstance(delivery.get('paper_id'), str)
                    or DELIVERY_ID.fullmatch(delivery['paper_id']) is None
                    or not isinstance(delivery.get('attachment_id'), str)
                    or DELIVERY_ID.fullmatch(delivery['attachment_id']) is None
                    or delivery['attachment_id'] in seen_ids
                    or not all(_hex(delivery.get(k)) for k in ('source_sha256','md_sha256','delivery_version'))):
                return False
            source, md = _pure_path(delivery.get('source_path')), _pure_path(delivery.get('md_path'))
            if (source is None or md is None or source == md
                    or artifacts.get(source) != delivery['source_sha256']
                    or artifacts.get(md) != delivery['md_sha256']):
                return False
            named = {'original_source_path', 'named_proof_path', 'named_proof_sha256'}
            if named & delivery.keys():
                original, proof = (_pure_path(delivery.get('original_source_path')),
                                   _pure_path(delivery.get('named_proof_path')))
                if (not named <= delivery.keys() or original is None or proof is None
                        or not _hex(delivery.get('named_proof_sha256'))
                        or artifacts.get(original) != delivery['source_sha256']
                        or artifacts.get(proof) != delivery['named_proof_sha256']):
                    return False
            component = component_id_fn(task_id=identity['task_id'], group_id=identity['group_id'],
                receipt_id=identity['receipt_id'], kind='attachment', file_id=delivery['attachment_id'],
                source_sha256=delivery['source_sha256'])
            row = expected.get(component)
            if not row or row['source_sha256'] != delivery['source_sha256'] or component in seen_components:
                return False
            seen_ids.add(delivery['attachment_id'])
            seen_components.add(component)
        return seen_components == set(expected)
    except Exception:
        return False


def _receipt_ok(identity, claim, result, saved, admitted, component_id_fn):
    """Check the stored receipt JSON and identity only; never open artifacts."""
    if (not saved or saved.get('claim_id') != claim.get('claim_id')
            or result.get('claim_id') != claim.get('claim_id')
            or not isinstance(saved.get('receipt_json'), str)
            or not _hex(saved.get('receipt_sha256'))
            or result.get('stage_receipt_sha256') != saved['receipt_sha256']
            or hashlib.sha256(saved['receipt_json'].encode('utf8')).hexdigest() != saved['receipt_sha256']):
        return False
    try:
        value = json.loads(saved['receipt_json'])
        artifacts, body = value.get('artifacts'), value.get('body')
        if (value.get('schema') != 'paperflow-postprocessing-receipt/1'
                or value.get('identity') != {key: identity[key] for key in FIELDS}
                or value.get('claim_id') != claim['claim_id']
                or value.get('manifest_sha256') != identity['manifest_sha256']
                or value.get('validation_status') != 'passed'
                or not isinstance(value.get('attachments'), list)
                or not isinstance(body, dict) or not _hex(body.get('manifest_sha256'))
                or not _absolute(body.get('package_path'))
                or not isinstance(artifacts, list) or not artifacts):
            return False
        paths = {}
        for artifact in artifacts:
            if not isinstance(artifact, dict) or not _hex(artifact.get('sha256')):
                return False
            path = _pure_path(artifact.get('path'))
            if path is None or path in paths:
                return False
            paths[path] = artifact['sha256']
        package = _pure_path(body['package_path'])
        if (paths.get(package / 'manifest.json') != body['manifest_sha256']
                or not _hex(paths.get(package / 'READY.json'))
                or not _hex(paths.get(package.parent / 'body-result.json'))):
            return False
        return _receipt_attachments_ok(identity, admitted, value['attachments'], paths, component_id_fn)
    except (AttributeError, TypeError, ValueError):
        return False


def _proof_ok(row, required=False):
    path, digest = row.get('proof_path'), row.get('proof_sha256')
    if path is None and digest is None:
        return not required
    return _absolute(path) and _hex(digest)


def _unavailable(reason, result_filter, page, page_size):
    now = _now()
    stage = {'available': False, 'basis': BASIS, 'as_of': now,
             'counts': dict.fromkeys(STAGE_COUNT_KEYS), 'partial': True,
             'message': '登记状态暂不可读取'}
    results = {'available': False, 'basis': BASIS, 'as_of': now,
               'counts': {'passed': None, 'failed': None, 'unrecognized': None},
               'partial': True, 'unresolved': None, 'message': '登记结果暂不可读取'}
    if result_filter is not None:
        results['list'] = {'filter': result_filter, 'page': page, 'page_size': page_size,
                           'total': None, 'has_next': False, 'items': []}
    return {'available': False, 'basis': BASIS, 'as_of': now, 'revision': None,
            'current_delivery_verified': False, 'reason': reason, 'stage': stage, 'results': results}


def snapshot(database, *, result_filter=None, page=1, page_size=20, expected_revision=None,
             component_id_fn=None):
    """Project all tables in one read transaction; no outside file probing."""
    if (result_filter not in {None, 'passed', 'failed', 'unrecognized'}
            or type(page) is not int or not 1 <= page <= 1_000_000
            or type(page_size) is not int or not 1 <= page_size <= 100
            or (expected_revision is not None and not _hex(expected_revision))):
        raise ValueError('invalid registered projection query')
    if result_filter is not None and page > 1 and expected_revision is None:
        return _unavailable('snapshot_revision_required', result_filter, page, page_size)
    try:
        path = Path(database).resolve(strict=True)
        if not path.is_file():
            raise ValueError('control database unavailable')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            as_of = _now()
            for name in TABLES:
                columns = {row['name'] for row in db.execute('PRAGMA table_info(' + name + ')')}
                if not REQUIRED_COLUMNS[name] <= columns:
                    raise ValueError('required ledger columns are missing')
            rows = {name: [dict(row) for row in db.execute('SELECT * FROM ' + name)] for name in TABLES}
        # This digest stabilizes database pagination, not artifact integrity.
        canonical = {name: sorted(json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
                                  for row in values) for name, values in rows.items()}
        ledger_revision = hashlib.sha256(json.dumps(canonical, sort_keys=True, ensure_ascii=False,
                                                    separators=(',', ':')).encode('utf8')).hexdigest()
        tasks = _unique(rows['tasks'], lambda r: r['task_id'])
        deleted = {r['task_id'] for r in rows['task_deletions']}
        key = lambda r: (r['task_id'], r['group_id'])
        folders = _unique(rows['llm_folder_queue'], key)
        jobs = _unique(rows['paperflow_postprocess_stage_jobs'], key)
        claims = _unique(rows['postprocessing_claims'], key)
        results = _unique(rows['postprocessing_results'], key)
        receipts = _unique(rows['postprocessing_receipts'], lambda r: r['claim_id'])
        _unique(rows['llm_request_admissions'], lambda r: (r['task_id'], r['component_id']))
        admissions = defaultdict(list)
        for row in rows['llm_request_admissions']:
            admissions[(row['task_id'], row['task_run_id'])].append(row)
        counts = dict.fromkeys(STAGE_COUNT_KEYS, 0)
        counts['postprocessing_in_progress'] = None  # no process or Prefect liveness claim
        buckets = {'passed': [], 'failed': [], 'unrecognized': []}
        unresolved = waiting = inflight = 0
        linked_receipts = {c.get('claim_id') for c in claims.values()}
        unmapped_receipts = sum(1 for claim_id in receipts if claim_id not in linked_receipts)
        for item in sorted(set(folders) | set(jobs) | set(claims) | set(results)):
            task_id, group_id = item
            if task_id in deleted:
                continue
            folder, job, claim, result = folders.get(item), jobs.get(item), claims.get(item), results.get(item)
            if folder and folder.get('state') != 'waiting_postprocessing' and not any((job, claim, result)):
                continue
            state = job.get('state') if job else None
            task = tasks.get(task_id)
            valid = (folder is not None and _identity(folder) and _hex(folder.get('manifest_sha256'))
                     and folder.get('state') == 'waiting_postprocessing' and task is not None
                     and task.get('prefect_order_model') == 'v2'
                     and task.get('prefect_run_id') == folder.get('parent_run_id')
                     and isinstance(task.get('name'), str)
                     and task.get('desired_state') in {'running', 'paused'}
                     and task.get('task_status') in {'ready', 'running', 'pausing', 'paused',
                                                     'complete', 'needs_review', 'failed'}
                     and type(task.get('start_pending')) is int and task['start_pending'] == 0
                     and type(folder.get('generation')) is int and folder['generation'] >= 1
                     and type(task.get('start_generation')) is int
                     and task['start_generation'] >= folder['generation'])
            if valid:
                admitted = admissions.get((task_id, folder['task_run_id']), [])
                valid = (bool(admitted) and any(row.get('kind') == 'body' for row in admitted)
                         and all(row.get('kind') in {'body', 'attachment'}
                                 and row.get('state') == 'raw_complete'
                                 and row.get('receipt_id') == folder['receipt_id']
                                 and type(row.get('generation')) is int
                                 and 1 <= row['generation'] <= folder['generation']
                                 and _hex(row.get('request_sha256')) and _hex(row.get('response_sha256'))
                                 and _hex(row.get('source_sha256')) for row in admitted))
            if valid and not job and not claim and not result:
                counts['waiting_postprocessing'] += 1
                waiting += 1
                continue
            if valid and job:
                generation, source = job.get('generation'), job.get('source_llm_generation')
                job_id = str(uuid5(JOB_NAMESPACE, folder['task_run_id']))
                valid = (_matches(job, folder) and job.get('job_id') == job_id
                         and job.get('folder_run_id') == folder['task_run_id']
                         and job.get('task_key') == 'paperflow-folder-postprocess-stage-v1-' + UUID(job_id).hex
                         and type(generation) is int and type(source) is int
                         and source == folder['generation'] and 1 <= source <= generation <= task['start_generation'])
                for record in (claim, result):
                    if record and not _matches(record, folder):
                        valid = False
                if claim and (type(claim.get('generation')) is not int
                              or not generation <= claim['generation'] <= task['start_generation']
                              or not _uuid(claim.get('claim_id'))):
                    valid = False
                if result and (result.get('generation') != generation or not _uuid(result.get('result_id'))):
                    valid = False
            else:
                valid = False
            if not valid:
                unresolved += 1
                continue
            if state in ACTIVE_JOBS:
                counts['postprocessing_registered_inflight'] += 1
                inflight += 1
            if state == 'executing':
                counts['postprocessing_registered_executing'] += 1
            if state in {'reserved', 'submitting', 'submitted'}:
                if claim or result:
                    unresolved += 1
                continue
            if state == 'executing':
                if (claim and claim.get('state') not in {'processing', 'waiting_rag', 'receipt_held'}
                        or result and (result.get('state') != 'unresolved'
                                       or result.get('code') not in UNRESOLVED_CODES)):
                    unresolved += 1
                continue
            if state in UNKNOWN_JOBS:
                unresolved += 1
                continue
            if state == 'finished' and claim and result and claim.get('state') in {'waiting_rag', 'receipt_held'}:
                if (result.get('state') == 'passed' and result.get('code') == 'passed'
                        and _receipt_ok(folder, claim, result, receipts.get(claim.get('claim_id')),
                                        admitted, component_id_fn)):
                    category = 'passed'
                    counts['waiting_rag_upload' if claim['state'] == 'waiting_rag' else 'postprocessing_held'] += 1
                else:
                    unresolved += 1
                    continue
            elif state == 'failed' and result and (not claim or claim.get('state') in {'unknown', 'failed'}):
                if (result.get('state') == 'failed' and result.get('code') in FAILURES
                        and _proof_ok(result, required=True)):
                    category = 'failed'
                elif (result.get('state') == 'unrecognized' and result.get('code') == 'unrecognized'
                      and _proof_ok(result, required=True)):
                    category = 'unrecognized'
                else:
                    unresolved += 1
                    continue
            else:
                unresolved += 1
                continue
            buckets[category].append({'task_id': task_id, 'group_id': group_id,
                'paper_id': folder['paper_id'], 'receipt_id': folder['receipt_id'],
                'task_name': task['name'], 'folder_label': '资料 ' + group_id,
                'code': result['code'], 'reason': {'passed': '已登记通过', 'failed': '已登记未通过',
                                                    'unrecognized': '已登记未识别'}[category],
                '_order': (result.get('created_at_ns') or 0, task_id, group_id)})
        counts['postprocessing_unresolved'] = unresolved
        # The same DB can project differently if an injected dependency changes.
        # Bind the revision to the complete, ordered classification as well as the DB rows.
        revision_value = {'basis': BASIS, 'ledger': ledger_revision, 'counts': counts,
                          'buckets': buckets, 'unmapped_receipts': unmapped_receipts,
                          'waiting': waiting, 'inflight': inflight, 'unresolved': unresolved}
        revision = hashlib.sha256(json.dumps(revision_value, sort_keys=True, ensure_ascii=False,
                                             separators=(',', ':')).encode('utf8')).hexdigest()
        if expected_revision is not None and expected_revision != revision:
            return _unavailable('snapshot_changed', result_filter, page, page_size)
        stage = {'available': True, 'basis': BASIS, 'as_of': as_of, 'counts': counts,
                 'unmapped_receipts': unmapped_receipts, 'partial': bool(unresolved or unmapped_receipts),
                 'message': '已登记阶段快照；未实时确认进程活性或交付材料。'}
        output = {'available': True, 'basis': BASIS, 'as_of': as_of, 'revision': revision,
                  'current_delivery_verified': False, 'stage': stage}
        summary = {'available': True, 'basis': BASIS, 'as_of': as_of,
                   'counts': {'passed': len(buckets['passed']), 'failed': len(buckets['failed']),
                              'unrecognized': None},
                   'registered_unrecognized': len(buckets['unrecognized']),
                   'unresolved': unresolved, 'partial': bool(unresolved or unmapped_receipts or waiting or inflight),
                   'message': '已登记结果；未识别总数、当前可交付性及上传状态未确认。'}
        if result_filter is not None:
            records = sorted(buckets[result_filter], key=lambda row: row['_order'])
            start = (page - 1) * page_size
            items = [{k: v for k, v in row.items() if k != '_order'} for row in records[start:start + page_size]]
            summary['list'] = {'filter': result_filter, 'page': page, 'page_size': page_size,
                               'total': None if result_filter == 'unrecognized' else len(records),
                               'registered_total': len(records), 'has_next': start + page_size < len(records),
                               'items': items}
        output['results'] = summary
        return output
    except Exception:
        return _unavailable('ledger_unavailable', result_filter, page, page_size)
