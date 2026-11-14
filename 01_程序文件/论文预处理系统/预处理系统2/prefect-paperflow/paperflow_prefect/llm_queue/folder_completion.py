"""Candidate folder-level raw completion; never dispatches or postprocesses.

The caller must provide a verified handoff, a trusted actual folder TaskRun,
all admission rows from one consistent snapshot, and a response verifier.
This module does not publish or atomically move a queue item.
"""
import hashlib
import json
import re


def _sha(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def _id(value):
    return isinstance(value, str) and bool(value)


def component_id(*, task_id, group_id, receipt_id, kind, file_id, source_sha256, part_id=None,
                 plan_revision=None, plan_sha256=None):
    identity = {'schema': 'paperflow-folder-llm-component/1', 'task_id': task_id,
                'group_id': group_id, 'receipt_id': receipt_id, 'kind': kind,
                'file_id': file_id, 'source_sha256': source_sha256, 'part_id': part_id}
    if plan_revision is not None:
        from uuid import UUID
        try:UUID(plan_revision)
        except (ValueError,TypeError,AttributeError):raise ValueError('正文重分段版本无效') from None
        if kind != 'body' or not _sha(plan_sha256):raise ValueError('正文重分段计划不可核验')
        identity.update(plan_revision=plan_revision,plan_sha256=plan_sha256)
    if (not all(_id(value) for value in (task_id, group_id, file_id)) or not _sha(receipt_id)
            or not _sha(source_sha256) or kind not in {'body', 'attachment'}
            or (kind == 'body' and not _id(part_id)) or (kind == 'attachment' and part_id is not None)):
        raise ValueError('文件夹内 LLM 处理项身份无效')
    encoded = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf8')
    return hashlib.sha256(encoded).hexdigest()


def expected_components(manifest, task_run_id):
    if not isinstance(manifest, dict) or not _id(task_run_id):
        raise ValueError('文件夹工单身份无效')
    task_id, group_id, receipt_id = (manifest.get(k) for k in ('task_id', 'group_id', 'receipt_id'))
    body, attachments = manifest.get('body'), manifest.get('attachments')
    if (not _id(task_id) or not _id(group_id) or not _sha(receipt_id)
            or not isinstance(body, dict) or not _id(body.get('file_id')) or not _sha(body.get('sha256'))
            or not isinstance(attachments, list)):
        raise ValueError('文件夹交接身份无效')
    capacity = manifest.get('capacity') or {}
    plan = capacity.get('plan_identity') or {}
    reading = manifest.get('reading_material') or {}
    parts, mode = plan.get('parts'), plan.get('mode')
    if (capacity.get('status') != 'planned' or plan.get('schema') != 'paperflow-body-input-plan/1'
            or plan.get('source_sha256') != body['sha256'] or not _sha(reading.get('sha256'))
            or plan.get('reading_sha256') != reading['sha256']
            or not _sha(plan.get('plan_sha256')) or not _sha(plan.get('coverage_sha256'))
            or not isinstance(parts, list) or not parts or capacity.get('parts') != len(parts)
            or plan.get('part_count') != len(parts) or capacity.get('mode') != mode
            or mode not in {'single', 'partitioned'} or (mode == 'single' and len(parts) != 1)
            or (mode == 'partitioned' and len(parts) < 2)):
        raise ValueError('正文分段计划不可核验')
    components = []
    for index, part in enumerate(parts, 1):
        if (not isinstance(part, dict) or part.get('id') != f'part-{index:03d}'
                or not _sha(part.get('request_sha256'))):
            raise ValueError('正文分段计划内容无效')
        ident = component_id(task_id=task_id, group_id=group_id, receipt_id=receipt_id,
                             kind='body', file_id=body['file_id'], source_sha256=body['sha256'],
                             part_id=part['id'], plan_revision=manifest.get('body_plan_revision'),
                             plan_sha256=plan['plan_sha256'])
        components.append({'component_id': ident, 'kind': 'body', 'source_sha256': body['sha256'],
                           'request_sha256': part['request_sha256'], 'part_id': part['id']})
    attachment_ids = []
    for attachment in attachments:
        if not isinstance(attachment, dict) or not _id(attachment.get('file_id')) or not _sha(attachment.get('sha256')):
            raise ValueError('附件交接身份无效')
        file_id, digest = attachment['file_id'], attachment['sha256']
        attachment_ids.append(file_id)
        ident = component_id(task_id=task_id, group_id=group_id, receipt_id=receipt_id,
                             kind='attachment', file_id=file_id, source_sha256=digest)
        components.append({'component_id': ident, 'kind': 'attachment', 'source_sha256': digest,
                           'file_id': file_id})
    if len(attachment_ids) != len(set(attachment_ids)) or len({c['component_id'] for c in components}) != len(components):
        raise ValueError('文件夹交接存在重复处理项')
    return components


def inspect_folder(manifest, task_run_id, admission_rows, verify_result):
    """Determine only whether this one folder has all raw responses verified."""
    if not callable(verify_result) or not isinstance(admission_rows, list):
        raise ValueError('文件夹响应核验不可用')
    components = expected_components(manifest, task_run_id)
    expected = {item['component_id']: item for item in components}
    rows = {}
    for value in admission_rows:
        try:
            row = dict(value)
        except (TypeError, ValueError) as exc:
            raise ValueError('文件夹请求身份无效') from exc
        ident = row.get('component_id')
        item = expected.get(ident)
        if (not item or ident in rows or row.get('task_id') != manifest['task_id']
                or row.get('receipt_id') != manifest['receipt_id'] or row.get('task_run_id') != task_run_id
                or row.get('kind') != item['kind'] or row.get('source_sha256') != item['source_sha256']
                or not _sha(row.get('request_sha256'))
                or (item['kind'] == 'body' and row['request_sha256'] != item['request_sha256'])):
            raise ValueError('文件夹请求身份不一致')
        if row.get('state') not in {'admitted', 'returned_unverified', 'unknown', 'raw_complete'}:
            raise ValueError('文件夹请求状态不可核验')
        rows[ident] = row
    pending = 0
    for component in components:
        row = rows.get(component['component_id'])
        if not row or row['state'] != 'raw_complete':
            pending += 1
            continue
        if not _sha(row.get('response_sha256')):
            raise ValueError('文件夹响应身份不可核验')
        verify_result(row)
    if pending:
        return {'status': 'incomplete', 'task_id': manifest['task_id'],
                'group_id': manifest['group_id'], 'task_run_id': task_run_id, 'pending_count': pending}
    return {'status': 'raw_complete', 'task_id': manifest['task_id'], 'group_id': manifest['group_id'],
            'task_run_id': task_run_id, 'body_parts': len(manifest['capacity']['plan_identity']['parts']),
            'attachments': len(manifest['attachments'])}
