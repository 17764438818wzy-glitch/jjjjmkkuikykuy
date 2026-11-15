"""Candidate attachment branch: verified evidence to one raw model reply.

The caller supplies the actual retained-work/history checks, the new Prefect
folder verifier and global leases. No attachment result is parsed or rendered.
"""
from pathlib import Path

from attachment_raw_input import check_attachment_source, prepare_attachment_body
from folder_completion import expected_components
from folder_ledger import _identity
from raw_request import run_raw_request


def select_attachment(manifest, row):
    if not isinstance(manifest, dict) or not isinstance(manifest.get('attachments'), list) or not isinstance(row, dict):
        raise ValueError('附件文件夹交接不可核验')
    matches = [item for item in manifest['attachments'] if isinstance(item, dict)
               and item.get('file_id') == row.get('file_id')]
    if len(matches) != 1 or matches[0] != row:
        raise ValueError('附件不属于当前文件夹交接')
    return matches[0]


def run_attachment_raw(*, identity, generation, manifest, row, work, connection, stop,
                       guard, upstream, lease, verify_work, check_history, request_once, opener=None):
    identity = _identity(identity)
    if (type(generation) is not int or generation < 0 or stop is None
            or not callable(getattr(guard, 'admit', None)) or not callable(getattr(guard, 'complete', None))
            or not callable(getattr(upstream, 'for_execution', None))
            or not all(callable(fn) for fn in (lease, verify_work, check_history, request_once))):
        raise ValueError('附件处理缺少工单、历史或发送准入')
    if stop.is_set():
        raise InterruptedError('任务已暂停，未开始附件处理')
    if (not isinstance(manifest, dict) or any(manifest.get(k) != identity[k] for k in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
        raise ValueError('附件文件夹交接身份不一致')
    select_attachment(manifest, row)
    components = expected_components(manifest, identity['task_run_id'])
    matched = [item for item in components if item['kind'] == 'attachment' and item['file_id'] == row['file_id']]
    if len(matched) != 1 or matched[0]['source_sha256'] != row['sha256']:
        raise ValueError('附件文件夹处理项身份不一致')
    item = matched[0]
    work = Path(work).resolve()

    def check_source():
        if upstream.for_execution(identity) != manifest:
            raise ValueError('附件上游交接身份发生变化')
        select_attachment(manifest, row)
        verify_work(row, work)
        check_attachment_source(row)

    def admit(transport):
        return guard.admit(identity['task_id'], generation, item['component_id'], transport['request_sha256'],
                           kind='attachment', work_dir=work, model_dir=transport['work_dir'],
                           receipt_id=identity['receipt_id'], task_run_id=identity['task_run_id'],
                           source_sha256=row['sha256'])

    result = run_raw_request(kind='attachment', request_body_factory=lambda: prepare_attachment_body(row, work, connection),
                             connection=connection, work=work, stop=stop, request_once=request_once, lease=lease,
                             admit=admit, check_history=check_history, check_source=check_source, opener=opener)
    guard.complete(identity['task_id'], item['component_id'], result['response_sha256'])
    return {'file_id': row['file_id'], **result}
