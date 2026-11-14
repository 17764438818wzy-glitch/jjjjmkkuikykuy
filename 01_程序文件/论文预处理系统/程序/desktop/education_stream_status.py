"""Read-only view of the live education importer receipts.

This never edits the GUI's active preprocessing manifest. A running GUI may
therefore keep writing its own batch state without a second writer racing it.
"""
from __future__ import annotations

import json
from pathlib import Path

PAPER_NAME = '教育论文研究范例·八篇试验 md'
IMAGE_NAME = '教育论文研究范例·八篇试验图片'
FAILURE_LABELS = {
    'queue_only': '解析失败，最终仅有队列提示',
    'outcome_unresolved': '解析结果待核查（队列提示不等于质量裁决）',
    'embedding_arrearage': '向量模型账户欠费',
    'annotation_invalid': '标注校验失败（技术）',
    'input_invalid': '交接或准入配置校验失败',
    'other_failure': '远端处理失败',
}
_cache: dict[str, tuple[int, dict]] = {}


def queue_paper_status(queue, service_url: str, paper_dataset_id: str,
                       paper_id: str) -> dict:
    """Read one new-batch primary task without scanning historical manifests."""
    row = queue.find_identity(service_url, paper_dataset_id, paper_id, 'paper_primary')
    if row is None:
        return {'upload': '待上传', 'process': '尚未提交', 'remote_id': '', 'status': ''}
    status = row['status']
    upload = ('已排队' if status == 'upload_waiting' else
              '上传中' if status == 'upload_intent' else
              '待核查' if status == 'upload_unknown' else
              '已上传' if row.get('remote_id') else '待核查')
    process = {
        'upload_waiting': '等待上传', 'upload_intent': '上传中',
        'upload_unknown': '上传结果不确定', 'uploaded': '等待解析',
        'parse_waiting': '等待解析', 'parse_intent': '解析提交中',
        'submitted': '解析中', 'running': '解析中',
        'accepted': '入库', 'quality_hold': '暂缓',
        'quality_reject': '剔除', 'technical_review': '技术待核查',
        'outcome_unresolved': '结果待核查', 'parse_unknown': '解析提交待核查',
    }.get(status, '待核查')
    return {'upload': upload, 'process': process,
            'remote_id': row.get('remote_id') or '', 'status': status}


def monitor_snapshot(batch: dict, ledger: dict, pilot_ids: set[str]) -> list[dict]:
    """One display row per passed education paper, without altering source data."""
    primary = ledger.get('primary') or {}
    images = ledger.get('images') or {}
    rows = []
    for item in batch.get('items') or []:
        if item.get('status') != 'pass' or item.get('category') != '教育教学类':
            continue
        paper_id = item.get('paper_id') or ''
        receipt = primary.get(paper_id) or {}
        if receipt and receipt.get('document_id') != item.get('id'):
            upload, process = '身份异常', '待核查'
        elif paper_id in pilot_ids:
            upload, process = '已在库', '试跑已完成'
        elif receipt:
            upload = '已上传' if receipt.get('ragflow_document_id') else '待确认'
            process = {'uploaded': '等待解析', 'parse_intent': '提交待确认',
                       'submitted': '解析中', 'done': '已完成',
                       'fail': '失败待核查', 'cancel': '已取消',
                       'unknown': '结果待核查'}.get(receipt.get('status'), '待确认')
            if receipt.get('status') == 'fail':
                process = FAILURE_LABELS.get(receipt.get('failure_kind'), process)
        else:
            upload, process = '待上传', '尚未提交'
        image_count = sum(key.startswith(paper_id + ':') for key in images)
        rows.append({'id': item['id'], 'paper_id': paper_id,
                     'title': item.get('title') or '', 'major': item.get('major') or '',
                     'upload': upload, 'process': process,
                     'admission': receipt.get('admission') or '',
                     'images': image_count, 'remote_id': receipt.get('ragflow_document_id') or ''})
    return rows


def load_external_ledger(data_root: Path, run_id: str) -> dict | None:
    if not run_id or Path(run_id).name != run_id or '/' in run_id or '\\' in run_id:
        return None
    path = Path(data_root) / 'private' / 'education_full_ingest' / run_id / 'receipts.json'
    try:
        stamp = path.stat().st_mtime_ns
    except OSError:
        return None
    key = str(path.resolve())
    cached = _cache.get(key)
    if cached and cached[0] == stamp:
        return cached[1]
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    if not isinstance(value, dict) or value.get('batch_id') != run_id:
        return None
    _cache[key] = (stamp, value)
    return value


def overlay_status(item: dict, base: dict, ledger: dict, paper_dataset_id: str,
                   image_dataset_id: str) -> dict:
    paper_id = item.get('paper_id')
    receipt = (ledger.get('primary') or {}).get(paper_id)
    if not isinstance(receipt, dict):
        return base
    if receipt.get('document_id') != item.get('id'):
        result = dict(base)
        result.update(upload='记录异常', reason='独立导入回执的文档编号与本批记录不一致', can_retry=False)
        return result
    status = receipt.get('status')
    remote_id = receipt.get('ragflow_document_id')
    result = dict(base)
    result.update(target=PAPER_NAME, target_id=paper_dataset_id,
                  image_target=IMAGE_NAME, image_target_id=image_dataset_id,
                  can_retry=False, receipt_owner=None, external_education=True)
    result['upload'] = ('已上传' if remote_id else '上传待确认'
                        if status in ('unknown', 'upload_intent') else '待上传')
    result['process'] = {'uploaded': '尚未提交', 'parse_intent': '已提交，待确认',
                         'submitted': '处理中', 'done': '已完成', 'fail': '处理失败',
                         'cancel': '已取消', 'unknown': '处理状态待确认'}.get(status, '尚未提交')
    if status == 'fail':
        result['process'] = FAILURE_LABELS.get(receipt.get('failure_kind'), result['process'])
    explanation = {
        'uploaded': '已上传，等待教育 Pipeline 解析',
        'parse_intent': '解析提交结果待确认',
        'submitted': '教育 Pipeline 正在处理或排队',
        'done': '教育 Pipeline 已完成',
        'fail': '远端解析失败，原因尚待核查',
        'cancel': '远端解析已取消',
        'unknown': '上传或解析结果不确定，已阻止重复提交',
    }.get(status, '独立导入正在处理')
    if status == 'fail' and receipt.get('failure_kind') in FAILURE_LABELS:
        explanation = '远端处理失败：' + FAILURE_LABELS[receipt['failure_kind']]
    if receipt.get('admission'):
        explanation += '；准入：' + str(receipt['admission'])
    if receipt.get('chunk_count') is not None:
        explanation += '；正文分块 ' + str(receipt['chunk_count'])
    result['reason'] = explanation
    paper_record = {'route': 'paper', 'dataset_id': paper_dataset_id,
                    'dataset_name': PAPER_NAME, 'document_id': remote_id,
                    'status': 'uploaded' if remote_id else 'unknown',
                    'parse_status': status, 'ragflow_status': status,
                    'chunk_count': receipt.get('chunk_count')}
    result['record'] = paper_record
    images = []
    for key, image in (ledger.get('images') or {}).items():
        if key.startswith(str(paper_id) + ':') and isinstance(image, dict):
            images.append({'path': key.split(':', 1)[1],
                           'document_id': image.get('ragflow_document_id'),
                           'status': 'uploaded' if image.get('ragflow_document_id') else image.get('status'),
                           'ragflow_status': image.get('status')})
    image_record = {'route': 'image', 'dataset_id': image_dataset_id,
                    'dataset_name': IMAGE_NAME, 'asset_uploads': images}
    result['records'] = [paper_record, image_record]
    if images:
        uploaded = sum(bool(image['document_id']) for image in images)
        result['assets'] = f'图片库已上传 {uploaded}/{len(images)} 项'
        failed = sum(image['ragflow_status'] in ('fail', 'cancel', 'unknown') for image in images)
        if failed:
            result['assets'] += f'；异常 {failed} 项'
            if status == 'done':
                result['process'] = '部分处理失败'
    elif status == 'done' and receipt.get('admission') == '入库':
        result['assets'] = '图片材料待分流或无可上传材料'
    else:
        result['assets'] = '主文档入库后分流图片材料'
    return result
