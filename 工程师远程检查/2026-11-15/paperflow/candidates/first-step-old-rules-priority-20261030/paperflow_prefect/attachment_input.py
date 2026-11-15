"""Consume the shared H2.1 validator; keep model input bounded and traceable."""
import copy
import json
from pathlib import Path
from .records import identity, read_json, sha_file


def load_received_evidence(row):
    """Read only hash-bound references preserved by the receipt; choose a unique latest version."""
    references = row.get('evidence_records', [])
    if not references: return None
    from .attachments import AttachmentError
    handoff_ref = row.get('handoff_ref') or {}
    path = handoff_ref.get('path')
    if not path or not Path(path).is_file() or sha_file(path) != handoff_ref.get('sha256'):
        raise AttachmentError('evidence_changed')
    handoff = read_json(path)
    records = {}; reference_by_version = {}
    for ref in references:
        p = Path(ref['path'])
        if not p.is_file() or sha_file(p) != ref['sha256']: raise AttachmentError('evidence_changed')
        record = read_json(p)
        binding = dict(record.get('source') or {}, group_id=record.get('group_id'))
        if binding != row.get('source_binding') or record['source']['source_sha256'] != row['sha256']:
            raise AttachmentError('evidence_changed')
        version = record['evidence_version']
        if version in records and records[version] != record: raise AttachmentError('evidence_changed')
        records[version] = record
        reference_by_version[version] = ref
    latest = max(records)
    if set(records) != set(range(1, latest + 1)):
        raise AttachmentError('history_unresolved')
    record = records[max(records)]
    report = checked_handoff_evidence(record, handoff, trusted_sidecar=reference_by_version[latest])
    report['history'] = list(records.values())
    return record, handoff, report


def checked_handoff_evidence(record, handoff, **trusted):
    from .attachment_evidence_handoff import validate_evidence_handoff
    report = validate_evidence_handoff(record, handoff, verify_files=True, **trusted)
    if not (report['valid'] and report['binding_verified'] and report['source_verified']):
        raise ValueError('附件证据没有绑定到已核验原件')
    return report


def card_evidence(record, report):
    """Return a projection, never rewrite the upstream historical record."""
    result = copy.deepcopy(record)
    listing = result['archive_listing']
    verified = {item['evidence_id'] for item in report['evidence_items'] if item['verified']}
    if listing['status'] in {'complete', 'partial'} and listing['evidence_id'] not in verified:
        listing.update(status='unavailable', entries=[], listed_entries=0, reason='历史目录来源待核，原条目保留在交接证据中')
        result['read_scope']['directory'] = {'status': 'unavailable', 'reason': listing['reason']}
        result['limitations'].append(listing['reason'])
    if any(not row['verified'] for row in report['images']):
        result['limitations'].append('部分图片引用尚未核验，未作为识别模型的图片输入')
    return result


def model_evidence(record, handoff, *, report=None):
    report = report if report is not None else checked_handoff_evidence(record, handoff)
    projected = card_evidence(record, report)
    parts, refs, images = [], [], []
    limits = list(record['limitations'])
    listing = projected['archive_listing']
    if listing['status'] in {'complete', 'partial'}:
        parts.append({'evidence_id': listing['evidence_id'], 'kind': 'archive_listing',
            'status': listing['status'], 'total_entries': listing['total_entries'], 'entries': listing['entries'][:2000]})
        refs.append(listing['evidence_id'])
        if len(listing['entries']) > 2000: limits.append('模型目录输入仅前2000项，完整留存目录见交接证据')
        limits.append('包内目录不表示已读取成员内容')
    budget = 24000
    selected = []
    for part in parts:
        encoded = json.dumps(part, ensure_ascii=False)
        if len(encoded) > budget:
            while part['entries'] and len(json.dumps(part, ensure_ascii=False)) > budget:
                part['entries'].pop()
            part['status'] = 'partial'; limits.append('模型目录输入按文字预算截断')
            encoded = json.dumps(part, ensure_ascii=False)
        budget -= len(encoded); selected.append(encoded)
    verified_content = {item['evidence_id'] for item in report['evidence_items'] if item['verified']}
    for sample in record['text_samples']:
        if sample['evidence_id'] not in verified_content:
            limits.append('历史文字来源待核，未用于本次识别'); continue
        if budget <= 0:
            limits.append('其余文字未进入本次有界模型输入'); break
        fragment = sample['text'][:min(8000, budget)]
        part = dict(evidence_id=sample['evidence_id'], text=fragment, location=sample['location'],
                    truncated=True if len(fragment) < len(sample['text']) else sample['truncated'])
        selected.append(json.dumps(part, ensure_ascii=False)); budget -= len(fragment)
        refs.append(sample['evidence_id'])
    verified = {item['evidence_id'] for item in report['images'] if item['verified']}
    for image in record['images']:
        if image['evidence_id'] not in verified: continue
        if len(images) >= 4:
            limits.append('模型图片输入仅前4张已核验预览'); break
        images.append(dict(image, ref=image['evidence_id'])); refs.append(image['evidence_id'])
    if len(verified) != len(record['images']): limits.append('未核验图片未进入模型')
    name = (record.get('file_info') or {}).get('name')
    row = next(x for x in handoff['attachments'] if x['file_id'] == record['source']['file_id'])
    name = name or Path(row['path']).name
    if not refs: refs = ['file-facts']; limits.append('没有可核验的内容预览，只依据原件文件信息')
    return dict(schema='attachment-evidence/1', name=name, format=Path(name).suffix.lower(),
        source_sha256=record['source']['source_sha256'], source_binding=dict(record['source'], group_id=record['group_id']),
        evidence_version=record['evidence_version'], handoff_evidence_sha256=identity(record),
        text='\n'.join(selected), images=images, refs=refs, bounded=True,
        limitations=list(dict.fromkeys(limits)), archive_listing=projected['archive_listing'],
        read_scope=projected['read_scope'])


def require_upstream_history_located(row):
    """Before a fresh dispatch, earlier evidence versions may reveal retained work."""
    from .attachments import AttachmentError
    loaded = load_received_evidence(row)
    if not loaded: return
    for record in loaded[2]['history']:
        identified = record.get('existing_identification')
        if identified and identified.get('status') not in {'not_available', 'not_read', 'unknown'}:
            raise AttachmentError('history_unresolved')
        for request in record['request_records']:
            task = request.get('task_type') or request.get('task') or request.get('kind')
            if task not in {'classification', 'nimble', 'group_classification'}:
                raise AttachmentError('history_unresolved')
