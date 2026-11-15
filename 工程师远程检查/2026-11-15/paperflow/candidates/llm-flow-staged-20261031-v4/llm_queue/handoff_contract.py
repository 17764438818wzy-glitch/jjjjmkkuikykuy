"""Read the exact upstream folder handoff, without dispatching model work."""
import hashlib
import json
from pathlib import Path
import re


IDENTITY_KEYS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')
SCHEMA = 'paperflow-local-preprocessing-handoff/1'
CONSUMER = 'prefect-local-preprocessing'


def _read_reference(reference, label, *, as_json=False):
    if not isinstance(reference, dict):
        raise ValueError(label + '引用无效')
    path, expected = reference.get('path'), reference.get('sha256')
    if not isinstance(path, str) or not re.fullmatch(r'[0-9a-f]{64}', str(expected or '')):
        raise ValueError(label + '引用无效')
    try:
        path = Path(path).resolve(strict=True)
        if as_json:
            raw = path.read_bytes()
            actual = hashlib.sha256(raw).hexdigest()
        else:
            with path.open('rb') as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
    except OSError as exc:
        raise ValueError(label + '不可读取') from exc
    if actual != expected:
        raise ValueError(label + '发生变化')
    if as_json:
        try:
            data = json.loads(raw)
        except (UnicodeError, ValueError) as exc:
            raise ValueError(label + '格式无效') from exc
        if not isinstance(data, dict):
            raise ValueError(label + '格式无效')
        return data
    return None


def load_frozen_handoff(record, expected_identity, verify_registered_receipt):
    """Validate the stored claim, receipt and frozen reading input as one unit.

    expected_identity must include an attempt_id independently obtained from
    the trusted claim / Prefect ticket. The read-only waiting-list record does
    not carry it. The caller remains responsible for live queue admission,
    image revalidation at request construction, and rechecking at dispatch.
    """
    if not isinstance(record, dict) or not isinstance(expected_identity, dict):
        raise ValueError('交接身份无效')
    if any(not isinstance(expected_identity.get(k), str) or not expected_identity[k]
           for k in IDENTITY_KEYS):
        raise ValueError('交接身份无效')
    if (any(record.get(k) != expected_identity[k] for k in IDENTITY_KEYS if k != 'attempt_id')
            or ('attempt_id' in record and record['attempt_id'] != expected_identity['attempt_id'])):
        raise ValueError('交接身份不一致')
    manifest = _read_reference({'path': record.get('manifest_path'),
                                'sha256': record.get('manifest_sha256')}, '前处理交接', as_json=True)
    if (manifest.get('schema') != SCHEMA or manifest.get('consumer_id') != CONSUMER
            or any(manifest.get(k) != expected_identity[k] for k in IDENTITY_KEYS)):
        raise ValueError('前处理交接身份不一致')
    receipt = manifest.get('receipt')
    _read_reference(receipt, '登记回执')
    verified = verify_registered_receipt(receipt['path'], expected_identity['receipt_id'])
    if (not isinstance(verified, dict)
            or any(verified.get(k) != expected_identity[k] for k in IDENTITY_KEYS if k != 'attempt_id')
            or verified.get('receipt_sha256') != receipt['sha256']
            or Path(verified.get('receipt_path', '')).resolve() != Path(receipt['path']).resolve()):
        raise ValueError('登记回执身份不一致')
    paper = verified.get('paper')
    if (not isinstance(paper, dict) or paper.get('paper_id') != expected_identity['paper_id']
            or paper.get('revision') != manifest.get('revision')
            or paper.get('body') != manifest.get('body')
            or paper.get('attachments') != manifest.get('attachments')):
        raise ValueError('交付正文或附件身份不一致')
    body = paper.get('body')
    if not isinstance(body, dict) or not isinstance(paper.get('attachments'), list):
        raise ValueError('交付材料身份不一致')
    material = _read_reference(manifest.get('material'), '正文材料', as_json=True)
    reading = _read_reference(manifest.get('reading_material'), '正文阅读材料', as_json=True)
    try:
        work = Path(manifest['body_work']).resolve(strict=True)
        material_path = Path(manifest['material']['path']).resolve(strict=True)
    except (KeyError, TypeError, OSError) as exc:
        raise ValueError('正文工作目录身份不可核验') from exc
    if not work.is_dir() or material_path.parent != work:
        raise ValueError('正文工作目录身份不一致')
    if (material.get('source', {}).get('source_sha256') != body.get('sha256')
            or reading.get('source', {}).get('source_sha256') != body.get('sha256')):
        raise ValueError('正文材料身份不一致')
    for name, label in (('body_text', '正文文本'), ('index', '正文索引')):
        if manifest.get(name) is not None:
            _read_reference(manifest[name], label)
    if not isinstance(manifest.get('gaps'), list):
        raise ValueError('前处理缺口记录无效')
    return {'manifest': manifest, 'paper': paper, 'material': material, 'reading_material': reading,
            'registered': verified}
