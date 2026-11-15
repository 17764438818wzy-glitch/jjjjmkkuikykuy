"""Immutable, group-bound attachment evidence; no model or workflow calls."""
from pathlib import Path, PurePosixPath, PureWindowsPath
import copy
import json
from collections import Counter
import math
import os
import re

from .records import identity, read_json, sha_file


SCHEMA = 'attachment-evidence-handoff/1'
SCOPES = ('directory', 'text', 'images', 'request_records', 'errors')
SCOPE_STATES = {'available', 'partial', 'not_read', 'unavailable', 'unknown', 'not_applicable'}
LISTING_STATES = {'complete', 'partial', 'unavailable', 'not_read', 'not_archive'}
SOURCE_FIELDS = ('handoff_id', 'result_version', 'file_id', 'source_sha256')
PROVENANCE_FIELDS = ('kind', 'record_ref', 'run_ref', 'entry_ref', 'record_sha256')


class EvidenceHandoffError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise EvidenceHandoffError(message)


def _present(value):
    return isinstance(value, str) and bool(value.strip())


def _sha(value):
    return isinstance(value, str) and re.fullmatch(r'[0-9a-fA-F]{64}', value) is not None


def _integer(value, *, minimum=0):
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _member_path(value):
    if not _present(value) or any(char in value for char in ('\x00', '\r', '\n')):
        return False
    windows = PureWindowsPath(value)
    posix = PurePosixPath(value.replace('\\', '/'))
    return not (windows.drive or windows.is_absolute() or posix.is_absolute() or '..' in posix.parts)


def _provenance(value):
    _require(isinstance(value, dict) and all(k in value for k in PROVENANCE_FIELDS), '证据缺少来源字段')
    _require(value['kind'] in {'current_read', 'historical_reuse'}, '证据来源类型不正确')
    for field in ('record_ref', 'run_ref', 'entry_ref'):
        _require(value[field] is None or _present(value[field]), '证据来源引用不正确')
    _require(value['record_sha256'] is None or _sha(value['record_sha256']), '证据来源记录指纹不正确')
    if value['kind'] == 'historical_reuse':
        _require(_present(value['record_ref']), '历史证据没有原记录引用')


def _location(value):
    _require(isinstance(value, dict) and all(k in value for k in ('member_path', 'page', 'line', 'region')),
             '证据位置字段不完整')
    _require(value['member_path'] is None or _member_path(value['member_path']), '成员位置不是安全相对路径')
    _require(value['page'] is None or _integer(value['page'], minimum=1), '页码必须从 1 开始')
    line = value['line']
    _require(line is None or _integer(line, minimum=1) or isinstance(line, dict), '行号格式不正确')
    _require(value['region'] is None or isinstance(value['region'], (str, dict)), '区域格式不正确')


def _scope(value):
    _require(isinstance(value, dict) and all(key in value for key in SCOPES), '读取范围字段不完整')
    for key in SCOPES:
        item = value[key]
        _require(isinstance(item, dict) and 'status' in item and 'reason' in item, '读取范围格式不正确')
        _require(item['status'] in SCOPE_STATES, '读取范围状态不正确')
        _require(item['reason'] is None or isinstance(item['reason'], str), '读取范围原因不正确')
        if item['status'] in {'partial', 'unavailable', 'unknown'}:
            _require(_present(item['reason']), '不完整或未知的读取范围必须说明原因')


def _listing(value, directory_scope):
    required = ('evidence_id', 'status', 'total_entries', 'listed_entries', 'entries', 'reason')
    _require(isinstance(value, dict) and all(key in value for key in required), '目录字段不完整')
    status = value['status']
    _require(status in LISTING_STATES, '目录状态不正确')
    _require(value['reason'] is None or isinstance(value['reason'], str), '目录原因不正确')
    _require(isinstance(value['entries'], list) and _integer(value['listed_entries']), '目录条目格式不正确')
    _require(value['listed_entries'] == len(value['entries']), '已列数量与目录不一致')
    total = value['total_entries']
    _require(total is None or (_integer(total) and total >= len(value['entries'])), '目录总数不正确')
    for entry in value['entries']:
        _require(isinstance(entry, dict) and all(k in entry for k in ('path', 'type', 'bytes')),
                 '目录条目字段不完整')
        _require(_member_path(entry['path']), '目录条目不是安全的包内相对路径')
        _require(entry['type'] in ('file', 'directory', None), '目录条目类型不正确')
        _require(entry['bytes'] is None or _integer(entry['bytes']), '目录条目大小不正确')
    expected = {
        'available': {'complete', 'partial'}, 'partial': {'complete', 'partial'}, 'not_read': {'not_read'},
        'unavailable': {'unavailable'}, 'unknown': {'unavailable'}, 'not_applicable': {'not_archive'},
    }
    _require(status in expected[directory_scope['status']], '目录状态与读取范围不一致')
    if status in {'complete', 'partial'}:
        _require(_present(value['evidence_id']), '已交付目录缺少证据编号')
        _provenance(value.get('provenance'))
    if status == 'complete':
        _require(total == len(value['entries']) and all(row['type'] is not None for row in value['entries']),
                 '无法证明目录完整')
    if status == 'partial':
        _require(bool(value['entries']) and _present(value['reason']), '部分目录需要条目及原因')
    if status in {'unavailable', 'not_read', 'not_archive'}:
        _require(not value['entries'], '没有可交付目录时不能填写目录条目')
    if status == 'not_archive':
        _require(total is None, '非压缩包没有目录总数')
    if status == 'unavailable':
        _require(_present(value['reason']), '不可用目录需要说明原因')


def _checked_row(handoff, record):
    from .group_classification import _group_id, _snapshot_id, _version
    _require(isinstance(handoff, dict) and handoff.get('schema') == 'paperflow-group-handoff/2',
             '缺少支持的原交接清单')
    try:
        library, group = Path(handoff['library_root']), Path(handoff['group_root'])
        _require(library.is_absolute() and group.is_absolute() and group != library
                 and group.is_relative_to(library), '资料组位置与资料库不一致')
        _require(handoff['group_id'] == _group_id(group, library)
                 and handoff['group_relative'] == group.relative_to(library).as_posix(), '资料组身份不一致')
        rows = ([handoff['body']] if handoff['body'] is not None else [])
        for key in ('attachments', 'trash', 'unresolved'):
            _require(isinstance(handoff[key], list), '原清单项目格式不正确')
            rows.extend(handoff[key])
        _require(all(isinstance(row, dict) and _present(row.get('relative')) and _present(row.get('file_id'))
                     and 'bytes' in row and 'sha256' in row for row in rows), '原清单项目不完整')
        _require(len({row['file_id'] for row in rows}) == len(rows)
                 and len({row['relative'].casefold() for row in rows}) == len(rows), '原清单项目重复')
        ordered = sorted(rows, key=lambda row: row['relative'].casefold())
        _require(handoff['handoff_id'] == _snapshot_id(handoff['group_id'], ordered)
                 and handoff['result_version'] == _version(handoff), '原交接清单身份或版本不一致')
        source = record['source']
        _require(record['group_id'] == handoff['group_id']
                 and source['handoff_id'] == handoff['handoff_id']
                 and source['result_version'] == handoff['result_version'], '证据与原组清单不一致')
        matches = [row for row in handoff['attachments'] if row['file_id'] == source['file_id']]
        _require(len(matches) == 1, '证据不属于原组唯一附件项')
        row = matches[0]
        _require(_sha(row['sha256']) and row['sha256'].casefold() == source['source_sha256'].casefold()
                 and _integer(row['bytes']), '附件指纹或大小不一致')
        relative = row['relative']
        _require(row['file_id'] == identity([handoff['group_id'], relative.casefold()])[:24],
                 '附件编号与原资料组及相对路径不一致')
        _require(_member_path(relative) and _present(row.get('path')), '附件路径不正确')
        expected, actual = os.path.normcase(os.path.normpath(str(group / Path(relative)))), os.path.normcase(os.path.normpath(row['path']))
        _require(expected == actual, '附件路径不属于原资料组')
        return row
    except (KeyError, TypeError, OSError) as exc:
        raise EvidenceHandoffError('原交接清单字段不完整') from exc


def _source_file(handoff, row):
    from .group_classification import _group_paths
    group, _ = _group_paths(handoff['group_root'], handoff['library_root'])
    path = Path(row['path'])
    _require(path.is_file() and path.resolve().is_relative_to(group), '附件原件不存在或越过资料组')
    for current in (path, *path.parents):
        if current == group:
            break
        _require(not current.is_symlink() and not current.is_junction(), '附件路径含链接')
    before = path.stat()
    digest = sha_file(path)
    after = path.stat()
    _require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
             and after.st_size == row['bytes'] and digest.casefold() == row['sha256'].casefold(),
             '附件原件与交接指纹不一致')


def _image_report(image, verify_files, record=None, row=None, trusted_image_records=None):
    result = {'evidence_id': image['evidence_id'], 'verified': False, 'reason': '图片尚未核验'}
    if not verify_files:
        return result
    location = image['location']
    if not (image['path'] and image['sha256'] and image['mime_type'] and image['width'] and image['height']
            and image['extraction_method'] and any(location.values())):
        result['reason'] = '图片引用或来源信息不完整'
        return result
    provenance = image['provenance']
    if not provenance['record_ref'] or not provenance['record_sha256']:
        result['reason'] = '图片缺少可核验的来源记录'
        return result
    # Caller must derive this allowlist from independently verified historical
    # artifacts or a trusted capture manifest, not the sidecar itself.
    trusted = trusted_image_records or {}
    trusted_hash = trusted.get(provenance['record_ref']) if isinstance(trusted, dict) else None
    if trusted_hash != provenance['record_sha256']:
        result['reason'] = '图片来源记录尚未由可信台账核定'
        return result
    try:
        path = Path(image['path'])
        if not path.is_absolute() or not path.is_file() or path.is_symlink() or path.is_junction():
            raise ValueError('图片文件不可用')
        before = path.stat()
        digest = sha_file(path)
        from PIL import Image
        with Image.open(path) as picture:
            size, mime = picture.size, Image.MIME.get(picture.format)
        after = path.stat()
        if ((before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
                or digest.casefold() != image['sha256'].casefold()
                or size != (image['width'], image['height']) or mime != image['mime_type']):
            raise ValueError('图片指纹、格式或尺寸不一致')
        origin = Path(provenance['record_ref'])
        if (not origin.is_absolute() or not origin.is_file() or origin.is_symlink()
                or origin.is_junction() or origin.stat().st_size > 16 * 1024 * 1024
                or sha_file(origin) != provenance['record_sha256']):
            raise ValueError('图片来源记录不可核验')
        # An existing record's hash alone does not tie its pixels to the original.
        source_record = json.loads(origin.read_text(encoding='utf-8-sig'))
        if not isinstance(source_record, dict) or row is None or record is None:
            raise ValueError('图片来源记录格式不正确')
        source_path = source_record.get('source_path')
        if (not isinstance(source_path, str)
                or os.path.normcase(os.path.normpath(source_path)) != os.path.normcase(os.path.normpath(row['path']))
                or source_record.get('source_sha256') != record['source']['source_sha256']):
            raise ValueError('图片来源不属于当前附件')
        expected = {k: image[k] for k in ('evidence_id', 'path', 'sha256', 'mime_type',
                                           'width', 'height', 'location', 'extraction_method')}
        if not any(isinstance(item, dict) and all(item.get(k) == v for k, v in expected.items())
                   for item in source_record.get('images', [])):
            raise ValueError('来源记录没有对应图片及原件位置')
    except (OSError, ValueError, TypeError, ImportError):
        result['reason'] = '图片文件、来源或属性不可核验'
        return result
    return {'evidence_id': image['evidence_id'], 'verified': True, 'reason': None}


def _trusted_current_sidecar(record, trusted_sidecar):
    if not isinstance(trusted_sidecar, dict): return False
    try:
        path = Path(trusted_sidecar['path'])
        digest = trusted_sidecar['sha256']
        return (path.is_absolute() and path.is_file() and not path.is_symlink() and not path.is_junction()
                and path.stat().st_size <= 16 * 1024 * 1024 and _sha(digest)
                and sha_file(path) == digest and read_json(path) == record)
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _historical_entry(provenance, row, trusted_evidence_records):
    trusted = trusted_evidence_records or {}
    ref, digest = provenance.get('record_ref'), provenance.get('record_sha256')
    if not isinstance(trusted, dict) or not ref or not digest or trusted.get(ref) != digest:
        return None
    try:
        path = Path(ref)
        if (not path.is_absolute() or not path.is_file() or path.is_symlink() or path.is_junction()
                or path.stat().st_size > 16 * 1024 * 1024 or sha_file(path) != digest):
            return None
        raw = path.read_text(encoding='utf-8-sig')
        try:
            parsed = json.loads(raw)
            candidates = parsed if isinstance(parsed, list) else [parsed]
        except ValueError:
            candidates = [json.loads(line) for line in raw.splitlines() if line.strip()]
        matches = [item for item in candidates if isinstance(item, dict)
                   and item.get('id') == provenance.get('entry_ref')]
        if len(matches) != 1: return None
        entry = matches[0]
        if (not isinstance(entry.get('path'), str) or row is None
                or os.path.normcase(os.path.normpath(entry['path'])) != os.path.normcase(os.path.normpath(row['path']))
                or entry.get('sha256') != row['sha256'] or not isinstance(entry.get('evidence'), dict)):
            return None
        return entry['evidence']
    except (OSError, ValueError, TypeError):
        return None


def _content_report(item, kind, *, record, row, verify_files, trusted_sidecar, trusted_evidence_records):
    result = {'evidence_id': item['evidence_id'], 'verified': False, 'reason': '证据来源尚未核验'}
    if not verify_files: return result
    provenance = item['provenance']
    if provenance['kind'] == 'current_read':
        if _trusted_current_sidecar(record, trusted_sidecar):
            return {'evidence_id': item['evidence_id'], 'verified': True, 'reason': None}
        result['reason'] = '当前证据文件缺少可信接收记录'
        return result
    old = _historical_entry(provenance, row, trusted_evidence_records)
    if old is None:
        result['reason'] = '历史记录未核实来源、指纹及附件归属'
        return result
    if kind == 'text':
        for section in old.get('sections', []):
            if not isinstance(section, dict): continue
            location = section.get('location') if isinstance(section.get('location'), str) else ''
            match = re.fullmatch(r'第\s*(\d+)\s*页', location)
            expected_location = {'member_path': None, 'page': int(match.group(1)) if match else None,
                                 'line': None, 'region': location or None}
            if (section.get('text') == item['text'] and item['location'] == expected_location
                    and item['extraction_method'] == 'historical_saved_section' and item['truncated'] is None):
                return {'evidence_id': item['evidence_id'], 'verified': True, 'reason': None}
    elif kind == 'directory':
        old_entries = []
        for member in old.get('members', []):
            if not isinstance(member, dict) or not _member_path(member.get('name')): continue
            size = member.get('size')
            size = int(size) if isinstance(size, str) and size.isdigit() else size if _integer(size) else None
            old_entries.append({'path': member['name'].replace('\\', '/'), 'type': None, 'bytes': size})
        key = lambda entry: (entry['path'], entry['type'], entry['bytes'])
        requested = Counter(map(key, item['entries']))
        available = Counter(map(key, old_entries))
        if (item['status'] == 'partial' and item['total_entries'] is None
                and all(count <= available[entry] for entry, count in requested.items())):
            return {'evidence_id': item['evidence_id'], 'verified': True, 'reason': None}
    result['reason'] = '保存内容与历史原记录不一致'
    return result


def validate_evidence_handoff(record, handoff=None, *, verify_files=False, trusted_image_records=None,
                              trusted_sidecar=None, trusted_evidence_records=None):
    """Validate schema; bind only when handoff is supplied; optionally check selected files."""
    required = ('schema', 'group_id', 'source', 'evidence_version', 'read_scope', 'archive_listing',
                'text_samples', 'images', 'decision_basis', 'limitations', 'errors',
                'existing_identification', 'request_records')
    _require(isinstance(record, dict) and all(key in record for key in required), '证据记录字段不完整')
    try:
        json.dumps(record, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise EvidenceHandoffError('证据记录不是有效 JSON') from exc
    _require(record['schema'] == SCHEMA and _present(record['group_id']), '证据记录身份不正确')
    source = record['source']
    _require(isinstance(source, dict) and all(_present(source.get(k)) for k in SOURCE_FIELDS), '来源身份不完整')
    _require(_sha(source['source_sha256']) and source['source_sha256'] == source['source_sha256'].lower(),
             '原附件指纹必须为小写 SHA-256')
    version = record['evidence_version']
    _require(_integer(version, minimum=1), '证据版本必须为正整数')
    previous = record.get('previous_evidence_version')
    _require((version == 1 and previous is None) or (version > 1 and previous == version - 1), '证据前版不正确')
    _scope(record['read_scope'])
    _listing(record['archive_listing'], record['read_scope']['directory'])
    for key in ('text_samples', 'images', 'limitations', 'errors', 'request_records'):
        _require(isinstance(record[key], list), '证据列表格式不正确')
    for key, scope_key in [('text_samples', 'text'), ('images', 'images'),
                           ('request_records', 'request_records'), ('errors', 'errors')]:
        if record[key]:
            _require(record['read_scope'][scope_key]['status'] in {'available', 'partial'}, '已保存内容与读取状态不一致')
    ids = set()
    listing_id = record['archive_listing']['evidence_id']
    if listing_id:
        ids.add(listing_id)
    for sample in record['text_samples']:
        _require(isinstance(sample, dict) and all(k in sample for k in
                 ('evidence_id', 'text', 'location', 'extraction_method', 'truncated', 'provenance')),
                 '文字证据字段不完整')
        _require(_present(sample['evidence_id']) and isinstance(sample['text'], str), '文字证据不正确')
        _require(sample['extraction_method'] is None or _present(sample['extraction_method']), '文字取得方式不正确')
        _require(sample['truncated'] is None or isinstance(sample['truncated'], bool), '文字截断状态不正确')
        _location(sample['location']); _provenance(sample['provenance'])
        _require(sample['evidence_id'] not in ids, '证据编号重复'); ids.add(sample['evidence_id'])
    for image in record['images']:
        _require(isinstance(image, dict) and all(k in image for k in
                 ('evidence_id', 'path', 'sha256', 'mime_type', 'width', 'height', 'location', 'extraction_method', 'provenance')),
                 '图片证据字段不完整')
        _require(_present(image['evidence_id']), '图片证据编号不正确')
        _require(image['path'] is None or _present(image['path']), '图片引用不正确')
        _require(image['sha256'] is None or _sha(image['sha256']), '图片指纹不正确')
        _require(image['mime_type'] is None or _present(image['mime_type']), '图片格式不正确')
        _require(all(image[k] is None or _integer(image[k], minimum=1) for k in ('width', 'height')), '图片尺寸不正确')
        _require(image['extraction_method'] is None or _present(image['extraction_method']), '图片取得方式不正确')
        _location(image['location']); _provenance(image['provenance'])
        _require(image['evidence_id'] not in ids, '证据编号重复'); ids.add(image['evidence_id'])
    if record['decision_basis'] is not None:
        _require(isinstance(record['decision_basis'], dict), '分类依据格式不正确')
        _provenance(record['decision_basis'].get('provenance'))
    if record['existing_identification'] is not None:
        _require(isinstance(record['existing_identification'], dict), '已有附件说明格式不正确')
        if record['existing_identification'].get('status') not in {'not_available', 'unknown', 'not_read'}:
            _provenance(record['existing_identification'].get('provenance'))
    for item in record['request_records']:
        _require(isinstance(item, dict), '请求记录格式不正确')
        _provenance(item.get('provenance'))
    _require(not verify_files or handoff is not None, '核验文件必须提供原交接清单')
    row = _checked_row(handoff, record) if handoff is not None else None
    if verify_files:
        _source_file(handoff, row)
    content = []
    if record['archive_listing']['status'] in {'complete', 'partial'}:
        content.append(_content_report(record['archive_listing'], 'directory', record=record, row=row,
            verify_files=verify_files, trusted_sidecar=trusted_sidecar, trusted_evidence_records=trusted_evidence_records))
    content.extend(_content_report(sample, 'text', record=record, row=row, verify_files=verify_files,
        trusted_sidecar=trusted_sidecar, trusted_evidence_records=trusted_evidence_records) for sample in record['text_samples'])
    return {'valid': True, 'binding_verified': handoff is not None, 'source_verified': bool(verify_files),
            'evidence_items': content,
            'images': [_image_report(image, verify_files, record, row, trusted_image_records) for image in record['images']]}


def _scope_state(status, reason=None):
    return {'status': status, 'reason': reason}


def make_evidence_handoff(handoff, row, *, preview=None, decision=None, provenance=None,
                          decision_provenance=None):
    """Project already-read routing evidence into an attachment-bound handoff.

    No files are opened and no model is called. Historical callers must supply
    the actual saved record origin; a limited legacy list is never complete.
    """
    preview = copy.deepcopy(preview) if isinstance(preview, dict) else {}
    provenance = copy.deepcopy(provenance) if provenance is not None else {
        'kind': 'current_read', 'record_ref': None, 'run_ref': None,
        'entry_ref': row['file_id'], 'record_sha256': None}
    _provenance(provenance)
    source = {'handoff_id': handoff['handoff_id'], 'result_version': handoff['result_version'],
              'file_id': row['file_id'], 'source_sha256': row['sha256'].lower()}
    seed = {'group_id': handoff['group_id'], 'source': source}
    checked = _checked_row(handoff, seed)
    _require(checked == row, '证据附件与原清单条目不一致')
    saved = preview.get('_handoff') if isinstance(preview.get('_handoff'), dict) else {}
    suffix = Path(row['relative']).suffix.lower()
    archive = bool(preview.get('archive_type')) or suffix in {'.zip', '.7z', '.rar', '.gz', '.tar', '.bz2', '.xz'}
    listing = copy.deepcopy(saved.get('archive_listing'))
    if listing:
        listing['provenance'] = copy.deepcopy(provenance) if listing['status'] in {'complete', 'partial'} else None
        directory_state = {'complete': 'available', 'partial': 'partial', 'unavailable': 'unavailable',
                           'not_read': 'not_read', 'not_archive': 'not_applicable'}[listing['status']]
        directory = _scope_state(directory_state, listing.get('reason'))
        if listing['status'] == 'complete' and any(item['bytes'] is None for item in listing['entries']):
            directory = _scope_state('partial', '目录条目完整，但部分大小未知')
    elif archive and preview.get('members'):
        entries = []
        for item in preview['members']:
            if not isinstance(item, dict) or not _member_path(item.get('name')):
                continue
            size = item.get('size')
            size = int(size) if isinstance(size, str) and size.isdigit() else size if _integer(size) else None
            entries.append({'path': item['name'].replace('\\', '/'), 'type': None, 'bytes': size})
        if entries:
            reason = '旧目录未保存完整条目类型和总数'
            listing = {'evidence_id': 'archive-1', 'status': 'partial', 'total_entries': None,
                       'listed_entries': len(entries), 'entries': entries, 'reason': reason,
                       'provenance': copy.deepcopy(provenance)}
            directory = _scope_state('partial', reason)
        else:
            listing = None
    if not listing:
        if archive:
            reason = preview.get('read_error') or '没有保存可交付的压缩包目录'
            listing = {'evidence_id': None, 'status': 'unavailable', 'total_entries': None,
                       'listed_entries': 0, 'entries': [], 'reason': reason, 'provenance': None}
            directory = _scope_state('unavailable' if preview.get('read_error') else 'unknown', reason)
        else:
            listing = {'evidence_id': None, 'status': 'not_archive', 'total_entries': None,
                       'listed_entries': 0, 'entries': [], 'reason': None, 'provenance': None}
            directory = _scope_state('not_applicable')
    samples = copy.deepcopy(saved.get('text_samples', []))
    if not samples and provenance['kind'] == 'historical_reuse':
        for index, item in enumerate(preview.get('sections', []), 1):
            if not isinstance(item, dict) or not isinstance(item.get('text'), str):
                continue
            location = item.get('location') if isinstance(item.get('location'), str) else ''
            match = re.fullmatch(r'第\s*(\d+)\s*页', location)
            samples.append({'evidence_id': f'text-{index}', 'text': item['text'],
                'location': {'member_path': None, 'page': int(match.group(1)) if match else None,
                             'line': None, 'region': location or None},
                'extraction_method': 'historical_saved_section', 'truncated': None})
    for sample in samples:
        sample['provenance'] = copy.deepcopy(provenance)
    if samples:
        partial = saved.get('text_scope_partial', True)
        text_scope = _scope_state('partial', saved.get('text_scope_reason') or '只保存部分文字') if partial else _scope_state('available')
    elif archive and provenance['kind'] == 'current_read' and saved:
        text_scope = _scope_state('not_read', '本次仅读取目录')
    elif saved and preview.get('format') in {'pdf', 'docx'}:
        text_scope = _scope_state('available', '所读位置没有可提取文字')
    else:
        text_scope = _scope_state('unknown', '没有保存可追溯的原文片段')
    # A count of images in a PDF is not a preview image.
    images = copy.deepcopy(saved.get('images', []))
    image_scope = (_scope_state('partial', '图片需要核验来源') if images else
                   _scope_state('not_read', '本次未生成图片预览') if saved and provenance['kind'] == 'current_read'
                   else _scope_state('unknown', '没有保存图片预览记录'))
    errors = copy.deepcopy(saved.get('errors', []))
    if not saved and preview.get('read_error'):
        errors.append(preview['read_error'])
    errors_scope = _scope_state('available') if saved or errors else _scope_state('unknown', '没有保存错误记录')
    from .nimble_group import _preview
    basis_origin = copy.deepcopy(decision_provenance) if decision_provenance is not None else (
        copy.deepcopy(provenance) if provenance['kind'] == 'current_read' else None)
    if basis_origin is not None: _provenance(basis_origin)
    basis = ({'classification': 'attachment', 'reason': row.get('reason'),
              'method': 'local_model' if decision is not None else 'saved_classification',
              'decision': copy.deepcopy(decision), 'provenance': basis_origin} if basis_origin else None)
    if preview and basis is not None and basis_origin == provenance:
        basis['classification_preview'] = _preview(preview, 3500)
    record = {'schema': SCHEMA, 'group_id': handoff['group_id'], 'source': source,
              'evidence_version': 1, 'previous_evidence_version': None,
              'file_info': {'name': Path(row['path']).name, 'path': row['path'], 'bytes': row['bytes'],
                            'actual_format': preview.get('archive_type') or preview.get('format') or suffix or None},
              'read_scope': {'directory': directory, 'text': text_scope, 'images': image_scope,
                             'request_records': _scope_state('unknown', '未核对请求原始记录'), 'errors': errors_scope},
              'archive_listing': listing, 'text_samples': samples, 'images': images,
              'decision_basis': basis, 'limitations': copy.deepcopy(preview.get('limitations', [])),
              'errors': errors, 'existing_identification': None, 'request_records': []}
    validate_evidence_handoff(record, handoff)
    return record


def read_evidence_history(path, *, handoff, verify_files=False):
    """Read the complete immutable chain; never infer history from its last item."""
    path = Path(path)
    _require(path.stem.isdecimal() and path.suffix == '.json', '证据版本路径不正确')
    latest_version = int(path.stem)
    _require(latest_version >= 1 and path.is_file(), '证据版本不存在')
    history = []
    for version in range(1, latest_version + 1):
        current = path.parent / (str(version) + '.json')
        _require(current.is_file(), '证据版本链不完整')
        item = read_json(current)
        _require(item.get('evidence_version') == version, '证据版本与路径不一致')
        validate_evidence_handoff(item, handoff, verify_files=verify_files)
        if history:
            _require(item['source'] == history[0]['source'] and item['group_id'] == history[0]['group_id'],
                     '证据版本来源不一致')
        history.append(item)
    return history


def write_evidence_handoff(record, directory, *, handoff):
    """Persist an immutable per-source version after verifying the selected original."""
    from .group_classification import _write_record
    validate_evidence_handoff(record, handoff, verify_files=True)
    source = record['source']
    key = identity([record['group_id']] + [source[field] for field in SOURCE_FIELDS])[:32]
    root = Path(directory) / key
    version = record['evidence_version']
    destination = root / (str(version) + '.json')
    if version > 1:
        previous_path = root / (str(version - 1) + '.json')
        _require(previous_path.is_file(), '缺少证据前版，不能写入新版本')
        previous = read_evidence_history(previous_path, handoff=handoff)[-1]
        _require(previous['source'] == source and previous['group_id'] == record['group_id']
                 and previous['evidence_version'] == version - 1, '证据前版身份不一致')
        before = {k: v for k, v in previous.items() if k not in {'evidence_version', 'previous_evidence_version'}}
        after = {k: v for k, v in record.items() if k not in {'evidence_version', 'previous_evidence_version'}}
        if before == after:
            return previous_path
    return _write_record(destination, record, source_library=handoff['library_root'])


def append_evidence_handoff(record, directory, *, handoff):
    """Idempotently append new evidence for the same immutable source tuple."""
    from .group_classification import GroupClassificationError
    candidate = copy.deepcopy(record)
    source = candidate['source']
    key = identity([candidate['group_id']] + [source[field] for field in SOURCE_FIELDS])[:32]
    root = Path(directory) / key
    for _ in range(3):
        versions = sorted(int(p.stem) for p in root.glob('*.json') if p.stem.isdecimal() and p.is_file())
        if versions:
            previous = read_evidence_history(root / (str(versions[-1]) + '.json'), handoff=handoff,
                                             verify_files=True)[-1]
            before = {k: v for k, v in previous.items() if k not in {'evidence_version', 'previous_evidence_version'}}
            after = {k: v for k, v in candidate.items() if k not in {'evidence_version', 'previous_evidence_version'}}
            if before == after:
                validate_evidence_handoff(previous, handoff, verify_files=True)
                return root / (str(versions[-1]) + '.json')
            candidate['evidence_version'] = versions[-1] + 1
            candidate['previous_evidence_version'] = versions[-1]
        try:
            return write_evidence_handoff(candidate, directory, handoff=handoff)
        except GroupClassificationError:
            # Concurrent writers may have won this exact version; rescan, never overwrite.
            if not (root / (str(candidate['evidence_version']) + '.json')).is_file():
                raise
    raise EvidenceHandoffError('证据版本并发更新，请核对后重试')
