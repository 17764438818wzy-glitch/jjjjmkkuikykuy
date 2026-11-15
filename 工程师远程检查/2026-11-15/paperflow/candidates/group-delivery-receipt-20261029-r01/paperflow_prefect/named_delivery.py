"""Make a separately owned, hash-bound named input without changing the receipt."""
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import uuid
from filelock import FileLock

from .records import sha_file


class NamedDeliveryError(ValueError):
    pass


def resolved_delivery_row(paper, row, data_root):
    """Apply an independently source-verified title correction without changing receipts."""
    paper_id = paper.get('paper_id', '')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', paper_id):
        return dict(row, display_name=None)
    override = Path(data_root).absolute() / 'naming-overrides' / paper_id / 'title.json'
    _safe_directory(override)
    if not override.exists():
        return _generated_delivery_row(paper, row, data_root)
    try:
        raw_record = override.read_bytes()
        override_sha256 = hashlib.sha256(raw_record).hexdigest()
        record = json.loads(raw_record.decode('utf8'))
        body = paper.get('body') or {}
        if (record.get('schema') not in {'paperflow-title-correction/1', 'paperflow-title-correction/2'}
                or record.get('paper_id') != paper_id or record.get('group_id') != paper.get('group_id')
                or record.get('body_file_id') != body.get('file_id')
                or record.get('source_sha256') != body.get('sha256')):
            raise NamedDeliveryError('题目修正不属于当前论文')
        delivery = None
        if paper.get('delivery_ref') is not None:
            from .delivery_receipt import reload_receipt_delivery
            from .intake_handoff import verify_received_file
            delivery = reload_receipt_delivery(paper['delivery_ref'], verify_files=False)
            verify_received_file(body, delivery=delivery)
            original_body = delivery['handoff'].get('body') or {}
            if (record['source_path'] not in {original_body.get('path'), body.get('path')}
                    or original_body.get('file_id') != body.get('upstream_file_id')):
                raise NamedDeliveryError('题目修正与交付正文的原来源不符')
            source = Path(body['path'])
        else:
            source = Path(record['source_path'])
        if not source.is_file() or sha_file(source) != body['sha256']:
            raise NamedDeliveryError('题目修正来源无法核验')
        from .group_classification import _separate_cover_proof, _title_proof
        converted = record.get('converted_evidence')
        cover_path, handoff_path = None, None
        if record['schema'] == 'paperflow-title-correction/2':
            from .group_sort_cli import _read_for_sort
            if converted:
                raise NamedDeliveryError('独立封面修正不能混用正文转换依据')
            ref = paper.get('handoff_ref') or {}
            handoff_path = Path(ref['path'])
            if (ref.get('hash_kind') != 'file-sha256' or ref.get('sha256') != record['handoff_sha256']
                    or (delivery is None and handoff_path.resolve() != Path(record['handoff_path']).resolve())
                    or sha_file(handoff_path) != record['handoff_sha256']):
                raise NamedDeliveryError('独立封面的原交接清单不符')
            handoff = json.loads(handoff_path.read_text(encoding='utf-8-sig'))
            original = handoff.get('body') or {}
            if (handoff.get('group_id') != (paper.get('group_id') if delivery else paper.get('origin_group_id'))
                    or handoff.get('handoff_id') != paper.get('handoff_id')
                    or handoff.get('result_version') != paper.get('result_version')
                    or original.get('file_id') != body.get('upstream_file_id')
                    or original.get('sha256') != body.get('sha256')
                    or original.get('relative') != body.get('relative')):
                raise NamedDeliveryError('独立封面与正文不属于同一接收身份')
            covers = [item for key in ('attachments', 'trash', 'unresolved')
                      for item in handoff.get(key, []) if '封面' in Path(item.get('relative', '')).stem]
            if len(covers) != 1:
                raise NamedDeliveryError('同组独立封面不唯一')
            cover = covers[0]
            body_relative, cover_relative = Path(body['relative']), Path(cover['relative'])
            if (body_relative.is_absolute() or cover_relative.is_absolute()
                    or '..' in body_relative.parts or '..' in cover_relative.parts):
                raise NamedDeliveryError('独立封面路径越界')
            if delivery is not None:
                from .group_delivery import delivered_file
                group = Path(handoff['group_root'])
                cover_path = delivered_file(delivery, cover)
                if (record['cover_source_path'] not in {cover['path'], str(cover_path)}
                        or record['cover_sha256'] != cover['sha256']):
                    raise NamedDeliveryError('独立封面与原始来源身份不符')
            else:
                group = source.resolve()
                for _ in body_relative.parts:
                    group = group.parent
                cover_path = Path(record['cover_source_path'])
                if ((group / body_relative).resolve() != source.resolve()
                        or (group / cover_relative).resolve() != cover_path.resolve()
                        or not cover_path.resolve().is_relative_to(group)):
                    raise NamedDeliveryError('独立封面与原件路径不符')
            if sha_file(cover_path) != record['cover_sha256']:
                raise NamedDeliveryError('独立封面与原件哈希不符')
            evidence = _read_for_sort(cover_path)
            if not _separate_cover_proof(group, cover, evidence, record['title'], record):
                raise NamedDeliveryError('独立封面的题目或个人身份不符')
        elif converted:
            from .group_evidence import read_evidence
            material_path = Path(converted['material_path']).resolve()
            allowed = (Path(data_root) / 'papers' / paper_id / 'body').resolve()
            evidence_path = Path(converted['path']).resolve()
            if (material_path.name != 'material.json' or not material_path.is_relative_to(allowed)
                    or not evidence_path.is_relative_to(material_path.parent / 'private')
                    or sha_file(material_path) != converted['material_sha256']
                    or sha_file(evidence_path) != converted['sha256']):
                raise NamedDeliveryError('转换证据路径或哈希不符')
            material = json.loads(material_path.read_text(encoding='utf8'))
            if (material.get('source', {}).get('source_sha256') != body['sha256']
                    or material.get('file_hashes', {}).get(str(evidence_path)) != converted['sha256']):
                raise NamedDeliveryError('转换证据不属于当前正文')
            evidence = read_evidence(evidence_path)
        else:
            from .group_sort_cli import _read_for_sort
            evidence = _read_for_sort(source)
        if (record['schema'] == 'paperflow-title-correction/1'
                and not _title_proof(evidence, record['title'], record['excerpt'], labelled=True, reference=record['reference'])):
            raise NamedDeliveryError('题目修正与原文证据不符')
        _safe_directory(override)
        if sha_file(override) != override_sha256 or sha_file(source) != body['sha256']:
            raise NamedDeliveryError('题目依据在读取期间发生变化')
        if converted and (sha_file(material_path) != converted['material_sha256']
                          or sha_file(evidence_path) != converted['sha256']):
            raise NamedDeliveryError('转换依据在读取期间发生变化')
        if cover_path and (sha_file(cover_path) != record['cover_sha256']
                           or sha_file(handoff_path) != record['handoff_sha256']):
            raise NamedDeliveryError('独立封面或交接依据在读取期间发生变化')
        from .intake_handoff import _display
        name = _display(record['title'], row['kind'], row.get('sequence'), Path(row['path']).suffix)
        if not name: raise NamedDeliveryError('题目修正不能用于文件名')
        return dict(row, display_name=name, title_override_sha256=override_sha256)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError):
        # A broken correction is an unknown title, not a new block on body work.
        return dict(row, display_name=None)


def _generated_delivery_row(paper, row, data_root):
    """Use a separately journaled generated title only when no source title exists."""
    body = paper.get('body') or {}
    if not body or body.get('display_name'):
        return row
    try:
        paper_id, file_id, source_hash = paper['paper_id'], body['file_id'], body['sha256']
        if (not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', paper_id)
                or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', file_id)
                or not re.fullmatch(r'[0-9a-f]{64}', source_hash)):
            raise NamedDeliveryError('生成题目的论文身份无效')
        directory = Path(data_root).absolute() / 'title-requests' / paper_id / (file_id + '-' + source_hash)
        record_path, binding_path = directory / 'title.json', directory / 'binding.json'
        _safe_directory(record_path)
        if (directory / 'not-applied-to-profile.json').exists():
            return dict(row, display_name=None)
        if not record_path.exists():
            return row
        result_hash, binding_hash = sha_file(record_path), sha_file(binding_path)
        record, binding = json.loads(record_path.read_text(encoding='utf8')), json.loads(binding_path.read_text(encoding='utf8'))
        identity = {'paper_id': paper_id, 'body_file_id': file_id, 'source_sha256': source_hash,
                    'group_id': paper.get('group_id')}
        if (record.get('schema') != 'paperflow-generated-title/1' or record.get('origin') != 'model_generated'
                or binding.get('schema') != 'paperflow-generated-title-binding/1'
                or binding.get('purpose') != 'generated_paper_title'
                or any(record.get(k) != v or binding.get(k) != v for k, v in identity.items())
                or record.get('binding_sha256') != binding_hash
                or record.get('text_sha256') != binding.get('text_sha256')
                or record.get('request_sha256') != binding.get('request_sha256')
                or sha_file(body['path']) != source_hash):
            raise NamedDeliveryError('生成题目与论文身份不符')
        from .generated_title_journal import verify_response
        title, state = verify_response(directory / 'model', binding)
        if (state.get('http_invoked') is not True or record.get('title') != title
                or record.get('response_sha256') != state.get('response_sha256')):
            raise NamedDeliveryError('生成题目与已保存回答不符')
        from .intake_handoff import _display
        name = _display(title, row['kind'], row.get('sequence'), Path(row['path']).suffix)
        if not name or sha_file(record_path) != result_hash or sha_file(binding_path) != binding_hash:
            raise NamedDeliveryError('生成题目不能安全用于文件名')
        return dict(row, display_name=name, title_origin='model_generated', generated_title_sha256=result_hash)
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, ImportError):
        return dict(row, display_name=None)


def body_request_started(work):
    work = Path(work)
    old = ('profile-result.json', 'private/candidate.json', 'private/validated-bundle.json',
           'stages/profile.json', 'prepared-publication.json', 'publication', 'delivery-receipt.json',
           'active-revision.json', 'revisions', 'private/profile-recovery/retry-intent.json')
    model = work / 'model'
    return (any((work / name).exists() for name in old)
            or (model.is_dir() and any(p.is_file() for p in model.rglob('*'))))


def mark_title_not_applied(paper, data_root):
    from .records import read_json, write_json
    body = paper['body']
    directory = (Path(data_root).absolute() / 'title-requests' / paper['paper_id'] /
                 (body['file_id'] + '-' + body['sha256']))
    path = directory / 'not-applied-to-profile.json'
    _safe_directory(path)
    record = {'schema': 'generated-title-not-applied/1', 'paper_id': paper['paper_id'],
              'body_file_id': body['file_id'], 'source_sha256': body['sha256'],
              'reason': 'profile_started_or_identity_unavailable'}
    if path.exists():
        if read_json(path) != record:
            raise NamedDeliveryError('已有未应用题目记录不一致')
    else:
        write_json(path, record)


def stage_generated_profile_name(paper, work, data_root):
    """Bind a generated name to a new body's already-prepared model material."""
    from filelock import Timeout
    from .profile_recovery import recovery_lock
    work = Path(work)
    try:
        with recovery_lock(work):
            if body_request_started(work):
                return None
            return _stage_generated_profile_name(paper, work, data_root)
    except Timeout:
        return None


def _stage_generated_profile_name(paper, work, data_root):
    from .records import read_json, write_json
    row = resolved_delivery_row(paper, paper['body'], data_root)
    if row.get('title_origin') != 'model_generated' or not row.get('display_name'):
        return None
    named = materialize_named_input(paper, row, work)
    proof, material = work / 'named-input-proof.json', work / 'material.json'
    title_path = (Path(data_root).absolute() / 'title-requests' / paper['paper_id'] /
                  (paper['body']['file_id'] + '-' + paper['body']['sha256']) / 'title.json')
    record = {'schema': 'generated-profile-name/1', 'paper_id': paper['paper_id'],
              'body_file_id': paper['body']['file_id'], 'source_sha256': paper['body']['sha256'],
              'group_id': paper.get('group_id'), 'title_path': str(title_path),
              'generated_title_sha256': row['generated_title_sha256'],
              'display_name': Path(named).name, 'material_sha256': sha_file(material),
              'proof_sha256': sha_file(proof)}
    path = work / 'generated-profile-name.json'
    _safe_directory(path)
    if path.exists():
        if read_json(path) != record:
            raise NamedDeliveryError('已有正文命名映射不一致')
    else:
        write_json(path, record)
    return record['display_name']


def apply_generated_profile_name(material, work):
    """Use the bound name in the model input without altering prepared material."""
    import copy
    work = Path(work)
    marker = work / 'generated-profile-name.json'
    if not marker.is_file():
        return material
    _safe_directory(marker)
    record = json.loads(marker.read_text(encoding='utf8'))
    proof_path, material_path = work / 'named-input-proof.json', work / 'material.json'
    proof = json.loads(proof_path.read_text(encoding='utf8'))
    source_hash = record.get('source_sha256')
    if not isinstance(record.get('title_path'), str):
        raise NamedDeliveryError('生成题目的结果路径缺失')
    title_path = Path(record['title_path'])
    _safe_directory(title_path)
    title_root = title_path.parent
    if (title_path.name != 'title.json' or title_root.name != record['body_file_id'] + '-' + source_hash
            or title_root.parent.name != record['paper_id'] or title_root.parent.parent.name != 'title-requests'
            or sha_file(title_path) != record.get('generated_title_sha256')):
        raise NamedDeliveryError('生成题目的结果身份不符')
    title_record = json.loads(title_path.read_text(encoding='utf8'))
    binding_path = title_root / 'binding.json'
    binding = json.loads(binding_path.read_text(encoding='utf8'))
    identity = {'paper_id': record['paper_id'], 'body_file_id': record['body_file_id'],
                'source_sha256': source_hash, 'group_id': record.get('group_id')}
    from .generated_title_journal import verify_response
    title, state = verify_response(title_root / 'model', binding)
    from .intake_handoff import _display
    if (record.get('schema') != 'generated-profile-name/1'
            or record.get('material_sha256') != sha_file(material_path)
            or record.get('proof_sha256') != sha_file(proof_path)
            or title_record.get('schema') != 'paperflow-generated-title/1'
            or title_record.get('origin') != 'model_generated'
            or binding.get('schema') != 'paperflow-generated-title-binding/1'
            or binding.get('purpose') != 'generated_paper_title'
            or any(title_record.get(k) != v or binding.get(k) != v for k, v in identity.items())
            or title_record.get('binding_sha256') != sha_file(binding_path)
            or title_record.get('text_sha256') != binding.get('text_sha256')
            or title_record.get('request_sha256') != binding.get('request_sha256')
            or title_record.get('response_sha256') != state.get('response_sha256')
            or state.get('http_invoked') is not True or title_record.get('title') != title
            or material.get('source', {}).get('source_sha256') != source_hash
            or proof.get('paper_id') != record.get('paper_id')
            or proof.get('file_id') != record.get('body_file_id') or proof.get('kind') != 'body'
            or proof.get('source_sha256') != source_hash
            or proof.get('title_origin') != 'model_generated'
            or proof.get('generated_title_sha256') != record.get('generated_title_sha256')
            or proof.get('display_name') != record.get('display_name')
            or _display(title, 'body', None, Path(proof['source_path']).suffix) != record.get('display_name')
            or Path(proof['delivery_path']).name != record.get('display_name')
            or sha_file(proof['source_path']) != source_hash or sha_file(proof['delivery_path']) != source_hash
            or not _valid_name(record['display_name'], proof['source_path'])):
        raise NamedDeliveryError('生成正文名称与准备材料不符')
    result = copy.deepcopy(material)
    result['original_filename'] = record['display_name']
    return result


def _valid_name(value, original):
    if not isinstance(value, str) or not value or value != value.strip(' .'):
        return False
    if (PureWindowsPath(value).name != value or Path(value).name != value
            or re.search(r'[<>:"/\\|?*\x00-\x1f]', value)):
        return False
    device = value.split('.', 1)[0].upper()
    if device in {'CON', 'PRN', 'AUX', 'NUL', *(f'COM{i}' for i in range(1, 10)),
                  *(f'LPT{i}' for i in range(1, 10))}:
        return False
    return Path(value).suffix.casefold() == Path(original).suffix.casefold()


def _safe_directory(path):
    for part in (path, *path.parents):
        if part.is_symlink() or (hasattr(part, 'is_junction') and part.is_junction()):
            raise NamedDeliveryError('交付目录不能经过链接')


def _expected(paper, row, directory):
    source = Path(row['path']).absolute() if row.get('delivery_ref') else Path(row['path']).resolve()
    name = row.get('display_name')
    if not name:
        return None
    if not _valid_name(name, source.name):
        raise NamedDeliveryError('交付文件名不安全或扩展名不符')
    if not paper.get('paper_id') or not row.get('file_id') or row.get('kind') not in {'body', 'attachment'}:
        raise NamedDeliveryError('缺少论文或原文件身份')
    result = {'schema': 'named-delivery-input/1', 'paper_id': paper['paper_id'],
            'file_id': row['file_id'], 'kind': row['kind'], 'source_path': str(source),
            'source_sha256': row['sha256'], 'bytes': row['bytes'], 'display_name': name,
            'delivery_path': str(directory / name)}
    if row.get('title_override_sha256'):
        result['title_override_sha256'] = row['title_override_sha256']
    if row.get('generated_title_sha256'):
        result['generated_title_sha256'] = row['generated_title_sha256']
        result['title_origin'] = 'model_generated'
    return result


def materialize_named_input(paper, row, work):
    """Return the named copy, or None for an unconfirmed title; never alter source."""
    work = Path(work).absolute()
    if row.get('delivery_ref') is not None:
        from .intake_handoff import verify_received_file
        verify_received_file(row)
    directory = work / 'named-input'
    _safe_directory(directory)
    expected = _expected(paper, row, directory)
    if expected is None:
        return None
    proof_path = work / 'named-input-proof.json'
    if row.get('delivery_ref') is not None and proof_path.is_file():
        _safe_directory(proof_path)
        previous = json.loads(proof_path.read_text(encoding='utf8'))
        if previous != expected:
            from .delivery_receipt import verify_history_copy
            if ({k:v for k,v in previous.items() if k!='source_path'} !=
                    {k:v for k,v in expected.items() if k!='source_path'}
                    or not verify_history_copy(row, previous['source_path'])):
                raise NamedDeliveryError('既有交付映射与本次身份不符')
            expected = previous
    source, target = Path(expected['source_path']), Path(expected['delivery_path'])
    if not source.is_file() or source.stat().st_size != expected['bytes'] or sha_file(source) != expected['source_sha256']:
        raise NamedDeliveryError('原件与接收记录不符')
    work.mkdir(parents=True, exist_ok=True)
    lock_path = work / 'named-input.lock'
    _safe_directory(lock_path)
    with FileLock(str(lock_path), timeout=120):
        _safe_directory(directory)
        directory.mkdir(parents=True, exist_ok=True)
        _safe_directory(directory)
        proof_path = work / 'named-input-proof.json'
        _safe_directory(proof_path)
        _safe_directory(target)
        if proof_path.exists() and json.loads(proof_path.read_text(encoding='utf8')) != expected:
            raise NamedDeliveryError('既有交付映射与本次身份不符')
        directory_identity = directory.stat()
        def check_directory():
            _safe_directory(directory)
            current = directory.stat()
            if (current.st_dev, current.st_ino) != (directory_identity.st_dev, directory_identity.st_ino):
                raise NamedDeliveryError('交付目录在处理期间发生变化')
        if target.exists():
            if not target.is_file() or target.stat().st_size != expected['bytes'] or sha_file(target) != expected['source_sha256']:
                raise NamedDeliveryError('既有交付文件与原件不符')
        else:
            temporary = directory / ('.named-' + uuid.uuid4().hex + '.tmp')
            try:
                before = source.stat()
                digest = hashlib.sha256()
                with source.open('rb') as inp, temporary.open('xb') as out:
                    for chunk in iter(lambda: inp.read(1024 * 1024), b''):
                        digest.update(chunk)
                        out.write(chunk)
                    out.flush()
                    os.fsync(out.fileno())
                after = source.stat()
                if ((before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
                        or digest.hexdigest() != expected['source_sha256']
                        or sha_file(source) != expected['source_sha256']):
                    raise NamedDeliveryError('复制期间原件发生变化')
                check_directory()
                _safe_directory(target)
                try:
                    os.link(temporary, target)
                except FileExistsError:
                    _safe_directory(target)
                    if not target.is_file() or sha_file(target) != expected['source_sha256']:
                        raise NamedDeliveryError('并发交付文件与原件不符')
            finally:
                temporary.unlink(missing_ok=True)
        check_directory()
        _safe_directory(target)
        if not proof_path.exists():
            temporary = work / ('.named-proof-' + uuid.uuid4().hex + '.tmp')
            try:
                encoded = json.dumps(expected, ensure_ascii=False, sort_keys=True, indent=2)
                with temporary.open('x', encoding='utf8') as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                _safe_directory(proof_path)
                try:
                    os.link(temporary, proof_path)
                except FileExistsError:
                    _safe_directory(proof_path)
                    if json.loads(proof_path.read_text(encoding='utf8')) != expected:
                        raise NamedDeliveryError('并发交付映射与本次身份不符')
            finally:
                temporary.unlink(missing_ok=True)
    return target
