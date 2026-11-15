"""Candidate-only direct-source preparation and planning entry points."""

import copy
from contextlib import contextmanager
import hashlib
import importlib
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import sys
import uuid


ROOT = Path(__file__).resolve().parent
PROGRAM = Path(r'F:\论文预处理系统\程序')
RELEASE = PROGRAM / 'updates' / 'unified-prefect-20261029-material-r02'


def _native_paths():
    paths = [ROOT / 'desktop', RELEASE / 'desktop', PROGRAM / 'desktop', RELEASE / 'core']
    for path in paths:
        while str(path) in sys.path:sys.path.remove(str(path))
    sys.path[:0] = [str(path) for path in paths]
    existing = sys.modules.get('preextract_material')
    if existing is not None and Path(existing.__file__).resolve() != (ROOT / 'desktop' / 'preextract_material.py').resolve():
        raise RuntimeError('同进程已加载其他正文解析版本，不能混用')
    watched = {'native_docx', 'evidence', 'knowledge_package', 'knowledge_outline',
               'preextract_request', 'preextract_partition', 'preextract_images',
               'engine', 'office_input', 'preextract_input_repair'}
    for name in watched:
        module = sys.modules.get(name)
        location = getattr(module, '__file__', None) if module else None
        expected = importlib.machinery.PathFinder.find_spec(name, [str(p) for p in paths])
        if location and (expected is None or not expected.origin
                         or Path(location).resolve() != Path(expected.origin).resolve()):
            raise RuntimeError('同进程已加载不兼容的解析依赖：' + name)


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _guard_delivery_work(path):
    path = Path(path).resolve()
    if any(parent.name.casefold() == 'deliveries' or (parent/'delivery.json').is_file()
           or (parent/'handoff.json').is_file()
           for parent in (path,*path.parents)):
        raise ValueError('处理目录不能写入任何已交付资料组')


def _guard_history_path(work, path):
    path = Path(path)
    if path.is_symlink() or (hasattr(path, 'is_junction') and path.is_junction()):
        raise ValueError('附件历史目录不能使用链接')
    resolved = path.resolve()
    if not resolved.is_relative_to(Path(work).resolve()):
        raise ValueError('附件历史目录必须位于工作目录内')
    _guard_delivery_work(resolved)


def _write_json_new(path, data):
    """Publish complete JSON without overwriting an existing record."""
    path = Path(path)
    temporary = path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')
    with temporary.open('x', encoding='utf8') as stream:
        json.dump(data, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    if os.name == 'nt':
        os.rename(temporary, path)
    else:
        os.link(temporary, path)
        temporary.unlink()


@contextmanager
def _retrying_file_lock(path, *, timeout, attempts=3):
    from filelock import FileLock, Timeout
    lock = FileLock(str(path), timeout=timeout)
    for number in range(attempts):
        try:
            lock.acquire()
        except Timeout:
            if number + 1 == attempts:
                raise
        else:
            try:
                yield lock
            finally:
                lock.release()
            return


def _material_work_lock(work):
    work = Path(work).resolve()
    _guard_delivery_work(work)
    work.parent.mkdir(parents=True, exist_ok=True)
    return _retrying_file_lock(work.parent / ('.' + work.name + '.material.lock'), timeout=60)


def _local_retry_work(work, source, expected_sha256):
    """Reuse a compatible local result or create a new attempt, never overwrite history."""
    if not work.exists():
        return work
    manifest = work / 'material.json'
    old_identity = None
    if manifest.is_file():
        try:
            old = json.loads(manifest.read_text(encoding='utf8'))
            old_source = old.get('source') or {}
            same_source = (old_source.get('file') == str(source)
                           and old_source.get('source_sha256') == expected_sha256.lower())
            stale_version = (old.get('parser_version') != 'candidate-direct-clean/25'
                             or (old.get('cleanup') or {}).get('version') != 'reading-scope/25')
            if same_source and stale_version:
                old_identity = {'source_path': str(source), 'source_sha256': expected_sha256.lower()}
            else:
                return work
        except (OSError, ValueError, TypeError):
            return work
    allowed = {'registered-binding.json', 'registered-binding.lock'}
    if old_identity is None and not any(path.name not in allowed for path in work.iterdir()):
        return work
    identity = old_identity
    for name in ('registered-binding.json', 'scope-pending.json'):
        path = work / name
        if path.is_file():
            identity = json.loads(path.read_text(encoding='utf8'))
            break
    if (not isinstance(identity, dict) or identity.get('source_path') != str(source)
            or identity.get('source_sha256') != expected_sha256.lower()):
        return work
    attempts = work / 'attempts'
    if attempts.is_dir():
        for candidate in sorted((p for p in attempts.iterdir() if p.is_dir()), reverse=True):
            manifest = candidate / 'material.json'
            if manifest.is_file():
                data = json.loads(manifest.read_text(encoding='utf8'))
                if (data.get('parser_version') == 'candidate-direct-clean/25'
                        and (data.get('cleanup') or {}).get('version') == 'reading-scope/25'
                        and (data.get('source') or {}).get('file') == str(source)
                        and (data.get('source') or {}).get('source_sha256') == expected_sha256.lower()):
                    return candidate
    attempts.mkdir(parents=True, exist_ok=True)
    number = 1
    while (candidate := attempts / f'attempt-{number:04d}').exists():
        number += 1
    candidate.mkdir()
    return candidate


def prepare_source(source, expected_sha256, work_dir, catalog_path):
    work = Path(work_dir).resolve()
    with _material_work_lock(work):
        source = Path(source).resolve(strict=True)
        delivery_source = any(parent.name.casefold() == 'files' and
            ((parent.parent/'delivery.json').is_file() or (parent.parent/'handoff.json').is_file()
             or any(ancestor.name.casefold() == 'deliveries' for ancestor in parent.parents))
            for parent in source.parents)
        if delivery_source and not (work/'registered-binding.json').is_file():
            raise ValueError('已交付正文须使用登记入口建立身份绑定')
        if not isinstance(expected_sha256, str) or _sha(source) != expected_sha256.lower():
            raise ValueError('正文哈希与接收记录不一致')
        selected = _local_retry_work(work, source, expected_sha256)
        root_binding = work / 'registered-binding.json'
        if root_binding.is_file() and selected != work:
            identity = json.loads(root_binding.read_text(encoding='utf8'))
            if (identity.get('schema') != 'clean-local-binding/1' or identity.get('source_path') != str(source)
                    or identity.get('source_sha256') != expected_sha256.lower()
                    or _sha(identity['receipt_path']) != identity.get('receipt_sha256')):
                raise ValueError('登记身份或回执已发生变化')
            selected_binding = selected / 'registered-binding.json'
            if selected_binding.exists():
                if json.loads(selected_binding.read_text(encoding='utf8')) != identity:
                    raise ValueError('重试工作目录绑定其他登记身份')
            else:
                with selected_binding.open('x', encoding='utf8') as stream:
                    json.dump(identity, stream, ensure_ascii=False, sort_keys=True, indent=2)
        return _prepare_source_unlocked(source, expected_sha256, selected, catalog_path)


def _prepare_source_unlocked(source, expected_sha256, work_dir, catalog_path):
    source, work = Path(source).resolve(strict=True), Path(work_dir).resolve()
    catalog = Path(catalog_path).resolve(strict=True)
    _guard_delivery_work(work)
    if not source.is_file() or not catalog.is_file(): raise ValueError('正文或目录文件不存在')
    if not isinstance(expected_sha256, str) or _sha(source) != expected_sha256.lower():
        raise ValueError('正文哈希与接收记录不一致')
    binding = work / 'registered-binding.json'
    if binding.is_file():
        identity = json.loads(binding.read_text(encoding='utf8'))
        if (identity.get('schema') != 'clean-local-binding/1'
                or identity.get('source_path') != str(source)
                or identity.get('source_sha256') != expected_sha256.lower()):
            raise ValueError('工作目录已绑定其他登记正文')
    for parent in source.parents:
        if parent.name.lower() == 'files' and work.is_relative_to(parent.parent):
            raise ValueError('处理目录不能写入交付目录')
    if work.exists() and not (work / 'material.json').is_file():
        allowed = {'registered-binding.json', 'registered-binding.lock'}
        if any(path.name not in allowed for path in work.iterdir()):
            raise ValueError('已有未完成的工作或请求历史，保留原产物')
    _native_paths()
    from direct_source import detect_format
    detected, evidence = detect_format(source)
    native = importlib.import_module('preextract_material')
    from reading_cleanup import ScopeUncertain
    try:
        bundle = native.prepare_material(source, work, catalog, render_missing=True,
                                         detected_format=detected, expected_sha256=expected_sha256.lower())
    except ScopeUncertain as exc:
        work.mkdir(parents=True, exist_ok=True)
        record = {'schema':'local-scope-pending/1', 'state':'review_required',
                  'source_path':str(source), 'source_sha256':expected_sha256.lower(),
                  'source_sha256_after':_sha(source), 'reason':str(exc)}
        with (work/'scope-pending.json').open('x', encoding='utf8') as stream:
            json.dump(record, stream, ensure_ascii=False, indent=2)
        raise
    if _sha(source) != expected_sha256.lower(): raise ValueError('正文在处理期间发生变化')
    return {'material_path': str(work / 'material.json'), 'source_sha256': expected_sha256.lower(),
            'detected_format': detected, 'format_evidence': evidence,
            'cleanup_version': bundle['cleanup']['version'],
            'handoff_status': bundle['readiness']['state'],
            'review_count': len(bundle['readiness']['items'])}


def _observe_registered(item, receipt_root):
    path = ROOT.parent / 'local-upstream-alignment-20261030' / 'local_handoff.py'
    spec = importlib.util.spec_from_file_location('_clean_material_handoff', path)
    if spec is None or spec.loader is None:raise RuntimeError('登记交接读取器不可用')
    reader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reader)
    return reader.read_registered_item(item, receipt_root=receipt_root)


def _observation_state(observed):
    keys = ('file_id','kind','state','error','sha256','bytes','path','unverified_evidence')
    return {'items':[{key:row.get(key) for key in keys} for row in observed['items']],
            'observation':observed.get('observation') or {}}


def _binding_item(binding):
    return {'status':'passed', 'registration_status':'registered', 'delivery_complete':True,
            **{key:binding[key] for key in ('paper_id','group_id','receipt_id','receipt_path','delivery_dir')}}


def prepare_registered(item, *, receipt_root, work_dir, catalog_path):
    observed = _observe_registered(item, receipt_root)
    paper = observed.get('paper') or {}
    body = paper.get('body') or {}
    if observed.get('body_available') is not True or not body:
        raise ValueError('登记正文不可用')
    delivery = Path(observed['delivery_dir']).resolve(strict=True)
    source = Path(body['path']).resolve(strict=True)
    work = Path(work_dir).resolve()
    _guard_delivery_work(work)
    if work.is_relative_to(delivery) or not source.is_relative_to(delivery / 'files'):
        raise ValueError('正文或处理目录不符合交付范围')
    if _sha(source) != body['sha256']:
        raise ValueError('交付正文哈希不符')
    identity = {'schema': 'clean-local-binding/1', 'paper_id': observed['paper_id'],
                'group_id': observed['group_id'], 'receipt_id': observed['receipt_id'],
                'receipt_path': observed['receipt_path'], 'receipt_sha256': observed['receipt_sha256'],
                'delivery_dir': str(delivery), 'body_file_id': body['file_id'],
                'attachment_count': sum(row.get('kind') == 'attachment' for row in observed['items']),
                'source_path': str(source), 'source_sha256': body['sha256']}
    with _material_work_lock(work):
        if work.exists() and any(work.iterdir()) and not (work / 'registered-binding.json').is_file():
            raise ValueError('已有工作目录没有本次登记绑定，保留原历史')
        work.mkdir(parents=True, exist_ok=True)
        with _retrying_file_lock(work / 'registered-binding.lock', timeout=15):
            binding = work / 'registered-binding.json'
            if binding.exists():
                if json.loads(binding.read_text(encoding='utf8')) != identity:
                    raise ValueError('已有工作目录绑定其他登记身份')
            else:
                with binding.open('x', encoding='utf8') as stream:
                    json.dump(identity, stream, ensure_ascii=False, sort_keys=True, indent=2)
            selected = _local_retry_work(work, source, body['sha256'])
            if selected != work:
                selected_binding = selected / 'registered-binding.json'
                if selected_binding.exists():
                    if json.loads(selected_binding.read_text(encoding='utf8')) != identity:
                        raise ValueError('重试工作目录绑定其他登记身份')
                else:
                    with selected_binding.open('x', encoding='utf8') as stream:
                        json.dump(identity, stream, ensure_ascii=False, sort_keys=True, indent=2)
            result = _prepare_source_unlocked(source, body['sha256'], selected, catalog_path)
            if _sha(observed['receipt_path']) != observed['receipt_sha256'] or _sha(source) != body['sha256']:
                raise ValueError('交接回执或正文在处理期间变化')
    return {**result, 'paper_id': observed['paper_id'], 'group_id': observed['group_id'],
            'receipt_id': observed['receipt_id'], 'attachments': paper.get('attachments', []),
            'observation': observed.get('observation') or {}}


def prepare_registered_attachments(item, *, receipt_root, work_dir):
    """Keep registered attachment identity and upstream evidence without previewing it."""
    observed = _observe_registered(item, receipt_root)
    delivery = Path(observed['delivery_dir']).resolve(strict=True)
    work = Path(work_dir).resolve()
    _guard_delivery_work(work)
    if work.is_relative_to(delivery):
        raise ValueError('附件处理目录不能写入交付目录')
    rows = []
    for attachment in (r for r in observed['items'] if r.get('kind') == 'attachment'):
        source = Path(attachment['path']).resolve()
        if not source.is_relative_to(delivery/'files'):
            raise ValueError('附件不在已登记的交付目录')
        available = attachment.get('state') == '已接收'
        digest = attachment.get('sha256')
        if available and (not source.is_file() or _sha(source) != digest or source.stat().st_size != attachment.get('bytes')):
            raise ValueError('附件哈希或大小与回执不一致')
        row = copy.deepcopy(attachment)
        row.update(path=str(source),
                   format=attachment.get('detected_format') or source.suffix.lstrip('.').lower() or 'unknown',
                   format_evidence='registered_record', content_status='upstream_only',
                   availability='available' if available else 'unavailable',
                   evidence_unverified=bool(attachment.get('unverified_evidence')))
        rows.append(row)
    if len({row['file_id'] for row in rows}) != len(rows):
        raise ValueError('附件编号重复，不能建立独立清单')
    inventory = {'schema': 'registered-attachment-inventory/3', 'paper_id': observed['paper_id'],
                 'group_id': observed['group_id'], 'receipt_id': observed['receipt_id'],
                 'receipt_path': observed['receipt_path'], 'receipt_sha256': observed['receipt_sha256'],
                 'delivery_dir': str(delivery), 'items': rows,
                 'observation_state': _observation_state(observed)}
    if _sha(observed['receipt_path']) != observed['receipt_sha256'] or any(
            _sha(r['path']) != r['sha256'] for r in rows if r['availability'] == 'available'):
        raise ValueError('准备期间附件或回执发生变化')
    if _observation_state(_observe_registered(item,receipt_root)) != inventory['observation_state']:
        raise ValueError('准备期间交付或附件证据发生变化')
    work.mkdir(parents=True, exist_ok=True)
    with _retrying_file_lock(work/'attachment-inventory.lock', timeout=15):
        if _sha(observed['receipt_path']) != observed['receipt_sha256'] or any(
                _sha(r['path']) != r['sha256'] for r in rows if r['availability'] == 'available'):
            raise ValueError('等待附件锁期间附件或回执发生变化')
        if _observation_state(_observe_registered(item,receipt_root)) != inventory['observation_state']:
            raise ValueError('等待附件锁期间交付或附件证据发生变化')
        target = work/'attachment-inventory.json'
        binding = work/'attachment-binding.json'
        attempts = work/'attempts'
        _guard_history_path(work, attempts)
        keys = ('paper_id','group_id','receipt_id','receipt_sha256','delivery_dir')
        identity = {'schema':'registered-attachment-binding/1', **{k:inventory[k] for k in keys}}
        def own_temporary(path):
            return (path.name.startswith(('.attachment-binding.json.', '.attachment-inventory.json.'))
                    and path.name.endswith('.tmp'))
        if any(path.name not in {'attachment-inventory.lock','attachment-inventory.json','attachment-binding.json','attempts'}
               and not own_temporary(path) for path in work.iterdir()):
            raise ValueError('附件处理目录已有其他历史')
        if binding.exists():
            _guard_history_path(work, binding)
            if json.loads(binding.read_text(encoding='utf8')) != identity:
                raise ValueError('附件历史绑定其他资料')
        else:
            if target.exists():
                _guard_history_path(work, target)
                try:previous = json.loads(target.read_text(encoding='utf8'))
                except (OSError, ValueError) as exc:
                    raise ValueError('已有附件历史的身份无法核对') from exc
                if not isinstance(previous,dict) or not all(k in previous for k in keys):
                    raise ValueError('已有附件历史的身份无法核对')
                if any(previous.get(k) != identity[k] for k in keys):
                    raise ValueError('已有附件清单绑定其他资料，保留历史')
                if attempts.exists():
                    for folder in attempts.iterdir():
                        _guard_history_path(work, folder)
                        candidate = folder/'attachment-inventory.json'
                        _guard_history_path(work, candidate)
                        try:old = json.loads(candidate.read_text(encoding='utf8'))
                        except (OSError, ValueError) as exc:
                            raise ValueError('附件尝试历史的身份无法核对') from exc
                        if not isinstance(old,dict) or not all(k in old for k in keys):
                            raise ValueError('附件尝试历史的身份无法核对')
                        if any(old.get(k) != identity[k] for k in keys):
                            raise ValueError('附件尝试历史绑定其他资料')
            elif attempts.exists() and any(attempts.iterdir()):
                raise ValueError('附件根清单缺失，已有历史身份无法核对')
            _write_json_new(binding, identity)
        if target.exists():
            _guard_history_path(work, target)
            try:previous = json.loads(target.read_text(encoding='utf8'))
            except (OSError, ValueError):previous = None
            if not isinstance(previous,dict) or not all(k in previous for k in keys):previous = None
            if previous is not None and any(previous.get(k) != inventory.get(k) for k in keys):
                raise ValueError('已有附件清单绑定其他资料，保留历史')
            if previous != inventory:
                attempts.mkdir(exist_ok=True)
                target = None
                for candidate in sorted(attempts.glob('attempt-*/attachment-inventory.json')):
                    _guard_history_path(work, candidate.parent)
                    _guard_history_path(work, candidate)
                    try:
                        if json.loads(candidate.read_text(encoding='utf8')) == inventory:
                            target = candidate;break
                    except (OSError, ValueError):
                        continue
                if target is None:
                    number = 1
                    while (attempts/f'attempt-{number:04d}').exists():number += 1
                    folder = attempts/f'attempt-{number:04d}'
                    _guard_history_path(work, folder);folder.mkdir()
                    target = folder/'attachment-inventory.json'
        if not target.exists():
            _guard_history_path(work, target)
            _write_json_new(target, inventory)
    return {'paper_id': observed['paper_id'], 'receipt_id': observed['receipt_id'],
            'attachment_count': len(rows), 'inventory_path': str(target)}


def verify_source_spans(source_rows, spans):
    for span in spans:
        row = source_rows.get(span.get('source_block_id'))
        start, end = span.get('source_start'), span.get('source_end')
        if (not row or not isinstance(start,int) or not isinstance(end,int)
                or not 0 <= start < end <= len(row.get('text') or '')
                or (span.get('position') or {}) != (row.get('position') or {})):
            raise ValueError('清理文本的来源映射不完整或超出原解析块')


def write_review_template(material_path, destination):
    """Create a blank, material-bound checklist; it is never a review decision."""
    material_path = Path(material_path).resolve(strict=True)
    destination = Path(destination).resolve()
    _guard_delivery_work(destination)
    material_sha = _sha(material_path)
    material = json.loads(material_path.read_text(encoding='utf8'))
    source = material.get('source') or {}
    index_path = Path(source['index_file']).resolve(strict=True)
    if _sha(index_path) != source.get('index_sha256'):
        raise ValueError('待核清单的来源索引已变化')
    index = json.loads(index_path.read_text(encoding='utf8'))
    readiness = material.get('readiness') or {}
    items = readiness.get('items')
    if (readiness != index.get('readiness') or readiness.get('schema') != 'local-material-readiness/1'
            or readiness.get('state') != 'review_required' or not isinstance(items, list) or not items
            or any(not isinstance(item, dict) or not isinstance(item.get('id'), str) for item in items)
            or len({item['id'] for item in items}) != len(items)):
        raise ValueError('正文材料没有可核对的待核清单')
    assets = {asset['id']: asset for asset in material.get('assets', [])}
    rows = []
    for item in items:
        asset = assets.get(item.get('asset_id')) or {}
        rows.append({'id': item['id'], 'kind': item.get('kind'),
                     'description': item.get('description'), 'position': item.get('position'),
                     'asset_path': asset.get('image_path'), 'decision': '', 'evidence': ''})
    checklist = {'schema': 'local-material-review/1', 'material_sha256': material_sha,
                 'source_file': source.get('file'), 'reviewer': '', 'decisions': rows}
    if _sha(material_path) != material_sha or _sha(index_path) != source.get('index_sha256'):
        raise ValueError('待核清单生成期间材料已变化')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open('x', encoding='utf8') as stream:
        json.dump(checklist, stream, ensure_ascii=False, indent=2)
    return str(destination)


def map_request_range(material_path, part, block_id, start, end):
    """Resolve one request-local prose range through cleanup to the parser view."""
    from reading_cleanup import map_clean_range
    material = json.loads(Path(material_path).read_text(encoding='utf8'))
    source = material['source']
    if _sha(source['index_file']) != source['index_sha256']:
        raise ValueError('来源索引已变化')
    index = json.loads(Path(source['index_file']).read_text(encoding='utf8'))
    if index.get('source_view') != source.get('parsed_view'):
        raise ValueError('来源解析视图身份不一致')
    if 'bundle' in part:
        request_blocks = part['bundle']['blocks']
    else:
        content = part['body']['messages'][1]['content'][0]['text']
        request_blocks = json.loads(content)['material']['blocks']
    rows = [b for b in request_blocks if b['id'] == block_id]
    if len(rows) != 1 or not (isinstance(start,int) and isinstance(end,int)
                              and 0 <= start < end <= len(rows[0].get('text',''))):
        raise ValueError('请求块或本地偏移无效')
    request_block = rows[0]
    local_span = request_block.get('source_span')
    clean_id = local_span['block_id'] if local_span else request_block['id']
    clean = next((b for b in material['blocks'] if b['id'] == clean_id), None)
    if clean is None:
        raise ValueError('请求块未绑定清理正文')
    base = local_span['start'] if local_span else 0
    limit = local_span['end'] if local_span else len(clean.get('text',''))
    if request_block.get('text','') != clean.get('text','')[base:limit]:
        raise ValueError('请求文字与清理正文不一致')
    spans = map_clean_range(clean, base+start, base+end)
    source_rows = {b['id']:b for b in index.get('source_blocks',[])}
    verify_source_spans(source_rows,spans)
    for span in spans:
        row = source_rows.get(span['source_block_id'])
        if not row or span['source_end'] > len(row.get('text','')):
            raise ValueError('原解析块不包含映射范围')
        span['source_text'] = row['text'][span['source_start']:span['source_end']]
    return {'source_view': source['parsed_view'], 'spans': spans}


def plan_clean_material(material_path, template, catalog_path, config, plan_dir, *,
                        attachment_inventory_path=None, review_path=None):
    """Build a frozen local request plan; never sends a model request."""
    required = {'model', 'context_window', 'max_tokens', 'max_output_tokens',
                'context_reserve_tokens', 'max_request_bytes', 'capacity_source'}
    image_required = {'image_token_upper_bound', 'max_images', 'max_total_image_bytes',
                      'max_single_image_bytes', 'max_image_side'}
    if not isinstance(config, dict) or any(k not in config or config[k] is None for k in required):
        raise ValueError('缺少目标模型的容量参数，不能规划分段')
    if not config.get('model') or not config.get('capacity_source'):
        raise ValueError('缺少模型或容量依据，不能规划分段')
    if not isinstance(template, str):
        raise ValueError('模型模板无效')
    positive = required - {'model', 'capacity_source', 'context_reserve_tokens'}
    if any(not isinstance(config[k], int) or isinstance(config[k], bool) or config[k] <= 0 for k in positive):
        raise ValueError('目标模型的容量参数无效')
    if not isinstance(config['context_reserve_tokens'], int) or config['context_reserve_tokens'] < 0:
        raise ValueError('模型容量预留无效')
    material_path = Path(material_path).resolve(strict=True)
    catalog_path = Path(catalog_path).resolve(strict=True)
    work = material_path.parent
    plan_dir = Path(plan_dir).resolve()
    _guard_delivery_work(plan_dir)
    if plan_dir.exists():
        raise ValueError('规划目录已存在，原记录保留')
    material_sha = _sha(material_path)
    material = json.loads(material_path.read_text(encoding='utf8'))
    source = material.get('source') or {}
    hashes = material.get('file_hashes') or {}
    expected = {source.get('file'), source.get('text_file'), source.get('index_file'), str(catalog_path)}
    expected.update(a.get('image_path') for a in material.get('assets', []) if a.get('image_path'))
    if (None in expected or not expected.issubset(hashes)
            or material.get('parser_version') != 'candidate-direct-clean/25'
            or material.get('cleanup', {}).get('version') != 'reading-scope/25'
            or _sha(catalog_path) != source.get('catalog_sha256')
            or any(not Path(path).is_file() or _sha(path) != sha for path, sha in hashes.items())):
        raise ValueError('清理材料、源文件或容量目录的版本无法核对')
    original = Path(source['file']).resolve(strict=True)
    if _sha(original) != source.get('source_sha256') or _sha(source['text_file']) != source.get('text_sha256') or _sha(source['index_file']) != source.get('index_sha256'):
        raise ValueError('清理材料的原文或索引哈希不符')
    for parent in original.parents:
        if parent.name.lower() == 'files' and plan_dir.is_relative_to(parent.parent):
            raise ValueError('规划目录不能写入交付目录')
    index = json.loads(Path(source['index_file']).read_text(encoding='utf8'))
    for key in ('blocks', 'sections', 'assets', 'formulas', 'removed', 'cover_facts', 'metadata', 'cleanup', 'readiness'):
        if material.get(key) != index.get(key):
            raise ValueError('清理材料和来源索引不一致')
    if Path(source['text_file']).read_text(encoding='utf8') != '\n\n'.join(b.get('text', '') for b in material['blocks']):
        raise ValueError('清理正文和索引不一致')
    readiness = material.get('readiness') or {}
    if readiness.get('schema') != 'local-material-readiness/1' or readiness.get('state') not in {'ready','review_required'}:
        raise ValueError('清理材料缺少交接状态')
    pending = readiness.get('items') or []
    if (not isinstance(pending,list) or bool(pending) != (readiness['state'] == 'review_required')
            or any(not isinstance(row,dict) or not isinstance(row.get('id'),str) for row in pending)
            or len({row['id'] for row in pending}) != len(pending)):
        raise ValueError('交接状态和待核事项不一致')
    review_sha = None
    decisions = []
    partial_pending = bool(pending and review_path is None)
    if pending and review_path is not None:
        review_path = Path(review_path).resolve(strict=True)
        review_sha = _sha(review_path)
        review = json.loads(review_path.read_text(encoding='utf8'))
        decisions = review.get('decisions') or []
        if not isinstance(decisions, list) or any(not isinstance(row, dict) for row in decisions):
            raise ValueError('待核复核记录格式无效')
        ids = [row.get('id') for row in decisions if isinstance(row,dict)]
        expected_ids = {row['id'] for row in pending}
        by_id = {row['id']:row for row in pending}
        if (review.get('schema') != 'local-material-review/1' or review.get('material_sha256') != material_sha
                or not isinstance(review.get('reviewer'),str) or not review['reviewer'].strip()
                or len(ids) != len(expected_ids) or set(ids) != expected_ids
                or any(row.get('decision') != ('not_research' if by_id[row['id']]['kind'] in {'unresolved_visual', 'unresolved_boundary'} else 'confirmed')
                       or not str(row.get('evidence') or '').strip() for row in decisions)):
            raise ValueError('待核事项未全部完成或复核记录与正文版本不符')
    binding_path = work / 'registered-binding.json'
    binding_sha = None
    registered_identity = None
    attachment_rows = []
    attachment_inventory_sha = None
    attachment_inventory_pending = False
    available_attachment_sources = []
    observed_state = None
    if binding_path.is_file():
        binding_sha = _sha(binding_path)
        binding = json.loads(binding_path.read_text(encoding='utf8'))
        if (binding.get('source_path') != str(original) or binding.get('source_sha256') != source['source_sha256']
                or _sha(binding['receipt_path']) != binding.get('receipt_sha256')):
            raise ValueError('登记身份或回执已发生变化')
        binding_item = _binding_item(binding)
        registered_identity = {key: binding.get(key) for key in
                               ('paper_id', 'group_id', 'receipt_id', 'receipt_sha256', 'body_file_id')}
        registered_identity['binding_sha256'] = binding_sha
        binding_root = Path(binding['receipt_path']).parent.parent
        observed_state = _observation_state(_observe_registered(binding_item,binding_root))
        if binding.get('attachment_count', 0) and attachment_inventory_path is None:
            attachment_inventory_pending = True
        if attachment_inventory_path is not None:
            inventory_path = Path(attachment_inventory_path).resolve(strict=True)
            attachment_inventory_sha = _sha(inventory_path)
            inv = json.loads(inventory_path.read_text(encoding='utf8'))
            if (inv.get('schema') != 'registered-attachment-inventory/3'
                    or any(inv.get(k) != binding.get(k) for k in ('paper_id','group_id','receipt_id','receipt_sha256','delivery_dir'))
                    or inv.get('observation_state') != observed_state
                    or len(inv.get('items', [])) != binding.get('attachment_count', 0)):
                raise ValueError('附件清单与正文登记身份不一致')
            delivery = Path(binding['delivery_dir'])
            receipt = json.loads(Path(binding['receipt_path']).read_text(encoding='utf8'))
            registered = {row['file_id']:row for row in receipt['items'] if row.get('kind') == 'attachment'}
            observed_items = {row['file_id']:row for row in observed_state['items'] if row.get('kind') == 'attachment'}
            if len(registered) != binding.get('attachment_count') or {r.get('file_id') for r in inv['items']} != set(registered):
                raise ValueError('附件清单与登记回执逐项不一致')
            for row in inv['items']:
                declared = registered[row['file_id']]
                current = observed_items.get(row['file_id']) or {}
                path=Path(row['path']).resolve()
                if (not path.is_relative_to(delivery/'files') or path != Path(declared['path']).resolve()
                        or row.get('sha256') != declared.get('sha256') or row.get('bytes') != declared.get('bytes')
                        or row.get('availability') != ('available' if current.get('state') == '已接收' else 'unavailable')
                        or row.get('evidence_unverified') is not bool(current.get('unverified_evidence'))):
                    raise ValueError('附件清单与已登记文件不一致')
                if row['availability'] == 'available':
                    if not path.is_file() or _sha(path) != row['sha256'] or path.stat().st_size != row['bytes']:
                        raise ValueError('附件文件与登记回执不一致')
                    available_attachment_sources.append((path,row['sha256']))
                limits=['只提供附件元信息，附件内容和像素未交模型阅读']
                if row['availability'] == 'unavailable':limits.append('附件当前不可用，未读取，需核验交付文件')
                if row.get('evidence_unverified'):limits.append('附件交付证据尚未核验')
                attachment_rows.append({'file_ref':'attachment_'+row['file_id'], 'kind':'attachment',
                    'format':row['format'], 'bytes':row['bytes'], 'sha256':row['sha256'],
                    'parse_status':'metadata_only' if row['availability']=='available' else 'unavailable', 'preview_truncated':False,
                    'association_status':'registered_group', 'association_basis':'上游登记的同组附件；内容关联待独立核验',
                    'limits':limits})
    elif attachment_inventory_path is not None:
        raise ValueError('正文缺少登记身份，不能绑定附件清单')
    catalog = json.loads(catalog_path.read_text(encoding='utf-8-sig'))
    _native_paths()
    from preextract_images import prepare_request_images
    from preextract_request import plan_requests, material_view
    from reading_cleanup import contains_inline_image
    projection_report = {'pending_count': 0, 'held_asset_count': 0,
                         'held_page_count': 0, 'held_block_count': 0}
    projected = material
    from partial_projection import pdf_image_clip_requirements
    clip_pending = pdf_image_clip_requirements(material)
    # Missing objects and uncertain pages are recorded as gaps.  They must not
    # suppress other already extracted content, including positioned pictures.
    if partial_pending:
        projection_pending = [row for row in pending if row.get('kind') == 'visual_scope']
    elif review_path is not None:
        # A supplied review can explicitly identify a specific range as not
        # research; retain that existing decision without automatic page holds.
        excluded_ids = {row['id'] for row in decisions if row.get('decision') == 'not_research'}
        projection_pending = [row for row in pending if row.get('id') in excluded_ids]
    else:
        projection_pending = []
    if projection_pending:
        from partial_projection import project_available_material
        projected, projection_report = project_available_material(material, projection_pending)
    gap_codes = {'ocr_page_incomplete', 'pdf_page_incomplete', 'post_ocr_scope_unverified',
                 'scope_incomplete', 'ocr_scope_suspect', 'toc_transition_text', 'toc_transition_visual',
                 'pdf_column_removed_review', 'pdf_column_kept_review',
                 'missing_note', 'external_images_missing', 'inline_image_review', 'embedded_object_review',
                 'native_chart_visual', 'body_container', 'scanned_page', 'image_missing', 'formula_review',
                 'image_format_unreadable', 'image_position_unresolved', 'formula_position_unresolved',
                 'pdf_vector_review', 'pdf_table_review', 'pdf_visual_incomplete', 'pdf_image_clip_review',
                 'pdf_scan_crop_scope_review'}
    reading_gaps = [copy.deepcopy(i) for i in material.get('issues', []) if i.get('code') in gap_codes]
    reading_gaps.extend({'code': 'image_position_missing', 'position': row.get('position') or {},
                         'asset_id': row.get('asset_id'), 'message': row.get('description', '')}
                        for row in projection_pending if row.get('kind') == 'visual_scope')
    reading_gaps.extend({'code': 'pdf_image_clip_review', 'position': row.get('position') or {},
                         'asset_id': row.get('asset_id'), 'message': row.get('description', '')}
                        for row in clip_pending)
    incomplete_pages = sorted({(i.get('position') or {}).get('page') for i in reading_gaps
                               if i.get('code') in {'ocr_page_incomplete', 'pdf_page_incomplete'}
                               and isinstance((i.get('position') or {}).get('page'), int)})
    incomplete = bool(pending or reading_gaps or attachment_inventory_pending)
    if incomplete:
        projected = copy.deepcopy(projected)
        scope_risk = any(i.get('code') == 'post_ocr_scope_unverified' for i in material.get('issues', []))
        context_instruction = ('部分页面未识别，其他可用文字已继续处理；后续范围可能含未识别的排除内容，建议核对，不得称为整篇完整。'
                               if scope_risk else '仅包含已经确认可交接的材料；缺口仍待处理，不得称为整篇完整。')
        projected['reading_context'] = {'source_completeness': 'partial',
            'instruction': context_instruction,
            'withheld': projection_report,
            'attachment_inventory_pending': attachment_inventory_pending}
        if incomplete_pages:
            projected['reading_context']['ocr_incomplete_pages'] = incomplete_pages
        scope = projected.get('source', {}).get('reading_scope')
        if isinstance(scope, dict):
            scope['notes'] = context_instruction
    if registered_identity:
        if projected is material:
            projected = copy.deepcopy(projected)
        projected.setdefault('reading_context', {})['registered_identity'] = registered_identity
    if any(asset.get('image_path') for asset in projected.get('assets', [])):
        if any(k not in config or not isinstance(config[k], int) or isinstance(config[k], bool)
               or config[k] <= 0 for k in image_required):
            raise ValueError('有图片时缺少目标通道的图片容量参数，不能规划分段')
    if contains_inline_image(material_view(projected)):
        raise ValueError('正文投影仍含未恢复的内联图片，需先核对并补全')
    plan_dir.mkdir(parents=True, exist_ok=False)
    adapted, image_report = prepare_request_images(projected, plan_dir, config,
                                                   continue_on_image_error=True)
    if image_report.get('held_count'):
        incomplete = True
        positions = {a.get('id'): a.get('position') or {} for a in projected.get('assets', [])}
        reading_gaps.extend({'code': 'image_capacity_unavailable', 'asset_id': row['id'],
                             'position': positions.get(row['id'], {}), 'message': row.get('reason', '')}
                            for row in image_report['images'] if row.get('held'))
        context = adapted.setdefault('reading_context', {})
        instruction = context.get('instruction', '')
        instruction = (instruction + ' ' if instruction else '') + '部分图片无法适配，未加入本次材料；不得称为整篇完整。'
        context.update(source_completeness='partial',
                       instruction=instruction,
                       image_capacity_held=image_report['held_count'])
        adapted.setdefault('issues', []).append({'code':'image_capacity_hold',
            'message':'部分图片无法适配当前通道，已保留原图并继续处理其他正文。','position':{}})
    if attachment_rows:
        from associated_materials import inventory_hash
        adapted['attachments'] = attachment_rows
        adapted['associated_materials_sha256'] = inventory_hash(attachment_rows)
    if adapted.get('blocks') or adapted.get('assets'):
        from preextract_request import RequestCapacityError, build_request
        try:
            plan = plan_requests(adapted, template, catalog, config)
        except RequestCapacityError:
            # If a single image cannot fit even an otherwise empty request,
            # record that specific gap and continue with all other material.
            # Do not drop images merely because a full paper needs splitting.
            from preextract_partition import PART_RULES
            impossible = []
            for asset in adapted.get('assets', []):
                if not asset.get('image_path'):continue
                probe = copy.deepcopy(adapted)
                probe.update(blocks=[], sections=[], assets=[copy.deepcopy(asset)], issues=[], formulas=[])
                probe['assets'][0]['block_ids'] = []
                probe['assets'][0]['section_ids'] = []
                scope = (probe.get('source') or {}).get('reading_scope')
                if isinstance(scope, dict):scope['extraction_issues'] = []
                _, capacity = build_request(probe, template, catalog, config,
                                            instruction=PART_RULES, enforce=False)
                if capacity['violations']:impossible.append(asset)
            if not impossible:
                raise
            impossible_ids = {a['id'] for a in impossible}
            adapted['assets'] = [a for a in adapted['assets'] if a.get('id') not in impossible_ids]
            for block in adapted.get('blocks', []):
                if block.get('object_ids'):
                    block['object_ids'] = [ident for ident in block['object_ids'] if ident not in impossible_ids]
            reading_gaps.extend({'code': 'image_capacity_unavailable', 'asset_id': asset['id'],
                                 'position': asset.get('position') or {},
                                 'message': '图片单独加入时仍超过当前模型容量，原图保留，其他内容继续。'}
                                for asset in impossible)
            incomplete = True
            context = adapted.setdefault('reading_context', {})
            instruction = context.get('instruction', '')
            context.update(source_completeness='partial', instruction=(instruction + ' ' if instruction else '') +
                           '部分图片超过当前模型容量，未加入本次材料；不得称为整篇完整。')
            plan = plan_requests(adapted, template, catalog, config)
    else:
        plan = {'schema': 'preextract-input-plan/1', 'mode': 'no_available_material',
                'parts': [], 'coverage': {'complete': True, 'block_count': 0,
                'character_count': 0, 'ranges': [], 'asset_ids': []}}
    if plan.get('partition_gaps'):
        reading_gaps.extend(copy.deepcopy(plan['partition_gaps']))
        incomplete = True
    # This is only an offline input plan, never proof of downstream completion.
    plan['coverage']['source_scope_fully_planned'] = not incomplete
    plan['coverage']['whole_paper_complete'] = False
    plan['source_completeness'] = 'partial' if incomplete else 'prepared'
    plan['projection_version'] = 'partial-projection/2'
    plan['reading_gaps'] = reading_gaps
    if (_sha(material_path) != material_sha or any(_sha(path) != sha for path, sha in hashes.items())
            or review_sha is not None and _sha(review_path) != review_sha
            or binding_sha is not None and _sha(binding_path) != binding_sha
            or binding_path.is_file() and _sha(binding['receipt_path']) != binding['receipt_sha256']
            or attachment_inventory_sha is not None and _sha(inventory_path) != attachment_inventory_sha
            or any(_sha(path) != sha for path,sha in available_attachment_sources)):
        raise ValueError('材料在规划期间发生变化，未提交模型')
    if observed_state is not None and _observation_state(_observe_registered(binding_item,binding_root)) != observed_state:
        raise ValueError('规划期间交付或附件证据发生变化，未提交模型')
    # Every primary prose range is mapped to the retained parser view. Tables
    # retain their block/cell positions without pretending to have text offsets.
    clean_blocks = {b['id']:b for b in material['blocks']}
    source_rows = {b['id']:b for b in index.get('source_blocks',[])}
    from reading_cleanup import map_clean_range
    for part in plan['parts']:
        mapped = []
        for span in part['ranges']:
            block = clean_blocks[span['block_id']]
            if block.get('type') == 'paragraph' and span['end'] > span['start']:
                source_spans = map_clean_range(block, span['start'], span['end'])
                verify_source_spans(source_rows,source_spans)
                mapped.append({'clean_range':span, 'precision':'text',
                               'source_spans':source_spans})
            else:
                mapped.append({'clean_range':span, 'precision':'block',
                               'position':block.get('position')})
        part['source_mapping'] = mapped
    # Persist only the projected request bodies and primary ownership, not the
    # private source bundle or excluded content. These are not sent requests.
    saved = {'schema': 'clean-input-plan/1', 'state': 'not_sent', 'material_sha256': material_sha,
             'source_sha256': source['source_sha256'], 'catalog_sha256': source['catalog_sha256'],
             'template_sha256': hashlib.sha256(template.encode('utf8')).hexdigest(),
             'attachment_inventory_sha256': attachment_inventory_sha,
             'registered_identity': registered_identity,
             'review_sha256': review_sha,
             'source_view': source['parsed_view'],
             'cleanup_version': material['cleanup']['version'],
             'projection_version': plan['projection_version'],
             'capacity_source': config['capacity_source'], 'mode': plan['mode'],
             'source_completeness': plan['source_completeness'],
             'reading_gaps': reading_gaps,
             'projection_report': projection_report,
             'attachment_inventory_pending': attachment_inventory_pending,
             'coverage': plan['coverage'], 'image_report': image_report,
             'parts': [{k: v for k, v in part.items() if k != 'bundle'} for part in plan['parts']]}
    with (plan_dir / 'offline-plan.json').open('x', encoding='utf8') as stream:
        json.dump(saved, stream, ensure_ascii=False, sort_keys=True)
    return {'state': 'not_sent', 'plan': plan, 'plan_path': str(plan_dir/'offline-plan.json'),
            'material_sha256': material_sha, 'image_report': image_report,
            'capacity_is_estimate': True}
