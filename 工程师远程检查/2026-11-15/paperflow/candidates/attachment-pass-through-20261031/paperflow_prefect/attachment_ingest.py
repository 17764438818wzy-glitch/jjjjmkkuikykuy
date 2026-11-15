"""Read the frozen original + integrated MD handoff without model or network work."""
import copy
import html
import json
from pathlib import Path, PureWindowsPath
import re
from urllib.parse import unquote

from .records import identity, sha_file, read_json

FIELDS = {'schema', 'paper_id', 'attachment_id', 'source_name', 'source_format',
          'source_bytes', 'source_sha256'}
HEADINGS = ['## 1. 附件类型', '## 2. 简单说明', '## 3. 读取依据与限制', '## 4. 包内目录']
HEX = re.compile(r'[0-9a-f]{64}')


def _require(condition, message):
    if not condition: raise ValueError(message)


def read_frozen_handoff(path, expected_sha256):
    """Validate the one-item joint trial without reinterpreting model output."""
    from .stage3_handoff import read_receipt
    path = Path(path).resolve()
    _require(bool(HEX.fullmatch(expected_sha256)) and sha_file(path) == expected_sha256, '冻结清单哈希不符')
    manifest = read_json(path)
    _require(manifest.get('schema') == 'single-attachment-live-handoff/1', '不是单附件冻结清单')
    refs = []
    def verify_refs(value):
        if isinstance(value, dict):
            if 'path' in value and 'sha256' in value:
                p = Path(value['path'])
                _require(p.is_file() and sha_file(p) == value['sha256'], '冻结证据文件已改变：' + str(p))
                refs.append({'path': str(p.resolve()), 'sha256': value['sha256']})
            for child in value.values(): verify_refs(child)
        elif isinstance(value, list):
            for child in value: verify_refs(child)
    verify_refs(manifest)
    delivery = manifest['delivery']; card = read_attachment_delivery(delivery)
    constraints = manifest.get('constraints', {})
    required = {'original_items':1, 'original_stored_once':True, 'no_body_publication':True,
                'no_nas_operations':True, 'parent_link_status':'paper_identity_only',
                'body_remote_document_id':None, 'unknown_requests_resend':False}
    _require(all(k in constraints and constraints[k] == v for k,v in required.items()), '本入口仅接受冻结单附件联验范围')
    receipt, paper = read_receipt(manifest['intake_receipt']['path'])
    _require(paper is not None and paper.get('paper_id') == delivery['paper_id'], '附件与接收回执论文编号不符')
    rows = [r for r in paper['attachments'] if r['file_id'] == delivery['attachment_id']]
    _require(len(rows) == 1 and rows[0]['sha256'] == delivery['source_sha256']
             and rows[0]['bytes'] == card['metadata']['source_bytes']
             and Path(rows[0]['path']).resolve() == Path(delivery.get('original_source_path', delivery['source_path'])).resolve(), '附件与已接收原件不符')
    body = manifest['body_context']; context = manifest['attachment_context']
    body_sha = body['body_source_sha256']
    _require((paper.get('body') or {}).get('sha256') == body_sha, '正文来源与论文接收记录不符')
    for key in ('original_body','copied_body'):
        _require(body[key]['sha256'] == body_sha, '正文原件与副本不一致')
    for key in ('original_attachment','copied_attachment'):
        _require(context[key]['sha256'] == delivery['source_sha256'], '附件原件与副本不一致')
    _require(Path(context['copied_attachment']['path']).resolve() == Path(delivery['source_path']).resolve(), '交付附件不是已核验副本')
    _require(body.get('routing_json_pointer') == '/profile/routing_candidates', '未知画像路由位置')
    candidate = read_json(body['body_candidate']['path'])
    routing = candidate.get('profile', {}).get('routing_candidates')
    _require(isinstance(routing, list) and routing and routing == body.get('routing_candidates'), '冻结专业顺序与原画像不符')
    codes = [r.get('code') for r in routing]
    _require(all(isinstance(c,str) and re.fullmatch(r'[0-9]{6}[A-Z]?',c) for c in codes)
             and len(codes) == len(set(codes)), '专业路由无效或重复')
    history = read_json(manifest['history_audit']['path'])
    associated = read_json(context['associated_record']['path'])
    _require(history.get('schema') == 'single-attachment-history-audit/1'
             and history.get('source_sha256') == associated.get('sha256') == delivery['source_sha256']
             and history.get('body_source_sha256') == body_sha
             and history.get('body_candidate') == body['body_candidate']
             and history.get('associated_record') == context['associated_record']
             and history.get('routing_candidates') == routing, '历史核验不属于当前原件或画像')
    _require(all(v is False for v in (history.get('remote_original_uploaded'),
             associated.get('remote_original_uploaded'), context.get('remote_original_uploaded')))
             and history.get('independent_associated_request_records') == [], '历史上传状态未知或已有请求，应先回查已有交付')
    run = read_json(manifest['stage3_run']['path'])
    _require(run.get('paper_id') == delivery['paper_id'] and run.get('receipt_id') == receipt['receipt_id']
             and run.get('deliveries') == [delivery]
             and run.get('body',{}).get('status') == 'not_selected'
             and run.get('attachments',{}).get(delivery['attachment_id'],{}).get('status') == 'complete', '第三状态未交付这个精确附件版本')
    _require(sha_file(path) == expected_sha256, '冻结清单读取期间发生变化')
    return {'card': card, 'manifest': manifest, 'handoff_path': str(path),
            'handoff_sha256': expected_sha256, 'verified_references': refs,
            'routing_candidates': routing, 'routing_codes': codes,
            'profile_sha256': body['body_candidate']['sha256'], 'body_source_sha256':body_sha}


def _decode(value):
    return unquote(html.unescape(value), errors='strict')


def _number(value, *, nullable=False):
    if nullable and value == 'null': return None
    _require(re.fullmatch(r'0|[1-9][0-9]*', value) is not None, '目录数量或大小不是非负整数')
    return int(value)


def _field(lines, label):
    prefix = '- ' + label + '：'
    values = [line[len(prefix):] for line in lines if line.startswith(prefix)]
    _require(len(values) == 1 and bool(values[0]), 'MD 字段缺失或重复：' + label)
    return values[0]


def _listing(lines, source_format):
    status = _field(lines, '目录状态')
    _require(status in {'complete', 'partial', 'unavailable', 'not_archive', 'not_read'}, '未知目录状态')
    total = _number(_field(lines, '总条目数'), nullable=True)
    listed = _number(_field(lines, '已列条目数'))
    reason = _decode(_field(lines, '状态说明'))
    table = [line for line in lines if line.startswith('|')]
    _require(table[:2] == ['| 相对路径 | 条目类型 | 大小（字节） |', '| --- | --- | --- |'], '目录表头不符')
    entries = []; paths = set()
    for line in table[2:]:
        cells = [cell.strip() for cell in line.split('|')]
        _require(len(cells) == 5 and cells[0] == cells[-1] == '', '目录行或竖线编码不合法')
        path, kind, size = _decode(cells[1]), cells[2], _number(cells[3], nullable=True)
        _require(bool(path) and not PureWindowsPath(path).drive and not path.startswith(('/', '\\'))
                 and '..' not in path.replace('\\', '/').split('/') and '\x00' not in path,
                 '目录成员必须是安全的相对路径')
        _require(path not in paths, '目录成员路径重复')
        _require(kind in {'file', 'directory'} and (kind != 'directory' or size is None), '目录成员类型或大小不符')
        paths.add(path); entries.append({'path': path, 'type': kind, 'bytes': size})
    _require(listed == len(entries), '已列条目数与表格不符')
    _require(total is None or total >= listed, '总条目数小于已列数')
    if status == 'complete': _require(total == listed, '完整目录没有可靠总数或数量不符')
    if status == 'partial': _require(listed > 0, '部分目录必须保留已列条目')
    if status in {'unavailable', 'not_read', 'not_archive'}:
        _require(listed == 0, '无可交付目录的状态不能附带条目')
    if status == 'not_archive':
        _require(total is None and source_format not in {'.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz'},
                 '非压缩包目录状态不符')
    return {'status': status, 'total_entries': total, 'listed_entries': listed,
            'reason': reason, 'entries': entries}


def read_attachment_delivery(item):
    """Validate a stage3_handoff.attachment_deliveries item; retain exact MD text."""
    _require(isinstance(item, dict), '附件交付必须是单项对象')
    required = {'paper_id', 'attachment_id', 'source_path', 'source_sha256',
                'md_path', 'md_sha256', 'delivery_version'}
    _require(required.issubset(item), '附件交付字段不完整')
    _require(all(isinstance(item[key], str) and item[key] for key in required), '附件交付字段类型不符')
    for key in ('source_sha256', 'md_sha256', 'delivery_version'):
        _require(HEX.fullmatch(item[key]) is not None, '附件交付哈希或版本不合法')
    for key in ('paper_id', 'attachment_id'):
        _require(re.fullmatch(r'[A-Za-z0-9_-]{1,128}', item[key]) is not None, '附件归属编号不合法')
    named_fields = {'original_source_path', 'named_proof_path', 'named_proof_sha256'}
    has_named = bool(named_fields & set(item))
    _require(not has_named or named_fields.issubset(item), '命名副本缺少原件或核验映射')
    if has_named:
        from .named_delivery import _safe_directory, _valid_name
        _safe_directory(Path(item['source_path']).absolute())
        _safe_directory(Path(item['named_proof_path']).absolute())
    source, md = Path(item['source_path']).resolve(strict=True), Path(item['md_path']).resolve(strict=True)
    original = Path(item.get('original_source_path', item['source_path'])).resolve(strict=True)
    _require(source.is_file() and md.is_file() and source != md, '原件和说明必须是两个文件')
    _require(sha_file(source) == item['source_sha256'], '原件哈希与冻结交付不符')
    _require(original.is_file() and sha_file(original) == item['source_sha256'], '来源原件哈希与交付不符')
    _require(sha_file(md) == item['md_sha256'], 'MD 哈希与冻结交付不符')
    raw = md.read_bytes()
    import hashlib
    _require(hashlib.sha256(raw).hexdigest() == item['md_sha256'], 'MD 在读取期间改变')
    text = raw.decode('utf8'); lines = text.splitlines()
    _require(bool(lines) and lines[0] == '---' and '---' in lines[1:], 'MD 元数据区缺失')
    end = lines.index('---', 1); meta = {}
    for line in lines[1:end]:
        key, sep, value = line.partition(':')
        _require(bool(sep) and key in FIELDS and key not in meta, 'MD 元数据字段未知或重复')
        meta[key] = json.loads(value.strip())
    _require(set(meta) == FIELDS and meta['schema'] == 'attachment-card/1', 'MD 模板版本或身份字段不符')
    _require(type(meta['source_bytes']) is int and meta['source_bytes'] >= 0, 'MD 文件大小不合法')
    _require(all(isinstance(meta[key], str) for key in FIELDS - {'source_bytes'}), 'MD 字符串字段类型不符')
    for key in ('paper_id', 'attachment_id', 'source_sha256'):
        _require(meta[key] == item[key], 'MD 与原件或论文/附件归属不符')
    _require(meta['source_name'] == original.name and meta['source_format'] == original.suffix.lower()
             and meta['source_bytes'] == original.stat().st_size, 'MD 文件事实与原件不符')
    if has_named:
        proof_path = Path(item['named_proof_path'])
        _require(HEX.fullmatch(item['named_proof_sha256']) is not None
                 and sha_file(proof_path) == item['named_proof_sha256'], '命名映射哈希不符')
        proof = read_json(proof_path)
        _require(proof.get('schema') == 'named-delivery-input/1' and proof.get('kind') == 'attachment'
                 and proof.get('paper_id') == item['paper_id'] and proof.get('file_id') == item['attachment_id']
                 and proof.get('source_sha256') == item['source_sha256'] and proof.get('bytes') == meta['source_bytes']
                 and Path(proof.get('source_path', '')).resolve() == original
                 and Path(proof.get('delivery_path', '')).resolve() == source
                 and proof.get('display_name') == source.name and _valid_name(source.name, original.name)
                 and source.stat().st_size == meta['source_bytes'], '命名副本与原件或论文归属不符')
    body = lines[end+1:]
    _require([line for line in body if line.startswith('## ')] == HEADINGS, 'MD 固定章节缺失、重复或顺序不符')
    positions = [body.index(heading) for heading in HEADINGS] + [len(body)]
    sections = [body[positions[i]+1:positions[i+1]] for i in range(4)]
    kind, summary = [_decode('\n'.join(section).strip()) for section in sections[:2]]
    _require(bool(kind) and bool(summary), '附件类型或简单说明缺失')
    read_scope = _decode(_field(sections[2], '实际读取范围'))
    evidence = _decode(_field(sections[2], '判断依据'))
    limitations = _decode(_field(sections[2], '未读取或未确认内容'))
    listing = _listing(sections[3], meta['source_format'])
    frozen = copy.deepcopy(item)
    frozen.update(source_path=str(source), md_path=str(md))
    return {'delivery': frozen, 'metadata': meta, 'md_text': text, 'kind': kind,
            'summary': summary, 'read_scope': read_scope, 'evidence': evidence,
            'limitations': limitations, 'listing': listing,
            'raw_identity': identity([meta['paper_id'], meta['attachment_id'], meta['source_sha256']])}
