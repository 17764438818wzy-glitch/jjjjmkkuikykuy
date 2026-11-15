"""Read-only projection of the retained attachment-card/1 renderer."""
import html
import json
from pathlib import Path, PureWindowsPath
from paperflow_prefect.records import identity, read_json, sha_file


def _inline(value):
    value = str(value).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
    value = html.escape(value, quote=False)
    return ''.join('&#' + str(ord(char)) + ';' if char in '|\\`*_[]#' else char for char in value)


def _directory(evidence):
    original = evidence.get('archive_listing') or {}
    status = original.get('status', 'unavailable')
    reason = original.get('reason') or '历史记录没有留存可交付目录'
    entries = []; omitted = 0
    for row in original.get('entries', []):
        path = row.get('path', '')
        if (not isinstance(path, str) or not path or PureWindowsPath(path).drive
                or path.startswith(('/', '\\')) or '..' in path.replace('\\', '/').split('/')):
            omitted += 1; continue
        if row.get('type') not in {'file', 'directory'}:
            omitted += 1; continue
        entries.append(row)
    if omitted:
        status = 'partial' if entries else 'unavailable'
        reason += f'；{omitted} 项类型或相对路径未核实，保留在原证据中'
    if status == 'not_archive':
        return status, None, [], '非压缩包，不适用'
    if status not in {'complete', 'partial', 'unavailable', 'not_read'}: raise ValueError('目录状态不合法')
    total = original.get('total_entries')
    if status == 'complete' and (type(total) is not int or total != len(entries)):
        status = 'partial' if entries else 'unavailable'; reason = '不能证明目录完整'
    if status == 'complete': reason = '已完整列出；目录完整不表示成员内容已读'
    return status, total, entries, reason


def _prepare_card(row, paper_id, description, evidence):
    """Retained renderer's exact deterministic content, before any filesystem writes."""
    from paperflow_prefect.attachments import validate_description
    validate_description(description, row, paper_id)
    source = Path(row['path'])
    if sha_file(source) != row['sha256']: raise ValueError('附件原件校验不符')
    if evidence.get('schema') == 'attachment-evidence-handoff/1':
        binding = dict(evidence.get('source') or {}, group_id=evidence.get('group_id'))
        if binding != row.get('source_binding') or binding.get('source_sha256') != row['sha256']:
            raise ValueError('附件目录证据不属于本项来源')
    elif evidence.get('source_sha256') != row['sha256']:
        raise ValueError('附件目录证据不属于本原件')
    meta = {'schema': 'attachment-card/1', 'paper_id': paper_id, 'attachment_id': row['file_id'],
        'source_name': row.get('name', source.name), 'source_format': source.suffix.lower(),
        'source_bytes': source.stat().st_size, 'source_sha256': row['sha256']}
    status, total, entries, reason = _directory(evidence)
    scope = evidence.get('read_scope', {})
    scopes = '；'.join(f'{key}={value.get("status", "unknown")}' for key, value in scope.items())
    if not scopes: scopes = '仅能核对已有说明所引用的证据；旧读取范围以原证据记录为准'
    limitations = list(dict.fromkeys(description.get('limitations', []) + evidence.get('limitations', [])))
    seen = evidence.get('identification_input')
    if seen:
        scopes += '；原识别输入图片 ' + str(seen['image_count']) + ' 张，证据 ' + '、'.join(seen['refs'])
        limitations = list(dict.fromkeys(limitations + seen['limitations']))
    lines = ['---'] + [key + ': ' + json.dumps(value, ensure_ascii=False) for key, value in meta.items()]
    lines += ['---', '', '# 附件交付说明', '', '## 1. 附件类型', '', _inline(description['kind']), '',
        '## 2. 简单说明', '', _inline(description['summary']), '', '## 3. 读取依据与限制', '',
        '- 实际读取范围：' + _inline(scopes),
        '- 判断依据：已有说明引用 ' + _inline('、'.join(description['evidence_refs']) or '文件信息'),
        '- 未读取或未确认内容：' + _inline('；'.join(limitations) or '无已知额外限制'),
        '- 说明沿用原识别结果；补充证据和本次目录不表示旧模型已读取。', '', '## 4. 包内目录', '',
        '- 目录状态：' + status, '- 总条目数：' + ('null' if total is None else str(total)),
        '- 已列条目数：' + str(len(entries)), '- 状态说明：' + _inline(reason), '',
        '| 相对路径 | 条目类型 | 大小（字节） |', '| --- | --- | --- |']
    for item in entries:
        size = item.get('bytes') if item['type'] == 'file' else None
        lines.append('| ' + _inline(item['path']) + ' | ' + item['type'] + ' | '
                     + (str(size) if type(size) is int and size >= 0 else 'null') + ' |')
    data = ('\n'.join(lines) + '\n').encode('utf8')
    inputs = {'renderer': 'attachment-card-renderer/1', 'identity': meta,
              'description_sha256': identity(description), 'evidence_sha256': identity(evidence)}
    return source, data, inputs, identity(inputs)


def _result(row, paper_id, evidence, source, inputs, version, path):
    return dict(schema='attachment-delivery/1', version=version, paper_id=paper_id,
        attachment_id=row['file_id'], original=str(source.resolve()), source_sha256=row['sha256'],
        path=str(path.resolve()), md_sha256=sha_file(path), evidence_version=evidence.get('evidence_version'),
        inputs=inputs, model_requests_added=0)


def inspect_attachment_card(row, paper_id, description, evidence, directory):
    """Recompute exact card and delivery from inputs; read existing files only."""
    source, data, inputs, version = _prepare_card(row, paper_id, description, evidence)
    directory = Path(directory)
    target = directory / version
    path = target / 'attachment.md'
    if not target.is_dir() or not path.is_file() or path.read_bytes() != data:
        raise ValueError('附件交付历史内容发生改变')
    result = _result(row, paper_id, evidence, source, inputs, version, path)
    expected = json.dumps(result, ensure_ascii=False, indent=2).encode('utf8')
    if ((target / 'delivery.json').read_bytes() != expected
            or read_json(directory / 'current.json') != result):
        raise ValueError('附件交付记录发生改变')
    return result
