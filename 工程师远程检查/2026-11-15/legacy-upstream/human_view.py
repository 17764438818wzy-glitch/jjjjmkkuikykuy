"""Derived Chinese browsing folders; machine packages remain the source of truth."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

from content_rules import valid_title
from engine import digest, safe_component, write_json

BUCKETS = ('通过', '有争议', '未通过')
# Covers links produced by evidence.export_package, including angle-delimited paths.
LINK = re.compile(r'(!?\[[^\]\n]*\]\()(?P<target><[^>\n]+>|[^\s()]+(?:\([^()]*\)[^\s()]*)*)(?P<tail>\s+"[^"\n]*")?\)')


def _under(path, root):
    path, root = Path(path).resolve(), Path(root).resolve()
    if not path.is_relative_to(root) or path == root:
        raise ValueError('文件路径超出对应的程序目录')
    return path


def classify(item, package=None):
    """A browse label is not an academic grade, or a new upload authorization."""
    if item.get('status') == 'reject':
        return '未通过', item.get('reason') or '基础筛选未通过'
    if not re.search(r'[\u4e00-\u9fff]', item.get('major', '')) or '未识别' in item.get('major', ''):
        return '有争议', '专业尚未确认'
    if item.get('status') != 'pass':
        return '有争议', item.get('reason') or '基础筛选有待复核项'
    if item.get('title_status') == 'review' or not valid_title(item.get('title', '')):
        return '有争议', '论文题名尚未确认'
    if package is None:
        return '有争议', '旧版结果未保存原生图表核对记录；需从原文件重新处理'
    try:
        meta = json.loads((package / 'metadata.json').read_text(encoding='utf-8'))
        if meta.get('completeness_verified') is not True:
            return '有争议', '正文及图表完整性尚未对照原文核对'
        if any(i.get('status') != 'resolved' for i in meta.get('issues', [])):
            return '有争议', '仍有未解决的解析或核对问题'
        if any(r.get('privacy_status') != 'approved' for r in meta.get('resources', [])):
            return '有争议', '图表资源的脱敏尚未全部核对'
        from evidence import package_fingerprint
        ingest = _under(item['path'], package)
        if (not item.get('reviewed_hash') or item['reviewed_hash'] != digest(ingest)
                or item.get('reviewed_package_hash') != package_fingerprint(package)):
            return '有争议', '当前文档版本未确认，或确认后内容发生变化'
    except (OSError, ValueError, KeyError, TypeError):
        return '有争议', '程序资料缺失或检查记录不可用'
    return '通过', '已完成基础筛选及所需核对；此状态不代表学术质量评级'


def _relative_link(target, parent):
    return quote(os.path.relpath(target, parent).replace('\\', '/'), safe='/.-_~')


def _rebase(text, source, target, allowed_root, issues):
    def replace(match):
        # Plain statistical matrix products can look like Markdown links.
        # Recognize only a transposed vector * inverse matrix * vector product;
        # ordinary links (including extensionless files) remain validated.
        prefix = text[max(0, match.start()-120):match.start()]
        suffix = text[match.end():match.end()+1]
        # Native prose uses square-bracket references followed by parenthesized
        # nationalities, publication dates or explanations. It is not a file
        # reference. Explicit paths/images and existing resources take priority.
        literal = match[0]
        if (not literal.startswith('!') and not match['tail']
                and not (source.parent / match['target']).is_file()
                and (re.fullmatch(r'\[[1-9]\d{0,2}\]\((?:苏|英|美|法|德|日|俄|意|加|澳|中)\)', literal)
                     or re.fullmatch(r'\[(?:M|J|D|EB/OL)\]\((?:18|19|20)\d{2}(?:[.\-]\d{1,2}){1,2}\)', literal)
                     or re.fullmatch(r'\[增订[一二三四五六七八九十\d]+版\]\([上下中]\)', literal)
                     or (prefix and suffix and re.fullmatch(r'\[\^footnoteReference\d+\]\((?:18|19|20)\d{2}\)', literal))
                     or (prefix and suffix and re.fullmatch(r'\[[1-9]\d{0,2}\]\(如“[^”\n]{1,60}”\)', literal)))):
            return literal.replace('[', r'\[').replace(']', r'\]')
        if (not match[0].startswith('!')
                and re.search(r'[A-Za-z\u4e00-\u9fff]$', prefix)
                and re.match(r'[A-Za-z\u4e00-\u9fff，,。.；;：:、]', suffix)
                and re.fullmatch(r'\[[1-9]\d{0,2}\]\((?:18|19|20)\d{2}\)', match[0])
                and not (source.parent / match['target']).is_file()):
            return match[0].replace('[', r'\[').replace(']', r'\]')
        if (not match[0].startswith('!')
                and re.search(r"\([A-Za-z_][\w]*\s*[-+]\s*[A-Za-z_][\w]*\)\s*['′]\s*$", prefix)
                and re.fullmatch(r"\[\([A-Za-z_][\w]*\s*[-+]\s*[A-Za-z_][\w]*\)\s*\^\s*\(-?\d+\)\]\([A-Za-z_][\w]*\s*[-+]\s*[A-Za-z_][\w]*\)", match[0])):
            return match[0].replace('[', r'\[').replace(']', r'\]')
        raw = match['target'].strip('<>')
        uri = urlsplit(raw)
        if raw.startswith('#') or uri.scheme or uri.netloc:
            return match[0]
        try:
            destination = _under(source.parent / unquote(uri.path), allowed_root)
            if not destination.is_file():
                raise ValueError('本地资源缺失')
        except (ValueError, OSError) as exc:
            issues.append('图片或证据链接不可用：' + str(exc))
            # Preserve the original label but do not make an unsafe path clickable.
            return match[1].lstrip('!')[:-1] + '〔本地证据缺失或路径无效，待检查〕'
        suffix = ('#' + uri.fragment) if uri.fragment else ''
        return match[1] + '<' + _relative_link(destination, target.parent) + suffix + '>' + (match['tail'] or '') + ')'
    return LINK.sub(replace, text)


def build_view(store, manifest, progress=lambda message: None):
    """Refresh only derived files. Metadata/index stay in runs, outside the view."""
    with store.lock:
        run = _under(store.manifest_path(manifest['run_id']).parent, store.runs)
        view = _under(store.root / '人工浏览' / ('批次_' + safe_component(manifest['run_id'].removeprefix('UP_'))), store.root)
        index_path = _under(run / 'human_view.json', run)
        index = json.loads(index_path.read_text(encoding='utf-8')) if index_path.exists() else {'files': []}
        previous = {}
        for entry in index['files']:
            path = Path(entry['path'])
            if path.is_absolute() or '..' in path.parts:
                raise ValueError('浏览索引包含无效路径')
            resolved = _under(view / path, view)
            previous[resolved] = entry
        for bucket in BUCKETS:
            _under(view / bucket, view).mkdir(parents=True, exist_ok=True)

        counts = dict.fromkeys(BUCKETS, 0)
        result = {'path': str(view), 'counts': counts, 'issues': [], 'preserved': [], 'status': 'ready'}
        outputs = []
        used = set()
        items = list(manifest.get('items', []))
        failures = [e for e in manifest.get('events', []) if e.get('status') == 'failed'
                    or (e.get('status') == 'archive' and e.get('reason'))]
        for number, event in enumerate(failures, 1):
            # Show a report stub without copying the raw source or its identifying name.
            items.append({'id': f'ERROR_{number:04d}', 'title': f'解析失败记录 {number:03d}',
                          'major': event.get('major', '未识别专业'), 'status': 'review',
                          'reason': '解析失败，尚未作出筛选结论；详情见工作台处理报告',
                          '_failure_number': number})
        result['failed_records'] = len(failures)
        # Sorting guarantees repeatability when result rows are re-ordered.
        for number, item in enumerate(sorted(items, key=lambda i: str(i.get('id', '')))):
            key = str(item.get('id') or f'item-{number}')
            local_issues = []
            package = None
            try:
                if item.get('_failure_number'):
                    raise ValueError('未生成正文，请查看工作台中的失败记录')
                if item.get('package_path'):
                    package = _under(item['package_path'], run)
                source = _under(item.get('display_path') or item['path'], package or run)
                body = source.read_text(encoding='utf-8')
            except (OSError, ValueError, KeyError) as exc:
                source = run / 'missing-document.md'
                body = '正文文件缺失或路径无效。请在工作台查看处理报告并从原文件重新处理。\n'
                local_issues.append('正文文件缺失或路径无效：' + str(exc))
            bucket, reason = classify(item, package)
            major = item.get('major', '')
            major = safe_component(major)[:32] if re.search(r'[\u4e00-\u9fff]', major) and '未识别' not in major else '未识别专业'
            title = item.get('title', '')
            title = safe_component(title)[:70] if valid_title(title) else '待确认题名'
            short_id = re.sub(r'[^a-zA-Z0-9]', '', key.removeprefix('UP_'))[-10:] or hashlib.sha256(key.encode()).hexdigest()[:10]
            name = f'{title}.md' if title != '待确认题名' else f'{title}（{short_id}）.md'
            target = _under(view / bucket / major / name, view)
            if str(target).casefold() in used:
                name = f'{title}（{short_id}）.md'
                target = _under(view / bucket / major / name, view)
            if str(target).casefold() in used:
                name = f'{title}（{hashlib.sha256((key + str(number)).encode()).hexdigest()[:14]}）.md'
                target = _under(view / bucket / major / name, view)
            body = _rebase(body, source, target, package or run, local_issues)
            if local_issues and bucket == '通过':
                bucket, reason = '有争议', '正文或图表证据文件缺失，需重新检查'
                target = _under(view / bucket / major / name, view)
                if str(target).casefold() in used:
                    name = f'{title}（{short_id}）.md'
                    target = _under(view / bucket / major / name, view)
            used.add(str(target).casefold())
            counts[bucket] += 1
            description = str(reason).replace('\n', ' ').replace('\r', ' ')
            details = '\n'.join('> 待检查：' + s.replace('\n', ' ') for s in local_issues)
            note = (f'> 程序说明（非论文正文）：{bucket} · {major} · 编号 {key}\n'
                    f'> 原因：{description}\n'
                    '> 此文件供本机阅读和核对；图片链接依赖程序资料。RAGFlow 从工作台导入对应的入库文本。\n')
            if details:
                note += details + '\n'
            text = note + '\n---\n\n' + body
            # Append a link to structure records without adding JSON files to the folder.
            if package and (package / 'metadata.json').is_file():
                text += '\n\n---\n\n程序资料：[图表结构及问题记录](<' + _relative_link(package / 'metadata.json', target.parent) + '>)\n'
            outputs.append((key, target, text.encode('utf-8')))
            result['issues'].extend({'id': key, 'reason': message} for message in local_issues)

        def preserve(path):
            # Do not erase manual edits when a generated file moves or is refreshed.
            backup = _under(run / 'human_view_edits' / (datetime.now().strftime('%Y%m%d_%H%M%S_') + uuid.uuid4().hex[:8]) / path.name, run)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup)
            if digest(path) != digest(backup):
                raise OSError('浏览文件修改副本未能完整保存')
            result['preserved'].append(str(backup))

        new_index = []
        for key, target, data in outputs:
            target.parent.mkdir(parents=True, exist_ok=True)
            target = _under(target, view)
            expected = hashlib.sha256(data).hexdigest()
            if target.exists() and digest(target) != expected:
                if target not in previous or digest(target) != previous[target]['sha256']:
                    preserve(target)
            if not target.exists() or digest(target) != expected:
                temporary = _under(target.with_name('.' + uuid.uuid4().hex + '.tmp'), view)
                try:
                    temporary.write_bytes(data)
                    temporary.replace(target)
                finally:
                    if temporary.exists():
                        temporary.unlink()
            new_index.append({'id': key, 'path': target.relative_to(view).as_posix(), 'sha256': expected})
            progress(f'整理浏览目录 {len(new_index)}/{len(outputs)}')
        current = {p for _, p, _ in outputs}
        for path, entry in previous.items():
            if path not in current and path.exists():
                path = _under(path, view)
                if digest(path) != entry['sha256']:
                    preserve(path)
                path.unlink()  # Only files recorded by this generator, never the directory tree.
                if path.parent.parent.name in BUCKETS:
                    try:
                        path.parent.rmdir()  # Empty obsolete major folder only.
                    except OSError:
                        pass
        write_json(index_path, {'version': 1, 'path': str(view), 'files': new_index,
                                'counts': counts, 'issues': result['issues'],
                                'updated_at': datetime.now().isoformat(timespec='seconds')})
        return result
