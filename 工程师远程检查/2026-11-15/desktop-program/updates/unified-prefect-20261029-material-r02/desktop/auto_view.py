"""Binary automatic outcomes with safe, regenerable Chinese browsing copies."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

from content_rules import valid_title
from engine import digest, safe_component, write_json
from human_view import LINK, _rebase, _under

BUCKETS = ('通过', '不通过', '关联资料', '待解析', '已删除')
REASONS = {'screening': '基础筛除', 'evidence': '证据不完整', 'privacy': '脱敏失败',
           'classification': '分类失败', 'technical': '技术失败', 'stopped': '已停止',
           'duplicate': '重复跳过'}


def outcome(item, run_status=None):
    """Describe old records faithfully; never grant a new approval from history."""
    status = item.get('status')
    if status=='deleted' and item.get('reason_kind')=='material_deleted':
        return '已删除',item.get('reason','过程表单已在 AI 前删除')
    if status=='pending' and item.get('reason_kind')=='material_pending':
        return '待解析',item.get('reason') or '原件已保留，待取得可读内容；未判定为无学习价值'
    if status=='associated' and item.get('content_format')=='associated_material':
        return '关联资料',item.get('reason') or '关联原件已私有保存'
    if run_status == 'running' and status in {'pending', 'running'}:
        return '处理中', item.get('reason') or '正在执行自动预处理'
    if status == 'pass' and item.get('automatic_approval') is True:
        if not valid_title(item.get('title', '')):
            return '不通过', '题名未确认，未满足全自动处理要求'
        if not re.search(r'[\u4e00-\u9fff]', item.get('major', '')) or '未识别' in item.get('major', ''):
            return '不通过', '专业未识别，未满足全自动处理要求'
        return '通过', item.get('reason') or '全自动预处理通过，不代表学术质量评级'
    if status == 'reject':
        label = REASONS.get(item.get('reason_kind'), '不通过')
        return '不通过', label + '：' + (item.get('reason') or '未满足自动预处理要求')
    if status == 'pass':
        reason = '旧版结果未完成全自动处理，需从源文件重新处理'
    elif status == 'review':
        reason = '旧版待复核记录未完成全自动处理，需从源文件重新处理'
    else:
        reason = '旧版或中断记录未完成全自动处理，未形成自动通过结论'
    if item.get('reason'):
        reason += '；历史记录：' + item['reason']
    return '不通过', reason


def batch_summary(manifest):
    """Derive history counts using the same final outcome and browse checks."""
    if manifest.get('pipeline') not in {None,'automatic'}:return manifest.get('message','')
    if manifest.get('status')=='running':return manifest.get('message','处理中')
    issues={i.get('id') for i in manifest.get('human_view',{}).get('issues',[]) if isinstance(i,dict)}
    passed=failed=broken=0
    retained={'关联资料':0,'待解析':0,'已删除':0}
    for item in result_items(manifest):
        if item.get('reason_kind')=='duplicate':continue
        label,_=outcome(item,manifest.get('status'))
        if label in retained:
            retained[label]+=1
            continue
        if label=='通过' and item.get('id') in issues:
            failed+=1;broken+=1
        elif label=='通过':passed+=1
        else:failed+=1
    summary=f'通过 {passed}，不通过 {failed}'
    for label,count in retained.items():
        if count:summary+=f'，{label} {count}'
    for status,label in [('duplicate','重复跳过'),('uploaded_skip','已上传跳过')]:
        count=sum(e.get('status')==status for e in manifest.get('events',[]))
        if count:summary+=f'，{label} {count}'
    if broken:summary+=f'；浏览异常 {broken}（已计入不通过）'
    if manifest.get('human_view',{}).get('status')=='failed':summary+='；浏览目录生成失败，请查看处理报告'
    return summary


def result_items(manifest):
    """Include event-only historical failures without duplicating current items."""
    items = list(manifest.get('items', []))
    known = {str(i.get('source_path', '')).casefold() for i in items}
    used_ids = {str(i.get('id', '')) for i in items}
    for number, event in enumerate(manifest.get('events', []), 1):
        source = str(event.get('source', ''))
        if event.get('status') not in {'failed', 'duplicate'} and not (event.get('status') == 'archive' and event.get('reason')):
            continue
        if source and source.casefold() in known:
            continue
        key = f'ERROR_{number:04d}'
        while key in used_ids:
            key += '_event'
        used_ids.add(key)
        duplicate = event.get('status') == 'duplicate'
        items.append({'id': key, 'title': f'{"重复跳过" if duplicate else "处理失败记录"} {number:03d}',
                      'major': event.get('major') or '未识别专业', 'status': 'reject',
                      'automatic_approval': False, 'reason_kind': 'duplicate' if duplicate else 'technical', 'uploads': {},
                      'reason': event.get('reason') if duplicate else '旧版解析失败，未生成可入库正文；详情见工作台处理报告',
                      'source_path': source, 'input_source_path': event.get('input_source_path', ''),
                      '_failure_record': not duplicate})
        known.add(source.casefold())
    return items


def _published(path, root):
    path = _under(path, root)
    if any(part.casefold() in {'private', '_review'} for part in path.relative_to(Path(root).resolve()).parts):
        raise ValueError('私有原件或未脱敏证据路径不可进入浏览目录')
    return path


def _safe_links(body, source, package, issues):
    """Reject file URLs, drive paths and private evidence before rebasing links."""
    def replace(match):
        raw = match['target'].strip('<>')
        try:
            uri = urlsplit(raw)
            if raw.startswith('#') or uri.scheme in {'http', 'https', 'mailto'}:
                return match[0]
            if uri.scheme or uri.netloc or Path(unquote(uri.path)).is_absolute():
                raise ValueError('本地证据路径无效')
            _published(source.parent / unquote(uri.path), package)
            return match[0]
        except (OSError, ValueError):
            issues.append('图片或证据路径无效，或指向私有未脱敏资料')
            return '〔证据路径无效〕'
    return LINK.sub(replace, body)


def build_view(store, manifest, progress=lambda message: None):
    """Only generated files in the indexed view may be replaced or removed."""
    from filelock import FileLock
    from preextract_delivery_queue import queue_path,project_manifest
    path=queue_path(store.root)
    if path.is_file():
        with FileLock(str(path.with_name('cleanup.lock'))):
            project_manifest(store.root,manifest)
            return _build_view(store,manifest,progress)
    return _build_view(store,manifest,progress)


def _build_view(store, manifest, progress):
    with store.lock:
        runs = _under(store.runs, store.root)
        run = _under(store.manifest_path(manifest['run_id']).parent, runs)
        # A distinct root leaves earlier three-way views and unindexed copies intact.
        view = _under(store.root / '处理结果' / (safe_component(manifest['run_id']) + '_全自动'), store.root)
        index_path = _under(run / 'auto_view.json', run)
        index = json.loads(index_path.read_text(encoding='utf-8')) if index_path.exists() else {'files': []}
        previous = {}
        for entry in index.get('files', []):
            path = Path(entry['path'])
            if path.is_absolute() or '..' in path.parts or path.suffix.lower() != '.md':
                raise ValueError('浏览索引包含无效路径')
            resolved = _under(view / path, view)
            if not path.parts or path.parts[0] not in BUCKETS:
                raise ValueError('浏览索引包含无效路径')
            previous[resolved] = entry
        extra_buckets={outcome(i,manifest.get('status'))[0] for i in result_items(manifest)}
        active_buckets=tuple(b for b in BUCKETS if b in {'通过','不通过'} or b in extra_buckets)
        for bucket in active_buckets:
            _under(view / bucket, view).mkdir(parents=True, exist_ok=True)

        counts = dict.fromkeys(active_buckets, 0)
        # Duplicate events are shown in the workbench result table, but do not
        # create a misleading "不通过" browsing document: no processing was
        # attempted and the original result remains in its other batch.
        rows = [row for row in result_items(manifest) if row.get('reason_kind') != 'duplicate']
        result = {'path': str(view), 'counts': counts, 'issues': [], 'preserved': [], 'status': 'ready',
                  'failed_records': sum(bool(i.get('_failure_record')) for i in rows), 'pending_count': 0,
                  'partial_count': 0, 'unrecovered_objects': []}
        outputs, used = [], set()
        from preextract_retirement import jobs,terminal
        retired={j['id']:j for j in jobs(store.root,compact=True,terminal_only=True) if terminal(j)}
        for number, item in enumerate(sorted(rows, key=lambda row: str(row.get('id', '')))):
            key = str(item.get('id') or f'item-{number}')
            saved=retired.get((item.get('preextract_retirement') or {}).get('delivery_id'))
            if (saved and item.get('source_hash')==saved['payload']['item'].get('source_hash')
                    and item.get('preextract_delivery',{}).get('manifest_sha256')==saved['payload']['manifest_sha256']):
                counts['通过']+=1
                result['retired_count']=result.get('retired_count',0)+1
                continue
            bucket, reason = outcome(item, manifest.get('status'))
            if bucket == '处理中':
                result['pending_count'] += 1
                continue
            local_issues = []
            package = source = None
            unrecovered = list(item.get('unrecovered_objects') or [])
            body = '未生成通过的入库正文。原文件与私有处理资料保留在程序资料中。\n'
            if bucket=='已删除':body='已确认的过程表单原件已删除；仅保存删除原因和哈希审计，未进入 AI 或知识库。\n'
            if bucket == '通过':
                try:
                    package = _published(item['package_path'], run)
                    manifest_path = package / 'manifest.json'
                    if manifest_path.is_file():
                        package_manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
                        unrecovered = list(package_manifest.get('unrecovered_objects') or unrecovered)
                    source = _published(item.get('display_path') or item['path'], package)
                    body = source.read_text(encoding='utf-8')
                    body = _safe_links(body, source, package, local_issues)
                except (OSError, UnicodeError, ValueError, KeyError, TypeError):
                    local_issues.append('正文文件缺失、不可读取或路径无效')
            major = item.get('major', '')
            major = safe_component(major)[:32] if re.search(r'[\u4e00-\u9fff]', major) else '未识别专业'
            title = item.get('title', '')
            title = safe_component(title)[:65] if valid_title(title) else '未确认题名'
            suffix = re.sub(r'[^a-zA-Z0-9]', '', key)[-10:] or hashlib.sha256(key.encode()).hexdigest()[:10]
            target = _under(view / bucket / major / (title + '.md'), view)
            if source is not None and not local_issues:
                body = _rebase(body, source, target, package, local_issues)
            if local_issues:
                bucket = '不通过'
                reason = '技术失败：正文或证据文件缺失或路径无效；未生成可用浏览正文'
                body = '请查看工作台处理报告，并从原文件重新处理。私有处理资料仍保留在程序资料中。\n'
                target = _under(view / bucket / major / (title + '.md'), view)
            if str(target).casefold() in used or title == '未确认题名':
                target = _under(target.with_name(f'{title}（{suffix}）.md'), view)
            if str(target).casefold() in used:
                suffix = hashlib.sha256((key + str(number)).encode()).hexdigest()[:14]
                target = _under(target.with_name(f'{title}（{suffix}）.md'), view)
            used.add(str(target).casefold())
            counts[bucket] += 1
            reason = str(reason).replace('\n', ' ').replace('\r', ' ')
            note = (f'> 处理结果：{bucket} · {major} · 编号 {key}\n'
                    f'> 原因：{reason}\n'
                    '> 通过仅表示预处理合格，不代表学术质量评级。RAGFlow 由工作台交接入库文本。\n')
            if bucket == '通过':
                if unrecovered:
                    result['partial_count'] += 1
                    result['unrecovered_objects'].append({'id': key, 'objects': unrecovered})
                    note += f'> 证据完整性：带缺陷通过；未恢复对象 {len(unrecovered)} 个。\n'
                    for gap in unrecovered:
                        note += f'> 未恢复对象：{gap.get("id", "unknown")} · {gap.get("reason", "未记录原因")}\n'
                else:
                    note += '> 证据完整性：完整；未恢复对象 0 个。\n'
            note += ''.join('> 文件问题：' + message + '\n' for message in local_issues)
            outputs.append((key, target, (note + '\n---\n\n' + body).encode('utf-8')))
            result['issues'].extend({'id': key, 'reason': message} for message in local_issues)

        def preserve(path):
            backup = _under(run / 'human_view_edits' / (datetime.now().strftime('%Y%m%d_%H%M%S_') + uuid.uuid4().hex[:8]) / path.name, run)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup)
            if digest(path) != digest(backup):
                raise OSError('浏览文件修改副本未能完整保存')
            result['preserved'].append(str(backup))

        new_index = []
        for key, target, data in outputs:
            target = _under(target, view)
            target.parent.mkdir(parents=True, exist_ok=True)
            expected = hashlib.sha256(data).hexdigest()
            if target.exists() and digest(target) != expected:
                if target not in previous or digest(target) != previous[target].get('sha256'):
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
        current = {path for _, path, _ in outputs}
        for path, entry in previous.items():
            if path not in current and path.exists():
                path = _under(path, view)
                if digest(path) != entry.get('sha256'):
                    preserve(path)
                path.unlink()
                if path.parent.parent.name in BUCKETS:
                    try:
                        path.parent.rmdir()
                    except OSError:
                        pass
        write_json(index_path, {'version': 2, 'path': str(view), 'files': new_index,
                                'counts': counts, 'issues': result['issues'],
                                'updated_at': datetime.now().isoformat(timespec='seconds')})
        return result
