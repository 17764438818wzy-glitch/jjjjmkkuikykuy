"""One first-step task summary, independent of the internal batch size."""
from pathlib import Path
import time
from typing import Literal
import uuid

from .records import identity, read_json, write_json
from .group_summary_store import GroupSummaryStore


def task_id_text(value):
    return str(uuid.UUID(str(value)))


def summary_path(output_dir, task_id):
    return Path(output_dir) / 'group-task-summaries' / (task_id_text(task_id) + '.json')


def display_title(handoff, group_hash):
    """Provide a title and subtitle without inventing an unconfirmed title."""
    handoff = handoff if isinstance(handoff, dict) else {}
    body = handoff.get('body') if isinstance(handoff.get('body'), dict) else {}
    file_hash = body.get('sha256')
    if not isinstance(file_hash, str) or len(file_hash) != 64 or any(
            c not in '0123456789abcdefABCDEF' for c in file_hash):
        file_hash = None
    fallback = file_hash or handoff.get('group_id') or group_hash
    title = handoff.get('paper_title')
    candidate = handoff.get('paper_title_candidate')
    if isinstance(title, str) and title.strip():
        status, primary, secondary = '已提取', title.strip(), ''
    elif isinstance(candidate, str) and candidate.strip():
        status, primary, secondary = '待核对', fallback, candidate.strip()
    else:
        status, primary, secondary = '未提取', fallback, '题目未识别'
    return dict(title_status=status, display_title=primary, display_subtitle=secondary,
                display_hash=fallback, hash_type='file_sha256' if file_hash else 'group_id' if fallback else None)


class GroupTaskSummary:
    def __init__(self, output_dir, library, total, task_id=None):
        self.task_id = task_id_text(task_id or uuid.uuid4())
        self.path = summary_path(output_dir, self.task_id)
        self.library = Path(library).resolve()
        self.seen = set()
        self.last_write = 0.0
        self.data = dict(schema='paperflow-group-task-summary/1', task_id=self.task_id,
                         status='running', total_groups=total, passed_groups=0,
                         failed_groups=0, review_groups=0, not_passed_groups=0,
                         waiting_groups=total, issues=[], groups=[])
        self.flush(force=True)
        self.store = GroupSummaryStore(output_dir)
        self.store.create_task(self.task_id, total)

    def record(self, index, item):
        if index in self.seen:
            return
        root = Path(item['group_root'])
        group_hash = None
        try:
            group = root.resolve().relative_to(self.library).as_posix()
            if root.is_dir():
                group_hash = identity(['source-group/1', str(self.library).casefold(), group.casefold()])[:32]
        except (OSError, ValueError, RuntimeError):
            group = root.name
        issues, status, handoff = [], None, None
        if item.get('error'):
            reason = str(item['error'])[:500]
            status = 'failed'
            issues.append(dict(file=None, reason=reason))
        for key, label in (('delivery_errors', '交付文件复制失败'),
                           ('evidence_errors', '附件证据处理失败')):
            for entry in item.get(key, []):
                status = 'failed'
                detail = entry.get('error_type') or entry.get('error')
                issues.append(dict(file=entry.get('relative') or entry.get('file_id'),
                                   reason=label + ('：' + str(detail) if detail else '')))
        path = item.get('handoff') or item.get('unreceived') or item.get('summary_handoff')
        if path:
            try:
                handoff = read_json(path)
                files = ([handoff['body']] if handoff.get('body') else [])
                files += handoff.get('attachments', []) + handoff.get('trash', []) + handoff.get('unresolved', [])
                filenames = {row.get('file_id'): row.get('relative') for row in files}
                for issue in issues:
                    issue['file'] = filenames.get(issue['file']) or issue['file']
                for row in handoff.get('unresolved', []):
                    reason = row.get('reason') or '未确认'
                    if reason.startswith(('读取失败：', '分类失败：', '正文判断失败：', '空文件')):
                        status = 'failed'
                    issues.append(dict(file=row.get('relative'), reason=reason))
                if not handoff.get('body'):
                    issues.append(dict(file=None, reason='未识别出正文'))
                if issues and status != 'failed':
                    status = 'review'
                elif not issues and item.get('delivery_complete') is True:
                    status = 'passed'
                elif not issues:
                    status = 'failed'
                    issues.append(dict(file=None, reason='资料组交付未完成'))
            except (OSError, ValueError, TypeError, KeyError, AttributeError):
                status = 'failed'
                issues.append(dict(file=None, reason='无法读取本组处理结果'))
        if status is None:
            status = 'failed'
            issues.append(dict(file=None, reason='没有本组处理结果'))
        group_id = (handoff or {}).get('group_id') or group_hash
        title = display_title(handoff, group_id)
        reason = '；'.join((f"{issue['file']}：" if issue.get('file') else '') + issue['reason']
                          for issue in issues) or None
        row = dict(group=group, group_id=group_id, status='unrecognized' if status == 'review' else status,
                   paper_title=(handoff or {}).get('paper_title'),
                   paper_title_candidate=(handoff or {}).get('paper_title_candidate'),
                   reason=reason, delivery_dir=item.get('delivery_dir'),
                   delivery_complete=item.get('delivery_complete') is True, **title)
        self.store.record(self.task_id, index, row)
        self.seen.add(index)
        self.data[{'passed': 'passed_groups', 'failed': 'failed_groups', 'review': 'review_groups'}[status]] += 1
        self.data['not_passed_groups'] = self.data['failed_groups'] + self.data['review_groups']
        self.data['waiting_groups'] = self.data['total_groups'] - len(self.seen)
        if status != 'passed':
            self.data['issues'].append(dict(group=group, status=status, reasons=issues))
        self.data['groups'].append(dict(row, status=status))
        self.flush()

    def flush(self, *, force=False):
        now = time.monotonic()
        if force or now - self.last_write >= 2:
            self.data['updated_at'] = time.time()
            write_json(self.path, self.data)
            self.last_write = now

    def finish(self):
        self.data['status'] = 'complete' if self.data['waiting_groups'] == 0 else 'incomplete'
        self.store.finish(self.task_id, self.data['status'])
        self.flush(force=True)


def install_group_summary_routes(app, output_dir, *, verify_run=None, sanitize=None):
    """Mount a read-only endpoint in the existing dashboard backend."""
    from fastapi import HTTPException, Query

    @app.get('/api/v1/first-step/tasks/{task_id}')
    def group_summary(task_id: str, status_filter: Literal['passed', 'failed', 'unrecognized'] | None = Query(None, alias='filter'),
                      page: int = Query(1, ge=1),
                      page_size: int = Query(50, ge=1, le=200)):
        try:
            if task_id != 'current':
                task_id = task_id_text(task_id)
            result = GroupSummaryStore(output_dir).read(task_id, status_filter, page, page_size)
            if verify_run is not None:
                verify_run(result['task_id'])
        except (OSError, ValueError, TypeError, LookupError):
            raise HTTPException(status_code=404, detail='第一步任务统计不存在') from None
        if sanitize is not None:
            for row in result['items']:
                for name, limit in [('group', 200), ('display_title', 300), ('display_subtitle', 300),
                                    ('paper_title', 300), ('paper_title_candidate', 300), ('reason', 500)]:
                    if row.get(name) is not None:
                        row[name] = sanitize(row[name], limit)
        return result

    @app.get('/api/v1/first-step/tasks/{task_id}/groups')
    def group_titles(task_id: str, page: int = Query(1, ge=1),
                     page_size: int = Query(50, ge=1, le=200)):
        try:
            if verify_run is not None:
                verify_run(task_id)
            result = read_json(summary_path(output_dir, task_id))
        except (OSError, ValueError, TypeError, LookupError):
            raise HTTPException(status_code=404, detail='第一步任务统计不存在') from None
        groups = result.get('groups', [])
        start = (page - 1) * page_size
        rows = groups[start:start + page_size]
        if sanitize is not None:
            rows = [dict(row, group=sanitize(row.get('group'), 200),
                         display_title=sanitize(row.get('display_title'), 300),
                         display_subtitle=sanitize(row.get('display_subtitle'), 300)) for row in rows]
        return dict(task_id=result['task_id'], rows=rows, total=len(groups),
                    page=page, page_size=page_size, has_next=start + page_size < len(groups))
