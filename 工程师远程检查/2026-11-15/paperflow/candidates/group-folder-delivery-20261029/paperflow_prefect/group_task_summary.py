"""One first-step task summary, independent of the internal batch size."""
from pathlib import Path
import time
import uuid

from .records import read_json, write_json


def task_id_text(value):
    return str(uuid.UUID(str(value)))


def summary_path(output_dir, task_id):
    return Path(output_dir) / 'group-task-summaries' / (task_id_text(task_id) + '.json')


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
                         waiting_groups=total, issues=[])
        self.flush(force=True)

    def record(self, index, item):
        if index in self.seen:
            return
        self.seen.add(index)
        root = Path(item['group_root'])
        try:
            group = root.resolve().relative_to(self.library).as_posix()
        except (OSError, ValueError, RuntimeError):
            group = root.name
        issues, status = [], None
        if item.get('error'):
            reason = str(item['error'])[:500]
            status = 'review' if '未确认' in reason or '待核对' in reason else 'failed'
            issues.append(dict(file=None, reason=reason))
        for key, label in (('delivery_errors', '交付文件复制失败'),
                           ('evidence_errors', '附件证据处理失败')):
            for entry in item.get(key, []):
                status = 'failed'
                issues.append(dict(file=entry.get('relative') or entry.get('file_id'), reason=label))
        path = item.get('handoff') or item.get('unreceived')
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
                    if (reason.startswith(('读取失败：', '分类失败：', '正文判断失败：', '空文件'))
                            and 'TimeoutError' not in reason and '未确认' not in reason):
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
        self.data[{'passed': 'passed_groups', 'failed': 'failed_groups', 'review': 'review_groups'}[status]] += 1
        self.data['not_passed_groups'] = self.data['failed_groups'] + self.data['review_groups']
        self.data['waiting_groups'] = self.data['total_groups'] - len(self.seen)
        if status != 'passed':
            self.data['issues'].append(dict(group=group, status=status, reasons=issues))
        self.flush()

    def flush(self, *, force=False):
        now = time.monotonic()
        if force or now - self.last_write >= 2:
            self.data['updated_at'] = time.time()
            write_json(self.path, self.data)
            self.last_write = now

    def finish(self):
        self.data['status'] = 'complete' if self.data['waiting_groups'] == 0 else 'incomplete'
        self.flush(force=True)


def install_group_summary_routes(app, output_dir, *, verify_run=None, sanitize=None):
    """Mount a read-only endpoint in the existing dashboard backend."""
    from fastapi import HTTPException, Query

    @app.get('/api/v1/first-step/tasks/{task_id}')
    def group_summary(task_id: str, page: int = Query(1, ge=1),
                      page_size: int = Query(50, ge=1, le=200)):
        try:
            if verify_run is not None:
                verify_run(task_id)
            result = read_json(summary_path(output_dir, task_id))
        except (OSError, ValueError, TypeError, LookupError):
            raise HTTPException(status_code=404, detail='第一步任务统计不存在') from None
        issues = result.pop('issues', [])
        start = (page - 1) * page_size
        rows = issues[start:start + page_size]
        if sanitize is not None:
            rows = [dict(group=sanitize(row.get('group'), 200), status=row.get('status'),
                         reasons=[dict(file=sanitize(reason.get('file'), 200) if reason.get('file') else None,
                                       reason=sanitize(reason.get('reason'), 300))
                                  for reason in row.get('reasons', [])]) for row in rows]
        return dict(result, issues=rows, issues_total=len(issues),
                    page=page, page_size=page_size, has_next=start + page_size < len(issues))
