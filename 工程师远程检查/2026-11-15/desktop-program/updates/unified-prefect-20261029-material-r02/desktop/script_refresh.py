"""Display-only row indexes and progress buffering; no processing or persistence."""
from bisect import bisect_left
from collections import Counter, OrderedDict


FINISHED = {'通过', '重复跳过', '已保存', '已删除'}
TERMINAL = {'通过', '不通过', '跳过', '任务结束', '脚本处理结束', '待解析',
            '关联资料保存', '已删除过程表单', '脚本操作失败', '用户停止'}


def matches(row, selected):
    status = row.get('script_status', '待处理')
    if selected == '未完成':
        return status not in FINISHED
    return selected == '全部结果' or status == selected


def tree_batch(tree, commands):
    """Reuse sync_tree's literal Tcl-list quoting for a bounded command batch."""
    if not commands:
        return
    script = []
    for command in commands:
        script.extend((command, ';'))
    tree.tk.call('eval', *script)


class RowIndex:
    def __init__(self, rows, selected, key, values):
        self.key, self.values_for = key, values
        self.paths = tuple(row['path'] for row in rows)
        self.keys = [key(path) for path in self.paths]
        self.names = [row.get('name') for row in rows]
        self.by_path, self.by_name = {}, {}
        for index, row in enumerate(rows):
            self.by_path.setdefault(self.keys[index], []).append(index)
            self.by_name.setdefault(row.get('name'), []).append(index)
        self.values = [values(row) for row in rows]
        self.statuses = [row.get('script_status', '待处理') for row in rows]
        self.counts = Counter(self.statuses)
        self.rows = rows
        self.selected = selected
        self.visible = [i for i, row in enumerate(rows) if matches(row, selected)]
        self.visible_set = set(self.visible)

    def find(self, progress):
        source = progress.get('source_path')
        return self.by_path.get(self.key(source), []) if source else self.by_name.get(progress.get('file'), [])

    def update(self, index):
        row = self.rows[index]
        name = row.get('name')
        old_name = self.names[index]
        if name != old_name:
            self.by_name[old_name].remove(index)
            indices = self.by_name.setdefault(name, [])
            indices.insert(bisect_left(indices, index), index)
            self.names[index] = name
        status = row.get('script_status', '待处理')
        old = self.statuses[index]
        if status != old:
            self.counts[old] -= 1
            self.counts[status] += 1
            self.statuses[index] = status
        self.values[index] = self.values_for(row)
        visible = matches(row, self.selected)
        was_visible = index in self.visible_set
        if visible != was_visible:
            position = bisect_left(self.visible, index)
            if visible:
                self.visible.insert(position, index)
                self.visible_set.add(index)
            else:
                self.visible.pop(position)
                self.visible_set.remove(index)

    def replace(self, rows):
        changed = [i for i, row in enumerate(rows) if self.values_for(row) != self.values[i]]
        self.rows = rows
        for index in changed:
            self.update(index)
        return changed

    def filter(self, selected):
        if selected == self.selected:
            return []
        old = self.visible_set
        self.selected = selected
        self.visible = [i for i, row in enumerate(self.rows) if matches(row, selected)]
        self.visible_set = set(self.visible)
        return sorted(old ^ self.visible_set)


class ProgressBuffer:
    """Coalesce transient display states, keeping control and terminal barriers."""
    def __init__(self, key):
        self.key = key
        self.run_id = None
        self.pending = OrderedDict()
        self.latest = {}

    @staticmethod
    def terminal(payload):
        progress = payload.get('progress') or {}
        return (progress.get('stage') in TERMINAL or progress.get('level') == 'error'
                or payload.get('status') in {'completed', 'failed', 'stopped', 'cancelled', 'interrupted'}
                or bool(payload.get('ai_item')))

    def add(self, payload):
        run_id = payload.get('run_id')
        if run_id != self.run_id:
            self.pending.clear()
            self.latest.clear()
            self.run_id = run_id
        for field in ('pipeline_concurrency', 'billing', 'processed_sources', 'source_count'):
            if field in payload:
                self.latest[field] = payload[field]
        progress = payload.get('progress') or {}
        source = progress.get('source_path')
        identity = ('path', self.key(source)) if source else ('name', progress.get('file'))
        self.pending[(run_id, identity)] = payload

    def pop(self):
        _, payload = self.pending.popitem(last=False)
        return dict(payload, **self.latest)
