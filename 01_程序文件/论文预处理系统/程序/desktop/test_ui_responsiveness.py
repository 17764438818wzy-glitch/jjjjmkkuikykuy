"""Large retry histories and progress backlogs must leave the desktop responsive."""
from engine import Store
from test_rag_results import load_batch
from test_script_task_ui import ui


class CountingStore(Store):
    def __init__(self, root):
        super().__init__(root)
        self.loaded_runs = []

    def load(self, run_id):
        self.loaded_runs.append(run_id)
        return super().load(run_id)


def test_inherited_remote_rows_reuse_history_reads_and_refresh_receipts(ui):
    _, workbench = ui
    original = load_batch(workbench)
    retry = {**original, 'run_id': 'retry', 'retry_of': 'remote',
             'status': 'interrupted', 'items': [], 'events': [], 'inputs': [],
             'task_settings': {'destinations': {}}}
    workbench.store.save(retry)
    store = CountingStore(workbench.store.root)
    workbench.store = store
    workbench.current = retry

    workbench.render_rag_results()

    assert store.loaded_runs.count('remote') == 1
    assert workbench._rag_states['0']['target'] == '论文库'
    assert workbench._rag_states['0']['process'] == '已完成'
    original['items'][0]['uploads']['http://localhost|paper']['ragflow_status'] = 'failed'
    store.save(original)
    store.loaded_runs.clear()
    workbench.render_rag_results()
    assert store.loaded_runs.count('remote') == 1
    assert workbench._rag_states['0']['process'] == '处理失败'


def test_poll_yields_with_progress_backlog_without_losing_pending_events(ui):
    _, workbench = ui
    workbench.busy = True
    workbench.task_kind = 'processing'
    for count in range(1, 5001):
        workbench.events.put(('progress', {
            'source_count': 5000, 'processed_sources': count,
            'progress': {'stage': 'AI 文本处理'},
        }))

    workbench.poll()

    shown = float(workbench.progress_bar['value'])
    assert 0 < shown < 5000
    assert workbench.events.qsize() == 5000 - int(shown)
