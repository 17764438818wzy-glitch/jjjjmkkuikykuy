"""Small native UI hooks; stopped authority never becomes automatic resume."""
from preextract_coordinator import persisted_scope,control_path
from pathlib import Path
import sqlite3


def persist_stop(root):
    if not (Path(root)/'prepared-current-scope.json').exists():return
    from paperflow.prepared_store import mark_stop_pending
    mark_stop_pending(control_path(root))
    saved=persisted_scope(root)
    if not saved:return
    from paperflow.prepared_store import PreparedRegistry
    from paperflow.store import Store
    registry=PreparedRegistry(Store(control_path(root)))
    if saved['intent']=='running' or saved['stop_pending']:
        registry.set_intent(saved['scope_id'],expected_generation=saved['generation'],intent='stopped')


def resume(window,*,startup_recovery=False):
    if window.busy:return
    try:
        saved=persisted_scope(window.store.root)
        if not saved:
            window.state.set('没有已登记范围；在 AI 结果页选择已有解析材料后续跑。');return
        if saved['intent']=='completed' and not saved['stop_pending']:
            window.state.set('当前范围正文与必需附件已核验完成；清理按现有策略独立处理。');return
        if startup_recovery and (saved['intent']!='running' or saved['stop_pending']):return
        window._prepared_resume_scope_id=saved['scope_id']
        window._prepared_startup_recovery=startup_recovery
        if not window.current:
            window.current=dict(run_id='PREPARED_SCOPE_'+saved['scope_id'],task_settings={})
        window._pending_operation='retry_ai'
        window.start_automatic(saved['records'])
    except (OSError,ValueError,KeyError,sqlite3.Error) as exc:
        window.state.set('持续范围保持停止；控制记录需核对（'+type(exc).__name__+'）。')


def startup(window):
    try:
        saved=persisted_scope(window.store.root)
        if not saved:return False
        if saved['intent']=='running' and not saved['stop_pending']:
            resume(window,startup_recovery=True)
        elif saved['intent']=='completed' and not saved['stop_pending']:
            window.state.set('当前范围正文与必需附件已核验完成；清理按现有策略独立处理。')
        else:window.state.set('当前范围已停止；已有结果保留，可点“继续当前范围”。')
    except (OSError,ValueError,KeyError,sqlite3.Error):
        window.state.set('持续范围控制记录缺失或损坏；保持停止，等待核对。')
    return True
