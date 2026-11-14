"""Read live batch projections on one worker, publish them only on the Tk thread."""
from pathlib import Path
import json
import sqlite3
import threading
import time

from script_task import task_snapshot, task_rows, effective_run
from upload_tracking import UploadIndex, paper_id_for_item
from preextract_delivery_queue import queue_stamp as delivery_queue_stamp


def sync_tree(tree, rows):
    """Keep stable rows and issue bounded Tcl batches with literal list quoting."""
    existing = set(tree.get_children())
    desired = {iid for iid, _ in rows}
    remove = existing - desired
    if remove:
        tree.delete(*remove)
    prior = getattr(tree, '_live_values', {})
    commands = []
    for iid, values in rows:
        if iid not in existing:
            commands.append((str(tree), 'insert', '', 'end', '-id', iid, '-values', values))
        elif prior.get(iid) != values:
            commands.append((str(tree), 'item', iid, '-values', values))
    order = tuple(iid for iid, _ in rows)
    if tuple(tree.get_children()) != order:
        commands.append((str(tree), 'children', '', order))
    for start in range(0, len(commands), 100):
        # Each command is passed as a Tcl list, including nested values. Names,
        # paths, brackets and semicolons in documents cannot become Tcl code.
        from script_refresh import tree_batch
        tree_batch(tree, commands[start:start+100])
    tree._live_values = dict(rows)


class ResultReader:
    def __init__(self, store):
        self.store = store
        self.records = {}
        self.last_key = None
        self.last = None

    @staticmethod
    def stamp(path):
        try:
            stat = Path(path).stat()
            return stat.st_mtime_ns, stat.st_size
        except OSError:
            return None

    def load(self, run_id):
        # Capture before reading: a concurrent save must invalidate this entry.
        stamp = (self.stamp(self.store.manifest_path(run_id)), delivery_queue_stamp(self.store.root))
        prior = self.records.get(run_id)
        if prior is None or prior[0] != stamp:
            prior = (stamp, self.store.load(run_id))
            self.records[run_id] = prior
        return effective_run(self.store.root,prior[1])

    def read(self, run_id):
        from auto_config import load_config
        config=load_config(self.store.root)
        queue_path=Path(config.get('work_pool_queue') or self.store.root/'private/universal_ingest/queue.sqlite3')
        queue_stamp=(self.stamp(queue_path),self.stamp(str(queue_path)+'-wal'))
        ledger_path=self.store.root/'private/education_full_ingest'/run_id/'receipts.json'
        settings_path=self.store.root/'settings.json'
        deployment_path=self.store.root/'private/education_pilot/deployment.json'
        versions = tuple((p.parent.name, self.stamp(p))
                         for p in sorted(self.store.runs.glob('*/manifest.json')))
        lifecycle=[]
        for rid,_ in versions:
            try:lifecycle.append((rid,(self.load(rid) or {}).get('status')))
            except (OSError,ValueError):lifecycle.append((rid,'unreadable'))
        key = (run_id, versions, str(queue_path),queue_stamp,self.stamp(ledger_path),
               self.stamp(settings_path),self.stamp(deployment_path),delivery_queue_stamp(self.store.root))
        key+=(tuple(lifecycle),)
        if key == self.last_key:
            return self.last
        manifest = self.load(run_id)
        if not manifest:
            raise ValueError('当前批次记录缺失')
        self.root=self.store.root
        snapshot = task_snapshot(self, manifest)
        ids={task_id for item in snapshot['items'] for task_id in item.get('queue_task_ids',[])}
        queued={}
        if ids and queue_path.is_file():
            db=sqlite3.connect(queue_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.25)
            try:
                db.row_factory=sqlite3.Row
                wanted=list(ids)
                for start in range(0,len(wanted),500):
                    group=wanted[start:start+500]
                    for row in db.execute('SELECT * FROM tasks WHERE id IN ('+','.join('?' for _ in group)+')',group):
                        queued[row['id']]=dict(row)
            finally:db.close()
        from education_stream_status import load_external_ledger
        ledger=load_external_ledger(self.store.root,run_id)
        def optional_json(path):
            try:return json.loads(path.read_text(encoding='utf8'))
            except (OSError,ValueError):return {}
        history = []
        for rid, _ in versions:
            try:
                record = self.load(rid)
            except (OSError, ValueError):
                # As with Store.history(), unrelated corrupt history must not
                # hide the valid current batch. Required ancestors failed above.
                continue
            if record:history.append(record)
        identities={id(item):paper_id_for_item(item) for batch in [*history,snapshot]
                    for item in batch.get('items',[])}
        index=UploadIndex(history,identity_resolver=lambda item:
                          item.get('paper_id') or identities.get(id(item),''))
        result = dict(manifest=manifest, snapshot=snapshot, rows=task_rows(snapshot,for_display=True),
                      index=index, history_stamp=versions,
                      queue=queued,queue_path=queue_path,ledger=ledger,
                      service_url=optional_json(settings_path).get('ragflow_url',''),
                      deployment=optional_json(deployment_path))
        self.last_key, self.last = key, result
        return result


class LiveResultView:
    def live_results_active(self):
        return getattr(self, '_live_results_rendering', False) or (
                getattr(self, '_live_results_enabled', False) and self.busy
                and self.task_kind == 'processing')

    def idle_delivery_context(self):
        """Observe the selected batch without holding local processing busy."""
        current = self.current or {}
        if self.busy or not current.get('run_id') or self.tabs.select() != str(self.rag_result_tab):
            return None
        if (current.get('task_settings', {}).get('preextract_delivery_mode') != 'disk_queue_v1'
                and not any(row.get('preextract_delivery_queue', {}).get('mode') == 'disk_queue_v1'
                            for row in current.get('items', []))):
            return None
        stamp = delivery_queue_stamp(self.store.root)
        return (current['run_id'], stamp) if any(stamp) else None

    def idle_delivery_refresh_needed(self):
        context = self.idle_delivery_context()
        return context is not None and context != getattr(self, '_idle_delivery_read', None)

    def request_live_results(self, run_id=None):
        if getattr(self, '_live_results_rendering', False):
            return
        live = self.live_results_active()
        idle = self.idle_delivery_context() if not live else None
        if not live and (idle is None or idle == getattr(self, '_idle_delivery_read', None)):
            return
        if idle is not None:
            run_id = idle[0]
        if run_id is None and getattr(self,'_live_waiting_run',False):
            return
        if run_id is not None:
            self._live_waiting_run=False
        run_id = run_id or getattr(self,'_live_requested_run',None) or (self.current or {}).get('run_id')
        if not run_id:
            return
        self._live_requested_run = run_id
        if getattr(self, '_live_read_token', None) is not None:
            return
        if time.monotonic() < getattr(self, '_live_next_read', 0):
            return
        reader = getattr(self, '_live_reader', None)
        if reader is None or reader.store is not self.store:
            reader = self._live_reader = ResultReader(self.store)
        token = self._live_read_token = object()
        self._idle_delivery_inflight = (token, idle)
        self._live_next_read = time.monotonic() + 1.0
        events = self.events
        def read():
            try:
                events.put(('live_results', (token, run_id, reader.read(run_id), None)))
            except Exception as exc:
                events.put(('live_results', (token, run_id, None, type(exc).__name__)))
        try:
            threading.Thread(target=read, daemon=True).start()
        except Exception:
            self._live_read_token = None
            raise

    def finish_live_results(self, value):
        token, run_id, result, error = value
        if token is not getattr(self, '_live_read_token', None):
            return
        self._live_read_token = None
        inflight = getattr(self, '_idle_delivery_inflight', None)
        idle = inflight[1] if inflight and inflight[0] is token else None
        idle_current = self.idle_delivery_context() if idle is not None else None
        if ((not self.live_results_active() and not (idle_current and idle_current[0] == run_id))
                or run_id != getattr(self, '_live_requested_run', None)):
            return
        if error:
            self.state.set('结果列表暂未更新（'+error+'），后台处理继续；稍后自动重读。')
            return
        if idle is not None:
            self._idle_delivery_read = idle
        if result is getattr(self, '_live_result', None):
            return
        self._live_result = result
        self.current = result['manifest']
        self._script_manifest = result['snapshot']
        # Rendering consumes the projection read above, including its upload
        # index, rather than opening SQLite or scanning history on the Tk thread.
        self._live_results_rendering = True
        try:
            self.render_script_rows(result['rows'])
            selected = self.tabs.select()
            if selected == str(self.result_tab):
                self.render_ai_results(refresh=False)
            elif selected == str(self.rag_result_tab):
                self.render_rag_results(refresh=False)
        finally:
            self._live_results_rendering = False

    def live_result_snapshot(self):
        self.request_live_results()
        result = getattr(self, '_live_result', None)
        run_id = (self.current or {}).get('run_id')
        if result and result['manifest']['run_id'] == run_id:
            return result['snapshot']
        # Keep an empty, cheap view until the first background projection arrives.
        return dict(run_id=run_id, status='running', items=[], inputs=[])
