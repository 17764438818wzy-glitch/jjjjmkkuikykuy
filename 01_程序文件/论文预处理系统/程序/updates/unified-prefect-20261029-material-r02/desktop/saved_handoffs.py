"""Coalesce native handoff calls after their producer manifest is durable.

The saved manifest remains the intent; the existing queue owns identity,
transport, receipts and cleanup. No model or delivery is implemented here.
"""
from collections import OrderedDict
import json
import sqlite3
import threading
from contextlib import closing


class SavedHandoffs:
    def __init__(self, store, stop, process, *, retry_of=None):
        self.store, self.stop, self.process = store, stop, process
        self.retry_of = retry_of
        self.pending = OrderedDict()
        self.condition = threading.Condition()
        self.closed = False
        self.errors = []
        self.thread = threading.Thread(target=self._run, name='saved-result-handoff')
        self.thread.start()

    def submit(self, run_id, items):
        rows = [dict(id=i['id'], status=i.get('status'), content_format=i.get('content_format'),
                     automatic_approval=i.get('automatic_approval', False)) for i in items
                if i.get('content_format') == 'associated_material' or
                i.get('status') == 'pass' and i.get('automatic_approval')]
        with self.condition:
            for row in rows:
                self.pending[(run_id, row['id'])] = row
            # Sealed bodies enter the native uploader before attachment
            # discovery/parent waits. Keep FIFO order within each class.
            self.pending = OrderedDict(sorted(self.pending.items(),key=lambda pair:
                not (pair[1].get('status')=='pass' and pair[1].get('content_format')=='paper_preextract')))
            self.condition.notify_all()

    def _restore(self):
        """Only the explicitly continued lineage; existing registrations win."""
        from preextract_delivery_queue import queue_path
        registered = set()
        path = queue_path(self.store.root)
        if path.is_file():
            with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
                registered = set(db.execute('SELECT run_id,item_id FROM references_to_deliveries'))
        run_id, seen = self.retry_of, set()
        while run_id and run_id not in seen and not self.stop.is_set():
            seen.add(run_id)
            manifest = self.store.load(run_id)
            self.submit(run_id, [i for i in manifest.get('items', [])
                                 if (run_id, i.get('id')) not in registered])
            run_id = manifest.get('retry_of')

    def _run(self):
        from processing_stop import stop_scope
        from engine import write_json
        with stop_scope(self.stop):
            try:
                self._restore()
                while not self.stop.is_set():
                    with self.condition:
                        while not self.pending and not self.closed and not self.stop.is_set():
                            self.condition.wait(.2)
                        if self.stop.is_set() or not self.pending and self.closed:
                            break
                        run_id = next(iter(self.pending))[0]
                        body = next(iter(self.pending.values())).get('content_format')=='paper_preextract'
                        keys = [key for key,row in self.pending.items() if key[0] == run_id
                                and (row.get('content_format')=='paper_preextract')==body][:64]
                        rows = [self.pending.pop(key) for key in keys]
                    try:
                        self.process(run_id, rows)
                    except InterruptedError:
                        if self.stop.is_set():
                            break
                        raise
                    except Exception as exc:
                        self.errors.append(dict(run_id=run_id, item_ids=[r['id'] for r in rows],
                                                error_type=type(exc).__name__))
            except Exception as exc:
                self.errors.append(dict(run_id=self.retry_of, error_type=type(exc).__name__))
            finally:
                if self.errors:
                    write_json(self.store.root/'private'/'deferred-handoff.json', dict(
                        status='saved_results_retained', errors=self.errors))

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        self.thread.join()
