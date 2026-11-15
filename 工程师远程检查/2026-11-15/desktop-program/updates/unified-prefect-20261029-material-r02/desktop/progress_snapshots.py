"""Coalesce only replaceable parent progress, using the original atomic Store writer.

No background writer, delayed mutable references, new lock order or worker store.
Structural/terminal saves and every explicit required save remain synchronous.
"""
import time
import sys
from types import CodeType


def critical_state(manifest):
    return tuple(manifest.get(k) for k in ('run_id', 'status', 'processed_sources',
        'source_count', 'formal_task_count')) + tuple(len(manifest.get(k) or ()) for k in (
            'inputs', 'items', 'child_runs', 'discovered_inputs', 'events'))


class ProgressSnapshots:
    def __init__(self, store, run_id, *, interval=5, clock=time.monotonic):
        if not 0 < interval <= 5:
            raise ValueError('bounded progress persistence interval required')
        self.store, self.run_id = store, run_id
        self.interval, self.clock = interval, clock
        self.original_save = store.save
        self.last_at, self.last_critical = None, None
        self.writes, self.skipped = 0, 0

    def _save(self, manifest, progress):
        with self.store.lock:
            now, critical = self.clock(), critical_state(manifest)
            if (progress and manifest.get('run_id') == self.run_id
                    and manifest.get('status') == 'running'
                    and self.last_at is not None and critical == self.last_critical
                    and 0 <= now-self.last_at < self.interval):
                self.skipped += 1
                return
            try:
                result = self.original_save(manifest)
            except PermissionError:
                # Only a replaceable progress snapshot may be deferred. New
                # sources/results and every required save must remain durable.
                if not progress or critical != self.last_critical:raise
                self.skipped += 1
                return
            self.last_at, self.last_critical = now, critical
            self.writes += 1
            return result

    def progress(self, manifest):
        return self._save(manifest, True)

    def required(self, manifest):
        return self._save(manifest, False)


def bind_live_progress(store, run_id, sites, *, interval=5, clock=time.monotonic):
    """Narrow adapter for a currently running older parent frame.

    Only validated exact code objects AND call lines are transient. All other
    saves, including discoveries/result collection/termination, are mandatory.
    """
    if (not sites or any(not isinstance(code, CodeType) or not lines
            or any(type(line) is not int for line in lines) for code,lines in sites.items())
            or hasattr(store, '_progress_snapshot_cadence')):
        raise ValueError('unique exact live progress scope required')
    sites = {code:frozenset(lines) for code,lines in sites.items()}
    cadence = ProgressSnapshots(store, run_id, interval=interval, clock=clock)
    def saved(manifest):
        caller = sys._getframe(1)
        progress = caller.f_lineno in sites.get(caller.f_code, ())
        return cadence.progress(manifest) if progress else cadence.required(manifest)
    store._progress_snapshot_cadence = cadence
    store.save = saved
    return cadence
