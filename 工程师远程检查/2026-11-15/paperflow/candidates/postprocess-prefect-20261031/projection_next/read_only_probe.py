"""Explicit read-only probe. It does not initialize a queue, start a service or dispatch."""
import importlib.util
import json
import logging
from pathlib import Path
import sqlite3
import sys
import time
import warnings


warnings.filterwarnings('ignore')
logging.disable(logging.CRITICAL)
workspace = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(workspace / 'candidates' / 'local-intake-live-link-20261031'))
from local_queue import LocalQueue, _current_receiver_session, _delivery_fingerprint
from waiting_cli import configured_orders


class ReadOnlyQueue(LocalQueue):
    def _connect(self):
        db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=3)
        db.row_factory = sqlite3.Row
        return db

    def snapshot(self, *, max_age_seconds=60, include_in_progress=True):
        return super().snapshot(max_age_seconds=max_age_seconds, include_in_progress=False)


def main():
    try:
        queue = ReadOnlyQueue.__new__(ReadOnlyQueue)
        queue.path = (workspace / 'local-intake-runtime' / 'waiting-pool.sqlite3').resolve(strict=True)
        queue.require_retention = True
        queue.require_live_watcher = True
        queue.consumer_id = 'local-intake'
        queue.receiver_session_id = None
        queue.watch_owner_thread = None
        observer = configured_orders(queue)
        path = Path(__file__).resolve().parent / 'projections.py'
        spec = importlib.util.spec_from_file_location('_read_only_projection_probe', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        stage, results = module.build_projections(queue, observer)
        projection = stage.__self__
        timings = {}
        if '--timing' in sys.argv:
            def track(label, call):
                def inner(*args, **kwargs):
                    started = time.perf_counter()
                    try:
                        return call(*args, **kwargs)
                    finally:
                        timing = timings.setdefault(label, {'calls': 0, 'seconds': 0.0})
                        timing['calls'] += 1
                        timing['seconds'] += time.perf_counter() - started
                return inner
            projection._prefect = track('prefect', projection._prefect)
            projection._waiting = track('waiting', projection._waiting)
            projection._snapshot = track('snapshot', projection._snapshot)
            observer._llm_handoff = track('llm_handoff', observer._llm_handoff)
            original_factory = projection.binding_factory
            def timed_factory(*args, **kwargs):
                binding = original_factory(*args, **kwargs)
                binding.load = track('binding_load', binding.load)
                return binding
            projection.binding_factory = timed_factory
        original = projection._observe
        original_snapshot = projection._snapshot
        snapshot_basis = []
        def captured_snapshot():
            value = original_snapshot()
            snapshot_basis.append(value)
            return value
        projection._snapshot = captured_snapshot
        error = []
        safe_codes = {value: key for key, value in {
            'receiver': 'receiver or registered source unavailable',
            'registered_source': 'registered source unavailable',
            'handoffs': 'retained handoffs are unavailable',
            'root_identity': 'root order identity mismatch',
            'orphan_stage': 'orphan stage or result',
            'prefect_identity': 'Prefect folder identity unavailable',
            'frozen_material': 'frozen material is unavailable',
            'result_evidence': 'result evidence unavailable',
            'source_changed': 'source changed during observation',
            'ledger_changed': 'ledger snapshot changed',
            'handoff_changed': 'handoff snapshot changed',
            'receiver_changed': 'receiver snapshot changed',
            'registered_source_changed': 'registered source changed',
            'database_changed': 'database version changed',
            'database_changed_control': 'database version changed control',
            'database_changed_queue': 'database version changed queue',
            'database_changed_both': 'database version changed control+queue',
            'prefect_changed': 'Prefect states changed during observation',
            'result_changed': 'result evidence changed after Prefect observation',
        }.items()}
        def captured(*args, **kwargs):
            snapshot_basis.clear()
            try:
                return original(*args, **kwargs)
            except Exception as exc:
                code = safe_codes.get(str(exc)) if type(exc).__name__ == 'ProjectionUnavailable' else None
                if code == 'ledger_changed' and len(snapshot_basis) >= 2:
                    before, after = snapshot_basis[0], snapshot_basis[-1]
                    changed = [key for key in before if key != 'local_basis' and before[key] != after[key]]
                    changed += ['local_' + key for key in before['local_basis']
                                if before['local_basis'][key] != after['local_basis'][key]]
                    if changed:
                        code += '_' + '+'.join(sorted(changed))
                    if 'local_metadata' in changed:
                        def metadata(basis):
                            return {row['key']: row['value'] for row in
                                    (json.loads(raw) for raw in basis['local_basis']['metadata'])}
                        left, right = metadata(before), metadata(after)
                        keys = [key for key in set(left) | set(right) if left.get(key) != right.get(key)]
                        if keys:
                            code += '_keys_' + '+'.join(sorted(keys))
                    if 'local_retention_requests' in changed:
                        def retention_rows(basis):
                            rows = [json.loads(raw) for raw in basis['local_basis']['retention_requests']]
                            return {(row['task_id'], row['group_id'], row['receipt_id'], row['consumer_id']): row
                                    for row in rows}
                        left, right = retention_rows(before), retention_rows(after)
                        fields = set()
                        if set(left) != set(right):
                            fields.add('row_identity')
                        for key in set(left) & set(right):
                            fields.update(field for field in left[key] if left[key].get(field) != right[key].get(field))
                        if fields:
                            code += '_fields_' + '+'.join(sorted(fields))
                error.append(type(exc).__name__ + (':' + code if code else ''))
                raise
        projection._observe = captured
    except Exception as exc:
        print(json.dumps({'setup': {'available': False, 'error_type': type(exc).__name__}}), flush=True)
        return 1
    for name, fn in (('stage', lambda: stage(queue.snapshot())), ('results', lambda: results(None, 1, 20))):
        error.clear()
        value = None
        started = time.perf_counter()
        try:
            value = fn()
            output = {'available': value.get('available') is True, 'counts': value.get('counts'),
                      'elapsed_seconds': round(time.perf_counter() - started, 3),
                      'error_type': error[-1] if error else None}
        except Exception as exc:
            output = {'available': False, 'counts': None,
                      'elapsed_seconds': round(time.perf_counter() - started, 3),
                      'error_type': type(exc).__name__}
        print(json.dumps({name: output}, ensure_ascii=False), flush=True)
        witness = getattr(value, '_cache_witness', None)
        if '--timing' in sys.argv and witness is not None:
            for kind, check in (('local_witness', lambda: witness.local(queue.snapshot())),
                                ('remote_witness', witness.remote)):
                started = time.perf_counter()
                try:
                    available = check() is True
                    error_type = None
                except Exception as exc:
                    available = False
                    error_type = type(exc).__name__
                print(json.dumps({kind: {'available': available,
                      'elapsed_seconds': round(time.perf_counter() - started, 3), 'error_type': error_type}}),
                      flush=True)
        if timings:
            print(json.dumps({'timing': {key: {'calls': item['calls'], 'seconds': round(item['seconds'], 3)}
                                        for key, item in timings.items()}}), flush=True)
            timings.clear()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
