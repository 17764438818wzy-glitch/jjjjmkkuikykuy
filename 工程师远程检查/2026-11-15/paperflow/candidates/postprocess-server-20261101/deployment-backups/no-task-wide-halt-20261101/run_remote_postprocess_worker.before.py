"""Coordinate the existing Prefect folder tickets with the server executor.

New local admissions require the original task to be running. A remote start
already sent before pause may still arrive and must be reconciled separately.
This worker never calls an LLM or a RAG uploader and never falls back to local
postprocessing. The existing coordinator owns the stage, claim and Prefect slot.
"""
import argparse
import asyncio
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time


LIMIT = 8
POLL_SECONDS = 5
CONTROL_SOURCE_SHA256 = '5f19e321c1b5f9835ff2217b56462a96b0864e40ffa6d7184f735f9cc19ad26f'


class GenerationView:
    """Apply the current task generation to the original state inspector."""
    TABLES = ('paperflow_postprocess_stage_jobs', 'postprocessing_claims', 'postprocessing_results')
    FILTERED = {'SELECT * FROM ' + name + ' WHERE task_id=?': name for name in TABLES}
    PASSTHROUGH = frozenset((
        'PRAGMA table_info(paperflow_postprocess_stage_jobs)',
        'SELECT 1 FROM postprocessing_receipts LIMIT 1',
        'SELECT receipt_json,receipt_sha256 FROM postprocessing_receipts WHERE claim_id=?',
    ))

    def __init__(self, database, task_id, generation):
        self.database, self.task_id, self.generation = database, task_id, generation

    def execute(self, sql, parameters=()):
        normalized = ' '.join(sql.split())
        if normalized == 'SELECT * FROM llm_folder_queue WHERE task_id=?':
            if tuple(parameters) != (self.task_id,):
                raise ValueError('folder state inspection changed its identity')
            return self.database.execute('''SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id IN (
                SELECT group_id FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND generation=?
                UNION SELECT group_id FROM postprocessing_claims WHERE task_id=? AND generation=?
                UNION SELECT group_id FROM postprocessing_results WHERE task_id=? AND generation=?)''',
                (self.task_id, self.task_id, self.generation, self.task_id, self.generation,
                 self.task_id, self.generation))
        if normalized in self.FILTERED:
            if tuple(parameters) != (self.task_id,):
                raise ValueError('task state inspection changed its identity')
            # Include every related row of a current-generation group, even a
            # conflicting older claim or result. Entirely historical groups are
            # left alone; mixed-generation identities must be reconciled.
            table = self.FILTERED[normalized]
            rows = self.database.execute('SELECT * FROM ' + table + ''' WHERE task_id=? AND group_id IN (
                SELECT group_id FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND generation=?
                UNION SELECT group_id FROM postprocessing_claims WHERE task_id=? AND generation=?
                UNION SELECT group_id FROM postprocessing_results WHERE task_id=? AND generation=?)''',
                (self.task_id, self.task_id, self.generation, self.task_id, self.generation,
                 self.task_id, self.generation)).fetchall()
            if any(row['generation'] != self.generation for row in rows):
                raise ValueError('current folder has mixed-generation state')
            return rows
        if normalized in self.PASSTHROUGH:
            return self.database.execute(sql, parameters)
        raise ValueError('task state inspector uses an unrecognized query')


def log(event, **fields):
    print(json.dumps(dict(event=event, **fields), ensure_ascii=False), flush=True)


def load_control_inspector(root):
    """Load only the original control inspector's pinned source bytes."""
    source = root.parent / 'candidates' / 'first-step-control-20261029' / 'paperflow_prefect_v2' / 'llm_admission_state.py'
    data = source.read_bytes()
    if hashlib.sha256(data).hexdigest() != CONTROL_SOURCE_SHA256:
        raise ValueError('original task state inspector changed')
    scope = {'__name__': '_paperflow_pinned_postprocess_state', '__file__': str(source)}
    exec(compile(data, str(source), 'exec', dont_inherit=True), scope)
    inspector = scope.get('_postprocessing_counts')
    if not callable(inspector):
        raise ValueError('original task state inspector is unavailable')
    return source, inspector


def running_tasks(database, control_source, inspector):
    """Read task authority and the two existing placeholders in one snapshot."""
    if hashlib.sha256(control_source.read_bytes()).hexdigest() != CONTROL_SOURCE_SHA256:
        raise ValueError('original task state inspector changed')

    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        gates = dict(db.execute('SELECT name,blocked FROM paperflow_queue_barriers'))
        if gates != {'postprocessing': 0, 'rag_upload': 1}:
            return [], [], []
        candidates = db.execute('''SELECT t.task_id,t.start_generation FROM tasks t
            WHERE t.desired_state='running' AND t.start_pending=0
              AND t.prefect_order_model='v2' AND t.prefect_run_id IS NOT NULL
              AND t.task_status NOT IN ('deleting','delete_failed','unknown')
              AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=t.task_id)
            ORDER BY t.started_at_ns,t.task_id''').fetchall()
        tasks = []
        issues = []
        for row in candidates:
            try:
                if type(row['start_generation']) is not int or row['start_generation'] < 1:
                    issues.append((row['task_id'], 'InvalidGeneration'))
                    continue
                view = GenerationView(db, row['task_id'], row['start_generation'])
                _, unknown = inspector(view, row['task_id'])
            except (sqlite3.Error, ValueError, KeyError, IndexError, TypeError) as exc:
                issues.append((row['task_id'], type(exc).__name__))
                continue
            if not unknown:
                tasks.append((row['task_id'], row['start_generation']))
            else:
                issues.append((row['task_id'], 'UnresolvedPostprocessing'))
        active = db.execute('''SELECT j.task_id,j.group_id,t.start_generation
            FROM paperflow_postprocess_stage_jobs j JOIN tasks t ON t.task_id=j.task_id
            WHERE j.state IN
                ('reserved','submitting','submitted','executing')''').fetchall()
        return tasks, active, issues


async def run_folder(launcher, identity, generation):
    # Policy discovery is read-only; the launcher rechecks the same original
    # task and folder before it reserves or starts the remote Prefect stage.
    prepared = await asyncio.to_thread(launcher.prepare, identity, generation)
    return await launcher.launch(identity, generation, prepared)


async def serve(runtime, launcher, control_source, inspector):
    active = {}
    halted = set()
    last_error = None
    last_task_issues = set()
    log('remote_postprocessing_coordinator_started', limit=LIMIT)
    while True:
        for task in [item for item in active if item.done()]:
            task_id, group_id, generation = active.pop(task)
            try:
                state = task.result()
            except Exception as exc:
                halted.add((task_id, generation))
                log('folder_unconfirmed', task_id=task_id, group_id=group_id,
                    error_type=type(exc).__name__)
            else:
                if state != 'finished':
                    halted.add((task_id, generation))
                log('folder_stage_observed', task_id=task_id, group_id=group_id, state=state)

        if len(active) < LIMIT:
            try:
                task_rows, stage_rows, issues = await asyncio.to_thread(
                    running_tasks, runtime.original_runtime['database'], control_source, inspector)
                last_error = None
                current_task_issues = set(issues)
                for task_id, error in sorted(current_task_issues - last_task_issues):
                    log('task_waiting_for_reconciliation', task_id=task_id, error_type=error)
                last_task_issues = current_task_issues
                local_active = {(t, g) for t, g, _ in active.values()}
                foreign_active = set()
                # A stage owned by an earlier coordinator or another process
                # is never assumed stopped merely because this process began.
                for task_id, group_id, generation in stage_rows:
                    if (task_id, group_id) not in local_active:
                        halted.add((task_id, generation))
                        foreign_active.add((task_id, group_id))
                for task_id, generation in task_rows:
                    if len(active) + len(foreign_active) >= LIMIT:
                        break
                    if (task_id, generation) in halted:
                        continue
                    inflight = {(t, g) for t, g, _ in active.values()}
                    # Exclude local in-flight choices without installing a new
                    # cross-ticket lock. The existing durable reservation is
                    # still the authority if another coordinator races us.
                    count = min(10000, LIMIT + sum(1 for t, _, _ in active.values() if t == task_id))
                    selections = await asyncio.to_thread(runtime.batch.snapshot_waiting,
                        runtime.original_runtime['database'], task_id, count)
                    for identity, current_generation in selections:
                        if len(active) + len(foreign_active) >= LIMIT:
                            break
                        if (identity['task_id'] != task_id or current_generation != generation
                                or (task_id, identity['group_id']) in inflight):
                            continue
                        item = asyncio.create_task(run_folder(launcher, dict(identity), generation))
                        active[item] = (task_id, identity['group_id'], generation)
                        inflight.add((task_id, identity['group_id']))
            except Exception as exc:
                error = type(exc).__name__
                if error != last_error:
                    log('coordinator_waiting_for_authority', error_type=error)
                    last_error = error

        if active:
            await asyncio.wait(tuple(active), timeout=POLL_SECONDS, return_when=asyncio.FIRST_COMPLETED)
        else:
            await asyncio.sleep(POLL_SECONDS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--config-sha256', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    config_path = root / 'config.json'
    settings = json.loads(config_path.read_text(encoding='utf-8-sig'))
    api = str(settings.get('prefect_api_url', '')).rstrip('/')
    if not api:
        raise RuntimeError('the original Prefect address is unavailable')
    os.environ.update(PAPERFLOW_CONFIG=str(config_path), PREFECT_API_URL=api, PYTHONUTF8='1',
                      PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS='false')
    server_candidate = root.parent / 'candidates' / 'postprocess-server-20261101'
    sys.path.insert(0, str(server_candidate))
    from scoped_runtime_factory import build
    # A busy response is retried only when the pinned server confirms that the
    # same admission did not start; ambiguous replies remain unknown.
    runtime, launcher = build(args.config, args.config_sha256,
                              admission_wait={'wait': lambda: time.sleep(.5), 'max_attempts': None})
    control_source, inspector = load_control_inspector(root)
    asyncio.run(serve(runtime, launcher, control_source, inspector))


if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    log_path = root / 'runtime' / 'remote-postprocessing' / 'mainline-worker.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf8', buffering=1) as stream:
        sys.stdout = sys.stderr = stream
        try:
            main()
        except BaseException as exc:
            log('remote_postprocessing_coordinator_stopped', error_type=type(exc).__name__)
            raise
