"""Dispatch an explicitly started first-step task with bounded group workers."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing
from functools import partial
from pathlib import Path

from .first_step_control import PauseRequested, UnknownRequest
from .first_step_limits import GROUP_CONCURRENCY
from .group_task_summary import make_group_summary_row


def run_controlled(control, task_id, generation, run_id, process, *, dependency_check=None):
    if not control.begin_run(task_id, generation, run_id):
        return
    if dependency_check is not None:
        try:
            dependency_check()
        except Exception:
            control.pause_for_nimble(task_id, generation)
            control.finish_run(task_id, generation, run_id)
            return
    with closing(control.summary._connect()) as db:
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if task is None:
            return
        library = control.source_library(task)

    def run_group(seq, path):
        if not control.group_allowed(task_id, seq, generation):
            raise PauseRequested()
        return process(seq, path)

    with ThreadPoolExecutor(max_workers=GROUP_CONCURRENCY, thread_name_prefix='first-step-group') as executor:
        running = {}
        while True:
            while len(running) < GROUP_CONCURRENCY:
                group = control.claim_next(task_id, generation)
                if group is None:
                    break
                seq, path = group
                running[executor.submit(run_group, seq, path)] = (seq, path)
            if not running:
                break
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                seq, path = running.pop(future)
                try:
                    item = future.result()
                except PauseRequested:
                    control.defer_group(task_id, seq, generation)
                    continue
                except UnknownRequest as exc:
                    control.mark_group_unknown(task_id, seq, generation, str(exc))
                    continue
                except Exception as exc:
                    item = {'group_root': path, 'error': '分类任务失败：' + type(exc).__name__}
                row, _, _ = make_group_summary_row(item, library)
                control.finish_group(task_id, seq, generation, row)
    control.finish_run(task_id, generation, run_id)


def run_first_step(control, task_id, generation, run_id, *, endpoint, receipt_root=None, history_roots=(), dependency_check=None):
    from .controlled_nimble import ControlledNimbleJudge
    from .group_classification import process_one_group
    from .group_sort_cli import _read_for_sort, _title_from_body
    receipt_root = Path(receipt_root) if receipt_root is not None else control.receipt_root
    with closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        library = control.source_library(task)
        output = control.artifact_root(task)
        source_boundary = task['source_root'] or str(library)
        conversion_root = output / 'conversion' if task['artifact_scoped'] else None

    def process(seq, path):
        from .first_step_registration import register_delivery
        group_scope = control.group_scope(task, path)
        reader = partial(_read_for_sort, work_root=conversion_root, task_id=task_id,
                         stop_requested=lambda: not control.group_allowed(task_id, seq, generation))
        item = process_one_group(path, library, output,
            lambda snapshot: ControlledNimbleJudge(control, task_id, seq, snapshot, generation,
                                                    endpoint=endpoint, group_scope=group_scope),
            reader, title_reader=_title_from_body, task_id=task_id, durable_journal=True,
            source_library=source_boundary,
            source_binder=lambda digest, group_id: control.bind_group_source(task_id, seq, digest, group_id),
            capture_saver=lambda snapshot, version, capture:
                control.save_evidence_capture(task_id, seq, snapshot, version, capture),
            capture_loader=lambda snapshot, version:
                control.load_evidence_capture(task_id, seq, snapshot, version))
        _, classification, _ = make_group_summary_row(item, library)
        if classification != 'passed':
            item['registration_status'] = 'not_eligible'
            return item
        try:
            receipt = register_delivery(item['delivery_dir'], receipt_root=receipt_root,
                history_roots=history_roots, lock_path=control.data / 'control-locks' / 'first-step-registration.lock',
                stop_requested=lambda: not control.group_allowed(task_id, seq, generation))
            body = next((row for row in receipt.get('items', []) if row.get('kind') == 'body'), None)
            if receipt.get('state') == '已接收' and receipt.get('paper_id') and body and body.get('state') == '已接收':
                item.update(registration_status='registered', receipt_path=receipt['record_path'],
                            receipt_id=receipt['receipt_id'], paper_id=receipt['paper_id'])
            else:
                reasons = [receipt.get('error')]
                for row in receipt.get('items', []):
                    reasons.append(row.get('error'))
                    reasons.extend(issue.get('reason') for issue in row.get('unverified_evidence', []))
                reason = next((str(value) for value in reasons if value), '交付资料未完整登记')
                item.update(registration_status='failed', registration_reason=reason[:500],
                            receipt_path=receipt.get('record_path'), receipt_id=receipt.get('receipt_id'),
                            paper_id=receipt.get('paper_id'))
        except (PauseRequested, UnknownRequest):
            raise
        except Exception as exc:
            item.update(registration_status='failed',
                        registration_reason=(str(exc) or type(exc).__name__)[:500])
        return item

    run_controlled(control, task_id, generation, run_id, process, dependency_check=dependency_check)
