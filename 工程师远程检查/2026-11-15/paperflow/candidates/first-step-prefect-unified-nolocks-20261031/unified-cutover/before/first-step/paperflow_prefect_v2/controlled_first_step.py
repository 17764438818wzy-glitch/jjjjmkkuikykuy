"""Submit each first-step group as an individual Prefect task."""
from concurrent.futures import FIRST_COMPLETED, wait
from contextlib import closing
from functools import partial
from pathlib import Path

from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.states import Cancelled, Completed, Failed

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
        task_row = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if task_row is None:
            return
        library = control.source_library(task_row)

    @task(name='资料组筛选分类', task_run_name='资料组-{seq}', retries=0,
          cache_policy=NO_CACHE, persist_result=False)
    def run_group(seq):
        # A queued Prefect task is not processing yet. Claim only when this
        # particular task starts, so a pause leaves queued groups pending.
        group = control.claim_next(task_id, generation, seq=seq)
        if group is None:
            return Cancelled(name='未领取', message='任务已暂停或资料组状态已变化')
        _, path = group
        try:
            if not control.group_allowed(task_id, seq, generation):
                raise PauseRequested()
            item = process(seq, path)
        except PauseRequested:
            control.defer_group(task_id, seq, generation)
            return Cancelled(name='已暂停', message='资料组未处理完，已保留待处理状态')
        except UnknownRequest as exc:
            control.mark_group_unknown(task_id, seq, generation, str(exc))
            return Failed(name='结果待核对', message='模型请求结果未知，未自动重发')
        except Exception as exc:
            item = {'group_root': path, 'error': '分类任务失败：' + type(exc).__name__}
        row, _, _ = make_group_summary_row(item, library)
        control.finish_group(task_id, seq, generation, row)
        if row['status'] == 'failed':
            return Failed(name='分类失败', message=row.get('reason') or '资料组处理失败')
        if row.get('registration_status') == 'failed':
            return Failed(name='登记失败', message=row.get('registration_reason') or '资料登记未完成')
        if row['status'] == 'unrecognized':
            return Completed(name='待核对', message=row.get('reason') or '资料组需要核对')
        return Completed(name='已登记' if row.get('registration_status') == 'registered' else '分类完成')

    with closing(control.summary._connect()) as db:
        pending = [row[0] for row in db.execute(
            "SELECT seq FROM control_groups WHERE task_id=? AND state='pending' ORDER BY seq", (task_id,))]
    pending = iter(pending)
    futures = {}
    exhausted = False
    failures = []

    def submit_available():
        nonlocal exhausted
        while not exhausted and len(futures) < GROUP_CONCURRENCY and not failures:
            if not control.run_is_requested(task_id, generation):
                return
            seq = next(pending, None)
            if seq is None:
                exhausted = True
                return
            future = run_group.submit(seq)
            futures[future.wrapped_future] = future

    submit_available()
    while futures:
        completed, _ = wait(futures, return_when=FIRST_COMPLETED)
        for wrapped in completed:
            future = futures.pop(wrapped)
            try:
                future.wait()
                state = future.state
                expected_failure = state.is_failed() and state.name in {'结果待核对', '分类失败', '登记失败'}
                expected_stop = state.is_cancelled() and state.name in {'未领取', '已暂停'}
                if not (state.is_completed() or expected_failure or expected_stop):
                    failures.append(RuntimeError('Prefect 资料组任务异常：' + str(state.message or state.name)))
            except Exception as exc:
                failures.append(exc)
        submit_available()
    if failures:
        raise failures[0]
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
