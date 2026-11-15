"""Dispatch an explicitly started first-step task, four groups at a time."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing
from functools import partial
from pathlib import Path

from .first_step_control import PauseRequested, UnknownRequest
from .group_task_summary import make_group_summary_row


def run_controlled(control, task_id, generation, run_id, process):
    if not control.begin_run(task_id, generation, run_id):
        return
    with closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        library = control.source_library(task)

    def run_group(seq, path):
        if not control.group_allowed(task_id, seq, generation):
            raise PauseRequested()
        return process(seq, path)

    with ThreadPoolExecutor(max_workers=4, thread_name_prefix='first-step-group') as executor:
        running = {}
        while True:
            while len(running) < 4:
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


def run_first_step(control, task_id, generation, run_id, *, endpoint):
    from .controlled_nimble import ControlledNimbleJudge
    from .group_classification import process_one_group
    from .group_sort_cli import _read_for_sort, _title_from_body
    with closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        library = control.source_library(task)
        output = control.artifact_root(task)
        source_boundary = task['source_root'] or str(library)
        reader = partial(_read_for_sort, work_root=output / 'conversion') if task['artifact_scoped'] else _read_for_sort

    def process(seq, path):
        group_scope = control.group_scope(task, path)
        return process_one_group(path, library, output,
            lambda snapshot: ControlledNimbleJudge(control, task_id, seq, snapshot, generation,
                                                    endpoint=endpoint, group_scope=group_scope),
            reader, title_reader=_title_from_body, task_id=task_id, durable_journal=True,
            source_library=source_boundary,
            source_binder=lambda digest, group_id: control.bind_group_source(task_id, seq, digest, group_id),
            capture_saver=lambda snapshot, version, capture:
                control.save_evidence_capture(task_id, seq, snapshot, version, capture),
            capture_loader=lambda snapshot, version:
                control.load_evidence_capture(task_id, seq, snapshot, version))

    run_controlled(control, task_id, generation, run_id, process)
