"""Independent Prefect entry point for local first-step classification only."""
from pathlib import Path
from functools import partial
import os
import uuid

from contextlib import closing

from prefect import flow, get_run_logger
from prefect.flow_runs import suspend_flow_run
from prefect.runtime import flow_run
from prefect.task_runners import ThreadPoolTaskRunner

from paperflow_prefect.first_step_limits import GROUP_CONCURRENCY
from paperflow_prefect.group_classification import MAX_GROUPS, classify_batch
from paperflow_prefect.group_sort_cli import _read_for_sort, _title_from_body
from paperflow_prefect.group_task_summary import summary_path
from paperflow_prefect.group_summary_store import GroupSummaryStore
from paperflow_prefect.nimble_group import NimbleJudge
from paperflow_prefect.records import read_json
from paperflow_prefect.settings import Settings


@flow(name='筛选空文件和资料分类', retries=0, persist_result=False,
      task_runner=ThreadPoolTaskRunner(max_workers=GROUP_CONCURRENCY))
def screen_and_classify(task_id: str, generation: int, run_id: str):
    """Run the existing first step as one task beneath the page task order."""
    from paperflow_prefect.first_step_control import FirstStepControl
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.first_step_registration import registration_history_roots
    from paperflow_prefect.nimble_dependency import ensure_nimble
    settings = Settings.load()
    control = FirstStepControl(settings.path('classification_root'), settings.path('data_root'),
                               receipt_root=settings.path('receipt_root'),
                               nimble_lock_path=settings.values.get('nimble_lock_path'))
    settings.require_local_classification()
    endpoint = settings.values.get('nimble_endpoint', 'http://127.0.0.1:11435/v1/systemone')
    nimble_root = next((parent / 'prefect-first-step-independent' for parent in Path(__file__).resolve().parents
                        if (parent / 'prefect-first-step-independent' / 'start-nimble.ps1').is_file()), None)
    if nimble_root is None:
        raise RuntimeError('找不到第一步专用 Nimble 启动入口')
    run_first_step(control, task_id, generation, run_id,
                   endpoint=endpoint,
                   receipt_root=settings.path('receipt_root'),
                   history_roots=registration_history_roots(settings.path('data_root'),
                                                            settings.values.get('history_roots', [])),
                   dependency_check=partial(ensure_nimble, endpoint, nimble_root, nimble_root / 'start-nimble.ps1',
                                            is_needed=lambda: control.run_is_requested(task_id, generation)))


@flow(name='论文第一步-任务工单候选', retries=0, persist_result=True)
def controlled_first_step(task_id: str, generation: int | None = None):
    """Keep one resumable parent flow for the lifetime of one page task."""
    from paperflow_prefect.first_step_control import FirstStepControl
    settings = Settings.load()
    if os.environ.get('PREFECT_API_URL', '').rstrip('/') != settings['prefect_api_url'].rstrip('/'):
        raise RuntimeError('第一步执行器与任务工单的 Prefect 服务不一致')
    control = FirstStepControl(settings.path('classification_root'), settings.path('data_root'),
                               receipt_root=settings.path('receipt_root'),
                               nimble_lock_path=settings.values.get('nimble_lock_path'))
    run_id = str(flow_run.id or '')
    if not run_id:
        raise RuntimeError('任务工单身份不可用')
    try:
        with closing(control.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            current = control._task(db, task_id)
            if current['prefect_run_id'] not in (None, run_id):
                raise RuntimeError('当前任务绑定了不同的大工单')
            db.execute('UPDATE tasks SET prefect_suspend_pending=0 WHERE task_id=?', (task_id,))
        while True:
            with closing(control.summary._connect()) as db:
                current = control._task(db, task_id)
                if current['prefect_run_id'] not in (None, run_id):
                    raise RuntimeError('当前任务绑定了不同的大工单')
                active_generation = current['start_generation']
                should_run = (current['desired_state'] == 'running'
                              and current['task_status'] not in {'complete', 'needs_review', 'failed', 'deleting', 'delete_failed'})
            if should_run:
                screen_and_classify(task_id, active_generation, run_id)
            # The transaction records which pause Start must verify remotely.
            with closing(control.summary._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                current = control._task(db, task_id)
                if (current['desired_state'] == 'running'
                        and current['start_generation'] != active_generation):
                    continue
                db.execute('UPDATE tasks SET prefect_suspend_pending=1, '
                           'prefect_suspend_sequence=prefect_suspend_sequence+1 WHERE task_id=?', (task_id,))
            suspend_flow_run(key='first-step-pause-' + uuid.uuid4().hex)
    except Exception as exc:
        control.mark_unknown(task_id, '本地分类执行失败：' + type(exc).__name__,
                             generation=locals().get('active_generation'))
        raise


@flow(name='论文第一步-本地预处理', retries=0, persist_result=True)
def first_step(person_folders: list[str] | None = None,
               library_root: str | None = None,
               verified_new_groups: list[str] | None = None,
               known_paper_ids: dict[str, str] | None = None):
    """Classify explicitly supplied group folders without starting later stages."""
    settings = Settings.load()
    groups = person_folders or []
    output = settings.path('classification_root')
    task_id = str(flow_run.id or uuid.uuid4())
    try:
        initial_total = (len({os.path.normcase(os.path.abspath(p)) for p in groups})
                         if isinstance(groups, list) and all(isinstance(p, str) for p in groups) else 0)
        GroupSummaryStore(output).create_task(task_id, initial_total)
        if not isinstance(groups, list) or not groups or len(groups) > MAX_GROUPS or not all(isinstance(p, str) for p in groups):
            raise ValueError('请提供 1 至 30000 个明确的资料组目录')
        library = Path(library_root).resolve() if library_root else settings.path('library_root')
        classify_batch(groups, library, output,
                       lambda: NimbleJudge(endpoint=settings.values.get('nimble_endpoint',
                                                'http://127.0.0.1:11435/v1/systemone')),
                       _read_for_sort, title_reader=_title_from_body,
                       known_paper_ids=known_paper_ids or {},
                       verified_new_groups=verified_new_groups or [], task_id=task_id)
        summary = read_json(summary_path(output, task_id))
        result = {key: summary[key] for key in ('task_id', 'status', 'total_groups', 'passed_groups',
                  'failed_groups', 'review_groups', 'not_passed_groups', 'waiting_groups')}
        result['summary_api'] = '/api/v1/first-step/tasks/' + task_id
        result['groups_api'] = result['summary_api'] + '/groups'
        get_run_logger().info('第一步结束：总组数 %s，通过 %s，失败 %s，待核对 %s',
                              result['total_groups'], result['passed_groups'],
                              result['failed_groups'], result['review_groups'])
        return result
    except Exception:
        try:
            GroupSummaryStore(output).mark_failed(task_id, len(groups) if isinstance(groups, list) else 0)
        except Exception:
            pass  # Preserve the original flow error if the local index is unavailable.
        raise
