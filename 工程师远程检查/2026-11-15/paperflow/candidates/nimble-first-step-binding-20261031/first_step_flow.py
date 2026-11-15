"""Independent Prefect entry point for local first-step classification only."""
from pathlib import Path
from functools import partial
import os
import uuid

from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from paperflow_prefect.group_classification import MAX_GROUPS, classify_batch
from paperflow_prefect.group_sort_cli import _read_for_sort, _title_from_body
from paperflow_prefect.group_task_summary import summary_path
from paperflow_prefect.group_summary_store import GroupSummaryStore
from paperflow_prefect.nimble_group import NimbleJudge
from paperflow_prefect.records import read_json
from paperflow_prefect.settings import Settings


@flow(name='论文第一步-可控预处理', retries=0, persist_result=True)
def controlled_first_step(task_id: str, generation: int):
    """Run only an explicitly started first-step workspace generation."""
    from paperflow_prefect.first_step_control import FirstStepControl
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.first_step_registration import registration_history_roots
    from paperflow_prefect.nimble_dependency import ensure_nimble
    settings = Settings.load()
    control = FirstStepControl(settings.path('classification_root'), settings.path('data_root'),
                               receipt_root=settings.path('receipt_root'))
    try:
        settings.require_local_classification()
        endpoint = settings.values.get('nimble_endpoint', 'http://127.0.0.1:11435/v1/systemone')
        nimble_root = Path(__file__).resolve().parents[2] / 'prefect-first-step-independent'
        run_first_step(control, task_id, generation, str(flow_run.id or uuid.uuid4()),
                       endpoint=endpoint,
                       receipt_root=settings.path('receipt_root'),
                       history_roots=registration_history_roots(settings.path('data_root'),
                                                                settings.values.get('history_roots', [])),
                       dependency_check=partial(ensure_nimble, endpoint, nimble_root,
                                                nimble_root / 'start-nimble.ps1',
                                                is_needed=lambda: control.run_is_requested(task_id, generation)))
        return control.summary.read(task_id, None, 1, 1)
    except Exception as exc:
        control.mark_unknown(task_id, '本地分类执行失败：' + type(exc).__name__, generation=generation)
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
