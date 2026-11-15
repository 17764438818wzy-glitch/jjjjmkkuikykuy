"""Runtime bridge from one existing folder FlowRun to the LLM-only stage."""
from contextlib import closing
import sqlite3
import sys
from uuid import UUID

from .flow_executor import execute_folder
from .flow_model_operations import FolderModelOperations
from .folder_ledger import FolderLedger
from .pause_admission import AdmissionGuard, ControlStop
from .upstream_flow_binding import UpstreamFlowBinding


def run_live_llm_folder(settings, task_id, group_id, receipt_id, attempt_id, folder_run_id,
                        *, execution_task_run_id=None, execution_task_key=None,
                        expected_generation=None, allow_terminal_retry=False):
    from prefect.client.orchestration import SyncPrefectClient
    from paperflow_prefect import legacy
    from paperflow_prefect.attachment_queue import settings_for_verified_receipt
    from paperflow_prefect.local_preprocessing import _control_paths, _queue, NAMESPACE, FIRST_STEP_CONFIG
    from paperflow_prefect.records import read_json
    from paperflow_prefect.registered_input import verify_registered_receipt
    from paperflow_prefect.resources import cancellable_concurrency

    task_id, folder_run_id = str(UUID(task_id)), str(UUID(folder_run_id))
    database, lock_root = _control_paths()
    queue = _queue()
    handoff = queue.completed_preprocessing('prefect-local-preprocessing', attempt_id)
    if (not handoff or any(handoff.get(key) != value for key, value in (
            ('task_id', task_id), ('group_id', group_id), ('receipt_id', receipt_id), ('attempt_id', attempt_id)))):
        raise ValueError('文件夹前处理交接身份不一致')
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        task = db.execute('SELECT prefect_run_id,prefect_order_model,desired_state,start_generation,start_pending '
                          'FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    if not task or task['prefect_order_model'] != 'v2' or not task['prefect_run_id']:
        raise ValueError('文件夹所属任务不可核验')
    generation = task['start_generation']
    if (task['desired_state'] != 'running' or task['start_pending'] or type(generation) is not int
            or (expected_generation is not None and generation != expected_generation)):
        return {'status': 'paused'}
    stop = ControlStop(database, task_id, generation)
    if stop.is_set():
        return {'status': 'paused'}
    identity = {'task_id': task_id, 'group_id': group_id, 'paper_id': handoff['paper_id'],
                'receipt_id': receipt_id, 'attempt_id': attempt_id, 'parent_run_id': task['prefect_run_id'],
                # The ledger retains this historic column name for the UUID of
                # the actual running folder flow, never a fabricated TaskRun.
                'task_run_id': folder_run_id}
    api_url = str(settings['prefect_api_url']).rstrip('/')
    first_step_api = str(read_json(FIRST_STEP_CONFIG).get('prefect_api_url', '')).rstrip('/')
    if not api_url or api_url != first_step_api:
        raise ValueError('文件夹工单与新 Prefect 地址不一致')
    guard, ledger = AdmissionGuard(database, lock_root), FolderLedger(database)
    guard.initialize()
    ledger.initialize()
    current_session = sys.modules[NAMESPACE + '.local_queue']._current_receiver_session
    with SyncPrefectClient(api_url, httpx_settings={'timeout': 3, 'trust_env': False}) as client:
        binding = UpstreamFlowBinding(queue.path, database, client, api_url, queue.consumer_id,
                                      verify_registered_receipt, receiver_session=current_session,
                                      execution_task_run_id=execution_task_run_id,
                                      execution_task_key=execution_task_key,
                                      allow_terminal_retry=allow_terminal_retry)
        verified = binding.load(identity, require_running=True)
        scoped = settings_for_verified_receipt(settings, verified['registered'])
        from paperflow_prefect.unified import UnifiedBackend
        backend = UnifiedBackend(scoped, invoke=None)
        if stop.is_set():
            return {'status': 'paused'}
        connection = legacy.connection(scoped)
        operations = FolderModelOperations(identity, generation, binding, backend, scoped, guard, stop,
                                            connection, api_url, cancellable_concurrency)
        try:
            result = execute_folder(identity, generation, ledger, binding, operations, guard, stop)
        except InterruptedError:
            if stop.is_set():
                return {'status': 'paused'}
            raise
    return {'status': 'waiting_postprocessing', 'task_id': task_id, 'group_id': group_id,
            'folder_run_id': folder_run_id, 'ledger_state': result['state']}
