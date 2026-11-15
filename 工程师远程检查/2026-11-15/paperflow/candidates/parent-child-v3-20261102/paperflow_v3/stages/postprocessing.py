"""Postprocess one folder, retaining each completed body or attachment."""
from contextlib import closing
import json
from pathlib import Path
from uuid import uuid4

from paperflow_prefect.records import identity as fingerprint, sha_file
from paperflow_prefect.resources import drain_thread
from paperflow_v3.control.admission import admitted_slot, wait_for_resources
from paperflow_v3.control.stage_admission import FolderStage
from paperflow_v3.control.receipts import read_receipts, save_receipt
from .attachment_adapter import process_attachment, _inputs, _safe_output
from .body_adapter import process_body, _output_boundary
from .folder_processor import _attachment_evidence, _check_inputs, _package_evidence
from .recovery import PostprocessingResourceWait


def _process_body(item, output, *, identity, catalog, paperflow_root, counter_factory, counter_settings=None):
    """Keep the offline counter and its complete lifecycle on this worker."""
    from paperflow.embedding_recovery import CounterProcessExited
    counter = None
    try:
        with closing(counter_factory()) as counter:
            return process_body(item, output, expected_identity=identity, catalog=catalog,
                                paperflow_root=paperflow_root, counter=counter)
    except (CounterProcessExited, MemoryError) as exc:
        if counter_settings is None:
            counter_settings = getattr(counter_factory, 'recovery_settings', lambda: None)()
        if counter_settings is not None:
            exc.counter_recovery = {'settings': {'command': counter_settings['command']},
                                    'runtime': getattr(counter, 'runtime', {})}
        raise


async def process_folder(*, inputs, catalog, paperflow_root, counter_factory, output_root,
                         admission, limit_names, retry_id=None, before_complete=None, counter_settings=None):
    """Use the real body and attachment adapters under one child FlowRun.

    ``inputs`` is the verified result of ``build_postprocessing_inputs``.
    ``retry_id`` is supplied only for an explicit user retry of this paper.
    Each completed component is persisted before another component starts.
    """
    await wait_for_resources(admission, limit_names)
    from paperflow.embedding_recovery import CounterProcessExited
    identity = inputs['identity']
    manifest_sha256 = inputs['manifest_sha256']
    _check_inputs(inputs, identity, manifest_sha256)
    folder = Path(output_root).absolute() / fingerprint(identity)
    _output_boundary(inputs['body'], folder)
    for item in inputs['attachments']:
        row, _, _, _, work, _ = _inputs(identity, item)
        _safe_output(folder, work, row)

    receipt_identity = dict(task_id=identity['task_id'], folder_id=identity['group_id'],
                            child_flow_run_id=identity['task_run_id'])
    async with FolderStage(admission, identity, 'postprocessing',
            deferred=True, retry_id=retry_id, wait_resources=limit_names,
            read_records=read_receipts) as stage:
        records = await read_receipts(identity['task_run_id'])
        parts = [('body', inputs['body'])] + [
            ('attachment:' + item['attachment_id'], item) for item in inputs['attachments']]
        results = {}
        for component_id, item in parts:
            events = [row for row in records if row.get('kind') in {
                'postprocessing_component_started', 'postprocessing_component_not_started',
                'postprocessing_component_failed', 'postprocessing_component_complete',
                'postprocessing_component_waiting'}
                and row.get('data', {}).get('component_id') == component_id]
            latest = events[0] if events else None
            if latest and latest['kind'] == 'postprocessing_component_complete':
                data = latest['data']
                result_path = Path(data['result_path'])
                if not result_path.is_file() or sha_file(result_path) != data['result_sha256']:
                    raise ValueError('已完成的后处理结果待核对')
                results[component_id] = (json.loads(result_path.read_text(encoding='utf8')), result_path.parent)
                continue
            if latest and latest['kind'] == 'postprocessing_component_started':
                raise RuntimeError('后处理结果待核对，不能自动重做')
            if latest and latest['kind'] == 'postprocessing_component_failed':
                failed_attempt = latest['data'].get('processing_attempt_id') or latest['data']['attempt_id']
                if not retry_id or retry_id == failed_attempt:
                    raise RuntimeError('后处理失败，等待用户人工重试')
                attempt_id = retry_id
            elif latest and latest['kind'] == 'postprocessing_component_not_started':
                attempt_id = latest['data']['attempt_id']
            elif latest and latest['kind'] == 'postprocessing_component_waiting':
                recovery = latest['data'].get('counter_recovery')
                if recovery:
                    from paperflow.embedding_recovery import helper_exited
                    if not await drain_thread(lambda: helper_exited(recovery['settings'], recovery['runtime'])):
                        raise PostprocessingResourceWait('原计数进程退出尚未确认，继续等待恢复')
                attempt_id = latest['data']['next_attempt_id']
            else:
                attempt_id = 'initial'

            component_dir = (folder / 'body' if component_id == 'body' else
                             folder / 'attachments' / fingerprint(component_id))
            output = component_dir / fingerprint([component_id, attempt_id])[:24]
            data = {'component_id': component_id, 'attempt_id': attempt_id, 'output_dir': str(output),
                    'processing_attempt_id': retry_id or 'initial'}
            event_id = identity['task_run_id'] + '-' + fingerprint([component_id, attempt_id])[:20]
            async with admitted_slot(stage, limit_names):
                await save_receipt(**receipt_identity, event_id=event_id + '-start',
                                   kind='postprocessing_component_started', data=data)
                if not await stage.running():
                    await save_receipt(**receipt_identity, event_id=event_id + '-not-started',
                                       kind='postprocessing_component_not_started', data=data)
                    raise InterruptedError('大工单已暂停，未开始下一个后处理操作')
                resource_wait = None
                try:
                    if component_id == 'body':
                        result = await drain_thread(lambda: _process_body(item, output, identity=identity,
                            catalog=catalog, paperflow_root=paperflow_root, counter_factory=counter_factory,
                            counter_settings=counter_settings))
                        result_path = output / 'body-result.json'
                    else:
                        result = await drain_thread(lambda: process_attachment(identity, item, output))
                        result_path = output / 'receipt.json'
                except (CounterProcessExited, MemoryError) as exc:
                    resource_wait = {**data, 'error_type': type(exc).__name__, 'reason': str(exc),
                                     'next_attempt_id': 'resource-' + str(uuid4())}
                    if getattr(exc, 'counter_recovery', None):
                        resource_wait['counter_recovery'] = exc.counter_recovery
                    await save_receipt(**receipt_identity, event_id=event_id + '-waiting',
                                       kind='postprocessing_component_waiting', data=resource_wait)
                except Exception as exc:
                    failure = {**data, 'error_type': type(exc).__name__, 'reason': str(exc)}
                    if getattr(exc, 'diagnostic', None):
                        failure['diagnostic'] = exc.diagnostic
                    await save_receipt(**receipt_identity, event_id=event_id + '-failed',
                                       kind='postprocessing_component_failed', data=failure)
                    raise
                if resource_wait is not None:
                    # Leave the resource exception's traceback and counter behind;
                    # the caller waits only after both slot and stage have exited.
                    raise PostprocessingResourceWait(resource_wait['reason'])
                completed = {**data, 'result_path': str(result_path), 'result_sha256': sha_file(result_path)}
                await save_receipt(**receipt_identity, event_id=event_id + '-complete',
                                   kind='postprocessing_component_complete', data=completed)
                results[component_id] = (result, output)

        await stage.ensure_admitted()
        body_result, body_output = results['body']
        artifacts = _package_evidence(body_result, body_output, identity)
        deliveries = []
        for item in inputs['attachments']:
            result, output = results['attachment:' + item['attachment_id']]
            references, delivery = _attachment_evidence(result, output, item, identity)
            artifacts.extend(references)
            deliveries.append(delivery)
        receipt = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': identity,
            'manifest_sha256': manifest_sha256, 'claim_id': identity['task_run_id'],
            'validation_status': 'passed',
            'body': {'package_path': body_result['package_path'], 'manifest_sha256': body_result['manifest_sha256']},
            'attachments': deliveries, 'artifacts': artifacts}
        try:
            if before_complete:
                await before_complete()
            await save_receipt(**receipt_identity, event_id=identity['task_run_id'] + '-postprocessing-complete',
                               kind='postprocessing_complete', data=receipt)
        except Exception:
            stage.mark_unknown('后处理完成回执未确认')
            raise
        return receipt
