"""One child FlowRun from registered paper to postprocessing receipt."""
import asyncio
from threading import Event

from paperflow_prefect.attachment_queue import settings_for_verified_receipt
from paperflow_prefect.llm_queue.folder_completion import expected_components
from pathlib import Path
from zipfile import BadZipFile

from paperflow_prefect.records import identity as fingerprint, read_json, sha_file
from paperflow_prefect.resources import drain_thread
from paperflow_prefect.unified import UnifiedBackend
from paperflow_v3.control.admission import admitted_slot, wait_for_resources
from paperflow_v3.control.stage_admission import FolderStage
from paperflow_v3.control.stage_queue import StageQueueWait, current_phase
from paperflow_v3.control.receipts import read_receipts, save_receipt
from .llm_handoff import build_postprocessing_inputs, saved_postprocessing_inputs
from .llm_operations import run_attachment, run_partitioned_body, run_single_body
from .postprocessing import process_folder
from .preprocessing import prepare_registered_folder
from .whole_paper_ranking import rank_whole_paper
from .gateway_admission import GatewayCapacityWait
from .work_inventory import stage_workspace, stage_work_files


class _FolderAdmission:
    """Stop new requests in this folder when either branch has failed."""
    def __init__(self, parent, failed):
        self.parent = parent
        self.failed = failed
        self.stop = parent.stop

    async def running(self):
        return not self.failed.is_set() and await self.parent.running()

    def running_sync(self):
        if self.failed.is_set():
            raise InterruptedError('本篇其他处理项失败，未开始新请求')
        return self.parent.running_sync()

    async def admit(self):
        if self.failed.is_set():
            raise InterruptedError('本篇其他处理项失败，未开始新请求')
        return await self.parent.admit()

    def admit_sync(self):
        if self.failed.is_set():
            raise InterruptedError('本篇其他处理项失败，未开始新请求')
        return self.parent.admit_sync()

    def wait_sync(self):
        if self.failed.is_set():
            raise InterruptedError('本篇其他处理项失败，未开始新请求')
        self.parent.wait_sync()

    async def wait(self):
        if self.failed.is_set():
            raise InterruptedError('本篇其他处理项失败，未开始新请求')
        await self.parent.wait()
        if self.failed.is_set():
            raise InterruptedError('本篇其他处理项失败，未开始新请求')


def _registered_files_match(registered):
    try:
        if sha_file(registered['receipt_path']) != registered['receipt_sha256']:
            return False
        paper = registered['paper']
        return all(sha_file(row['path']) == row['sha256'] for row in [paper['body'], *paper['attachments']])
    except (OSError, KeyError, TypeError):
        return False


async def process_paper(*, identity, registered, settings, connection, admission, limits,
                        catalog, paperflow_root, counter_factory, output_root, retry_id=None, opener=None,
                        first_step_work_root=None, records=None):
    """Run the existing per-paper scripts; return only a complete folder receipt.

    Caller supplies the actual parent/child identities and configured shared
    limits. RAG/NAS/cleanup are the next owner's stages, never called here.
    """
    for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'):
        if registered[key] != identity[key]:
            raise ValueError('登记资料不属于当前论文工单')
    work_root = stage_workspace(settings, identity, output_root)
    receipt_identity = dict(task_id=identity['task_id'], folder_id=identity['group_id'],
                            child_flow_run_id=identity['task_run_id'])
    if records is None:
        records = await read_receipts(identity['task_run_id'])
    native_post = bool((phase := current_phase()) and phase.names == tuple(sorted(limits['postprocessing'])))
    previous = next((row for row in records if row.get('kind') in
                     {'preprocessing_complete', 'preprocessing_failed'}), None)
    prepared = previous['data'] if previous and previous['kind'] == 'preprocessing_complete' else None
    if native_post and prepared is None:
        raise ValueError('后处理缺少已保存的前处理交接记录')
    if previous and previous['kind'] == 'preprocessing_failed':
        prior = previous['data']
        manual = next((row['data'] for row in records if row.get('kind') == 'manual_retry'), {})
        if (prior.get('retryable') is not True and
                (manual.get('stage') != 'preprocessing' or not manual.get('review_event_id')
                 or not previous.get('event_id')
                 or manual.get('failure_event_id') != previous['event_id'])):
            raise RuntimeError(prior.get('reason') or '前处理结果待核对，不能原样重试')
        if (not retry_id or retry_id == prior.get('processing_attempt_id')
                or manual.get('retry_id') != retry_id or manual.get('state') not in {'scheduled', 'running'}):
            raise RuntimeError('前处理失败，等待用户人工重试')

    def checked_manifest(result):
        if (result.get('status') != 'preprocessing_complete'
                or any(result.get(key) != identity[key] for key in
                ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))
                or sha_file(result['manifest_path']) != result['manifest_sha256']):
            raise ValueError('前处理交接记录待核对')
        manifest = read_json(result['manifest_path'])
        if (any(manifest.get(key) != identity[key] for key in
                ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))
                or manifest.get('body') != registered['paper']['body']
                or manifest.get('attachments') != registered['paper']['attachments']
                or manifest.get('receipt') != {'path': registered['receipt_path'],
                                               'sha256': registered['receipt_sha256']}):
            raise ValueError('前处理交接记录待核对')
        for reference in (manifest['receipt'], manifest['material'], manifest['reading_material']):
            if sha_file(reference['path']) != reference['sha256']:
                raise ValueError('前处理交接材料发生变化')
        if manifest.get('body_plan_revision'):
            from .body_plan_revision import validate_revision
            validate_revision(records, identity, manifest)
        return manifest

    async def save_preprocessing_failure(exc, code, retryable, attempt, event_id, plan_status=None):
        data = {**{key: identity[key] for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
            'processing_attempt_id': attempt, 'failure_reason': code, 'retryable': retryable,
            'reason': str(exc)[:500] or type(exc).__name__, 'error_type': type(exc).__name__}
        if plan_status is not None:
            data['plan_status'] = plan_status
        await save_receipt(**receipt_identity, event_id=event_id + '-failed',
                           kind='preprocessing_failed', data=data)

    if prepared is not None:
        try:
            manifest = checked_manifest(prepared)
            if manifest['capacity'].get('status') != 'planned':
                raise ValueError('正文容量或分段计划不可用')
            components = expected_components(manifest, identity['task_run_id'])
        except InterruptedError:
            raise
        except Exception as exc:
            event_id = identity['task_run_id'] + '-preprocessing-recheck'
            await save_preprocessing_failure(exc, 'preprocessing_unconfirmed', False,
                                             'recheck', event_id)
            raise
    else:
        attempt = retry_id or 'initial'
        event_id = identity['task_run_id'] + '-preprocessing-' + fingerprint(attempt)[:16]
        async with FolderStage(admission, identity, 'preprocessing',
                deferred=True, retry_id=retry_id, wait_resources=limits['preprocessing'],
                read_records=read_receipts) as stage, admitted_slot(stage, limits['preprocessing']):
            await save_receipt(**receipt_identity, event_id=event_id + '-started',
                kind='preprocessing_started', data={**{key: identity[key] for key in
                    ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
                    'processing_attempt_id': attempt})
            phase, plan_status = 'processing', None
            try:
                options = ({'first_step_work_root': first_step_work_root}
                           if first_step_work_root is not None else {})
                prepared = await drain_thread(lambda: prepare_registered_folder(
                    settings, registered, attempt_id=identity['attempt_id'], **options))
                phase = 'handoff'
                manifest = checked_manifest(prepared)
                plan_status = manifest['capacity'].get('status')
                if plan_status != 'planned':
                    raise ValueError('正文容量或分段计划不可用')
                components = expected_components(manifest, identity['task_run_id'])
            except InterruptedError:
                raise
            except Exception as exc:
                if plan_status is not None and plan_status != 'planned':
                    code, retryable = ('history_unconfirmed' if plan_status == 'existing_request_history'
                                       else 'plan_unavailable'), False
                elif isinstance(exc, OSError) and phase == 'processing' and _registered_files_match(registered):
                    code, retryable = 'local_io', True
                elif isinstance(exc, (BadZipFile, UnicodeDecodeError)):
                    code, retryable = 'local_parse', False
                else:
                    code, retryable = 'preprocessing_unconfirmed', False
                await save_preprocessing_failure(exc, code, retryable, attempt, event_id, plan_status)
                raise
            prepared = {**prepared, 'processing_attempt_id': attempt}
            try:
                await save_receipt(**receipt_identity, event_id=event_id + '-complete',
                                   kind='preprocessing_complete', data=prepared)
            except Exception:
                stage.mark_unknown('前处理完成回执未确认')
                raise
    paper = registered['paper']
    backend = UnifiedBackend(settings_for_verified_receipt(settings, registered), invoke=None)
    if manifest.get('body_plan_revision'):
        from .body_plan_revision import bind_revision_backend
        backend = bind_revision_backend(backend, identity, manifest, paper, records)

    async def finish(inputs):
        files = []

        async def save_work_inventory():
            nonlocal files
            owned_paths = [backend.paper_root(paper), manifest['body_work'], manifest['reading_material']['path'],
                           Path(output_root) / fingerprint(identity)]
            owned_paths.extend(backend.attachment_work(paper, row) for row in paper['attachments'])
            files = stage_work_files(work_root, owned_paths)
            await save_receipt(**receipt_identity, event_id=identity['task_run_id'] + '-stage-work-inventory',
                               kind='stage_work_inventory', data={'identity': identity, 'files': files})

        receipt = await process_folder(inputs=inputs, catalog=catalog, paperflow_root=paperflow_root,
            counter_factory=counter_factory, output_root=output_root, admission=admission,
            limit_names=limits['postprocessing'], retry_id=retry_id, before_complete=save_work_inventory)
        return {**receipt, 'stage_work_files': files}

    if native_post:
        inputs = saved_postprocessing_inputs(identity=identity, manifest=manifest, paper=paper,
                                              backend=backend, records=records)
        return await finish(inputs)

    llm_resources = limits['body']
    if phase := current_phase():
        if phase.names in {tuple(sorted(limits['body'])), tuple(sorted(limits['attachment']))}:
            llm_resources = phase.names
    if any(row.get('kind') == 'llm_folder_complete' and row.get('data', {}).get('ranking_complete') is True
           for row in records):
        llm_resources = limits['postprocessing']
    async with FolderStage(admission, identity, 'llm',
            deferred=True, retry_id=retry_id, wait_resources=llm_resources,
            read_records=read_receipts) as stage:
        failure = Event()
        folder_admission = _FolderAdmission(stage, failure)
        await wait_for_resources(folder_admission, llm_resources)

        async def body():
            try:
                if manifest['capacity']['plan_identity']['mode'] == 'single':
                    component_id = next(row['component_id'] for row in components if row['kind'] == 'body')
                    return await run_single_body(identity=identity, manifest=manifest, paper=paper, backend=backend,
                        connection=connection, admission=folder_admission, limit_names=limits['body'],
                        request_id=component_id, opener=opener, retry_id=retry_id)
                async def partitioned():
                    await run_partitioned_body(identity=identity, manifest=manifest, paper=paper, backend=backend,
                        connection=connection, admission=folder_admission, limit_names=limits['body'],
                        opener=opener, retry_id=retry_id)
                saved_body = {row.get('data', {}).get('component_id') for row in records
                              if row.get('kind') == 'llm_response_saved'}
                needs_body = any(row['kind'] == 'body' and row['component_id'] not in saved_body for row in components)
                if current_phase() and needs_body:
                    # Acquire before the partition helper creates any request
                    # journal; its serial inner leases reuse this same grant.
                    async with admitted_slot(folder_admission, limits['body']):
                        await partitioned()
                else:
                    await partitioned()
                return await rank_whole_paper(identity=identity, manifest=manifest, backend=backend, catalog=catalog,
                    connection=connection, admission=folder_admission, limit_names=limits['body'],
                    opener=opener, retry_id=retry_id)
            except StageQueueWait:
                raise
            except GatewayCapacityWait as wait:
                if current_phase():
                    raise StageQueueWait(limits['body'], wait.seconds) from None
                failure.set()
                raise
            except Exception:
                failure.set()
                raise

        async def attachments():
            try:
                attachment_components = {row['file_id']: row['component_id'] for row in components
                                         if row['kind'] == 'attachment'}
                for row in paper['attachments']:
                    await run_attachment(identity=identity, manifest=manifest, paper=paper, row=row, backend=backend,
                        connection=connection, admission=folder_admission, limit_names=limits['attachment'],
                        request_id=attachment_components[row['file_id']], opener=opener, retry_id=retry_id)
            except StageQueueWait:
                raise
            except GatewayCapacityWait as wait:
                if current_phase():
                    raise StageQueueWait(limits['attachment'], wait.seconds) from None
                failure.set()
                raise
            except Exception:
                failure.set()
                raise

        results = await asyncio.gather(body(), attachments(), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception) and not isinstance(result, (InterruptedError, GatewayCapacityWait, StageQueueWait)):
                raise result
        for result in results:
            if isinstance(result, StageQueueWait):
                raise result
        for result in results:
            if isinstance(result, GatewayCapacityWait):
                raise result
        for result in results:
            if isinstance(result, Exception):
                raise result
        try:
            await stage.ensure_admitted()
            inputs = await build_postprocessing_inputs(identity=identity, manifest=manifest, paper=paper, backend=backend)
        except Exception:
            stage.mark_unknown('LLM文件夹交接结果未确认')
            raise

    return await finish(inputs)
