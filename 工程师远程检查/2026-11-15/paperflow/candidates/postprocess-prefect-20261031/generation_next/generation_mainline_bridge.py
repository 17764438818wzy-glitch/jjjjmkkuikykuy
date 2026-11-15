"""Candidate connection to the existing folder handoff, without dispatch.

This entrypoint is deliberately not registered or invoked by Prefect. A future
caller must hold its existing folder execution ownership, supply the upstream
binding and stage store using the SAME pause/delete admission gate, and acquire
the agreed Prefect capacity separately. This module never starts such work.
"""
from contextlib import closing
from pathlib import Path
import sqlite3

from folder_processor import make_existing_receipt_validator, process_folder, reconcile_folder
from handoff_inputs import build_inputs


IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
            'parent_run_id', 'task_run_id')


def _snapshot(database, identity):
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        queue = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                           (identity['task_id'], identity['group_id'])).fetchone()
        if (not queue or queue['state'] != 'waiting_postprocessing'
                or any(queue[key] != identity[key] for key in IDENTITY)):
            raise ValueError('上游文件夹工单或 LLM 完成状态不可核验')
        rows = [dict(row) for row in db.execute('SELECT * FROM llm_request_admissions '
            'WHERE task_id=? AND task_run_id=?', (identity['task_id'], identity['task_run_id']))]
        return dict(queue), rows


def process_mainline_folder(*, identity, generation, control_database, binding, backend, store,
                            catalog, paperflow_root, counter, output_root,
                            expected_components=None, inspect_folder=None,
                            observe_request=None, frozen_plan=None, reconcile_claim_id=None,
                            accepted_hook=None, validator_only=False):
    """Run one verified folder using the selected upstream's business material."""
    database = Path(control_database).resolve(strict=True)
    if (not callable(getattr(store, 'admission_gate', None))
            or not callable(getattr(store, 'observe_run', None))
            or Path(getattr(store, 'database', '')).resolve() != database):
        raise ValueError('文件夹状态与暂停删除控制未绑定')
    if expected_components is None or inspect_folder is None:
        from paperflow_prefect.llm_queue.folder_completion import expected_components, inspect_folder
    if observe_request is None:
        from preextract_scheduler_adapter import request_observation as observe_request
    if frozen_plan is None:
        from body_adapter import frozen_plan_evidence as frozen_plan
    ledger, _ = _snapshot(database, identity)
    # This is the ledger's canonical JSON identity, not the same-named file-byte
    # SHA256 carried in the original preprocessing handoff record.
    manifest_sha256 = ledger['manifest_sha256']
    admission_check = reconcile_claim_id is None

    def load_inputs():
        nonlocal admission_check
        current, admissions = _snapshot(database, identity)
        if current['manifest_sha256'] != manifest_sha256:
            raise ValueError('文件夹交接身份发生变化')
        # A new stage must begin under a real running folder FlowRun. During
        # final read-only evidence checks the flow may already be suspended by
        # a pause; its identity still has to be verified, and the stage store
        # decides whether to retain the receipt or mark it waiting for RAG.
        verified = binding.load(identity, require_running=admission_check)
        inputs = build_inputs(identity=identity, verified=verified, admissions=admissions,
            manifest_sha256=manifest_sha256, expected_components=expected_components,
            inspect_folder=inspect_folder, observe_request=observe_request, frozen_plan=frozen_plan,
            attachment_work=backend.attachment_work)
        admission_check = False
        return inputs

    def body_processor(evidence, output):
        from body_adapter import process_body
        return process_body(evidence, output, expected_identity=identity, catalog=catalog,
                            paperflow_root=paperflow_root, counter=counter)

    def attachment_processor(ticket, handoff, output):
        from generation_attachment_adapter import process_attachment
        return process_attachment(ticket, handoff, output)

    def output_guard(inputs, output):
        from body_adapter import _output_boundary
        from generation_attachment_adapter import _inputs, _safe_output
        _output_boundary(inputs['body'], output)
        for item in inputs['attachments']:
            row, _, _, _, work, _ = _inputs(identity, item)
            _safe_output(output, work, row)

    options = dict(identity=identity, generation=generation, manifest_sha256=manifest_sha256,
        store=store, load_inputs=load_inputs, body_processor=body_processor,
        attachment_processor=attachment_processor, output_root=output_root, output_guard=output_guard)
    if validator_only:
        if not isinstance(reconcile_claim_id, str) or not reconcile_claim_id or accepted_hook is not None:
            raise ValueError('只读回执复核参数不完整')
        validate_options = dict(options)
        validate_options.pop('generation')
        validate_options.pop('store')
        return make_existing_receipt_validator(claim_id=reconcile_claim_id, **validate_options)
    if reconcile_claim_id is not None:
        if not isinstance(reconcile_claim_id, str) or not reconcile_claim_id:
            raise ValueError('原后处理领取身份不可核验')
        result = reconcile_folder(claim_id=reconcile_claim_id, **options)
    else:
        result = process_folder(**options)
    if accepted_hook is not None:
        if not callable(accepted_hook) or not isinstance(result, dict) or not result.get('claim_id'):
            raise ValueError('后处理通过结果或验收回调不可核验')
        validate_options = dict(options)
        validate_options.pop('generation')
        validate_options.pop('store')
        validate = make_existing_receipt_validator(claim_id=result['claim_id'], **validate_options)
        accepted_hook(result, validate)
    return result
