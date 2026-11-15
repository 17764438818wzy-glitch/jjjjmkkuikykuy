"""Isolated deterministic attachment postprocessing candidate.

process_attachment(ticket, handoff, output_dir) consumes the provisional
``attachment-postprocess-input/1`` contract, NOT the unfinished upstream's
operational contract. ticket has exactly TICKET_FIELDS; task_run_id is retained
opaquely (the upstream currently uses a folder FlowRun ID). handoff includes an
identical ticket, receipt_ref {path, sha256}, attachment_id, work_dir, and
model_refs for evidence/request/request_state/response (and response_events when
present). All paths point to retained, immutable upstream evidence.

Only existing H2.1 receipt formats and bound original or retry model journals
are supported. Unsupported layouts stop; no source-path fallback, stream
adoption, model calls, extraction, named copies, upload or queue access occurs.
The caller must configure the retained business modules on sys.path and provide
registered-folder admission, an exclusive folder claim, a ticket-specific output
directory, pause/cancel checks, scheduling and the final persisted transition.
There is no new adapter lock: callers must not concurrently invoke this function
for one output. The retained renderer's own internal lock is left unchanged.
This result alone is NOT admission
to RAG or proof of automatic privacy redaction.
"""
from __future__ import annotations

import copy
from pathlib import Path
import re
from types import SimpleNamespace

from filelock import Timeout
from paperflow_prefect.records import identity, read_json, sha_file
from paperflow_prefect.attachments import (
    AttachmentError, _immutable_json, _model_context_v1, _request_body,
    _saved_evidence, _upstream_file_facts, _validate_fields, validate_description,
)
from paperflow_prefect.attachment_card import render_attachment_card
from paperflow_prefect.attachment_input import load_received_evidence, model_evidence, card_evidence
from paperflow_prefect.attachment_ingest import read_attachment_delivery
from paperflow_prefect.intake_handoff import verify_received_file
from paperflow_prefect.profile_recovery import _checked_request, parse_response
from paperflow_prefect.stage3_handoff import read_receipt
from paperflow_prefect.unified_attachments import delivery_for

TICKET_FIELDS = {'task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id'}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _ref(reference):
    _require(isinstance(reference, dict) and set(reference) == {'path', 'sha256'}, 'Invalid evidence reference')
    _require(isinstance(reference['path'], str) and Path(reference['path']).is_absolute(), 'Evidence path must be absolute')
    _require(isinstance(reference['sha256'], str) and re.fullmatch('[0-9a-f]{64}', reference['sha256']), 'Invalid evidence hash')
    path = Path(reference['path'])
    _require(path.is_file() and sha_file(path) == reference['sha256'], 'Retained evidence changed: ' + str(path))
    return path.resolve()


def _inputs(ticket, handoff):
    _require(isinstance(ticket, dict) and set(ticket) == TICKET_FIELDS
             and all(isinstance(v, str) and v.strip() for v in ticket.values()), 'Invalid folder ticket identity')
    _require(isinstance(handoff, dict) and handoff.get('schema') == 'attachment-postprocess-input/1'
             and handoff.get('ticket') == ticket, 'Attachment does not belong to this folder ticket')
    receipt_path = _ref(handoff['receipt_ref'])
    receipt, paper = read_receipt(receipt_path)
    _require(paper is not None and all(receipt.get(k) == ticket[k] for k in ('group_id', 'paper_id', 'receipt_id')),
             'Attachment receipt identity differs from ticket')
    rows = [r for r in paper['attachments'] if r['file_id'] == handoff['attachment_id']]
    _require(len(rows) == 1, 'No uniquely received attachment for ticket')
    row = rows[0]
    verify_received_file(row)
    _require(not row.get('unverified_evidence'), 'Attachment contains unverified intake evidence')
    _require(Path(row['path']).is_file() and sha_file(row['path']) == row['sha256']
             and Path(row['path']).stat().st_size == row['bytes'], 'Attachment original/delivered copy changed')
    work = Path(handoff['work_dir'])
    _require(work.is_absolute() and work.is_dir(), 'Missing retained attachment work directory')
    work = work.resolve()
    model = Path(handoff['model_refs']['request']['path']).resolve().parent
    if model != work / 'model':
        retry_model = work / 'retries' / identity(handoff['component_id']) / handoff['request_id'] / 'model'
        _require(model == retry_model.resolve(), 'Model reference has wrong owner/layout')
    files = {'evidence': work / 'evidence.json', 'request': model / 'request.json',
             'request_state': model / 'request-state.json', 'response': model / 'response.txt'}
    if (model / 'response-events.jsonl').exists(): files['response_events'] = model / 'response-events.jsonl'
    _require(set(handoff['model_refs']) == set(files), 'Missing or unexpected saved model evidence references')
    for key, path in files.items():
        _require(_ref(handoff['model_refs'][key]) == path.resolve(), 'Model reference has wrong owner/layout')
    from preextract_scheduler_adapter import request_observation
    _checked_request(model)
    phase, step, observation = request_observation(model)
    state = read_json(model / 'request-state.json')
    _require(phase == 'response_complete' and step == 'postprocess'
             and state.get('status') == 'response_complete' and state.get('finish_reason') == 'stop',
             'Saved attachment response is not explicitly complete; no adoption or resend allowed')
    _require(state.get('response_sha256') == handoff['model_refs']['response']['sha256'], 'Raw response identity changed')
    evidence = _saved_evidence(row, work)
    loaded = load_received_evidence(row)
    if loaded:
        record, source_handoff, report = loaded
        expected = model_evidence(record, source_handoff, report=report)
        card = card_evidence(record, report)
    else:
        expected = _upstream_file_facts(row)
        card = copy.deepcopy(expected)
    version = evidence.get('model_input_version')
    _require(version in (None, 1), 'Unsupported saved model evidence projection')
    if version == 1:
        expected['model_input_version'] = 1
        if _model_context_v1(expected)['text_truncated']:
            expected['limitations'] = list(dict.fromkeys(expected['limitations'] +
                ['模型文字输入按24000字符总预算截断；完整留存证据另存，未全部传给模型']))
    _require(evidence == expected, 'Saved model evidence does not match this received attachment evidence')
    request = read_json(model / 'request.json')
    if version == 1:
        # This retained helper only builds a request dict. It verifies image bytes
        # and the exact text/image ordering; it never dispatches the request.
        expected_content = _request_body(evidence, {'model': request['model'], 'max_tokens': 2048})['messages'][1]['content']
        _require(request['messages'][1]['content'] == expected_content, 'Saved model images/text differ from evidence')
    else:
        _require(not evidence.get('images') and len(request['messages'][1]['content']) == 1,
                 'Legacy image requests require an explicit verified input mapping')
    card['identification_input'] = dict(image_count=len(evidence.get('images', [])),
        refs=copy.deepcopy(evidence['refs']), limitations=copy.deepcopy(evidence['limitations']))
    return row, paper, evidence, card, work, observation


def _description(raw, evidence, row, paper_id):
    _validate_fields(raw, evidence)
    description = dict(raw, schema='attachment-description/1', paper_id=paper_id,
        attachment_id=row['file_id'], source_sha256=row['sha256'], format=evidence['format'],
        limitations=list(dict.fromkeys(raw['limitations'] + evidence['limitations'])))
    return validate_description(description, row, paper_id, evidence)


def _description_or_retained(raw, evidence, row, paper_id):
    if raw is not None:
        try:
            return _description(raw, evidence, row, paper_id), False
        except AttachmentError as exc:
            if exc.details.get('code') != 'attachment_format_error':
                raise
    facts = {'kind': '附件（说明未解析）',
        'summary': '原附件和模型完整回答原样保留；未采用无法解析的说明字段。',
        'evidence_refs': [], 'limitations': ['模型回答格式未解析；未编造附件内容说明']}
    return _description(facts, evidence, row, paper_id), True


def _retain_response(handoff, output, delivery, retained):
    if not retained:
        return delivery, None
    source = _ref(handoff['model_refs']['response'])
    path = output / 'raw-response.txt'
    raw = source.read_bytes()
    if path.exists():
        _require(path.read_bytes() == raw, 'Retained attachment response changed')
    else:
        with path.open('xb') as stream:
            stream.write(raw)
    reference = {'path': str(path.resolve()), 'sha256': sha_file(path)}
    return {**delivery, 'format_status': 'unstructured_retained', 'retained_response': reference}, reference


def _safe_output(output, work, row):
    absolute = Path(output).absolute()
    for part in (absolute, *absolute.parents):
        _require(not part.is_symlink() and not (hasattr(part, 'is_junction') and part.is_junction()),
                 'Output cannot traverse links or junctions')
    output = absolute.resolve()
    _require(not output.is_relative_to(work) and not work.is_relative_to(output), 'Output overlaps retained model history')
    _require(not output.is_relative_to(Path(row['group_root']).resolve()), 'Output overlaps original material group')
    if row.get('delivery_ref'):
        _require(not output.is_relative_to(Path(row['delivery_ref']['path']).resolve().parent),
                 'Output overlaps retained upstream delivery')
    if output.exists():
        _require(output.is_dir(), 'Output must be a directory')
        for p in output.rglob('*'):
            _require(not p.is_symlink() and not (hasattr(p, 'is_junction') and p.is_junction()), 'Output contains a link')
    return output


def _render(row, paper, description, evidence, output):
    card = render_attachment_card(row, paper['paper_id'], description, evidence, output / 'cards')
    backend = SimpleNamespace(attachment_card=lambda p, r: card)
    delivery = delivery_for(backend, paper, row, use_named=False)
    parsed = read_attachment_delivery(delivery)
    _require(card['version'] == identity(card['inputs']) == delivery['delivery_version'], 'Renderer/delivery version differs')
    _require(card['md_sha256'] == delivery['md_sha256'] == sha_file(card['path']), 'Renderer MD hash differs')
    _require(parsed['kind'] == description['kind'] and parsed['summary'] == description['summary'], 'Rendered description differs')
    return card, delivery


def _process(ticket, handoff, output, row, paper, evidence, card_evidence_value, work, observation):
    model = Path(handoff['model_refs']['request']['path']).parent
    binding = dict(schema='attachment-postprocess-binding/1', ticket=copy.deepcopy(ticket),
        handoff=copy.deepcopy(handoff), attachment_id=row['file_id'], source_path=str(Path(row['path']).resolve()),
        source_sha256=row['sha256'], source_binding=copy.deepcopy(row['source_binding']),
        request_sha256=observation['request_sha256'])
    intent = output / 'input.json'
    existing = output / 'receipt.json'
    if not intent.exists():
        _require(not any(output.iterdir()), 'Unregistered output cannot be adopted')
    _immutable_json(intent, binding)
    if existing.exists():
        result = read_json(existing)
        _require(result.get('input_sha256') == sha_file(intent) and result.get('binding') == binding,
                 'Saved completion belongs to different input')
        required_artifacts = {'description', 'card_evidence', 'md', 'delivery'}
        _require(isinstance(result.get('artifacts'), dict)
                 and required_artifacts <= set(result['artifacts'])
                 and set(result['artifacts']) <= required_artifacts | {'format_recovery', 'retained_response'},
                 'Saved completion has no complete artifact map')
        for reference in result.get('artifacts', {}).values(): _ref(reference)
        # Reparse with the same syntax-only business parser, but avoid writing a
        # second format-recovery record during a successful no-op retry.
        from .raw_publication import parse_response as parse_saved, UnstructuredResponse
        try:
            raw = parse_saved((model / 'response.txt').read_bytes().decode('utf8'))
        except UnstructuredResponse:
            raw = None
        description, retained = _description_or_retained(raw, evidence, row, paper['paper_id'])
        _require(read_json(output / 'description.json') == description, 'Cached description differs from saved response')
        _require(read_json(output / 'card-evidence.json') == card_evidence_value, 'Cached card evidence differs')
        # Exclusive renderer recomputes exact bytes and the semantic input
        # version; valid-looking hashes and a self-rehashed receipt are not enough.
        card, delivery = _render(row, paper, description, card_evidence_value, output)
        delivery, retained_ref = _retain_response(handoff, output, delivery, retained)
        _require(result['artifacts'].get('retained_response') == retained_ref,
                 'Retained attachment response is not bound to its completion')
        _require(result.get('delivery') == delivery and result.get('render') == card,
                 'Cached delivery is not the exact deterministic rendering')
        for name, path in {'description': output / 'description.json', 'card_evidence': output / 'card-evidence.json',
                          'md': Path(card['path']), 'delivery': Path(card['path']).parent / 'delivery.json'}.items():
            _require(result['artifacts'][name] == dict(path=str(path.resolve()), sha256=sha_file(path)),
                     'Artifact reference does not belong to this exact output')
        _require(result.get('ticket') == ticket and result.get('status') == 'complete'
                 and result.get('raw_response') == handoff['model_refs']['response']
                 and result.get('privacy_redaction') == 'not_performed'
                 and result.get('model_requests_added') == 0, 'Saved completion metadata differs')
        _inputs(ticket, handoff)
        return result
    raw, repairs, report = parse_response(output, (model / 'response.txt').read_bytes().decode('utf8'),
                                         model=model, record_kind='attachment')
    description, retained = _description_or_retained(raw, evidence, row, paper['paper_id'])
    _immutable_json(output / 'description.json', description)
    _immutable_json(output / 'card-evidence.json', card_evidence_value)
    card, delivery = _render(row, paper, description, card_evidence_value, output)
    delivery, retained_ref = _retain_response(handoff, output, delivery, retained)
    # Recheck all upstream file, request, image and receipt bindings after writing
    # the output; the upstream never gets an update from this adapter.
    latest = _inputs(ticket, handoff)
    _require(latest[0] == row and latest[2] == evidence and latest[3] == card_evidence_value, 'Inputs changed during processing')
    artifacts = {name: dict(path=str(path.resolve()), sha256=sha_file(path)) for name, path in {
        'description': output / 'description.json', 'card_evidence': output / 'card-evidence.json',
        'md': Path(card['path']), 'delivery': Path(card['path']).parent / 'delivery.json'}.items()}
    if report:
        path = Path(report['receipt']); artifacts['format_recovery'] = dict(path=str(path), sha256=sha_file(path))
    if retained_ref:
        artifacts['retained_response'] = retained_ref
    result = dict(schema='attachment-postprocess-result/1', status='complete', ticket=copy.deepcopy(ticket),
        binding=binding, input_sha256=sha_file(intent), delivery=delivery, render=card,
        raw_response=copy.deepcopy(handoff['model_refs']['response']), artifacts=artifacts,
        privacy_redaction='not_performed', model_requests_added=0,
        scope='attachment_only', registry_verification='caller_required',
        limitations=['附件说明沿用已保存回答；未执行自动隐私脱敏',
                     '附件完成不代表文件夹完成或已入 RAG；目录完整不代表成员内容已读'])
    _immutable_json(existing, result)
    return result


def process_attachment(ticket, handoff, output_dir):
    """Return a durable attachment-only receipt, or fail without model retries.

    An identical completed output is fully revalidated and reused. A changed
    ticket/request/response/source cannot replace any recorded input or artifact.
    Incomplete writes with a matching durable input may resume deterministically;
    unregistered existing output is rejected. Folder admission remains external.
    """
    ticket, handoff = copy.deepcopy(ticket), copy.deepcopy(handoff)
    try:
        row, paper, evidence, card, work, observation = _inputs(ticket, handoff)
        output = _safe_output(output_dir, work, row)
        output.mkdir(parents=True, exist_ok=True)
        return _process(ticket, handoff, output, row, paper, evidence, card, work, observation)
    except Timeout as exc:
        raise AttachmentError('busy') from exc
    except (KeyError, TypeError, OSError) as exc:
        raise ValueError('Incomplete or changed attachment handoff: ' + type(exc).__name__) from exc
