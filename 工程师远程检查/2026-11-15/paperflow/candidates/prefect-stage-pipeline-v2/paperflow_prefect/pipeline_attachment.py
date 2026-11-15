"""Native attachment generator; same evidence projection and offline validator."""
from pathlib import Path
from .attachments import (Records, AttachmentError, verify_cached_description, has_request_history, _saved_evidence, evidence_for, _model_context_v1, _immutable_json, _request_body, sha_file, _validate_fields)

def attachment_steps(row, paper_id, work, connection, invoke):
    from .profile_recovery import _read_complete, parse_response
    saved = Records(work).cached('identify')
    if saved is not None:
        return verify_cached_description(row, paper_id, work, saved)
    if has_request_history(work):
        model, response = _read_complete(work)
        evidence = _saved_evidence(row, work)
    else:
        if (work / 'description.json').exists(): raise AttachmentError('history_unresolved')
        from .attachment_input import require_upstream_history_located
        require_upstream_history_located(row)
        if row.get('history', {}).get('status') in {'unknown', 'unresolved', 'needs_reconciliation'}:
            raise AttachmentError('history_unresolved')
        evidence = evidence_for(row, work / 'evidence')
        evidence['model_input_version'] = 1
        if _model_context_v1(evidence)['text_truncated']:
            evidence['limitations'] = list(dict.fromkeys(evidence['limitations'] +
                ['模型文字输入按24000字符总预算截断；完整留存证据另存，未全部传给模型']))
        _immutable_json(work / 'evidence.json', evidence)
        conn = connection() if callable(connection) else connection
        body = _request_body(evidence, conn)
        if sha_file(row['path']) != row['sha256']: raise AttachmentError('evidence_changed')
        response = yield (body, conn, work / 'model')
        model, response = _read_complete(work)
        _saved_evidence(row, work)
    result, repairs, report = parse_response(work, response, model=model, record_kind='attachment')
    if result is None:
        raise AttachmentError('attachment_format_error', diagnostic=report['receipt'],
            **{k: report['error'][k] for k in ('line', 'column') if k in report['error']})
    _validate_fields(result, evidence)
    result.update(schema='attachment-description/1', paper_id=paper_id,
                  attachment_id=row['file_id'], source_sha256=row['sha256'], format=evidence['format'],
                  limitations=list(dict.fromkeys(result['limitations'] + evidence['limitations'])))
    _immutable_json(work / 'description.json', result)
    return result

