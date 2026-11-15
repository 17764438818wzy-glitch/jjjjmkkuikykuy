"""Candidate attachment request input using the existing evidence adapter.

Call only while holding the attachment's existing identification and work
locks, after resolving and checking source-bound request history. This module
does not send, parse responses, render attachment cards, or publish files.
"""
from pathlib import Path

from paperflow_prefect import attachment_input, attachments
from paperflow_prefect.attachment_recovery import INTENT as ATTACHMENT_INTENT
from paperflow_prefect.profile_recovery import INTENT as PROFILE_INTENT, _other_revisions


def check_attachment_source(row):
    try:
        if attachments.sha_file(row['path']) != row['sha256']:
            raise attachments.AttachmentError('evidence_changed')
    except (KeyError, OSError, TypeError) as exc:
        raise attachments.AttachmentError('evidence_changed') from exc


def prepare_attachment_body(row, work, connection):
    """Prepare one unchanged attachment request or recover its saved body."""
    work = Path(work).resolve()
    check_attachment_source(row)
    if ((work / ATTACHMENT_INTENT).exists() or (work / PROFILE_INTENT).exists()
            or (work / 'revision.json').exists() or _other_revisions(work)):
        # Existing recovery revisions require their original, explicit path.
        raise attachments.AttachmentError('history_unresolved')
    if attachments.has_request_history(work):
        attachments._saved_evidence(row, work)
        request = attachments.read_json(work / 'model/request.json')
        check_attachment_source(row)
        return request
    if (work / 'description.json').exists():
        raise attachments.AttachmentError('history_unresolved')
    attachment_input.require_upstream_history_located(row)
    if row.get('history', {}).get('status') in {'unknown', 'unresolved', 'needs_reconciliation'}:
        raise attachments.AttachmentError('history_unresolved')
    evidence = attachments.evidence_for(row)
    evidence['model_input_version'] = 1
    if attachments._model_context_v1(evidence)['text_truncated']:
        evidence['limitations'] = list(dict.fromkeys(evidence['limitations'] +
            ['模型文字输入按24000字符总预算截断；完整留存证据另存，未全部传给模型']))
    attachments._immutable_json(work / 'evidence.json', evidence)
    body = attachments._request_body(evidence, connection)
    check_attachment_source(row)
    return body
