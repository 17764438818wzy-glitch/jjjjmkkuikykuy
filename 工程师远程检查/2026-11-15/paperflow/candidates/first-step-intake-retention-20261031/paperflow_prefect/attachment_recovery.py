"""Select one explicitly authorized attachment retry, retaining original evidence."""
from pathlib import Path
import time

from filelock import FileLock
from .records import read_json, sha_file

INTENT = 'private/attachment-recovery/retry-intent.json'
ATTEMPT = 'attachment-recovery-1'
PARENT_FILES = ('evidence.json', 'model/request.json', 'model/request-state.json',
                'model/response.txt', 'model/response-events.jsonl', 'model/http-error.txt')


def _authority(path, work, row, paper_id):
    from .profile_recovery import ProfileRecoveryError
    value = read_json(path)
    if (value.get('schema') != 'desktop108-retry54-authorized-scope/1'
            or value.get('max_additional_attempts_per_failed_request') != 1):
        raise ProfileRecoveryError('evidence_changed')
    matches = [r for r in value.get('requests', []) if r.get('kind') == 'attachment'
               and r.get('paper_id') == paper_id and Path(r.get('work', '')).resolve() == work]
    if len(matches) != 1:
        raise ProfileRecoveryError('evidence_changed')
    item = matches[0]
    expected = {str(work/name) for name in PARENT_FILES}
    protected = {str(Path(p).resolve()):v for p,v in item.get('protected_hashes', {}).items()}
    if (set(protected) != expected or len(protected) != len(item.get('protected_hashes', {}))
            or Path(item['model']).resolve() != work/'model'
            or item['source_sha256'] != row['sha256']
            or item['source']['file_id'] != row['file_id']
            or Path(item['source']['path']).resolve() != Path(row['path']).resolve()
            or sha_file(row['path']) != row['sha256']):
        raise ProfileRecoveryError('evidence_changed')
    for name, digest in item['protected_hashes'].items():
        p = Path(name)
        if (sha_file(p) if p.is_file() else None) != digest:
            raise ProfileRecoveryError('evidence_changed')
    return item


def selected_attachment_model(work):
    from .profile_recovery import ProfileRecoveryError, _checked_request, _other_revisions
    work = Path(work).resolve()
    intent = read_json(work/INTENT)
    if (intent.get('schema') != 'attachment-engineering-attempt/1'
            or intent.get('root_work') != str(work) or intent.get('attempt') != 1
            or intent.get('max_additional_attempts') != 1
            or sha_file(intent['authorization_path']) != intent['authorization_sha256']):
        raise ProfileRecoveryError('evidence_changed')
    item = _authority(intent['authorization_path'], work, intent['source'], intent['paper_id'])
    if (item['request_id'] != intent['request_id']
            or item['parent_request_sha256'] != intent['parent_request_sha256']
            or _other_revisions(work, (ATTEMPT,))):
        raise ProfileRecoveryError('evidence_changed')
    _checked_request(work/'model', require_saved_hash=True)
    if read_json(work/'model/request-state.json')['request_sha256'] != intent['parent_request_sha256']:
        raise ProfileRecoveryError('evidence_changed')
    child = work/'revisions'/ATTEMPT
    if child.exists():
        if not (child/'revision.json').is_file() or read_json(child/'revision.json') != intent:
            raise ProfileRecoveryError('evidence_changed')
        model = child/'model'
        if any((model/n).exists() for n in ('request.json', 'request-state.json', 'response.txt')):
            _checked_request(model, require_saved_hash=True)
            if read_json(model/'request-state.json')['request_sha256'] != intent['parent_request_sha256']:
                raise ProfileRecoveryError('evidence_changed')
    return child/'model'


def retry_attachment(work, row, paper_id, connection, invoke, authorization_path):
    from .profile_recovery import (ProfileRecoveryError, _checked_request, _exclusive_json,
        _other_revisions, inspect_request, recovery_lock, INTENT as BODY_INTENT)
    from .attachments import _saved_evidence
    work = Path(work).resolve(); authorization_path = Path(authorization_path).resolve()
    with FileLock(work/'attachment-identification.lock', timeout=0), recovery_lock(work):
        item = _authority(authorization_path, work, row, paper_id)
        if (work/INTENT).exists():
            observed = inspect_request(work)
            if observed['outcome'] == 'complete': return observed
            raise ProfileRecoveryError('retry_limit', outcome=observed['outcome'])
        if (work/BODY_INTENT).exists() or _other_revisions(work):
            raise ProfileRecoveryError('retry_limit')
        observed = inspect_request(work)
        state = read_json(work/'model/request-state.json')
        if (not observed['retry_allowed'] or observed['code'] != 'explicit_failure'
                or state.get('http_status') not in {429, 502, 503}):
            raise ProfileRecoveryError(observed['code'], outcome=observed['outcome'])
        request = _checked_request(work/'model', require_saved_hash=True)
        _saved_evidence(row, work)
        if request['model'] != connection['model']:
            raise ProfileRecoveryError('evidence_changed')
        intent = dict(schema='attachment-engineering-attempt/1', root_work=str(work),
            attempt=1, max_additional_attempts=1, paper_id=paper_id, source=row,
            parent_request_sha256=item['parent_request_sha256'], request_id=item['request_id'],
            authorization_path=str(authorization_path), authorization_sha256=sha_file(authorization_path),
            created_at=time.time())
        # A crash after this durable reservation consumes the only retry budget.
        _exclusive_json(work/INTENT, intent)
        child = work/'revisions'/ATTEMPT
        child.mkdir(parents=True, exist_ok=False)
        _exclusive_json(child/'revision.json', intent)
        model = selected_attachment_model(work)
        invoke(request, connection, model)
        observed = inspect_request(work)
        _exclusive_json(child/'engineering-result.json', observed)
        if observed['outcome'] != 'complete':
            raise ProfileRecoveryError(observed['code'], outcome=observed['outcome'])
        return observed
