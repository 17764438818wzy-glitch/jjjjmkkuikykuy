"""Engineering-only recovery of an existing body request; never semantic repair."""
import difflib
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

from filelock import FileLock
from .records import read_json, sha_file, write_json


MESSAGES = {
    'profile_format_error': '返回格式错误；原回答已保存，无法确定性修复，未重新调用模型',
    'upstream_interrupted': '上游响应中断；结果待核对，禁止直接重发',
    'timeout_unknown': '请求超时，结果未知；先核对原请求，禁止直接重发',
    'outcome_unknown': '请求结果未知；先核对原请求，禁止直接重发',
    'explicit_failure': '上游明确失败；可按原请求进行一次有上限的恢复',
    'not_sent': '请求明确未发送；可按原请求进行一次有上限的恢复',
    'retry_limit': '本请求的恢复名额已使用；保留历史，禁止再次发送',
    'evidence_changed': '原请求、回答或来源证据不匹配；停止恢复',
    'profile_validation_failed': '画像现有校验未通过；诊断已保留，未重新调用模型',
    'profile_error': '正文画像处理失败；请查看本地工程诊断，未自动重试',
    'busy': '此篇已有处理或恢复正在执行，请等待原任务结束',
    'unsupported_recovery': '此入口只恢复原始单请求工作记录；保留现场',
    'history_unresolved': '正文历史定位尚未核对；保留已准备材料，禁止按新请求发送',
}
INTENT = 'private/profile-recovery/retry-intent.json'
ATTEMPT = 'profile-recovery-1'
PARENT_FILES = ('material.json', 'model/request.json', 'model/request-state.json',
                'model/response.txt', 'model/response-events.jsonl', 'model/http-error.txt')


class ProfileRecoveryError(RuntimeError):
    def __init__(self, code, **details):
        self.details = {'code': code if code in MESSAGES else 'profile_error'}
        self.details['message'] = MESSAGES[self.details['code']]
        self.details.update({k: v for k, v in details.items()
                             if k in {'line', 'column', 'offset', 'diagnostic', 'outcome'}})
        super().__init__(self.details['message'])


def safe_error(error):
    if isinstance(error, ProfileRecoveryError): return dict(error.details)
    from .attachments import AttachmentError
    if isinstance(error, AttachmentError) and error.details['code'] in MESSAGES:
        code = error.details['code']; return dict(code=code, message=MESSAGES[code])
    if isinstance(error, TimeoutError): return dict(code='timeout_unknown', message=MESSAGES['timeout_unknown'])
    return dict(code='profile_error', message=MESSAGES['profile_error'])


def recovery_lock(work):
    return FileLock(str(Path(work) / 'profile-recovery.lock'), timeout=0)


def _exclusive_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush(); os.fsync(stream.fileno())


def _checked_request(model, *, require_saved_hash=False):
    from preextract_scheduler_adapter import _request_hash
    request = read_json(model / 'request.json')
    state = read_json(model / 'request-state.json')
    if _request_hash(request) != state.get('request_sha256'):
        raise ProfileRecoveryError('evidence_changed')
    saved_hash = state.get('stored_request_sha256')
    # Native records hash the LF serialization before its Windows text writer
    # may emit CRLF. Accept only that writer conversion, not arbitrary reformatting.
    stored_bytes = (model / 'request.json').read_bytes().replace(b'\r\n', b'\n')
    if ((require_saved_hash and not saved_hash)
            or (saved_hash and hashlib.sha256(stored_bytes).hexdigest() != saved_hash)):
        raise ProfileRecoveryError('evidence_changed')
    return request


def _other_revisions(work, allowed=()):
    # A dangling native pointer still proves retained recovery history.
    if (work / 'active-revision.json').exists(): return True
    revisions = work / 'revisions'
    return revisions.exists() and (not revisions.is_dir()
        or any(path.name not in allowed for path in revisions.iterdir()))


def _selected_model(work):
    work = Path(work).resolve()
    from .attachment_recovery import INTENT as ATTACHMENT_INTENT, selected_attachment_model
    if (work / ATTACHMENT_INTENT).exists():
        if (work / INTENT).exists(): raise ProfileRecoveryError('evidence_changed')
        return selected_attachment_model(work)
    path = work / INTENT
    if not path.exists(): return work / 'model'
    intent = read_json(path)
    if (intent.get('schema') != 'profile-engineering-attempt/1'
            or intent.get('root_work') != str(work) or intent.get('attempt') != 1
            or intent.get('max_additional_attempts') != 1
            or set(intent.get('parent_hashes', {})) != set(PARENT_FILES)):
        raise ProfileRecoveryError('evidence_changed')
    for name, digest in intent['parent_hashes'].items():
        p = work / name
        if (sha_file(p) if p.is_file() else None) != digest:
            raise ProfileRecoveryError('evidence_changed')
    _checked_request(work / 'model', require_saved_hash=True)
    if (read_json(work / 'model/request-state.json')['request_sha256'] != intent['parent_request_sha256']
            or sha_file(intent['source']) != intent['source_sha256']):
        raise ProfileRecoveryError('evidence_changed')
    if _other_revisions(work, (ATTEMPT,)):
        raise ProfileRecoveryError('retry_limit')
    child = work / 'revisions' / ATTEMPT
    if child.exists():
        if not (child / 'revision.json').is_file():
            raise ProfileRecoveryError('evidence_changed')
        from preextract_pipeline import _revision
        revision = _revision(intent['source'], child)
        if (revision.get('mode') != 'rerun' or revision.get('recover_failed') is not True
                or revision.get('refresh_material') or revision['parent_hashes'] != intent['parent_hashes']):
            raise ProfileRecoveryError('evidence_changed')
        model = child / 'model'
        if any((model / name).exists() for name in ('request.json', 'request-state.json', 'response.txt')):
            _checked_request(model, require_saved_hash=True)
            if read_json(model / 'request-state.json')['request_sha256'] != intent['parent_request_sha256']:
                raise ProfileRecoveryError('evidence_changed')
    return child / 'model'


def selected_model(work):
    try: return _selected_model(work)
    except ProfileRecoveryError: raise
    except (OSError, ValueError, KeyError, TypeError):
        raise ProfileRecoveryError('evidence_changed') from None


def _stream_cause(model):
    """Use provider prose only to select a safe label, never to allow a retry."""
    path = model / 'response-events.jsonl'
    if not path.is_file(): return None
    with path.open('rb') as stream:
        for line in stream:
            if not line.startswith(b'data:'): continue
            try: error = json.loads(line[5:].strip()).get('error')
            except (ValueError, UnicodeError, AttributeError): continue
            if not isinstance(error, dict): continue
            message = str(error.get('message', '')).upper()
            if 'TIMEOUT' in message or 'TIMED OUT' in message: return 'timeout_unknown'
            if error.get('code') == 'upstream_stream_error': return 'upstream_interrupted'
    return None


def inspect_request(work):
    """Read native evidence; a gateway stream error is not proof of no result."""
    from preextract_scheduler_adapter import request_observation
    work = Path(work).resolve()
    model = selected_model(work)
    try:
        state = read_json(model / 'request-state.json') if (model / 'request-state.json').exists() else {}
        if state: _checked_request(model)
        response = model / 'response.txt'
        if response.is_file() and state.get('response_sha256') and sha_file(response) != state['response_sha256']:
            raise ProfileRecoveryError('evidence_changed')
        phase, step, evidence = request_observation(model)
    except ProfileRecoveryError: raise
    except (OSError, ValueError, KeyError, TypeError):
        raise ProfileRecoveryError('evidence_changed') from None
    result = {'outcome': 'unknown', 'code': 'outcome_unknown', 'retry_allowed': False,
              'model_dir': str(model), 'request_sha256': evidence.get('request_sha256'),
              'request_state_sha256': evidence.get('request_state_sha256'), 'native_step': step}
    if phase == 'response_complete':
        result.update(outcome='complete', code='response_complete', response_sha256=evidence['response_sha256'])
    elif phase == 'no_model_request' and not (work / INTENT).exists():
        result.update(outcome='not_started', code='no_model_request')
    else:
        cause = _stream_cause(model)
        if cause or state.get('error_kind') == 'explicit_stream_error':
            result['code'] = cause or 'upstream_interrupted'
        elif state.get('error_type') in {'TimeoutError', 'ReadTimeout', 'ConnectTimeout'}:
            result['code'] = 'timeout_unknown'
        elif phase == 'failed_ended' and evidence.get('reason') == 'transport_not_invoked':
            events = model / 'response-events.jsonl'
            if (state.get('http_invoked') is False and state.get('client_http_requests') == 0
                    and not response.exists()
                    and (not events.exists() or (events.is_file() and events.stat().st_size == 0))):
                result.update(outcome='failed', code='not_sent', retry_allowed=True)
        elif phase == 'failed_ended' and evidence.get('reason') == 'explicit_transient_response':
            error = model / 'http-error.txt'
            events = model / 'response-events.jsonl'
            # Require the complete saved HTTP failure, not just a numeric field.
            if (not response.exists() and (not events.exists() or (events.is_file() and events.stat().st_size == 0))
                    and error.is_file() and state.get('error_response_sha256') == sha_file(error)):
                result.update(outcome='failed', code='explicit_failure', retry_allowed=True)
    if result['retry_allowed'] and _other_revisions(work):
        result.update(code='retry_limit', retry_allowed=False)
    if (work / INTENT).exists(): result['retry_allowed'] = False
    if result['code'] in MESSAGES: result['message'] = MESSAGES[result['code']]
    return result


def _format_error(raw):
    details = {'code': 'profile_format_error', 'message': MESSAGES['profile_format_error']}
    try: json.loads(raw)
    except json.JSONDecodeError as exc:
        details.update(line=exc.lineno, column=exc.colno, offset=exc.pos)
    except ValueError: pass
    return details


def parse_response(work, raw, *, model=None, force_record=False, record_kind='profile'):
    """Use the existing syntax-only parser. No content validator or model call."""
    from preextract_contract import parse_saved_candidate
    if record_kind not in {'profile', 'attachment'}: raise ValueError('Invalid recovery record kind')
    work = Path(work).resolve(); model = Path(model) if model else selected_model(work)
    repairs = []; candidate = None; error = None
    try: candidate = parse_saved_candidate(raw, repairs=repairs)
    except (ValueError, TypeError): error = _format_error(raw)
    if not (force_record or repairs or error): return candidate, repairs, None
    if error and record_kind == 'attachment':
        error.update(code='attachment_format_error', message='附件返回格式错误；原回答已保存，未重新调用模型')
    record_dir = work / ('private/' + record_kind + '-recovery') / ('format-' + uuid.uuid4().hex)
    record_dir.mkdir(parents=True, exist_ok=False)
    state = read_json(model / 'request-state.json')
    report = {'schema': record_kind + '-format-recovery/1', 'created_at': time.time(),
              'status': 'format_error' if error else 'parsed', 'root_work': str(work),
              'model_dir': str(model), 'request_sha256': state['request_sha256'],
              'request_state_sha256': sha_file(model / 'request-state.json'),
              'original_response': str(model / 'response.txt'),
              'original_response_sha256': sha_file(model / 'response.txt'),
              'model_requests_added': 0, 'repairs': repairs, 'content_quality_checked': False}
    if error: report['error'] = error
    else:
        output = record_dir / 'parsed.json'; write_json(output, candidate)
        fixed = output.read_text(encoding='utf8')
        diff = record_dir / 'changes.diff'
        diff.write_text(''.join(difflib.unified_diff(raw.splitlines(True), fixed.splitlines(True),
                        fromfile='original-response', tofile='parsed-result')), encoding='utf8')
        report.update(candidate=str(output), candidate_sha256=sha_file(output), diff=str(diff), diff_sha256=sha_file(diff))
    report['receipt'] = str(record_dir / 'receipt.json')
    _exclusive_json(report['receipt'], report)
    return candidate, repairs, report


def _read_complete(work):
    from preextract_request_control import adopt_saved_response
    observation = inspect_request(work)
    if observation['outcome'] != 'complete':
        raise ProfileRecoveryError(observation['code'], outcome=observation['outcome'])
    model = Path(observation['model_dir'])
    if observation['native_step'] == 'adopt_saved_response': adopt_saved_response(model)
    return model, (model / 'response.txt').read_text(encoding='utf8')


def repair_saved_profile(work):
    work = Path(work).resolve()
    with recovery_lock(work):
        model, raw = _read_complete(work)
        _, _, report = parse_response(work, raw, model=model, force_record=True)
        return report


def retry_saved_request(work, source, template_path, connection, invoke):
    """One explicit exact-payload attempt; never called by normal resume."""
    from preextract_pipeline import create_revision, validate_saved_request
    work = Path(work).resolve(); source = Path(source).resolve()
    if (work / 'revision.json').exists() or (work / 'model/partition-state.json').exists():
        raise ProfileRecoveryError('unsupported_recovery')
    with recovery_lock(work):
        if (work / INTENT).exists():
            observed = inspect_request(work)
            if observed['outcome'] == 'complete': return observed
            raise ProfileRecoveryError('retry_limit', outcome=observed['outcome'])
        observed = inspect_request(work)
        if not observed['retry_allowed']:
            raise ProfileRecoveryError(observed['code'], outcome=observed['outcome'])
        try:
            material = read_json(work / 'material.json')
            request = _checked_request(work / 'model', require_saved_hash=True)
            if (sha_file(source) != material['source']['source_sha256']
                    or Path(material['source']['file']).resolve() != source
                    or request['model'] != connection['model']):
                raise ProfileRecoveryError('evidence_changed')
            validate_saved_request(request, material, Path(template_path).read_text(encoding='utf-8-sig'))
        except ProfileRecoveryError: raise
        except (OSError, ValueError, KeyError, TypeError):
            raise ProfileRecoveryError('evidence_changed') from None
        intent = {'schema': 'profile-engineering-attempt/1', 'root_work': str(work), 'attempt': 1,
                  'max_additional_attempts': 1, 'source': str(source), 'source_sha256': sha_file(source),
                  'parent_request_sha256': observed['request_sha256'],
                  'template_sha256': sha_file(template_path), 'created_at': time.time(),
                  'parent_hashes': {name: sha_file(work / name) if (work / name).is_file() else None for name in PARENT_FILES}}
        # Reserve durably BEFORE revision creation or transport; a crash consumes the slot.
        _exclusive_json(work / INTENT, intent)
        try:
            child = create_revision(source, work, ATTEMPT, mode='rerun',
                                    reason='explicit engineering failure; exact saved request; max one additional attempt',
                                    recover_failed=True, template_path=template_path)
            # Native revision creation uses this same lock. Recheck after it,
            # then hold the gate through dispatch to exclude another revision.
            with FileLock(str(work / 'operation.lock'), timeout=0):
                model = selected_model(work)
                if (model != child / 'model' or sha_file(template_path) != intent['template_sha256']
                        or _checked_request(work / 'model', require_saved_hash=True) != request):
                    raise ProfileRecoveryError('evidence_changed')
                invoke(request, connection, model)
            current = inspect_request(work)
            if current['request_sha256'] != intent['parent_request_sha256']:
                raise ProfileRecoveryError('evidence_changed')
            _exclusive_json(child / 'engineering-result.json', current)
            if current['outcome'] != 'complete':
                raise ProfileRecoveryError(current['code'], outcome=current['outcome'])
            return current
        except BaseException as exc:
            path = work / 'private/profile-recovery' / ('attempt-error-' + uuid.uuid4().hex + '.json')
            _exclusive_json(path, {'attempt': 1, 'error': safe_error(exc), 'parent_request_sha256': intent['parent_request_sha256']})
            if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
            if isinstance(exc, ProfileRecoveryError): raise
            raise ProfileRecoveryError('outcome_unknown', diagnostic=str(path)) from exc
