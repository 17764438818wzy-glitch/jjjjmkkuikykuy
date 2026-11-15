"""Verify and parse a saved, independently journaled generated title."""
import hashlib
import json
from pathlib import Path
import re


class JournalError(ValueError):
    pass


def _sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def _sha_file(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as src:
        for block in iter(lambda: src.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def generated_title(response):
    raw = response.strip()
    if raw.startswith('```') and raw.endswith('```'):
        raw = '\n'.join(raw.splitlines()[1:-1]).strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = raw
    title = parsed.get('title') if isinstance(parsed, dict) else parsed
    if not isinstance(title, str):
        raise ValueError('模型回答没有可用题目')
    title = title.strip().strip('《》“”"\'').strip()
    if (not 4 <= len(title) <= 160 or '\n' in title or '\r' in title
            or re.search(r'[\x00-\x1f]', title)
            or title.startswith(('无法', '不能', '抱歉', '建议', '论文题目：', '题目：'))):
        raise ValueError('模型回答不是单一可用题目')
    return title


def verify_response(work, binding):
    work = Path(work)
    state_path = work / 'request-state.json'
    if not state_path.is_file():
        raise JournalError('找不到请求完成记录')
    state = json.loads(state_path.read_text(encoding='utf-8-sig'))
    if state.get('status') != 'response_complete' or state.get('finish_reason') != 'stop':
        raise JournalError('题目请求结果未完成或未知，禁止自动重发')
    request_path, response_path = work / 'request.json', work / 'response.txt'
    if not request_path.is_file() or not response_path.is_file():
        raise JournalError('题目请求或回答文件缺失')
    request = json.loads(request_path.read_text(encoding='utf-8-sig'))
    request_hash = _sha_bytes(json.dumps(request, ensure_ascii=False, separators=(',', ':')).encode())
    # Native hashes the formatted JSON before Windows text-mode writing.
    request_file = request_path.read_bytes().replace(b'\r\n', b'\n')
    if (request_hash != binding['request_sha256'] or state.get('request_sha256') != request_hash
            or state.get('stored_request_sha256') != _sha_bytes(request_file)
            or state.get('response_sha256') != _sha_file(response_path)):
        raise JournalError('题目请求或回答的哈希不符')
    if (work / 'response-events.jsonl').is_file():
        from preextract_scheduler_adapter import request_observation
        phase, next_step, _ = request_observation(work)
        if phase != 'response_complete' or next_step != 'postprocess':
            raise JournalError('题目响应流与已保存回答不一致')
    return generated_title(response_path.read_text(encoding='utf8')), state
