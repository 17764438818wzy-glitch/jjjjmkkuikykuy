"""One bounded authenticated connection to the existing offline counter."""
import hashlib
import hmac
import json


def serve_client(conn, counter, secret, *, path_verifier=None, snapshot_sha256=None):
    conn.settimeout(30)
    with conn.makefile('rb') as stream:
        raw = stream.readline(16 * 1024 * 1024 + 1)
    if not raw.endswith(b'\n') or len(raw) > 16 * 1024 * 1024:
        raise ValueError('invalid tokenizer request')
    try:
        value = json.loads(raw)
        supplied = value['secret']
    except (ValueError, UnicodeError, KeyError, TypeError) as exc:
        raise ValueError('invalid tokenizer request') from exc
    if (not isinstance(supplied, str) or not supplied.isascii()
            or not isinstance(secret, str) or not secret.isascii()
            or not hmac.compare_digest(supplied, secret)):
        raise ValueError('tokenizer request is unauthorized or invalid')
    if value.get('operation') == 'verify_snapshot_path':
        path = value.get('path')
        if (set(value) != {'secret', 'operation', 'path'} or not isinstance(path, str)
                or len(path) > 32767 or not callable(path_verifier)
                or not isinstance(snapshot_sha256, str) or len(snapshot_sha256) != 64
                or path_verifier(path) is not True):
            raise ValueError('snapshot path is not verified')
        response = dict(schema='paperflow-wine-verified-path/1', path=path,
                        snapshot_sha256=snapshot_sha256, verified=True)
        conn.sendall(json.dumps(response, ensure_ascii=True, separators=(',', ':')).encode() + b'\n')
        return
    texts = value.get('texts')
    if not isinstance(texts, list) or any(not isinstance(text, str) for text in texts):
        raise ValueError('tokenizer request is unauthorized or invalid')
    counts = counter.count_many(texts)
    response = {'identity': counter.identity,
                'hashes': [hashlib.sha256(text.encode('utf8')).hexdigest() for text in texts],
                'counts': counts}
    encoded = json.dumps(response, ensure_ascii=True, separators=(',', ':')).encode() + b'\n'
    if len(encoded) > 4 * 1024 * 1024:
        raise ValueError('tokenizer response exceeds transport limit')
    conn.sendall(encoded)
