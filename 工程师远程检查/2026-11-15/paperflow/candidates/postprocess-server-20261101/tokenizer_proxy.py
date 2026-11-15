"""Connect the compatibility process to its own server-local CPU counter."""
import copy
import hashlib
import json
import socket


class CounterProxy:
    def __init__(self, port, secret, identity, *, model_budget, cl_limit, timeout=30):
        if type(port) is not int or not 0 < port < 65536:
            raise ValueError('invalid local tokenizer port')
        if not isinstance(secret, str) or not secret or not isinstance(identity, dict):
            raise ValueError('invalid local tokenizer identity')
        if not 0 < timeout <= 120:
            raise ValueError('invalid local tokenizer timeout')
        if type(model_budget) is not int or model_budget <= 0 or type(cl_limit) is not int or cl_limit <= 0:
            raise ValueError('explicit positive tokenizer budgets required')
        self.port, self.secret, self.identity = port, secret, copy.deepcopy(identity)
        self.model_budget, self.cl_limit, self.timeout = model_budget, cl_limit, timeout

    def count_many(self, texts):
        texts = list(texts)
        if any(not isinstance(text, str) for text in texts):
            raise ValueError('tokenizer accepts text only')
        request = json.dumps({'secret': self.secret, 'texts': texts}, ensure_ascii=False).encode('utf8') + b'\n'
        if len(request) > 16 * 1024 * 1024:
            raise ValueError('tokenizer request exceeds transport limit')
        hashes = [hashlib.sha256(text.encode('utf8')).hexdigest() for text in texts]
        with socket.create_connection(('127.0.0.1', self.port), timeout=self.timeout) as conn:
            conn.sendall(request)
            with conn.makefile('rb') as stream:
                raw = stream.readline(4 * 1024 * 1024 + 1)
        if not raw.endswith(b'\n') or len(raw) > 4 * 1024 * 1024:
            raise ValueError('invalid tokenizer reply')
        try:
            result = json.loads(raw)
            counts = result['counts']
        except (ValueError, UnicodeError, KeyError, TypeError) as exc:
            raise ValueError('invalid tokenizer reply') from exc
        if (result.get('identity') != self.identity or result.get('hashes') != hashes
                or not isinstance(counts, list) or len(counts) != len(texts)
                or any(not isinstance(row, dict)
                       or any(type(row.get(key)) is not int or row[key] < 0
                              for key in ('model_tokens', 'cl100k_tokens')) for row in counts)):
            raise ValueError('tokenizer reply is not bound to the exact input')
        return counts

    def close(self):
        """The server process owns the counter; each proxy call closes its socket."""
        return None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
