"""The Windows compatibility process must receive counts for its exact texts."""
import hashlib
import json
import socket
import threading

import pytest

from tokenizer_proxy import CounterProxy
from tokenizer_service import serve_client


def proxy(port, secret, identity):
    return CounterProxy(port, secret, identity, model_budget=2000, cl_limit=2038)


def serve_once(reply):
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(1)
    port = server.getsockname()[1]
    received = []

    def worker():
        try:
            conn, _ = server.accept()
            with conn, conn.makefile('rb') as stream:
                request = json.loads(stream.readline())
                received.append(request)
                conn.sendall((json.dumps(reply(request)) + '\n').encode())
        finally:
            server.close()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return port, received, thread


def test_proxy_binds_count_reply_to_exact_input():
    def answer(request):
        return {'hashes': [hashlib.sha256(t.encode()).hexdigest() for t in request['texts']],
                'identity': {'test': 'identity'},
                'counts': [{'model_tokens': 7, 'cl100k_tokens': 3}] * len(request['texts'])}
    port, received, thread = serve_once(answer)
    counter = proxy(port, 'test-secret', {'test': 'identity'})
    result = counter.count_many(['中文', 'abc'])
    counter.close()
    thread.join(timeout=2)
    assert result == [{'model_tokens': 7, 'cl100k_tokens': 3}] * 2
    assert received == [{'secret': 'test-secret', 'texts': ['中文', 'abc']}]


@pytest.mark.parametrize('reply', [
    {'hashes': ['0' * 64], 'counts': [{'model_tokens': 1, 'cl100k_tokens': 2}]},
    {'hashes': [], 'counts': []},
    {'hashes': [hashlib.sha256(b'abc').hexdigest()], 'counts': [{'model_tokens': -1, 'cl100k_tokens': 2}]},
    {'hashes': [hashlib.sha256(b'abc').hexdigest()], 'counts': [{'model_tokens': True, 'cl100k_tokens': 2}]},
])
def test_proxy_rejects_wrong_or_invalid_results(reply):
    port, received, thread = serve_once(lambda request: dict(reply, identity={}))
    with pytest.raises(ValueError):
        proxy(port, 'test-secret', {}).count_many(['abc'])
    thread.join(timeout=2)


def test_proxy_rejects_non_loopback_configuration():
    with pytest.raises(ValueError):
        proxy(0, 'secret', {})


def test_counter_service_rejects_wrong_secret_without_counting():
    local, peer = socket.socketpair()
    class Counter:
        def count_many(self, texts):
            pytest.fail('unauthorized input must never be counted')
    try:
        peer.sendall(b'{"secret":"wrong","texts":["abc"]}\n')
        with pytest.raises(ValueError):
            serve_client(local, Counter(), 'correct-secret')
    finally:
        local.close(); peer.close()


def test_proxy_rejects_counts_from_different_counter_identity():
    reply = {'identity': {'model_sha256': 'wrong'},
             'hashes': [hashlib.sha256(b'abc').hexdigest()],
             'counts': [{'model_tokens': 1, 'cl100k_tokens': 2}]}
    port, received, thread = serve_once(lambda request: reply)
    with pytest.raises(ValueError, match='bound'):
        proxy(port, 'test-secret', {'model_sha256': 'expected'}).count_many(['abc'])
    thread.join(timeout=2)


def test_counter_requires_explicit_positive_budgets():
    with pytest.raises(TypeError):
        CounterProxy(1234, 'secret', {})
    with pytest.raises(ValueError):
        CounterProxy(1234, 'secret', {}, model_budget=0, cl_limit=2038)
