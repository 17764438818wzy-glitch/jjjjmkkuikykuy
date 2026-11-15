import io
import json
from urllib.error import HTTPError, URLError

import pytest


def _png():
    from PIL import Image
    stream = io.BytesIO()
    Image.new('RGB', (2, 2), 'white').save(stream, format='PNG')
    return stream.getvalue()


class _Response:
    headers = {'Content-Type': 'application/json'}

    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, _limit=-1):
        return self.body


def _ok(usage=None, result=None):
    result = result or {'decision': 'pass', 'reason': 'ok'}
    envelope = {
        'choices': [{'finish_reason': 'stop',
                     'message': {'content': json.dumps(result, ensure_ascii=False)}}]
    }
    if usage is not None:
        envelope['usage'] = usage
    return _Response(json.dumps(envelope).encode())


def _http(code, text=''):
    return HTTPError('https://vision.test/v1/chat/completions', code, text, {}, io.BytesIO(text.encode()))


class _Opener:
    def __init__(self, *actions):
        self.actions = list(actions)
        self.requests = []

    def open(self, request, **_kwargs):
        self.requests.append(request)
        action = self.actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return action


def _cfg(**overrides):
    cfg = {
        'timeout': 5,
        'attempts': 1,
        'max_tokens': 64,
        'concurrency': {'enabled': False, 'max_retries': 1, 'backoff_base_seconds': 0},
        'vision_model': {
            'enabled': True,
            'primary': {'base_url': 'https://primary.test/v1', 'api_key': '', 'model': 'primary-vision'},
            'fallback': {'enabled': True, 'base_url': 'https://fallback.test/v1', 'api_key': '', 'model': 'fallback-vision'},
            'retry': {'max_tries': 1, 'backoff_base_seconds': 0},
            'circuit_breaker': {'failure_threshold': 3, 'cooldown_seconds': 300,
                               'cooldown_multiplier': 2.0, 'max_cooldown_seconds': 3600},
        },
    }
    for key, value in overrides.items():
        if key == 'vision_model':
            cfg['vision_model'].update(value)
        else:
            cfg[key] = value
    return cfg


def _client(tmp_path, opener, **overrides):
    from agent_client import AgentClient
    from billing import Ledger
    client = AgentClient(_cfg(**overrides), vision_key='primary-key', fallback_key='fallback-key', state_root=tmp_path)
    client.opener = opener
    client.ledger = Ledger(tmp_path)
    return client


def test_primary_503_retries_then_falls_back_and_records_channel(tmp_path, monkeypatch):
    from billing import summarize
    monkeypatch.setattr('agent_client.time.sleep', lambda _seconds: None)
    opener = _Opener(_http(503), _http(503), _ok({'prompt_tokens': 3, 'completion_tokens': 2, 'total_tokens': 5}))
    client = _client(tmp_path, opener, vision_model={'retry': {'max_tries': 2, 'backoff_base_seconds': 0}})
    result = client.call('screen', {'text': 'x'}, [_png()])
    assert result['decision'] == 'pass'
    assert len(opener.requests) == 3
    assert opener.requests[-1].full_url.startswith('https://fallback.test')
    rows = client.ledger.records()
    assert [row['channel'] for row in rows if row['outcome'] != 'awaiting_response'] == ['primary', 'primary', 'fallback']
    assert rows[-1]['ok'] is True and rows[0]['error_type'] == 'service_unavailable'
    assert summarize(rows)['fallback_requests'] == 1


def test_429_retries_without_fallback(tmp_path, monkeypatch):
    from agent_client import AgentError
    monkeypatch.setattr('agent_client.time.sleep', lambda _seconds: None)
    opener = _Opener(_http(429), _http(429), _ok())
    client = _client(tmp_path, opener, vision_model={'retry': {'max_tries': 2, 'backoff_base_seconds': 0}})
    with pytest.raises(AgentError, match='429'):
        client.call('screen', {'text': 'x'}, [_png()])
    assert len(opener.requests) == 2
    assert all(request.full_url.startswith('https://primary.test') for request in opener.requests)
    assert not (tmp_path / 'vision_circuit.json').exists()


@pytest.mark.parametrize('marker', ['multimodal', 'enable-multimodal', '多模态'])
def test_multimodal_400_switches_immediately(tmp_path, marker):
    opener = _Opener(_http(400, f'error: {marker} is not enabled'), _ok())
    client = _client(tmp_path, opener)
    assert client.call('screen', {'text': 'x'}, [_png()])['decision'] == 'pass'
    assert len(opener.requests) == 2
    assert opener.requests[-1].full_url.startswith('https://fallback.test')


@pytest.mark.parametrize('code,text', [(400, 'bad request'), (401, 'unauthorized'), (403, 'forbidden'), (404, 'missing model')])
def test_fatal_configuration_errors_stop_without_switch(tmp_path, code, text):
    from agent_client import AgentError
    opener = _Opener(_http(code, text), _ok())
    client = _client(tmp_path, opener)
    with pytest.raises(AgentError) as exc:
        client.call('screen', {'text': 'x'}, [_png()])
    assert len(opener.requests) == 1
    assert '主通道' in str(exc.value)


def test_circuit_opens_skips_primary_and_persists(tmp_path, monkeypatch):
    from billing import summarize
    monkeypatch.setattr('agent_client.time.sleep', lambda _seconds: None)
    cfg = {'vision_model': {'circuit_breaker': {'failure_threshold': 2, 'cooldown_seconds': 300}}}
    opener = _Opener(_http(503), _ok(), _http(503), _ok())
    client = _client(tmp_path, opener, **cfg)
    client.call('screen', {'text': '1'}, [_png()])
    client.call('screen', {'text': '2'}, [_png()])
    assert len([r for r in opener.requests if r.full_url.startswith('https://primary.test')]) == 2
    assert len([r for r in opener.requests if r.full_url.startswith('https://fallback.test')]) == 2
    state = json.loads((tmp_path / 'vision_circuit.json').read_text(encoding='utf-8'))
    assert state['circuit']['primary']['state'] == 'open'
    assert summarize(client.ledger.records())['circuit_breaker_trips'] == 1


def test_cooldown_probe_success_closes_and_switches_back(tmp_path, monkeypatch):
    monkeypatch.setattr('agent_client.time.sleep', lambda _seconds: None)
    cfg = {'vision_model': {'circuit_breaker': {'failure_threshold': 1, 'cooldown_seconds': 0}}}
    opener = _Opener(_http(503), _ok(), _ok())
    client = _client(tmp_path, opener, **cfg)
    client.call('screen', {'text': '1'}, [_png()])
    client.call('screen', {'text': '2'}, [_png()])
    assert opener.requests[-1].full_url.startswith('https://primary.test')
    state = json.loads((tmp_path / 'vision_circuit.json').read_text(encoding='utf-8'))
    assert state['circuit']['primary']['state'] == 'closed'


def test_circuit_state_survives_client_restart(tmp_path):
    first = _Opener(_http(503), _ok())
    _client(tmp_path, first, vision_model={'circuit_breaker': {'failure_threshold': 1}}).call('screen', {'text': '1'}, [_png()])
    second = _Opener(_ok())
    client = _client(tmp_path, second)
    client.call('screen', {'text': '2'}, [_png()])
    assert second.requests[0].full_url.startswith('https://fallback.test')


def test_probe_is_billed(tmp_path, monkeypatch):
    from billing import summarize
    class DeterministicRandom:
        def shuffle(self, _values):
            return None
    monkeypatch.setattr('random.SystemRandom', lambda: DeterministicRandom())
    opener = _Opener(_ok({'prompt_tokens': 4, 'completion_tokens': 1, 'total_tokens': 5},
                         {'decision': 'pass', 'reason': 'ok', 'colors': ['red', 'green', 'blue', 'yellow']}))
    client = _client(tmp_path, opener)
    assert '图片读取测试通过' in client.probe(vision=True)
    rows = client.ledger.records()
    assert len(rows) == 1 and rows[0]['channel'] == 'primary'
    assert rows[0]['scope'] == 'processing' and rows[0]['total_tokens'] == 5
    assert summarize(rows)['requests'] == 1


def test_fallback_models_can_be_listed_before_model_name_is_selected(tmp_path):
    from agent_client import AgentClient
    payload={'data':[{'id':'fallback-one'},{'id':'fallback-two'}]}
    client=_client(tmp_path,_Opener(_Response(json.dumps(payload).encode())))
    client.config['vision_model']['fallback']['model']=''
    assert client.models(vision=True,channel='fallback') == ['fallback-one','fallback-two']


def test_fallback_request_uses_shared_llm_gate(tmp_path):
    class Gate:
        def __init__(self): self.calls = 0
        def acquire(self): self.calls += 1
        def release(self): self.calls += 1
    opener = _Opener(_http(503), _ok())
    client = _client(tmp_path, opener)
    client.llm_gate = Gate()
    client.call('screen', {'text': 'x'}, [_png()])
    assert client.llm_gate.calls == 4


def test_new_config_defaults_are_blank_and_legacy_vision_migrates(tmp_path):
    from auto_config import DEFAULTS, load_config, write_json
    cfg = load_config(tmp_path)
    vision = cfg['vision_model']
    assert vision['primary']['base_url'] == '' and vision['primary']['api_key'] == '' and vision['primary']['model'] == ''
    assert vision['fallback']['base_url'] == '' and vision['fallback']['model'] == ''
    write_json(tmp_path / 'ai_settings.json', {'vision_base_url': 'https://legacy.test', 'vision_model': 'legacy-model'})
    migrated = load_config(tmp_path)
    assert migrated['vision_model']['primary']['base_url'] == 'https://legacy.test'
    assert migrated['vision_model']['primary']['model'] == 'legacy-model'
    assert DEFAULTS['vision_model']['retry']['max_tries'] == 3
