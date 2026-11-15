import io
import json


def test_schema_failure_gets_a_third_attempt(tmp_path, monkeypatch):
    from agent_client import AgentClient

    monkeypatch.setattr('agent_client.time.sleep', lambda _seconds: None)
    responses = [
        {'decision': 'pass', 'reason': 'missing sensitive_strings'},
        {'decision': 'pass', 'reason': 'missing sensitive_strings'},
        {'decision': 'pass', 'reason': 'ok', 'sensitive_strings': []},
    ]

    class Opener:
        def __init__(self):
            self.calls = 0

        def open(self, *_args, **_kwargs):
            self.calls += 1
            body = {
                'choices': [{'finish_reason': 'stop',
                             'message': {'content': json.dumps(responses.pop(0))}}],
            }
            return io.BytesIO(json.dumps(body).encode())

    opener = Opener()
    client = AgentClient({'base_url': 'https://example.test/v1',
                          'text_model': 'model', 'attempts': 3}, 'key')
    client.opener = opener

    assert client.call('privacy_text', {'text': 'x'})['decision'] == 'pass'
    assert opener.calls == 3
