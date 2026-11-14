"""LAN endpoints work on every model channel without allowing public HTTP."""
import pytest

from agent_client import AgentClient, AgentError


@pytest.fixture(params=['text', 'legacy_vision', 'primary', 'fallback'])
def connect(request, tmp_path):
    def run(base):
        config = {'base_url': base, 'model': 'test-text'}
        if request.param == 'legacy_vision':
            config.update(vision_base_url=base, vision_model='test-vision')
        elif request.param in {'primary', 'fallback'}:
            config['vision_model'] = {request.param: {
                'enabled': True, 'base_url': base, 'model': 'test-vision'}}
        client = AgentClient(config, key='test-key', vision_key='test-key',
                             fallback_key='test-key', state_root=tmp_path)
        if request.param in {'primary', 'fallback'}:
            return client.connection(vision=True, channel=request.param)
        return client.connection(vision=request.param == 'legacy_vision')
    return run


@pytest.mark.parametrize('base,expected', [
    ('http://192.168.1.120:8081', 'http://192.168.1.120:8081/v1'),
    ('http://10.0.0.2:8081/v1/', 'http://10.0.0.2:8081/v1'),
    ('http://172.16.0.1/api/v1', 'http://172.16.0.1/api/v1'),
    ('http://172.31.255.254/v1', 'http://172.31.255.254/v1'),
    ('http://[fd12::1]:8081', 'http://[fd12::1]:8081/v1'),
    ('http://localhost:8081', 'http://localhost:8081/v1'),
    ('http://127.0.0.1:8081', 'http://127.0.0.1:8081/v1'),
    ('http://[::1]:8081', 'http://[::1]:8081/v1'),
    ('https://api.example.com/custom', 'https://api.example.com/custom'),
])
def test_local_http_and_public_https_preserve_endpoint(connect, base, expected):
    assert connect(base) == (expected, 'test-key')


@pytest.mark.parametrize('base', [
    'http://8.8.8.8/v1', 'http://api.example.com/v1',
    'http://192.168.1.120.example.com/v1',
    'http://172.15.255.254/v1', 'http://172.32.0.1/v1',
    'http://169.254.169.254/v1', 'http://0.0.0.0/v1',
    'http://[::]/v1', 'http://[2001:4860:4860::8888]/v1',
])
def test_public_and_special_http_addresses_stay_blocked(connect, base):
    with pytest.raises(AgentError, match='HTTPS'):
        connect(base)


@pytest.mark.parametrize('base', [
    '192.168.1.120:8081', 'ftp://192.168.1.120/v1',
    'http://user:password@192.168.1.120/v1',
    'http://192.168.1.120/v1?token=test', 'http://192.168.1.120/v1#fragment',
])
def test_lan_support_keeps_url_structure_validation(connect, base):
    with pytest.raises(AgentError):
        connect(base)
