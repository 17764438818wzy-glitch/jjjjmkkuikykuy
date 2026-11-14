import time
import tkinter as tk

import pytest

from agent_client import AgentClient, AgentError
from auto_config import load_config, load_key, save_config, save_key
from app import Workbench


@pytest.fixture
def ui(tmp_path):
    root = tk.Tk()
    root.withdraw()
    app = Workbench(root, tmp_path)
    root.update()
    yield root, app
    for callback in root.tk.call('after', 'info'):
        root.after_cancel(callback)
    root.destroy()


def wait_result(root, app):
    deadline = time.monotonic() + 3
    while app.busy and time.monotonic() < deadline:
        root.update()
        time.sleep(.01)
    assert not app.busy


@pytest.mark.parametrize('enabled', [False, True])
def test_model_buttons_honor_fallback_switch(ui, monkeypatch, enabled):
    root, app = ui
    app.ai_model_tabs.select(1)
    app.ai_vars['vision_fallback_enabled'].set(enabled)
    app.ai_vars['vision_fallback_base_url'].set('https://fallback.invalid/v1')
    app.ai_vars['vision_fallback_model'].set('retained-model')
    calls = []
    def models(self, vision=False, channel='primary'):
        calls.append(('models', channel))
        return ['fixture-model']
    def probe(self, vision=False, channel='primary'):
        calls.append(('probe', channel))
        return 'fixture ok'
    monkeypatch.setattr(AgentClient, 'models', models)
    monkeypatch.setattr(AgentClient, 'probe', probe)
    for button in (app.ai_read_button, app.ai_test_button):
        button.invoke()
        wait_result(root, app)
    channels = ['primary', 'fallback'] if enabled else ['primary']
    assert calls == [(action, channel) for action in ['models', 'probe'] for channel in channels]


@pytest.mark.parametrize('button_name', ['ai_read_button', 'ai_test_button'])
def test_text_failure_keeps_ui_alive_and_can_retry(ui, monkeypatch, button_name):
    root, app = ui
    errors = []
    root.report_callback_exception = lambda *args: errors.append(args)
    def fail(*args, **kwargs):
        raise AgentError('fixture HTTP 401')
    monkeypatch.setattr(AgentClient, 'models', fail)
    monkeypatch.setattr(AgentClient, 'probe', fail)
    getattr(app, button_name).invoke()
    wait_result(root, app)
    assert not errors
    assert 'HTTP 401' in app.ai_status.get()
    monkeypatch.setattr(AgentClient, 'models', lambda *args, **kwargs: ['new-model'])
    app.ai_read_button.invoke()
    wait_result(root, app)
    assert tuple(app.ai_model_combos['text_model']['values']) == ('new-model',)
    assert root.tk.call('after', 'info'), 'UI polling must continue after an error'


@pytest.mark.parametrize('vision', [False, True])
def test_invalid_key_fails_before_network(tmp_path, vision):
    config = load_config(tmp_path)
    config['base_url'] = 'https://fixture.invalid/v1'
    config['vision_model']['primary']['base_url'] = config['base_url']
    client = AgentClient(config, key='•' * 35, vision_key='•' * 35, state_root=tmp_path)
    class NeverNetwork:
        def open(self, *args, **kwargs):
            pytest.fail('Invalid key must never reach the network')
    client.opener = NeverNetwork()
    with pytest.raises(AgentError, match='API Key'):
        client.models(vision)


def test_invalid_key_save_preserves_existing_config_and_keys(tmp_path):
    config = load_config(tmp_path)
    config['vision_model']['fallback']['enabled'] = False
    save_config(tmp_path, config, 'real-text-fixture', 'real-vision-fixture')
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}
    config['text_model'] = 'new-model'
    with pytest.raises(ValueError, match='API Key'):
        save_config(tmp_path, config, 'real-text-fixture', '•' * 35)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir() if p.is_file()}


def test_disabled_fallback_does_not_require_or_overwrite_key(tmp_path):
    save_key(tmp_path, 'saved-fallback-fixture', 'ai_vision_fallback')
    config = load_config(tmp_path)
    config['vision_model']['fallback']['enabled'] = False
    save_config(tmp_path, config, 'text-fixture', 'vision-fixture', '•' * 35)
    assert load_key(tmp_path, 'ai_vision_fallback') == 'saved-fallback-fixture'
    assert not load_config(tmp_path)['vision_model']['fallback']['enabled']


@pytest.mark.parametrize('strategy', ['balanced','official_first'])
def test_balanced_provider_controls_round_trip(ui, strategy):
    from auto_ui import read_ai_form, ROUTING_LABELS
    _, app = ui
    app.ai_vars['preprocess_routing_enabled'].set(True)
    app.ai_vars['preprocess_routing_strategy'].set(ROUTING_LABELS[strategy])
    app.ai_vars['preprocess_lan_limit'].set('8')
    app.ai_vars['preprocess_official_limit'].set('16')
    cfg, _ = read_ai_form(app)
    assert cfg['preprocess_routing'] == dict(enabled=True, lan_limit=8, official_limit=16, strategy=strategy)
    save_config(app.store.root, cfg)
    assert load_config(app.store.root)['preprocess_routing'] == cfg['preprocess_routing']
    app.ai_vars['preprocess_routing_strategy'].set(ROUTING_LABELS['lan_first'])
    assert read_ai_form(app)[0]['preprocess_routing']['strategy'] == 'lan_first'


def test_config_only_save_preserves_existing_text_key(tmp_path):
    save_key(tmp_path, 'saved-text-fixture')
    cfg = load_config(tmp_path)
    cfg['preprocess_routing'].update(enabled=True, official_limit=16, strategy='balanced')
    save_config(tmp_path, cfg)
    assert load_key(tmp_path) == 'saved-text-fixture'
    save_config(tmp_path, cfg, key='')
    assert load_key(tmp_path) == ''
