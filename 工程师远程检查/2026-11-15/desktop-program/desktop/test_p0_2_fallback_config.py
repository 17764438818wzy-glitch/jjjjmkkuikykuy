import json
from pathlib import Path


def test_saved_runtime_config_enables_configured_fallback_channel():
    settings_path = Path(__file__).with_name('data') / 'ai_settings.json'
    settings = json.loads(settings_path.read_text(encoding='utf-8'))
    fallback = settings['vision_model']['fallback']
    assert fallback['enabled'] is True
    assert fallback['base_url']
    assert fallback['model']
