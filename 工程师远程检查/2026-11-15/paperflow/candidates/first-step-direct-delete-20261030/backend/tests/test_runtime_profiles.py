import json
import pytest

from paperflow_prefect.settings import Settings


def test_environment_selects_isolated_configuration_without_overriding_explicit_path(tmp_path, monkeypatch):
    isolated = tmp_path / 'isolated.json'
    explicit = tmp_path / 'explicit.json'
    isolated.write_text(json.dumps({'profile': 'isolated'}), encoding='utf8')
    explicit.write_text(json.dumps({'profile': 'explicit'}), encoding='utf8')
    monkeypatch.setenv('PAPERFLOW_CONFIG', str(isolated))
    assert Settings.load()['profile'] == 'isolated'
    assert Settings.load(explicit)['profile'] == 'explicit'


def test_classification_requires_its_existing_local_source_manifest(tmp_path):
    manifest = tmp_path / 'source-manifest.json'
    settings = Settings({'source_manifest': str(manifest)})
    with pytest.raises(FileNotFoundError):
        settings.require_local_classification()
    manifest.write_text('{"files":{}}', encoding='utf8')
    settings.require_local_classification()
