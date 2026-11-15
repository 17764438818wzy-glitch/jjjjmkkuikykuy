import json
import pytest


def route_config():
    return {'enabled': True, 'service_url': 'http://localhost:9380', 'dataset_id': 'edu-md',
            'source_pipeline_id': 'school-pipe', 'contract_version': 'hash-v1',
            'total_limit': 8, 'school': {'channel_key': 'school', 'pipeline_id': 'school-pipe',
                'model_id': 'school-model', 'provider': 'OpenAI-API-Compatible', 'instance': 'ustc',
                'initial_limit': 6, 'max_limit': 12},
            'official': {'channel_key': 'official', 'pipeline_id': 'official-pipe',
                'model_id': 'official-model', 'provider': 'DeepSeek', 'instance': 'ds',
                'initial_limit': 2, 'max_limit': 2}}


def test_routes_require_same_service_and_two_distinct_pipeline_bindings(tmp_path):
    from ingest_model_routes import load_routes
    path = tmp_path/'routes.json'
    path.write_text(json.dumps(route_config()), encoding='utf-8')
    result = load_routes(path, 'http://localhost:9380')
    assert result['school']['initial_limit'] == 6
    assert result['official']['model_id'] == 'official-model'
    with pytest.raises(ValueError):
        load_routes(path, 'http://other:9380')
    config = route_config(); config['official']['pipeline_id'] = 'school-pipe'
    path.write_text(json.dumps(config), encoding='utf-8')
    with pytest.raises(ValueError):
        load_routes(path, 'http://localhost:9380')


def test_missing_or_disabled_routes_preserve_legacy_dispatch(tmp_path):
    from ingest_model_routes import load_routes
    path = tmp_path/'missing.json'
    assert load_routes(path, 'http://localhost:9380') is None
    path.write_text(json.dumps({'enabled': False}), encoding='utf-8')
    assert load_routes(path, 'http://localhost:9380') is None


def test_invalid_capacity_and_secret_fields_are_rejected(tmp_path):
    from ingest_model_routes import load_routes
    path = tmp_path/'routes.json'
    for change in ({'initial_limit': 0}, {'api_key': 'never-save-this'}):
        config = route_config(); config['official'].update(change)
        path.write_text(json.dumps(config), encoding='utf-8')
        with pytest.raises(ValueError):
            load_routes(path, 'http://localhost:9380')
