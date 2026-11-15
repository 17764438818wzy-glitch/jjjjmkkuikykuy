"""Verified model route configuration, with no credentials in queue settings."""
from __future__ import annotations

import json
from pathlib import Path

from upload_tracking import service_key


def _no_credentials(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if any(token in key.lower() for token in ('api_key', 'password', 'secret', 'authorization')):
                raise ValueError('Route settings must not contain credentials')
            _no_credentials(item)
    elif isinstance(value, list):
        for item in value:
            _no_credentials(item)


def load_routes(path: Path, service_url: str) -> dict | None:
    if not Path(path).is_file():
        return None
    value = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('Model routes must be an object')
    _no_credentials(value)
    if not value.get('enabled'):
        return None
    if service_key(value.get('service_url', '')) != service_key(service_url):
        raise ValueError('Model routes belong to a different RAGFlow service')
    for key in ('dataset_id', 'source_pipeline_id', 'contract_version'):
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise ValueError('Route identity is incomplete')
    if type(value.get('total_limit')) is not int or value['total_limit'] < 1:
        raise ValueError('Invalid total capacity')
    for channel in ('school', 'official'):
        route = value.get(channel)
        if not isinstance(route, dict):
            raise ValueError('Both DeepSeek routes are required')
        for key in ('channel_key', 'pipeline_id', 'model_id', 'provider', 'instance'):
            if not isinstance(route.get(key), str) or not route[key].strip():
                raise ValueError('Model binding is incomplete')
        initial, maximum = route.get('initial_limit'), route.get('max_limit')
        if (type(initial) is not int or type(maximum) is not int
                or not 1 <= initial <= maximum):
            raise ValueError('Invalid model channel capacity')
        route.update(service_key=service_key(service_url), dataset_id=value['dataset_id'],
                     source_pipeline_id=value['source_pipeline_id'],
                     contract_version=value['contract_version'])
    if (value['school']['pipeline_id'] != value['source_pipeline_id']
            or value['school']['pipeline_id'] == value['official']['pipeline_id']
            or value['school']['channel_key'] == value['official']['channel_key']):
        raise ValueError('Two distinct verified Pipeline bindings are required')
    return value
