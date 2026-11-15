"""Normalize trusted execution failures; never inspect paper or model text."""
from __future__ import annotations

import math


def model_failure(remote: dict, diagnostic: dict | None) -> dict | None:
    if remote.get('run') != 'FAIL' or not isinstance(diagnostic, dict):
        return None
    if diagnostic.get('source') != 'execution_node':
        return None
    dependency = diagnostic.get('dependency')
    node = diagnostic.get('component_id')
    if dependency == 'llm':
        if node != 'Extractor:EducationAnnotation':
            return None
    elif dependency == 'embedding':
        if not isinstance(node, str) or not node.startswith('Indexer:'):
            return None
    else:
        return None
    kind = diagnostic.get('kind')
    if kind not in {'rate_limit', 'authentication', 'arrearage', 'unavailable', 'timeout'}:
        return None
    code = diagnostic.get('status_code')
    if code is not None and (type(code) is not int or not 400 <= code <= 599):
        return None
    try:
        delay = float(diagnostic.get('retry_after', 60))
        if not math.isfinite(delay) or delay < 0:
            raise ValueError('Invalid delay')
    except (TypeError, ValueError):
        delay = 60.0
    return {'dependency': dependency, 'kind': kind, 'status_code': code,
            'retry_after': min(delay, 3600.0)}
