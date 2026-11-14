"""Keep already-started folders bound to the backend that owns their writes."""


def select_backend(configured, records, targets, attachments):
    if configured == 'qdrant':
        # A new Qdrant publication is distinct from any old service request.
        return 'qdrant'
    backends={row.get('backend','ragflow') for row in [*targets.values(),*attachments.values()] if row}
    backends.update(row['data']['backend'] for row in records if row.get('kind')=='rag-backend')
    if any(row.get('kind','').startswith('rag_effect_') for row in records): backends.add('ragflow')
    if len(backends)>1: raise ValueError('mixed RAG backends in one folder require reconciliation')
    backend=next(iter(backends),configured)
    if backend not in ('ragflow','qdrant'): raise ValueError('unknown RAG backend')
    return backend


def progress_kinds(records, targets, attachments):
    """Keep historic service attempts distinct from the new backend's progress."""
    backends = {row.get('backend', 'ragflow')
                for row in [*targets.values(), *attachments.values()] if row}
    if len(backends) > 1:
        raise ValueError('mixed RAG backends in one progress record require reconciliation')
    old_targets = bool(backends - {'qdrant'})
    old_events = any(row.get('kind', '').startswith('rag_effect_') or
                     (row.get('kind') == 'rag-backend' and row.get('data', {}).get('backend') == 'ragflow')
                     for row in records)
    if old_targets or old_events:
        return 'rag-qdrant-body', 'rag-qdrant-attachments', 'rag-qdrant-backend'
    return 'rag-body', 'rag-attachments', 'rag-backend'


def qdrant_api_key(settings):
    from auto_config import load_key
    settings.require_execution()
    key=load_key(settings.path('settings_root'),'qdrant')
    if not key: raise ValueError('RAG credential unavailable')
    return key


async def probe_qdrant(settings):
    import httpx
    async with httpx.AsyncClient(timeout=5,trust_env=False) as client:
        response=await client.get(settings['qdrant_gateway_url'].rstrip('/')+'/paperflow/status',
            headers={'Authorization':'Bearer '+qdrant_api_key(settings)})
        response.raise_for_status()
        if response.json().get('backend')!='qdrant' or response.json().get('status')!='ok':
            raise ValueError('Qdrant readiness not confirmed')
        return response.json()
