"""Publish the existing integrated cards against the sealed body's ordered routes."""
from pathlib import Path
from urllib.parse import urlencode
from .records import Records, identity, read_json, sha_file, write_json
from .attachment_publish import ensure_original_folder, ingest_attachment
from .attachment_ingest import read_attachment_delivery


def delivery_for(backend, paper, row):
    card = backend.attachment_card(paper, row)
    return dict(paper_id=paper['paper_id'], attachment_id=row['file_id'],
                source_path=card['original'], source_sha256=card['source_sha256'],
                md_path=card['path'], md_sha256=card['md_sha256'], delivery_version=card['version'])


def receipt_root_for(backend, paper, row):
    key = identity([paper['paper_id'], row['file_id']])
    if any(not Path(p).is_dir() for p in backend.settings.values.get('attachment_receipt_roots', [])):
        raise ValueError('已配置的历史附件回执根不可用，不能视为首次上传')
    roots = [backend.root / 'attachment-publications',
             *(Path(p) for p in backend.settings.values.get('attachment_receipt_roots', []))]
    matches = [p for p in roots if (p / key / 'receipt.json').is_file()]
    if len(matches) > 1: raise ValueError('同一附件有多个入库回执位置，须核对原身份')
    if matches: return matches[0]
    from .unified_guards import audit_legacy_uploads
    audit_legacy_uploads(backend,paper,row)
    # The old description-only publisher has a different contract. Do not replace it
    # or infer that a received item with retained publications is a new upload.
    work = backend.attachment_work(paper, row)
    if (work / 'delivery').exists() and any((work / 'delivery').glob('*.json')):
        raise ValueError('此附件已有旧版说明入库回执，需按原回执核对迁移')
    if row.get('history', {}).get('limitations'): raise ValueError('附件历史尚未核对完成')
    return roots[0]


def sealed_routes(backend, paper):
    from paperflow.package import validate_package
    prepared = read_json(backend.prepared_path(paper))
    packet = validate_package(prepared['package_path'], prepared['task'], prepared['catalog'], strict_seal=True)
    if prepared['task']['source_sha256'] != paper['body']['sha256']:
        raise ValueError('附件路由的封包不属于本篇正文')
    routes = packet['routes']
    if not routes or packet.get('pending_routes'): raise ValueError('封印专业映射尚未完整')
    targets = [dict(code=r['code'], name=r['name'], dataset_id=r['image_dataset_id']) for r in routes]
    if len({r['dataset_id'] for r in targets}) != len(targets):
        raise ValueError('封印包使用共享试跑资料库，不能自动改投正式库')
    return targets, prepared['candidate_sha256']


def publish_cards(backend, paper):
    from . import legacy
    from .intake import verify_inventory
    from .rag_status import observe_client
    from preextract_pipeline import offline_counter
    from preextract_publish import _Budget
    verify_inventory(paper)
    parent = Records(backend.paper_root(paper)).cached('rag-body')
    if not parent or parent.get('status') != 'verified': raise ValueError('正文关联尚未核验')
    targets, profile_sha = sealed_routes(backend, paper)
    deliveries = [(row, delivery_for(backend, paper, row), receipt_root_for(backend, paper, row))
                  for row in paper['attachments']]
    counter = offline_counter(backend.runtime, backend.settings.path('release_root') / 'core')
    try:
        budget = _Budget(counter)
        for _, delivery, _ in deliveries:
            budget.split(read_attachment_delivery(delivery)['md_text'], lambda i,s:'<!-- PFA '+'x'*64+' '+str(i).zfill(6)+' -->\n'+s)
        from .unified_guards import StopAwareClient
        client = observe_client(StopAwareClient(legacy.rag_client(backend.settings),backend.stops[paper['paper_id']]))
        expected_model = backend.settings.values.get('rag_embedding_model_id')
        if not expected_model: raise ValueError('专业资料库的嵌入模型 ID 尚未配置')
        for target in targets:
            result = client.request('GET', '/datasets?' + urlencode(dict(id=target['dataset_id'], page=1, page_size=100)))
            rows = result if isinstance(result, list) else result.get('datasets', [])
            matches = [r for r in rows if r.get('id') == target['dataset_id']]
            expected_name = '【'+target['code']+'】'+target['name']+'｜资料库'
            if (len(matches) != 1 or matches[0].get('name') != expected_name or
                    matches[0].get('embedding_model') != expected_model):
                raise ValueError('专业资料库名称或嵌入模型与封印映射不符：'+target['code'])
        root = client.request('GET', '/files?page=1&page_size=1').get('parent_folder', {})
        if root.get('name') != '/' or root.get('type') != 'folder' or root.get('id') != root.get('parent_id'):
            raise ValueError('RAG Files 根目录未唯一读回')
        delivered = []
        for row, delivery, receipts in deliveries:
            if backend.stops[paper['paper_id']].is_set():raise InterruptedError('此篇已取消')
            primary = targets[0]
            folder = ensure_original_folder(client, root['id'], primary['code'],
                '【'+primary['code']+'】'+primary['name']+'｜附件原件', receipts)
            route = dict(paper_id=paper['paper_id'], body_source_sha256=paper['body']['sha256'],
                         profile_sha256=profile_sha, routing_codes=[r['code'] for r in targets],
                         primary=dict(primary, parent_id=folder), references=targets[1:],
                         history_reviewed=True, parent_targets=[])
            # Keep paper_identity_only metadata stable across the existing single-file
            # and unified entries; the whole-paper handoff binds verified body IDs.
            delivered.append(ingest_attachment(delivery, route, client, receipts, budget))
        return dict(status='verified', attachments=len(delivered),
                    receipt_paths=[r['receipt_path'] for r in delivered],
                    targets=[dict(attachment_id=r['binding']['attachment_id'],
                                  original=r['raw'], publications=r['publications']) for r in delivered])
    finally: counter.close()
