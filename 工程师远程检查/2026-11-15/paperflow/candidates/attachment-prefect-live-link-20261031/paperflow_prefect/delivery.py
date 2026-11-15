"""Publish only attachment descriptions; preserve uncertain write identities."""
from pathlib import Path
import time
from urllib.parse import quote
from .records import identity, read_json, write_json


def description_content(description):
    return '\n'.join([
        '附件类型：' + description['kind'], '附件说明：' + description['summary'],
        '论文编号：' + description['paper_id'], '附件编号：' + description['attachment_id'],
        '格式：' + description['format'],
        '读取限制：' + '；'.join(description['limitations']),
    ])


def admit_descriptions(descriptions, runtime, paperflow_root):
    """Use the same offline tokenizer/budget as the body; no remote embeddings."""
    if not descriptions: return {}
    from preextract_pipeline import offline_counter
    from preextract_publish import _Budget
    counter = offline_counter(runtime, paperflow_root)
    try:
        budget = _Budget(counter)
        result = {}
        for row in descriptions:
            content = description_content(row)
            if not budget.fits(content):
                raise ValueError('附件说明超过实际嵌入预算，请缩短说明后核对，不自动重复识别')
            result[row['attachment_id']] = {'content_sha256': identity(content),
                'counts': budget.count(content), 'model_budget': budget.model_budget,
                'cl_limit': budget.cl_limit, 'verified': True}
        return result
    finally:
        counter.close()


def publish_description(description, dataset_id, body_document_id, client, receipt_path, *, parent_identity):
    from paperflow.rag import RagPublisher, RemoteConflict
    receipt_path = Path(receipt_path)
    content = description_content(description)
    required = {'paperflow_task', 'paperflow_source_sha256', 'paperflow_manifest_sha256',
                'paperflow_revision', 'paperflow_specialty', 'paperflow_role'}
    if set(parent_identity) != required or parent_identity['paperflow_role'] != 'body':
        raise RemoteConflict('父正文身份不完整')
    key = identity([description, dataset_id, body_document_id, parent_identity, 'attachment-delivery/1'])
    name = 'PFA_' + key + '.md'
    meta = {'paper_id': description['paper_id'], 'attachment_id': description['attachment_id'],
        'attachment_source_sha256': description['source_sha256'], 'paperflow_role': 'attachment-description',
        'parent_body_document_id': body_document_id, 'description_sha256': key}
    stub = content.encode('utf8')
    state = read_json(receipt_path) if receipt_path.is_file() else {
        'schema': 'attachment-delivery/1', 'identity': key, 'dataset_id': dataset_id,
        'parent_body_document_id': body_document_id, 'status': 'pending', 'name': name}
    if state['identity'] != key: raise RemoteConflict('附件入库身份改变')
    publisher = RagPublisher(client)
    ds = quote(dataset_id, safe='')

    def save(): write_json(receipt_path, state)
    def verify_parent():
        parents = publisher.documents(dataset_id, id=body_document_id)
        if len(parents) != 1 or str(parents[0].get('status')) != '1' or any(
                (parents[0].get('meta_fields') or {}).get(k) != v for k, v in parent_identity.items()):
            raise RemoteConflict('父正文不存在、未启用或身份不符，不能关联附件')
    try:
        verify_parent()
        matches = publisher._document_lookup(dataset_id, **(
            {'id': state['document_id']} if state.get('document_id') else {'name': name}))
        matches = [row for row in matches if row.get('name') == name]
        if len(matches) > 1: raise RemoteConflict('附件说明远端身份重复')
        if not matches:
            if state.get('document_id') or state.get('upload_intent'):
                raise RuntimeError('附件说明上传结果未知或原文档缺失，不能盲目再次上传')
            state['upload_intent'] = True; save()
            state['document_id'] = client.upload_bytes(dataset_id, name, stub); save()
        else:
            doc = matches[0]
            existing = doc.get('meta_fields') or {}
            if existing and any(existing.get(k) != v for k, v in meta.items()):
                raise RemoteConflict('附件说明关联冲突')
            if not existing and publisher._download(dataset_id, doc['id']) != stub:
                raise RemoteConflict('附件说明未绑定文档内容不符')
            state['document_id'] = doc['id']; save()
        did = state['document_id']
        endpoint = f'/datasets/{ds}/documents/{quote(did, safe="")}'
        client.request('PATCH', endpoint, {'meta_fields': meta, 'enabled': 0})
        rows = publisher.documents(dataset_id, id=did)
        if len(rows) != 1 or rows[0].get('name') != name or any(
                (rows[0].get('meta_fields') or {}).get(k) != v for k, v in meta.items()):
            raise RemoteConflict('附件说明元数据回读不符')
        chunks = publisher.chunks(dataset_id, did)
        if chunks:
            if len(chunks) != 1 or chunks[0].get('content') != content:
                raise RemoteConflict('附件说明切片内容或数量不符')
            state['chunk_id'] = chunks[0]['id']; save()
        else:
            if state.get('chunk_id') or state.get('chunk_intent'):
                raise RuntimeError('附件说明切片写入结果未知，不能自动重发')
            state['chunk_intent'] = True; save()
            result = client.request('POST', endpoint + '/chunks', {'content': content})
            state['chunk_id'] = result['chunk']['id']; save()
        chunks = publisher.chunks(dataset_id, did)
        if len(chunks) != 1 or chunks[0]['id'] != state['chunk_id'] or chunks[0].get('content') != content:
            raise RemoteConflict('附件说明最终切片核验失败')
        client.request('PATCH', endpoint, {'enabled': 1})
        documents = publisher.documents(dataset_id, id=did)
        if len(documents) != 1 or str(documents[0].get('status')) != '1':
            raise RemoteConflict('附件说明未启用')
        # Repeat the identity check after activation as well.
        if any((documents[0].get('meta_fields') or {}).get(k) != v for k, v in meta.items()):
            raise RemoteConflict('附件说明启用后关联改变')
        retrieval = client.request('POST', '/retrieval', {
            'question': description['kind'] + ' ' + description['summary'],
            'dataset_ids': [dataset_id], 'document_ids': [did],
            'similarity_threshold': 0.0, 'vector_similarity_weight': 1.0,
            'top_k': 64, 'rerank_candidates_count': 64, 'page_size': 100})
        if not any(row.get('id') == state['chunk_id'] and row.get('document_id') == did
                   for row in retrieval.get('chunks', [])):
            raise RemoteConflict('附件说明尚不可检索')
        verify_parent()
        state.update(status='verified', content_sha256=identity(content), verified_at=time.time(),
                     paper_id=description['paper_id'], attachment_id=description['attachment_id'])
        state.pop('error_type', None); save()
        return state
    except BaseException as exc:
        state.update(status='uncertain', error_type=type(exc).__name__); save()
        raise
