"""Submit or inspect one isolated v5 education paper with durable receipts."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from education_direct_tree import bind_tree_chunks, extract_tree
from education_method_graph import build_method_graph
from education_stream_ingest import education_remote_name, write_json
from knowledge_package import knowledge_files

ROOT = Path(__file__).resolve().parent
PRIVATE = ROOT / 'data/private/education_pilot'
PACKAGE = ROOT / 'data/private/education_full_ingest/AUTO_20260924_215028_e9790d/canonical_packages/AUTO_c7aa51b128fd4415ab29'
RECEIPT = PRIVATE / 'education_fast_pilot.json'
DATASET_NAME = '教育方法复用快速首轮·隔离验证 md'


def context():
    sys.path.insert(0, str(PRIVATE))
    from deploy_pilot import client, state
    v5 = json.loads((PRIVATE / 'education_fast_pipeline.json').read_text(encoding='utf-8'))
    return client, state, v5


def one_document(client, dataset_id, document_id):
    docs = client.documents(dataset_id, document_id=document_id)
    if len(docs) != 1 or docs[0].get('id') != document_id:
        raise ValueError('隔离文档远端身份不一致')
    return docs[0]


def submit():
    client, deployment, v5 = context()
    knowledge_files(PACKAGE, require_stable_paper_id=True)
    manifest = json.loads((PACKAGE / 'manifest.json').read_text(encoding='utf-8'))
    filename = education_remote_name(PACKAGE / 'ingest.md')
    receipt = json.loads(RECEIPT.read_text(encoding='utf-8')) if RECEIPT.exists() else {}
    source = next(row for row in client.datasets() if row['id'] == deployment['paper_dataset_id'])
    matches = [row for row in client.datasets() if row['name'] == DATASET_NAME]
    if len(matches) > 1:
        raise ValueError('隔离知识库名称重复')
    dataset = matches[0] if matches else client.request('POST', '/datasets', {
        'name': DATASET_NAME, 'description': '教育 v5 首轮 Pipeline 速度和产物验证。',
        'language': source['language'], 'permission': 'me', 'chunk_method': 'naive',
        'embedding_model': source['embedding_model'], 'parser_config': source['parser_config']})
    dataset_id = dataset['id']
    if receipt and (receipt.get('dataset_id') != dataset_id
                    or receipt.get('paper_id') != manifest['paper_id']
                    or receipt.get('filename') != filename
                    or receipt.get('pipeline_id') != v5['pipeline_id']):
        raise ValueError('隔离试跑回执身份或 Pipeline 不一致')
    if not receipt:
        receipt = {'dataset_id': dataset_id, 'paper_id': manifest['paper_id'],
                   'filename': filename, 'pipeline_id': v5['pipeline_id'],
                   'status': 'upload_intent'}
        write_json(RECEIPT, receipt)
    if not receipt.get('ragflow_document_id'):
        found = [row for row in client.documents(dataset_id) if row['name'] == filename]
        if len(found) > 1:
            raise ValueError('隔离试跑远端文档重复')
        if not found:
            if receipt['status'] != 'upload_intent':
                raise ValueError('上传结果不确定，已停止重复提交')
            remote_id = client.upload(dataset_id, PACKAGE / 'ingest.md', remote_name=filename)
        else:
            remote_id = found[0]['id']
        receipt.update(ragflow_document_id=remote_id, status='uploaded')
        write_json(RECEIPT, receipt)
    document_id = receipt['ragflow_document_id']
    doc = one_document(client, dataset_id, document_id)
    if doc.get('name') != filename:
        raise ValueError('隔离试跑文档文件名不一致')
    if doc.get('pipeline_id') != v5['pipeline_id']:
        if doc['run'] != 'UNSTART':
            raise ValueError('已解析文档不能更换 Pipeline')
        client.request('PATCH', f'/datasets/{dataset_id}/documents/{document_id}',
                       {'pipeline_id': v5['pipeline_id']})
        doc = one_document(client, dataset_id, document_id)
        if doc.get('pipeline_id') != v5['pipeline_id']:
            raise ValueError('隔离 Pipeline 绑定未生效')
    if doc['run'] == 'UNSTART' and receipt['status'] == 'uploaded':
        receipt.update(status='parse_intent', parse_requested_at=time.time())
        write_json(RECEIPT, receipt)
        client.parse(dataset_id, document_id)
        receipt['status'] = 'submitted'
        write_json(RECEIPT, receipt)
    elif doc['run'] in ('RUNNING', 'DONE', 'FAIL'):
        receipt['status'] = doc['run'].lower()
        write_json(RECEIPT, receipt)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


def status():
    client, _, v5 = context()
    receipt = json.loads(RECEIPT.read_text(encoding='utf-8'))
    doc_id = receipt['ragflow_document_id']
    dataset_id = receipt['dataset_id']
    doc = one_document(client, dataset_id, doc_id)
    if doc.get('name') != receipt['filename'] or doc.get('pipeline_id') != v5['pipeline_id']:
        raise ValueError('隔离文档身份或 Pipeline 已改变')
    metadata = doc.get('meta_fields') or {}
    result = {'run': doc['run'], 'process_duration': doc.get('process_duration'),
              'chunk_count': doc.get('chunk_count'),
              'admission': metadata.get('education_admission'),
              'profile_present': bool(metadata.get('artifact_profile')),
              'progress_tail': (doc.get('progress_msg') or '')[-450:]}
    if doc['run'] == 'DONE':
        chunks = []
        for page in range(1, 101):
            payload = client.request('GET',
                f'/datasets/{dataset_id}/documents/{doc_id}/chunks?page={page}&page_size=100')
            batch = payload.get('chunks') or []
            chunks.extend(batch)
            if len(chunks) >= payload.get('total', 0):
                break
            if not batch:
                raise ValueError('试跑正文切片分页中断')
        roles = {}
        for chunk in chunks:
            for keyword in chunk.get('important_keywords') or []:
                if keyword.startswith('role='):
                    roles[keyword] = roles.get(keyword, 0) + 1
        tree = bind_tree_chunks(extract_tree(
            (PACKAGE / 'ingest.md').read_text(encoding='utf-8'), receipt['paper_id'], doc_id), chunks)
        graph = build_method_graph(receipt['paper_id'], doc_id, metadata, chunks)
        result.update(roles=roles, tree_headings=len(tree['nodes']),
                      tree_unlinked=tree['unlinked_headings'],
                      method_nodes=len(graph['methods']), method_edges=len(graph['edges']),
                      method_unlinked=graph['unlinked_methods'])
        write_json(PRIVATE / 'education_fast_pilot_artifacts.json',
                   {'tree': tree, 'method_graph': graph})
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    {'submit': submit, 'status': status}[sys.argv[1]]()
