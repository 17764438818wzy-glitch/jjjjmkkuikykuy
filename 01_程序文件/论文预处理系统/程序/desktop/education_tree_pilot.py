"""Run one isolated education document through the Graph-only pipeline."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from education_direct_tree import bind_tree_chunks, extract_tree
from education_stream_ingest import education_remote_name, write_json
from knowledge_package import knowledge_files


ROOT = Path(__file__).resolve().parent
PRIVATE = ROOT / 'data/private/education_pilot'
NATIVE = '--native' in sys.argv
SOURCE_DOCUMENT_ID = 'AUTO_c7aa51b128fd4415ab29'
SOURCE_PACKAGE = ROOT / 'data/private/education_full_ingest/AUTO_20260924_215028_e9790d/canonical_packages' / SOURCE_DOCUMENT_ID
RECEIPT = PRIVATE / ('tree_native_pilot.json' if NATIVE else 'tree_optimization_pilot.json')
DATASET_NAME = '教育原生目录直提·隔离验证 md' if NATIVE else '教育目录直提·隔离验证 md'
OPTIMIZATION_FILE = 'tree_native_optimization.json' if NATIVE else 'tree_optimization.json'
TREE_FILE = 'tree_native_pilot_tree.json' if NATIVE else 'tree_optimization_pilot_tree.json'


def context():
    sys.path.insert(0, str(PRIVATE))
    from deploy_pilot import client, state
    optimization = json.loads((PRIVATE / OPTIMIZATION_FILE).read_text(encoding='utf-8'))
    return client, state, optimization


def remote_document(client, dataset_id, doc_id):
    rows = client.documents(dataset_id, document_id=doc_id)
    if len(rows) != 1 or rows[0]['id'] != doc_id:
        raise ValueError('隔离试跑文档身份不一致')
    return rows[0]


def submit():
    client, deployment, optimization = context()
    knowledge_files(SOURCE_PACKAGE, require_stable_paper_id=True)
    manifest = json.loads((SOURCE_PACKAGE / 'manifest.json').read_text(encoding='utf-8'))
    if manifest['document_id'] != SOURCE_DOCUMENT_ID:
        raise ValueError('试跑源文档身份不一致')
    filename = education_remote_name(SOURCE_PACKAGE / 'ingest.md')
    receipt = json.loads(RECEIPT.read_text(encoding='utf-8')) if RECEIPT.exists() else {}
    source_dataset = next(row for row in client.datasets() if row['id'] == deployment['paper_dataset_id'])
    matches = [row for row in client.datasets() if row['name'] == DATASET_NAME]
    if len(matches) > 1:
        raise ValueError('隔离知识库同名重复')
    dataset = matches[0] if matches else client.request('POST', '/datasets', {
        'name': DATASET_NAME, 'description': '验证目录 Tree 直接取 ingest.md 标题、Graph 单独编译。',
        'language': source_dataset['language'], 'permission': 'me',
        'chunk_method': 'naive', 'embedding_model': source_dataset['embedding_model'],
        'parser_config': source_dataset['parser_config']})
    dataset_id = dataset['id']
    if receipt and (receipt.get('dataset_id') != dataset_id
                    or receipt.get('paper_id') != manifest['paper_id']
                    or receipt.get('filename') != filename):
        raise ValueError('隔离试跑回执身份不一致')
    if not receipt:
        receipt = {'dataset_id': dataset_id, 'paper_id': manifest['paper_id'],
                   'source_document_id': SOURCE_DOCUMENT_ID, 'filename': filename,
                   'pipeline_id': optimization['pipeline_id'], 'status': 'upload_intent'}
        write_json(RECEIPT, receipt)
    if not receipt.get('ragflow_document_id'):
        rows = client.documents(dataset_id)
        found = [row for row in rows if row['name'] == filename]
        if len(found) > 1:
            raise ValueError('隔离试跑远端文档重复')
        if not found:
            if receipt['status'] != 'upload_intent':
                raise ValueError('隔离上传状态不确定')
            remote_id = client.upload(dataset_id, SOURCE_PACKAGE / 'ingest.md', remote_name=filename)
        else:
            remote_id = found[0]['id']
        receipt.update(ragflow_document_id=remote_id, status='uploaded')
        write_json(RECEIPT, receipt)
    doc_id = receipt['ragflow_document_id']
    doc = remote_document(client, dataset_id, doc_id)
    if doc['name'] != filename:
        raise ValueError('隔离试跑文档名称不符')
    if doc.get('pipeline_id') != optimization['pipeline_id']:
        if doc['run'] != 'UNSTART':
            raise ValueError('不能在已解析文档上更换 Pipeline')
        client.request('PATCH', f'/datasets/{dataset_id}/documents/{doc_id}',
                       {'pipeline_id': optimization['pipeline_id']})
        doc = remote_document(client, dataset_id, doc_id)
        if doc['pipeline_id'] != optimization['pipeline_id']:
            raise ValueError('隔离 Pipeline 绑定未生效')
    if doc['run'] == 'UNSTART' and receipt['status'] != 'parse_intent':
        receipt.update(status='parse_intent', parse_requested_at=time.time())
        write_json(RECEIPT, receipt)
        client.parse(dataset_id, doc_id)
        receipt['status'] = 'submitted'
        write_json(RECEIPT, receipt)
    elif doc['run'] in ('RUNNING', 'DONE', 'FAIL'):
        receipt['status'] = doc['run'].lower()
        write_json(RECEIPT, receipt)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))


def status():
    client, _, optimization = context()
    receipt = json.loads(RECEIPT.read_text(encoding='utf-8'))
    dataset_id, doc_id = receipt['dataset_id'], receipt['ragflow_document_id']
    doc = remote_document(client, dataset_id, doc_id)
    if doc['name'] != receipt['filename'] or doc['pipeline_id'] != optimization['pipeline_id']:
        raise ValueError('隔离试跑身份或 Pipeline 变更')
    result = {key: doc.get(key) for key in ('id', 'run', 'chunk_count', 'progress', 'process_duration')}
    result['progress_tail'] = (doc.get('progress_msg') or '')[-450:]
    metadata = doc.get('meta_fields') or {}
    result['admission'] = metadata.get('education_admission')
    result['profile_present'] = bool(metadata.get('artifact_profile'))
    result['profile_tier'] = metadata.get('artifact_example_tier')
    if doc['run'] == 'DONE':
        chunks = []
        for page in range(1, 101):
            payload = client.request('GET', f'/datasets/{dataset_id}/documents/{doc_id}/chunks?page={page}&page_size=100')
            chunks.extend(payload.get('chunks') or [])
            if len(chunks) >= payload.get('total', 0):
                break
        else:
            raise ValueError('隔离试跑切片分页超限')
        ingest = (SOURCE_PACKAGE / 'ingest.md').read_text(encoding='utf-8')
        tree = bind_tree_chunks(extract_tree(ingest, receipt['paper_id'], doc_id), chunks)
        write_json(PRIVATE / TREE_FILE, tree)
        structure = client.request('GET', f'/datasets/{dataset_id}/documents/{doc_id}/structure/graph')
        result['template_kinds'] = [t['kind'] for t in structure.get('templates') or []]
        result['graph_entities'] = sum(len(t.get('entities') or []) for t in structure.get('templates') or []
                                       if t['kind'] == 'knowledge_graph')
        pages = [t for t in structure.get('templates') or [] if t['kind'] == 'page_index']
        if pages:
            native_nodes = pages[0].get('entities') or []
            original_titles = {node['title'] for node in tree['nodes']}
            native_titles = {node.get('name') for node in native_nodes if node.get('type') == 'title'}
            body_ids = {chunk['id'] for chunk in chunks if chunk.get('available')
                        and 'role=body' in (chunk.get('important_keywords') or [])}
            result['native_page_entities'] = len(native_nodes)
            result['native_non_title_entities'] = sum(node.get('type') != 'title' for node in native_nodes)
            result['native_missing_titles'] = sorted(original_titles - native_titles)[:20]
            result['native_extra_titles'] = sorted(native_titles - original_titles)[:20]
            result['native_unlinked_titles'] = sum(not set(node.get('source_chunk_ids') or []) <= body_ids
                                                    or not node.get('source_chunk_ids') for node in native_nodes)
        result['tree_headings'] = len(tree['nodes'])
        result['tree_unlinked'] = tree['unlinked_headings']
        result['tree_path'] = str(PRIVATE / TREE_FILE)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    {'submit': submit, 'status': status}[sys.argv[1]]()
