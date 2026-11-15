"""Probe structure deletion semantics on one isolated education document only."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from education_stream_ingest import write_json

ROOT = Path(__file__).resolve().parent
PRIVATE = ROOT / 'data/private/education_pilot'


def run():
    sys.path.insert(0, str(PRIVATE))
    from deploy_pilot import client

    receipt = json.loads((PRIVATE / 'tree_native_pilot.json').read_text(encoding='utf-8'))
    dataset_id = receipt['dataset_id']
    doc_id = receipt['ragflow_document_id']
    if dataset_id != 'd3784048b8f211f1a8846d29005ec004':
        raise ValueError('只允许操作原生目录隔离知识库')
    dataset = next(row for row in client.datasets() if row['id'] == dataset_id)
    if dataset['name'] != '教育原生目录直提·隔离验证 md':
        raise ValueError('隔离知识库名称不符')
    docs = client.documents(dataset_id, document_id=doc_id)
    if len(docs) != 1 or docs[0]['run'] != 'DONE' or docs[0]['pipeline_id'] != receipt['pipeline_id']:
        raise ValueError('隔离文档身份或状态不符')
    endpoint = f'/datasets/{dataset_id}/documents/{doc_id}/structure/graph'
    before = client.request('GET', endpoint)
    kinds = {row['kind'] for row in before['templates']}
    if kinds != {'knowledge_graph', 'page_index'}:
        raise ValueError('隔离文档结构种类不符')
    backup = PRIVATE / 'tree_native_structure_before_delete.json'
    if backup.exists():
        previous = json.loads(backup.read_text(encoding='utf-8'))
        if previous != before:
            raise ValueError('隔离结构与既有备份不一致')
    else:
        write_json(backup, before)
    old_chunks = docs[0]['chunk_count']
    graph_template_id = next(row['template_id'] for row in before['templates']
                             if row['kind'] == 'knowledge_graph')
    response = client.request('DELETE', endpoint, {'template_id': graph_template_id})
    after = client.request('GET', endpoint)
    final_doc = client.documents(dataset_id, document_id=doc_id)[0]
    result = {'before': {row['kind']: len(row['entities']) for row in before['templates']},
              'after': {row['kind']: len(row['entities']) for row in after['templates']},
              'chunks_before': old_chunks, 'chunks_after': final_doc['chunk_count'],
              'delete_response': response}
    write_json(PRIVATE / 'tree_native_structure_delete_probe.json', result)
    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == '__main__':
    run()
