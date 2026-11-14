"""Remove obsolete native education Graph templates, preserving source and Tree.

Only DONE, admitted, receipted main-dataset documents with a verified local
method graph are eligible. Backups and write intents are durable so a lost
DELETE acknowledgement cannot cause a blind retry.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from engine import digest, write_json

ROOT = Path(__file__).resolve().parent
RUN = ROOT / 'data/private/education_full_ingest/AUTO_20260924_215028_e9790d'
PILOT = ROOT / 'data/private/education_pilot'
WORK = RUN / 'native_graph_cleanup'


def complete_structure(structure: dict) -> bool:
    templates = structure.get('templates')
    if not isinstance(templates, list):
        return False
    returned_entities = structure.get('returned_entities')
    returned_relations = structure.get('returned_relations')
    # RAGFlow counts one document summary entity outside the templates. The
    # isolated delete probe showed it remains after the Graph template is gone.
    return (structure.get('total_entities') == returned_entities + 1
            and structure.get('total_relations') == returned_relations
            and sum(len(row.get('entities', ())) for row in templates) == returned_entities
            and sum(len(row.get('relations', ())) for row in templates) == returned_relations)


def cleanup_one(client, dataset_id: str, current_pipeline_id: str,
                paper_id: str, receipt: dict, ledger: dict,
                run_dir: Path = RUN, work_dir: Path = WORK) -> str:
    doc_id = receipt['ragflow_document_id']
    if receipt.get('status') != 'done' or receipt.get('admission') != '入库':
        return 'ineligible'
    method_path = run_dir / 'method_graphs' / (paper_id + '.json')
    if not method_path.exists():
        return 'method_missing'
    method = json.loads(method_path.read_text(encoding='utf-8'))
    package = run_dir / 'canonical_packages' / receipt['document_id'] / 'ingest.md'
    if (method.get('paper', {}).get('id') != paper_id
            or method.get('paper', {}).get('source_doc_id') != doc_id
            or method.get('ingest_sha256') != digest(package)):
        raise ValueError('方法 Graph 与论文身份或原文哈希不符：' + paper_id)
    docs = client.documents(dataset_id, document_id=doc_id)
    if (len(docs) != 1 or docs[0].get('id') != doc_id
            or docs[0].get('name') != receipt['upload_name']
            or docs[0].get('run') != 'DONE'
            or docs[0].get('pipeline_id') == current_pipeline_id
            or (docs[0].get('meta_fields') or {}).get('education_paper_id') != paper_id
            or (docs[0].get('meta_fields') or {}).get('education_admission') != '入库'):
        raise ValueError('旧教育文档远端身份、状态或准入不符：' + paper_id)
    doc = docs[0]
    endpoint = f'/datasets/{dataset_id}/documents/{doc_id}/structure/graph'
    before = client.request('GET', endpoint)
    if not complete_structure(before):
        raise ValueError('旧结构接口返回不完整，不能备份删除：' + paper_id)
    graphs = [row for row in before['templates'] if row['kind'] == 'knowledge_graph']
    others = [row for row in before['templates'] if row['kind'] != 'knowledge_graph']
    prior = ledger['items'].get(paper_id)
    if not graphs:
        if prior and prior['status'] in ('delete_intent', 'unknown'):
            backup = json.loads((work_dir / 'backups' / (paper_id + '.json')).read_text(encoding='utf-8'))
            if (backup['doc_id'] != doc_id or backup['doc_name'] != doc['name']
                    or backup['remaining_templates'] != others
                    or backup['chunk_count'] != doc['chunk_count']):
                raise ValueError('旧 Graph 已消失但其他来源发生变化：' + paper_id)
            prior['status'] = 'done'
            write_json(work_dir / 'receipts.json', ledger)
            return 'reconciled'
        return 'already_absent'
    if len(graphs) != 1:
        raise ValueError('旧教育文档包含多个 Graph 模板：' + paper_id)
    graph = graphs[0]
    if prior and prior.get('status') == 'done':
        raise ValueError('已删除的旧 Graph 再次出现：' + paper_id)
    if prior and prior.get('status') in ('delete_intent', 'unknown'):
        raise ValueError('上次删除结果未确定且旧 Graph 仍可见，需人工核查后再操作：' + paper_id)
    if prior and prior.get('template_id') != graph['template_id']:
        raise ValueError('删除回执模板 ID 与远端不一致：' + paper_id)
    backup_path = work_dir / 'backups' / (paper_id + '.json')
    expected = {'paper_id': paper_id, 'doc_id': doc_id, 'doc_name': doc['name'],
                'chunk_count': doc['chunk_count'], 'graph': graph,
                'remaining_templates': others}
    if backup_path.exists():
        if json.loads(backup_path.read_text(encoding='utf-8')) != expected:
            raise ValueError('旧 Graph 备份与当前远端不一致：' + paper_id)
    else:
        write_json(backup_path, expected)
    ledger['items'][paper_id] = {'doc_id': doc_id, 'template_id': graph['template_id'],
                                 'status': 'delete_intent',
                                 'entities': len(graph['entities']),
                                 'relations': len(graph['relations'])}
    write_json(work_dir / 'receipts.json', ledger)
    try:
        response = client.request('DELETE', endpoint, {'template_id': graph['template_id']})
    except Exception:
        ledger['items'][paper_id]['status'] = 'unknown'
        write_json(work_dir / 'receipts.json', ledger)
        raise
    after = client.request('GET', endpoint)
    final = client.documents(dataset_id, document_id=doc_id)[0]
    if (not complete_structure(after) or after['templates'] != others
            or final.get('chunk_count') != doc['chunk_count']
            or final.get('run') != 'DONE' or final.get('name') != doc['name']):
        ledger['items'][paper_id]['status'] = 'unknown'
        write_json(work_dir / 'receipts.json', ledger)
        raise ValueError('旧 Graph 删除后来源或目录校验未通过：' + paper_id)
    ledger['items'][paper_id].update(status='done', deleted=response.get('deleted'))
    write_json(work_dir / 'receipts.json', ledger)
    return 'deleted'


def run(limit: int | None = None) -> dict:
    sys.path.insert(0, str(PILOT))
    from deploy_pilot import client, state

    dataset_id = state['paper_dataset_id']
    if dataset_id == '01885874b4e311f1807c3d9794ef0978':
        raise ValueError('拒绝操作财会知识库')
    dataset = next(row for row in client.datasets() if row['id'] == dataset_id)
    if dataset['name'] != '教育论文研究范例·八篇试验 md':
        raise ValueError('教育主知识库名称不符')
    receipts = json.loads((RUN / 'receipts.json').read_text(encoding='utf-8'))['primary']
    ledger_path = WORK / 'receipts.json'
    ledger = json.loads(ledger_path.read_text(encoding='utf-8')) if ledger_path.exists() else {
        'dataset_id': dataset_id, 'items': {}}
    if ledger['dataset_id'] != dataset_id:
        raise ValueError('旧 Graph 删除回执知识库身份不符')
    counts = {}
    attempted = 0
    for paper_id, receipt in receipts.items():
        if limit is not None and attempted >= limit:
            break
        if receipt.get('status') != 'done' or receipt.get('admission') != '入库':
            continue
        if ledger['items'].get(paper_id, {}).get('status') == 'done':
            continue
        outcome = cleanup_one(client, dataset_id, state['full_ingest_pipeline_id'],
                              paper_id, receipt, ledger)
        counts[outcome] = counts.get(outcome, 0) + 1
        attempted += 1
    return {'dataset_id': dataset_id, 'attempted': attempted, 'outcomes': counts,
            'total_deleted': sum(row['status'] == 'done' for row in ledger['items'].values())}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int)
    args = parser.parse_args()
    print(json.dumps(run(args.limit), ensure_ascii=True, indent=2))
