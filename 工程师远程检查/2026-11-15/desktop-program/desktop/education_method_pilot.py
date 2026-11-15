"""Find two distinct education papers linked through one evidenced method."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from education_method_graph import build_method_graph, lookup_method, merge_method_graphs
from education_stream_ingest import write_json

ROOT = Path(__file__).resolve().parent
PRIVATE = ROOT / 'data/private/education_pilot'
RECEIPTS = ROOT / 'data/private/education_full_ingest/AUTO_20260924_215028_e9790d/receipts.json'
REPORT = PRIVATE / 'education_method_transfer_pilot.json'


def run(limit: int = 30) -> dict:
    sys.path.insert(0, str(PRIVATE))
    from deploy_pilot import client, state

    receipts = json.loads(RECEIPTS.read_text(encoding='utf-8'))['primary']
    graphs, titles, issues = [], {}, []
    dataset_id = state['paper_dataset_id']
    for paper_id, receipt in receipts.items():
        if len(graphs) >= limit:
            break
        if receipt.get('status') != 'done' or receipt.get('admission') != '入库':
            continue
        doc_id = receipt.get('ragflow_document_id')
        rows = client.documents(dataset_id, document_id=doc_id)
        if (len(rows) != 1 or rows[0].get('id') != doc_id
                or rows[0].get('name') != receipt.get('upload_name')
                or rows[0].get('run') != 'DONE'):
            issues.append({'paper_id': paper_id, 'issue': 'remote_identity'})
            continue
        doc = rows[0]
        chunks = []
        for page in range(1, 101):
            payload = client.request('GET',
                f'/datasets/{dataset_id}/documents/{doc_id}/chunks?page={page}&page_size=100')
            batch = payload.get('chunks') or []
            chunks.extend(batch)
            if len(chunks) >= payload.get('total', 0):
                break
            if not batch:
                break
        try:
            graph = build_method_graph(paper_id, doc_id, doc.get('meta_fields') or {}, chunks)
        except ValueError as exc:
            issues.append({'paper_id': paper_id, 'issue': str(exc)[:100]})
            continue
        graphs.append(graph)
        titles[paper_id] = doc['name']
    index = merge_method_graphs(graphs)
    groups = {}
    for method in index['methods']:
        edges = lookup_method(index, method['name'])
        if len(edges) >= 2:
            groups[method['name']] = edges
    winner = max(groups, key=lambda name: len(groups[name])) if groups else None
    sample = groups[winner][:2] if winner else []
    result = {'checked_papers': len(graphs), 'methods': len(index['methods']),
              'method_edges': len(index['edges']), 'shared_method': winner,
              'two_paper_sources': [{'paper_id': edge['paper_id'],
                  'title': titles[edge['paper_id']], 'source_doc_id': edge['source_doc_id'],
                  'source_chunk_ids': edge['source_chunk_ids']} for edge in sample],
              'issues': issues}
    write_json(REPORT, result)
    return result


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
