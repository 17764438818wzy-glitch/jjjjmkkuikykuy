"""Read-only audit of method labels and unlinked evidence in completed papers."""

from __future__ import annotations

import collections
import json
import sys
from pathlib import Path

from education_method_graph import METHOD_CUES, canonical_method
from education_stream_ingest import write_json

ROOT = Path(__file__).resolve().parent
RUN = ROOT / 'data/private/education_full_ingest/AUTO_20260924_215028_e9790d'
REPORT = RUN / 'method_graph_audit.json'


def rows(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    return value if isinstance(value, list) else []


def run(limit: int = 100) -> dict:
    sys.path.insert(0, str(ROOT / 'data/private/education_pilot'))
    from deploy_pilot import client, state

    ledger = json.loads((RUN / 'receipts.json').read_text(encoding='utf-8'))['primary']
    unknown = collections.Counter()
    unmatched = collections.Counter()
    examples = collections.defaultdict(list)
    checked = 0
    for paper_id, receipt in ledger.items():
        if checked >= limit:
            break
        if receipt.get('status') != 'done' or receipt.get('method_graph_status') not in ('done', 'needs_review'):
            continue
        source_id = receipt['ragflow_document_id']
        docs = client.documents(state['paper_dataset_id'], document_id=source_id)
        if len(docs) != 1 or docs[0].get('id') != source_id:
            continue
        meta = docs[0].get('meta_fields') or {}
        if meta.get('education_paper_id') != paper_id:
            continue
        graph_path = RUN / 'method_graphs' / (paper_id + '.json')
        graph = json.loads(graph_path.read_text(encoding='utf-8'))
        methods = rows(meta.get('education_method_details') or meta.get('education_research_methods'))
        steps = rows(meta.get('education_research_steps'))
        for item in methods:
            name = item.get('name') if isinstance(item, dict) else item
            if isinstance(name, str) and canonical_method(name) is None:
                unknown[name.strip()] += 1
        actual = [step for step in steps if isinstance(step, dict) and step.get('status') == '实际执行']
        for name in graph.get('unlinked_methods') or []:
            cues = METHOD_CUES[name]
            named = [step for step in actual if any(cue in (step.get('name') or '') for cue in cues)]
            if not named:
                reason = 'no_named_actual_step'
            elif not any(len(''.join((step.get('quote') or '').split())) >= 8 for step in named):
                reason = 'missing_step_quote'
            else:
                reason = 'quote_not_in_single_body_chunk'
            unmatched[(name, reason)] += 1
            if len(examples[name + ':' + reason]) < 3:
                examples[name + ':' + reason].append({
                    'paper_id': paper_id,
                    'actual_step_names': [step.get('name') for step in actual][:12]})
        checked += 1
    result = {'checked': checked,
              'unknown_declared_methods': dict(unknown.most_common()),
              'unlinked_reasons': [{'method': name, 'reason': reason, 'count': count}
                                   for (name, reason), count in unmatched.most_common()],
              'examples': dict(examples)}
    write_json(REPORT, result)
    return {key: result[key] for key in ('checked', 'unknown_declared_methods', 'unlinked_reasons')}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=True, indent=2))
