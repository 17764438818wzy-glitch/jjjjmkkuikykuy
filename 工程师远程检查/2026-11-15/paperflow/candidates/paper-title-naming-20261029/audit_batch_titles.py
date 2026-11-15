"""Read-only title evidence check for this batch; no models or remote calls."""
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import re

from paperflow_prefect.records import sha_file
from paperflow_prefect.group_sort_cli import _read_for_sort, _title_from_body
from paperflow_prefect.group_classification import _title_proof

PROJECT = Path(r'F:\论文预处理系统\预处理系统2\prefect-paperflow')
DATA = PROJECT / 'runtime/data'
DESKTOP = Path.home() / 'Desktop' / '108清单'
out = Path(__file__).resolve().parent / 'title-evidence-audit.json'
if out.exists(): raise FileExistsError('audit already exists; do not overwrite evidence')
rows = []
invs = []
for ip in (DATA / 'unified-papers').glob('*/*/*/received-inventory.json'):
    inventory = json.loads(ip.read_text(encoding='utf-8-sig'))
    if 'desktop108-20261029' in inventory.get('person_dir', ''):
        invs.append((ip, inventory))
assert len(invs) == 310 and len({x['paper_id'] for _, x in invs}) == 310
for ip, inv in invs:
    body = inv.get('body') or {}
    if not body: continue
    rag_path = ip.parent / 'stages/rag-body.json'
    names = []
    if rag_path.is_file():
        rag = json.loads(rag_path.read_text(encoding='utf-8-sig'))
        if rag.get('state') == 'complete' and rag.get('result', {}).get('status') == 'verified':
            names = [(target.get('body') or {}).get('name', '') for target in rag['result'].get('targets', {}).values()]
    received = Path(body['display_name']).stem if body.get('display_name') else None
    rag_titles = [re.sub(r'__[0-9a-f]{20}_[A-Za-z0-9]+_正文\.md$', '', name) for name in names]
    if received and (not rag_titles or all(title == received for title in rag_titles)):
        continue
    source = DESKTOP / Path(inv['person_dir']).name / Path(body['relative'])
    row = {'paper_id': inv['paper_id'], 'group': Path(inv['person_dir']).name,
           'received_title': received, 'rag_titles': sorted(set(rag_titles)),
           'source_path': str(source), 'source_sha256': body['sha256'], 'inventory': str(ip)}
    try:
        if not source.is_file() or sha_file(source) != body['sha256']:
            raise ValueError('source_missing_or_hash_mismatch')
        evidence = _read_for_sort(source)
        title, reference, excerpt, confirmed = _title_from_body(body, evidence)
        verified = bool(title and confirmed and _title_proof(evidence, title, excerpt, labelled=True, reference=reference))
        row.update(title=title or None, title_confirmed=verified,
                   title_reference=reference, title_excerpt=excerpt if title else None,
                   source_unchanged=sha_file(source) == body['sha256'])
    except Exception as exc:
        row.update(title=None, title_confirmed=False, error=type(exc).__name__)
    rows.append(row)
summary = Counter('title_confirmed' if row.get('title_confirmed') else 'title_unconfirmed' for row in rows)
result = {'observed_at':datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(),
          'scope':'desktop108-20261029 310 groups', 'papers_checked':len(rows),
          'summary':dict(summary), 'model_calls':0, 'remote_writes':0, 'rows':rows}
with out.open('x', encoding='utf8') as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print(json.dumps({'report':str(out), 'papers_checked':len(rows), 'summary':dict(summary),
                  'confirmed_examples':[{'group':r['group'],'title':r['title']} for r in rows if r.get('title_confirmed')][:12],
                  'errors':[{'group':r['group'],'error':r['error']} for r in rows if r.get('error')]}, ensure_ascii=False))
