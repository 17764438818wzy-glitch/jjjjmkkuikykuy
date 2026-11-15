"""Original native receipt predicates, shared without importing any worker."""
import json
from pathlib import Path


def retained_attachment_inventory(files,materials):
    """Keep display-only reading scope without changing the sealed upload plan."""
    by_hash={}
    def visit(rows):
        for row in rows:
            if not isinstance(row,dict):continue
            if row.get('sha256'):by_hash.setdefault(row['sha256'],[]).append(row)
            visit(row.get('members') or [])
    visit(materials)
    result=[]
    for row in files:
        saved={k:row.get(k) for k in ('file_ref','sha256','kind','source_name','parse_status')}
        matches=by_hash.get(row.get('sha256'),[])
        exact=[m for m in matches if row.get('original_path') and m.get('original_path')==row['original_path']]
        matches=exact or matches
        source=matches[0] if len(matches)==1 else {}
        saved['preview_truncated']=source.get('preview_truncated')
        saved['limits']=[str(v)[:240] for v in (source.get('limits') or [])[:3]]
        result.append(saved)
    return result
SCHEMA="uploaded-products-retirement/1"
FIELD="preextract_retirement"
def read(path):return json.loads(Path(path).read_text(encoding="utf-8-sig"))

def identity(payload):
    return {key: payload[key] for key in ('service', 'task', 'manifest_sha256')}


def terminal(job):
    """A committed verified row, including an interrupted local cleanup."""
    record = (job.get('projection') or {}).get(FIELD) or {}
    routes=record.get('routes') or [];targets=record.get('targets') or {}
    verified=bool(routes and set(targets)=={r['code'] for r in routes} and all(
        targets[r['code']].get('verified') is True and targets[r['code']].get('retrieval_verified') is True
        and targets[r['code']].get('body',{}).get('dataset_id')==r['paper_dataset_id']
        and targets[r['code']].get('body',{}).get('document_id') for r in routes))
    return bool(verified and job.get('status') == 'complete' and record.get('schema') == SCHEMA
        and record.get('identity') == identity(job['payload'])
        and record.get('delivery_id') == job['id'] and record.get('verified_at', 0) > 0
        and record.get('source_sha256') == job['payload']['item'].get('source_hash')
        and record.get('status') in {'pending', 'complete'})


def verify_associated_receipt(prepared, work, body, plan, *, receipt=None):
    if not plan or not plan['files']:
        return
    if receipt is None:receipt = read(Path(work) / 'associated-delivery-receipt.json')
    expected = {'service': body['service'], 'task': prepared['task'],
                'manifest_sha256': prepared['validation']['manifest_sha256'],
                'plan_sha256': prepared['associated_upload_plan_sha256']}
    routes = {r['code']: r for r in plan['routes']}
    if (receipt.get('schema') != 'associated-original-delivery/1' or receipt.get('status') != 'complete'
            or receipt.get('identity') != expected
            or set(receipt.get('files', {})) != {r['file_ref'] for r in plan['files']}
            or set(receipt.get('body_links', {})) != set(routes)):
        raise ValueError('associated receipt identity or exact reference set changed')
    for code, route in routes.items():
        parent = body['targets'][code]['body']; link = receipt['body_links'][code]
        if (link.get('verified') is not True or link.get('dataset_id') != route['paper_dataset_id']
                or link.get('document_id') != parent['document_id']):
            raise ValueError('associated reverse body link not verified')
    for row in plan['files']:
        entry = receipt['files'][row['file_ref']]
        if (entry.get('status') != 'verified' or not entry.get('file_id') or entry.get('sha256') != row['sha256']
                or set(entry.get('targets', {})) != set(routes)):
            raise ValueError('associated file target set not verified')
        for code, route in routes.items():
            target = entry['targets'][code]; parent = body['targets'][code]['body']
            if (target.get('verified') is not True or not target.get('document_id')
                    or target.get('dataset_id') != route['paper_dataset_id']
                    or target.get('parent_document_id') != parent['document_id']):
                raise ValueError('associated file belongs to another body or route')

