"""Inspect separately supplied covers in the explicitly scoped 310 groups."""
from pathlib import Path
from datetime import datetime, timezone
import json
import re

from paperflow_prefect.group_classification import _title_proof
from paperflow_prefect.group_sort_cli import _read_for_sort, _title_from_body
from paperflow_prefect.records import sha_file


ROOT = Path(__file__).resolve().parent
DATA = ROOT.parents[1] / 'prefect-paperflow/runtime/data'
DESKTOP = Path.home() / 'Desktop' / '108清单'
audit = json.loads((ROOT/'title-correction-candidates-v2.json').read_text(encoding='utf8'))
wanted = {item['group'] for item in audit['unresolved']}
out = ROOT/'separate-cover-evidence'
out.mkdir(exist_ok=False)
count = 0
for inv_path in (DATA/'unified-papers').glob('*/*/*/received-inventory.json'):
    inventory = json.loads(inv_path.read_text(encoding='utf-8-sig'))
    group = Path(inventory.get('person_dir','')).name
    if group not in wanted:
        continue
    handoff_ref = inventory['handoff_ref']
    handoff_path = Path(handoff_ref['path'])
    if sha_file(handoff_path) != handoff_ref['sha256']:
        raise ValueError('frozen handoff hash mismatch')
    handoff = json.loads(handoff_path.read_text(encoding='utf-8-sig'))
    covers = [row for key in ('attachments','trash') for row in handoff.get(key,[])
              if '封面' in Path(row.get('relative','')).stem]
    if not covers:
        continue
    if len(covers) != 1:
        raise ValueError('cover is not unique')
    item = covers[0]
    body = inventory['body']
    if (handoff.get('group_id') != inventory.get('origin_group_id')
            or handoff.get('handoff_id') != inventory.get('handoff_id')
            or handoff.get('result_version') != inventory.get('result_version')
            or (handoff.get('body') or {}).get('sha256') != body.get('sha256')):
        raise ValueError('cover and body do not share the received handoff')
    cover_path = DESKTOP/group/Path(item['relative'])
    body_path = DESKTOP/group/Path(body['relative'])
    record = {'observed_at':datetime.now(timezone.utc).isoformat(), 'group':group,
              'paper_id':inventory['paper_id'], 'group_id':inventory['group_id'],
              'body_file_id':body['file_id'], 'body_path':str(body_path), 'body_sha256':body['sha256'],
              'cover_path':str(cover_path), 'cover_sha256':item['sha256'],
              'handoff_path':str(handoff_path), 'handoff_sha256':handoff_ref['sha256'],
              'model_calls':0, 'remote_writes':0}
    try:
        if sha_file(cover_path) != item['sha256'] or sha_file(body_path) != body['sha256']:
            raise ValueError('original source hash mismatch')
        evidence = _read_for_sort(cover_path)
        title, reference, excerpt, confirmed = _title_from_body(item, evidence)
        found = bool(title and confirmed and _title_proof(evidence,title,excerpt,labelled=True,reference=reference))
        normalized = re.sub(r'\s+', '', evidence.get('text',''))
        person, sid = group.rsplit('_', 1)
        identity_matches = sid in normalized and person in normalized
        if (sha_file(cover_path) != item['sha256'] or sha_file(body_path) != body['sha256']
                or sha_file(handoff_path) != handoff_ref['sha256']):
            raise ValueError('evidence changed during reading')
        record.update(title=title if found else None, title_reference=reference,
                      title_excerpt=excerpt if found else None,
                      cover_title_confirmed=found, cover_person_and_id_match=identity_matches)
    except Exception as exc:
        record.update(cover_title_confirmed=False, error_type=type(exc).__name__)
    count += 1
    with (out/(f'{count:02d}.json')).open('x',encoding='utf8') as stream:
        json.dump(record,stream,ensure_ascii=False,indent=2)
    print(json.dumps({'group':group,'title':record.get('title'),
                      'cover_confirmed':record.get('cover_title_confirmed'),
                      'identity_matches':record.get('cover_person_and_id_match'),
                      'error_type':record.get('error_type')},ensure_ascii=False),flush=True)
print(json.dumps({'groups_inspected':count,'model_calls':0,'remote_writes':0}),flush=True)
