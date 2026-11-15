"""Stage independently checked same-person cover titles; no live writes."""
from pathlib import Path
import json

from paperflow_prefect.named_delivery import resolved_delivery_row
from paperflow_prefect.records import sha_file


ROOT = Path(__file__).resolve().parent
DATA = ROOT.parents[1]/'prefect-paperflow/runtime/data'
STAGING = ROOT/'staged-separate-cover-corrections'
evidence_files = sorted((ROOT/'separate-cover-evidence').glob('*.json'))
if len(evidence_files) != 4:
    raise ValueError('expected the four scoped separate covers')
inventories = {}
for path in (DATA/'unified-papers').glob('*/*/*/received-inventory.json'):
    inv = json.loads(path.read_text(encoding='utf-8-sig'))
    inventories[inv['paper_id']] = inv
STAGING.mkdir(exist_ok=False)
staged = []
for file in evidence_files:
    evidence = json.loads(file.read_text(encoding='utf8'))
    if not evidence.get('cover_title_confirmed') or not evidence.get('cover_person_and_id_match'):
        raise ValueError('cover title or identity was not verified')
    inv = inventories[evidence['paper_id']];body = inv['body']
    ref = inv['handoff_ref'];handoff_path=Path(ref['path'])
    if (ref['sha256'] != evidence['handoff_sha256'] or sha_file(handoff_path) != ref['sha256']
            or body['sha256'] != evidence['body_sha256'] or body['file_id'] != evidence['body_file_id']
            or sha_file(evidence['body_path']) != body['sha256']
            or sha_file(evidence['cover_path']) != evidence['cover_sha256']):
        raise ValueError('originals or frozen handoff changed')
    handoff = json.loads(handoff_path.read_text(encoding='utf-8-sig'))
    covers = [row for key in ('attachments','trash','unresolved') for row in handoff.get(key,[])
              if '封面' in Path(row.get('relative','')).stem]
    if len(covers) != 1 or covers[0]['sha256'] != evidence['cover_sha256']:
        raise ValueError('cover does not match frozen handoff')
    record = {'schema':'paperflow-title-correction/2','paper_id':inv['paper_id'],
              'group_id':inv['group_id'],'body_file_id':body['file_id'],'source_sha256':body['sha256'],
              'source_path':evidence['body_path'],'title':evidence['title'],
              'reference':'同组独立封面','excerpt':evidence['title_excerpt'],
              'cover_reference':evidence['title_reference'],'cover_file_id':covers[0]['file_id'],
              'cover_source_path':evidence['cover_path'],'cover_sha256':evidence['cover_sha256'],
              'handoff_path':str(handoff_path),'handoff_sha256':ref['sha256']}
    dest=STAGING/'naming-overrides'/inv['paper_id']/'title.json'
    dest.parent.mkdir(parents=True,exist_ok=False)
    with dest.open('x',encoding='utf8') as stream:json.dump(record,stream,ensure_ascii=False,indent=2)
    resolved=resolved_delivery_row(inv,body,STAGING)
    if not resolved.get('display_name'):
        raise ValueError('staged separate cover correction did not pass readback')
    staged.append({'group':evidence['group'],'paper_id':inv['paper_id'],'record':str(dest),
                   'record_sha256':sha_file(dest),'display_name':resolved['display_name']})
    print(json.dumps({'group':evidence['group'],'display_name':resolved['display_name']},ensure_ascii=False),flush=True)
with (STAGING/'manifest.json').open('x',encoding='utf8') as stream:
    json.dump({'scope':'desktop108-20261029','records':staged,'live_writes':0},stream,ensure_ascii=False,indent=2)
print(json.dumps({'staged':len(staged),'live_writes':0},ensure_ascii=False),flush=True)
