"""Stage source-bound corrections outside the running data root."""
from pathlib import Path
import hashlib
import json

from paperflow_prefect.group_classification import _title_proof
from paperflow_prefect.group_evidence import read_evidence
from paperflow_prefect.named_delivery import resolved_delivery_row
from paperflow_prefect.records import sha_file


ROOT = Path(__file__).resolve().parent
LIVE = ROOT.parents[1] / 'prefect-paperflow/runtime/data'
STAGING = ROOT / 'staged-corrections'
audit = json.loads((ROOT / 'title-correction-candidates-v2.json').read_text(encoding='utf8'))
if audit['scope'] != 'desktop108-20261029 310 groups': raise ValueError('unexpected scope')
inventories = {}
for path in (LIVE/'unified-papers').glob('*/*/*/received-inventory.json'):
    inventory = json.loads(path.read_text(encoding='utf-8-sig'))
    if 'desktop108-20261029' in inventory.get('person_dir',''):
        inventories[inventory['paper_id']] = inventory
if len(inventories) != 310: raise ValueError('scope changed')
STAGING.mkdir(exist_ok=False)
staged=[]
for item in audit['candidates']:
    record=item['record']; inv=inventories[record['paper_id']]; body=inv['body']
    if (record['group_id'] != inv['group_id'] or record['body_file_id'] != body['file_id']
            or record['source_sha256'] != body['sha256'] or sha_file(record['source_path']) != body['sha256']):
        raise ValueError('source identity changed')
    converted=record.get('converted_evidence')
    if converted:
        material_path=Path(converted['material_path']).resolve(); evidence_path=Path(converted['path']).resolve()
        allowed=(LIVE/'papers'/record['paper_id']/'body').resolve()
        if (not material_path.is_relative_to(allowed) or not evidence_path.is_relative_to(material_path.parent/'private')
                or sha_file(material_path)!=converted['material_sha256'] or sha_file(evidence_path)!=converted['sha256']):
            raise ValueError('conversion identity changed')
        material=json.loads(material_path.read_text(encoding='utf-8-sig'))
        if (material.get('source',{}).get('source_sha256') != body['sha256']
                or material.get('file_hashes',{}).get(str(evidence_path)) != converted['sha256']):
            raise ValueError('conversion evidence changed')
    else:
        evidence_path=Path(record['source_path'])
    if not _title_proof(read_evidence(evidence_path),record['title'],record['excerpt'],
                        labelled=True,reference=record['reference']):
        raise ValueError('title no longer matches source')
    destination=STAGING/'naming-overrides'/record['paper_id']/'title.json'
    destination.parent.mkdir(parents=True,exist_ok=False)
    with destination.open('x',encoding='utf8') as f: json.dump(record,f,ensure_ascii=False,indent=2)
    if not converted:
        actual=resolved_delivery_row(inv,body,STAGING).get('display_name')
        if actual != item['corrected_display_name']:raise ValueError('staged title not usable')
    staged.append({'paper_id':record['paper_id'],'group':item['group'],'record':str(destination),
                   'record_sha256':sha_file(destination),'display_name':item['corrected_display_name'],
                   'uses_retained_conversion':bool(converted)})
with (STAGING/'manifest.json').open('x',encoding='utf8') as f:
    json.dump({'scope':audit['scope'],'records':staged,'live_writes':0},f,ensure_ascii=False,indent=2)
print(json.dumps({'staged':len(staged),'locally_resolved':sum(not x['uses_retained_conversion'] for x in staged),
                  'converted_evidence_verified':sum(x['uses_retained_conversion'] for x in staged)},ensure_ascii=False))
