"""Check only this frozen 310-group batch against local source evidence."""
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json

from paperflow_prefect.records import sha_file
from paperflow_prefect.group_evidence import read_evidence
from paperflow_prefect.group_sort_cli import _title_from_body
from paperflow_prefect.group_classification import _title_proof
from paperflow_prefect.intake_handoff import _display

PROJECT = Path(r'F:\论文预处理系统\预处理系统2\prefect-paperflow')
DATA = PROJECT / 'runtime/data'
DESKTOP = Path.home() / 'Desktop' / '108清单'
out = Path(__file__).resolve().parent / 'title-correction-candidates-v2.json'
if out.exists(): raise FileExistsError('audit already exists; do not overwrite evidence')
invs=[]
for ip in (DATA/'unified-papers').glob('*/*/*/received-inventory.json'):
    inv=json.loads(ip.read_text(encoding='utf-8-sig'))
    if 'desktop108-20261029' in inv.get('person_dir',''):invs.append((ip,inv))
assert len(invs)==310 and len({inv['paper_id'] for _,inv in invs})==310
counts=Counter(); candidates=[]; unresolved=[]
for _,inv in invs:
    body=inv.get('body') or {}
    if not body: counts['no_body']+=1;continue
    original=DESKTOP/Path(inv['person_dir']).name/Path(body['relative'])
    try:
        if not original.is_file() or sha_file(original)!=body['sha256']:
            raise ValueError('desktop_source_not_verified')
        converted_info=None
        if original.suffix.lower() in {'.doc','.wps','.odt','.rtf'}:
            works=list((DATA/'papers'/inv['paper_id']/'body').glob('*/material.json'))
            if len(works)!=1: raise ValueError('no_unique_retained_conversion')
            material_path=works[0]; material=json.loads(material_path.read_text(encoding='utf-8-sig'))
            if material.get('source',{}).get('source_sha256')!=body['sha256']:
                raise ValueError('conversion_source_mismatch')
            matches=[]
            for filename, digest in material.get('file_hashes',{}).items():
                f=Path(filename)
                if (f.suffix.lower()=='.docx' and f.is_file() and
                        f.resolve().is_relative_to((material_path.parent/'private').resolve()) and sha_file(f)==digest):
                    matches.append((f,digest))
            if len(matches)!=1: raise ValueError('no_unique_verified_conversion')
            evidence_path,digest=matches[0]
            converted_info={'path':str(evidence_path.resolve()),'sha256':digest,
                            'material_path':str(material_path.resolve()),'material_sha256':sha_file(material_path)}
        else:
            evidence_path=original
        evidence=read_evidence(evidence_path)
        title,reference,excerpt,confirmed=_title_from_body(body,evidence)
        confirmed=bool(title and confirmed and _title_proof(evidence,title,excerpt,labelled=True,reference=reference))
        if not confirmed:
            counts['title_not_reconfirmed']+=1
            if not body.get('display_name'):unresolved.append({'group':Path(inv['person_dir']).name,'reason':'title_not_confirmed'})
            continue
        name=_display(title,'body',None,original.suffix)
        if name==body.get('display_name'):
            counts['name_confirmed_unchanged']+=1;continue
        record={'schema':'paperflow-title-correction/1','paper_id':inv['paper_id'],'group_id':inv['group_id'],
                'body_file_id':body['file_id'],'source_sha256':body['sha256'],'source_path':str(original.resolve()),
                'title':title,'reference':reference,'excerpt':excerpt}
        if converted_info:record['converted_evidence']=converted_info
        candidates.append({'group':Path(inv['person_dir']).name,'old_display_name':body.get('display_name'),
                           'corrected_display_name':name,'record':record})
        counts['source_verified_correction']+=1
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        counts['evidence_unavailable']+=1
        unresolved.append({'group':Path(inv['person_dir']).name,'reason':str(exc)[:160]})
result={'observed_at':datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(),
        'scope':'desktop108-20261029 310 groups','counts':dict(counts),'candidates':candidates,
        'unresolved':unresolved,'model_calls':0,'remote_writes':0}
with out.open('x',encoding='utf8') as f:json.dump(result,f,ensure_ascii=False,indent=2)
print(json.dumps({'path':str(out),'counts':dict(counts),
    'corrections':[{'group':x['group'],'old':x['old_display_name'],'new':x['corrected_display_name']} for x in candidates],
    'unresolved_count':len(unresolved)},ensure_ascii=False))
