"""Exact operator-owned mapping; methods and source major never grant admission."""
import json
from pathlib import Path


def load_catalog(path):
    value=json.loads(Path(path).read_text(encoding='utf8'))
    rows=value.get('majors') or []
    for key in ['code','paper_dataset_id','image_dataset_id']:
        vals=[r.get(key) for r in rows]
        if not vals or any(not isinstance(v,str) or not v for v in vals) or len(set(vals))!=len(vals):
            raise ValueError('specialty_catalog_not_unique:'+key)
    return value


def confirmed_routes(profile,catalog):
    entries={r['code']:r for r in catalog['majors']}
    result=[]
    seen=set()
    for route in profile.get('routes') or []:
        code=route.get('code')
        if (code in seen or code not in entries or route.get('verification')!='verified'
                or not route.get('evidence_ids') or not str(route.get('reason') or '').strip()):continue
        result.append(dict(entries[code],evidence_ids=route['evidence_ids'],reason=route['reason']))
        seen.add(code)
    return result
