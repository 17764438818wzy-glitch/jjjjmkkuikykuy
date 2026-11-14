"""Bounded saved introduction; shared by native retirement and the read-only dashboard."""
import hashlib
import json
from pathlib import Path
import time

def _object(value):
    return value if isinstance(value, dict) else {}


class DisplayFields:
    """Explicit field selection and a shared text budget, not a second summary model."""
    def __init__(self):
        self.remaining = 24000
        self.truncated = False

    def text(self, value, limit=1200):
        if not isinstance(value, str):
            return ''
        value = value.strip()
        size = min(limit, self.remaining)
        self.truncated |= len(value) > size
        self.remaining -= min(len(value), size)
        return value[:size] + ('…' if len(value) > size and size else '')

    def strings(self, value, count=8, limit=600):
        values = [value] if isinstance(value, str) else value if isinstance(value, list) else []
        values = list(dict.fromkeys(v.strip() for v in values if isinstance(v, str) and v.strip()))
        self.truncated |= len(values) > count
        return [text for v in values[:count] if (text := self.text(v, limit))]

    def records(self, value, field, count=4):
        rows = [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []
        self.truncated |= len(rows) > count
        result = []
        for row in rows[:count]:
            body = self.text(row.get(field), 700)
            kind = self.text(row.get('type'), 100)
            if body or kind:
                result.append(f'{kind}：{body}' if kind and body else body or kind)
        return result


def display_profile(profile):
    select = DisplayFields()
    article = _object(profile.get('article'))
    education = _object(profile.get('education_level'))
    applicable = _object(profile.get('applicable_level'))
    research = _object(profile.get('research_profile'))
    boundary = _object(profile.get('migration_boundary'))
    value = {
        'title': select.text(article.get('title') or article.get('original_title'), 300),
        'material_type': select.text(profile.get('material_type'), 200),
        'summary': select.text(article.get('summary'), 3000),
        'keywords': select.strings(article.get('keywords'), 16, 100),
        'education_level': select.text(education.get('level_as_stated'), 100),
        'original_major': select.text(education.get('major_as_stated'), 200),
        'degree_type': select.text(education.get('degree_type_as_stated'), 100),
        'applicable_levels': select.strings(applicable.get('labels'), 8, 150),
        'research': {key: select.text(research.get(key), 1800) for key in ('problem', 'object', 'overall_path')},
        'reusable_scope': select.text(boundary.get('reusable_scope'), 1800),
        'must_recollect': select.strings(boundary.get('must_recollect')),
        'do_not_reuse': select.strings(boundary.get('must_not_reuse_as_new_facts')),
        'limitations': select.strings(boundary.get('global_limits')),
        'source_hints': select.strings([r.get('locator_hint') for r in research.get('source_refs', []) if isinstance(r, dict)]
                                       if isinstance(research.get('source_refs'), list) else [], 4, 200),
    }
    units = profile.get('learning_units')
    units = [u for u in units if isinstance(u, dict)] if isinstance(units, list) else []
    value['learning_unit_count'] = len(units)
    # Core units first, preserving original relative order within each group.
    units = sorted(units, key=lambda unit: unit.get('is_core') is not True)
    select.truncated |= len(units) > 12
    value['learning_units'] = []
    for unit in units[:12]:
        value['learning_units'].append({
            'title': select.text(unit.get('title'), 200),
            'is_core': unit.get('is_core') is True,
            'work_attribution': select.text(unit.get('work_attribution'), 100),
            'learn': select.text(unit.get('what_to_learn'), 900),
            'method': select.text(unit.get('method'), 1200),
            'materials': select.records(unit.get('materials'), 'description'),
            'new_material_needs': select.strings(unit.get('new_material_needs'), 4),
            'outputs': select.records(unit.get('outputs'), 'content'),
            'conditions': select.strings(unit.get('conditions'), 4),
            'limitations': select.strings(unit.get('limitations'), 4),
        })
    value['truncated'] = select.truncated
    return value


PROFILE_SCHEMA='paper-asset-introduction/1'

def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf8')).hexdigest()

def capture_profile(work,prepared,binding):
    path=Path(work)/'private/candidate.json'
    if not path.is_file():
        return dict(schema=PROFILE_SCHEMA,status='missing',reason='历史画像文件未保留',binding=binding)
    if path.stat().st_size>8*1024*1024:raise ValueError('asset_profile_exceeds_read_limit')
    content=path.read_bytes();candidate_sha=hashlib.sha256(content).hexdigest()
    candidate=json.loads(content.decode('utf-8-sig'))
    source=binding['task']['source_sha256']
    if (prepared.get('task')!=binding['task']
            or prepared.get('validation',{}).get('manifest_sha256')!=binding['manifest_sha256']
            or candidate_sha!=prepared.get('candidate_sha256')
            or candidate.get('source',{}).get('source_sha256')!=source):
        raise ValueError('asset_profile_identity_changed')
    data=candidate.get('profile')
    if not isinstance(data,dict) or not data:raise ValueError('asset_profile_missing_fields')
    display=display_profile(data)
    return dict(schema=PROFILE_SCHEMA,status='available',binding=binding,source_sha256=source,
        candidate_sha256=candidate_sha,display_sha256=digest(display),display=display,
        saved_at=path.stat().st_mtime,captured_at=time.time())

def verified_profile(value,binding):
    return bool(isinstance(value,dict) and value.get('schema')==PROFILE_SCHEMA
        and value.get('status')=='available' and value.get('binding')==binding
        and value.get('source_sha256')==binding['task']['source_sha256']
        and value.get('candidate_sha256') and isinstance(value.get('display'),dict)
        and value.get('display_sha256')==digest(value['display']))
