"""Syntax-only recovery for profile JSON; every supplied scalar must survive.

Reuse json-repair 0.60.1, copied with its licence from the installed RAGFlow
runtime. Schema, source evidence and independent review remain separate gates.
The caller rejects length-truncated provider responses before invoking this.
"""
import copy
import hashlib
import json
import re
import sys
from pathlib import Path


_TOKEN = re.compile(r'\s+|"(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"|true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|[{}\[\],:]')


def _scalars(raw):
    values = []
    pos = 0
    while pos < len(raw):
        token = _TOKEN.match(raw, pos)
        if token is None:
            raise ValueError('profile_json_invalid_token')
        value = token.group()
        pos = token.end()
        if value.isspace() or value in '{}[],:':
            continue
        # Preserve string characters exactly, even when serialization changes
        # an equivalent Unicode escape. Keep numeric spelling and primitive type.
        values.append(('string', json.loads(value)) if value.startswith('"')
                      else ('literal', value))
    return values


def _repair(raw):
    vendor = Path(__file__).with_name('profile_runtime') / 'python'
    if str(vendor) not in sys.path:
        sys.path.insert(0, str(vendor))
    from json_repair import repair_json
    return repair_json(raw, ensure_ascii=False)


def loads_profile(raw):
    if not isinstance(raw, str):
        raise ValueError('profile_json_not_text')
    try:
        return json.loads(raw, parse_constant=_invalid_constant)
    except ValueError:
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError('profile_json_repair_size_limit') from None
        original = _scalars(raw)
        repaired = _repair(raw)
        if not isinstance(repaired, str) or _scalars(repaired) != original:
            raise ValueError('profile_json_repair_changed_scalar')
        return json.loads(repaired, parse_constant=_invalid_constant)


def _invalid_constant(value):
    raise ValueError('profile_json_non_json_constant')


def same_scalars(before, after):
    return _scalars(json.dumps(before, ensure_ascii=False)) == _scalars(json.dumps(after, ensure_ascii=False))


def resolve_generation_report(report, author_spans, source_version):
    """Resolve unambiguous exchange wiring; never rewrite a claim or rating.

    Moving terminal root fields is syntax only and retains the strict scalar
    guard. Adding exact source-span definitions is a separate compilation step,
    counted against the evidence budget and still subject to native import and
    independent semantic review. Original evidence definitions take priority.
    """
    if not isinstance(report,dict):raise ValueError('profile_structure_report_invalid')
    resolved=copy.deepcopy(report); moved=[]
    units=resolved.get('units',[])
    if isinstance(units,list):
        for index,unit in enumerate(units):
            if not isinstance(unit,dict):continue
            for ai,assessment in enumerate(unit.get('assessments',[])):
                if not isinstance(assessment,dict) or not isinstance(assessment.get('grades'),dict):continue
                grades=assessment['grades'];fields=[k for k in grades if k in ('allowed_scope','limitations','evidence_keys','reason')]
                if not fields:continue
                if any(k in assessment for k in fields):raise ValueError('profile_structure_assessment_field_conflict')
                if list(grades)[-len(fields):]!=fields:raise ValueError('profile_structure_nonterminal_assessment_field')
                rebuilt={}
                for key,value in assessment.items():
                    rebuilt[key]={k:v for k,v in grades.items() if k not in fields} if key=='grades' else value
                    if key=='grades':
                        for field in fields:rebuilt[field]=grades[field]
                unit['assessments'][ai]=rebuilt
                moved.extend(f'units.{index}.assessments.{ai}.grades.{k}' for k in fields)
        for index,unit in enumerate(units):
            if not isinstance(unit,dict):continue
            fields=[key for key in unit if key in ('applicable_levels','relations')]
            if not fields:continue
            if index!=len(units)-1:raise ValueError('profile_structure_nonterminal_root_field')
            for key in fields:
                if key in resolved:raise ValueError('profile_structure_root_field_conflict:'+key)
                resolved[key]=unit.pop(key);moved.append(key)
        if moved and not same_scalars(report,resolved):
            raise ValueError('profile_structure_repair_changed_scalar')
    definitions=resolved.get('evidence',[])
    if not isinstance(definitions,list):raise ValueError('profile_reference_definitions_invalid')
    known={e.get('key') for e in definitions if isinstance(e,dict) and isinstance(e.get('key'),str)}
    missing=[]
    def visit(value):
        if isinstance(value,dict):
            for key,child in value.items():
                if key=='evidence_keys' and isinstance(child,list):
                    for ref in child:
                        if isinstance(ref,str) and ref not in known and ref not in missing:missing.append(ref)
                else:visit(child)
        elif isinstance(value,list):
            for child in value:visit(child)
    visit(resolved)
    spans={s['id']:s for s in author_spans};aliases=[]
    for ref in missing:
        span=spans.get(ref)
        if span is None:raise ValueError('profile_reference_unknown:'+ref)
        if not isinstance(span.get('text'),str) or len(span['text'].strip().encode('utf-16-le'))//2<2:
            raise ValueError('profile_reference_source_too_short:'+ref)
        aliases.append({'key':ref,'span_ids':[ref],'quote':span['text']})
        definitions.append({'key':ref,'span_ids':[ref],'section':'原文引用'})
    if aliases:resolved['evidence']=definitions
    if not moved and not aliases:return resolved,None
    if not isinstance(source_version,str) or not source_version:raise ValueError('profile_reference_source_version_missing')
    digest=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False).encode('utf8')).hexdigest()
    return resolved,{'policy':'exact-source-reference/1.0','source_version':source_version,
        'candidate_sha256':digest(report),'resolved_sha256':digest(resolved),
        'structural_fields':moved,'aliases':aliases,
        'provided_evidence_count':len(report.get('evidence',[])),
        'resolved_evidence_count':len(definitions)}
