"""Offline contract checks only; synthetic examples do not prove real sources."""
import copy
import hashlib
import json
from pathlib import Path
import re
import sys
import unicodedata

from jsonschema import Draft202012Validator

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from knowledge_package import encoded

SCHEMA = json.loads((HERE / 'source-education.schema.json').read_text('utf-8'))
Draft202012Validator.check_schema(SCHEMA)
VALIDATOR = Draft202012Validator(SCHEMA)
LABELS = {
    'education_level': {'associate': {'专科', '大专'}, 'bachelor': {'本科', '大学本科'},
                        'master': {'硕士', '硕士研究生'}, 'doctor': {'博士', '博士研究生'}},
    'study_mode': {'correspondence': {'函授'}, 'adult_education': {'成人', '成人教育', '成人高等教育'},
                   'full_time': {'全日制'}, 'part_time': {'非全日制'}, 'amateur': {'业余'},
                   'off_job': {'脱产'}, 'self_study': {'自考', '自学考试'}, 'open_education': {'开放教育'},
                   'online_education': {'网络教育', '远程教育'}},
}


def revision(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def check(payload):
    value = payload['source_education']
    def numbers(node):
        if isinstance(node, dict):
            for child in node.values():
                numbers(child)
        elif isinstance(node, list):
            for child in node:
                numbers(child)
        elif isinstance(node, (int, float)):
            assert type(node) is int and abs(node) <= 9007199254740991, 'unsafe JSON number'
    numbers(value)
    VALIDATOR.validate(value)
    assert payload['source_education_revision'] == revision(value), 'revision mismatch'
    if value['source_relation'] == 'original':
        assert value['original_source_sha256'] == value['evidence_source_sha256']
        assert value['derivation'] is None
    else:
        derived = value['derivation']
        assert derived and derived['input_sha256'] == value['original_source_sha256']
        assert derived['output_sha256'] == value['evidence_source_sha256']
    evidence = {row['id']: row for row in value['evidence']}
    assert len(evidence) == len(value['evidence']), 'duplicate evidence'
    kind = {'pdf': 'pdf_page', 'docx': 'docx_paragraph', 'txt': 'text_lines', 'md': 'text_lines'}
    for row in evidence.values():
        assert row['locator']['kind'] == kind[value['evidence_source_format']]
        if row['locator']['kind'] == 'text_lines':
            assert row['locator']['end_line'] >= row['locator']['start_line']
    used = set()
    for name in ('education_level', 'study_mode'):
        field = value[name]

        def references(candidate):
            assert candidate['value'] != 'unknown'
            assert candidate['raw_labels'] and candidate['evidence_ids']
            quotes = []
            for ref in candidate['evidence_ids']:
                assert ref in evidence and name in evidence[ref]['supports'], 'invalid evidence reference'
                quotes.append(evidence[ref]['quote'])
            assert all(any(label in quote for quote in quotes) for label in candidate['raw_labels']), 'invented raw label'
            normalized = {re.sub(r'\s+', '', unicodedata.normalize('NFKC', label)) for label in candidate['raw_labels']}
            if name == 'study_mode' and candidate['value'] == 'other':
                assert normalized.isdisjoint(set().union(*LABELS[name].values())), 'known label classified as other'
            else:
                assert normalized <= LABELS[name].get(candidate['value'], set()), 'label normalization mismatch'

        if field['status'] == 'source_explicit':
            references(field)
            assert not field['candidates']
        elif field['status'] == 'unknown':
            assert field['value'] == 'unknown'
            assert not field['raw_labels'] and not field['evidence_ids'] and not field['candidates']
        else:
            assert field['value'] == 'unknown', 'conflict has an effective value'
            candidates = field['candidates']
            assert len(candidates) >= 2 and len({c['value'] for c in candidates}) == len(candidates)
            for candidate in candidates:
                references(candidate)
            assert set(field['evidence_ids']) == {ref for c in candidates for ref in c['evidence_ids']}
            assert set(field['raw_labels']) == {raw for c in candidates for raw in c['raw_labels']}
        used.update(field['evidence_ids'])
    assert used == set(evidence), 'unreferenced evidence'
    mode = value['study_mode']
    expected = {'value': 'unknown', 'status': 'unknown', 'basis': 'none', 'rule_id': None,
                'rule_version': 'adult-to-correspondence/1.0', 'evidence_ids': []}
    if mode['status'] == 'conflict':
        expected.update(status='conflict', evidence_ids=mode['evidence_ids'])
    elif mode['status'] == 'source_explicit' and mode['value'] in ('correspondence', 'adult_education'):
        expected.update(value='correspondence', status='resolved', evidence_ids=mode['evidence_ids'])
        if mode['value'] == 'correspondence':
            expected.update(basis='source_correspondence', rule_id='explicit_correspondence')
        else:
            expected.update(basis='business_rule', rule_id='adult_education_to_correspondence')
    assert value['business_classification'] == expected, 'business mapping mismatch'


def main():
    cases = json.loads((HERE / 'examples.json').read_text('utf-8'))
    for case in cases:
        check(case['metadata'])
        value = case['metadata']['source_education']
        assert [value['education_level']['value'], value['study_mode']['value'],
                value['business_classification']['value']] == case['expected_values']
    adult = next(c['metadata'] for c in cases if c['case'] == 'adult_bachelor')
    unknown = next(c['metadata'] for c in cases if c['case'] == 'entirely_unmarked')
    conflict = next(c['metadata'] for c in cases if c['case'] == 'conflicting_levels')
    negatives = []
    def bad(base, description, mutate, rehash=True):
        payload = copy.deepcopy(base)
        mutate(payload['source_education'])
        if rehash:
            payload['source_education_revision'] = revision(payload['source_education'])
        else:
            payload['source_education_revision'] = '0' * 64
        try:
            check(payload)
        except (AssertionError, ValueError, TypeError, KeyError) as error:
            negatives.append(description)
            return
        except Exception as error:
            from jsonschema import ValidationError
            if isinstance(error, ValidationError):
                negatives.append(description)
                return
            raise
        raise AssertionError('Invalid case was accepted: ' + description)
    bad(adult, 'wrong revision', lambda v: None, False)
    bad(unknown, 'unknown defaulted to bachelor', lambda v: v['education_level'].update(value='bachelor'))
    bad(adult, 'adult falsely treated as explicit correspondence',
        lambda v: v['business_classification'].update(basis='source_correspondence', rule_id='explicit_correspondence'))
    bad(adult, 'fabricated raw label', lambda v: v['study_mode'].update(raw_labels=['原文未出现的函授']))
    bad(adult, 'zero PDF page', lambda v: v['evidence'][0]['locator'].update(page=0))
    bad(adult, 'unsupported version', lambda v: v.update(schema_version='source-education/2.0'))
    bad(adult, 'wrong supported field', lambda v: v['evidence'][0].update(supports=['education_level']))
    bad(conflict, 'conflict silently overwritten', lambda v: v['education_level'].update(value='bachelor'))
    bad(adult, 'conversion without provenance', lambda v: v.update(source_relation='converted'))
    bad(adult, 'floating point locator', lambda v: v['evidence'][0]['locator'].update(page=1.0))
    bad(adult, 'integer exceeds JavaScript precision', lambda v: v['evidence'][0]['locator'].update(page=9007199254740993))
    bad(adult, 'bachelor quote mislabeled doctor', lambda v: v['education_level'].update(value='doctor'))
    print(json.dumps({'synthetic_examples_passed': len(cases), 'invalid_examples_rejected': len(negatives),
                      'real_source_verification': False, 'RAG_calls': 0}, ensure_ascii=False))


if __name__ == '__main__':
    main()
