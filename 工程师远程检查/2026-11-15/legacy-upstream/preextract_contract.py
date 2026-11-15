"""Collect the unchanged paper-preextract/1.0 contract without semantic repair.

All results remain private candidates. ``valid`` is a gate for deterministic
postprocessing, never proof of redaction, upload, reading or publication.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import re


DEFAULT_TEMPLATE = Path(r'E:\rag mid\exports\论文预提取_空白模板_v1.md')
VERSION = 'paper-preextract/1.0'
START = '<!-- paper-preextract:data:start -->'
END = '<!-- paper-preextract:data:end -->'
SOURCE_REF = dict(file_ref=None, block_id=None, section_id=None, asset_id=None,
                  start=None, end=None, source_kind=None, locator_hint=None)
IMAGE_REF = dict(asset_id=None, rendition_sha256=None, width=None, height=None)
METADATA_SELECTOR = dict(file_ref=None, part=None, selector=None, attribute=None)
ENUMS = {
    'record_kind': {'filled_candidate', 'partial_candidate'},
    'decision': {'keep', 'exclude', 'uncertain'},
    'work_attribution': {'author_work', 'cited_work', 'mixed', 'uncertain'},
    'status': {'documented', 'claimed_only', 'conflict', 'uncertain'},
    'storage': {'description', 'image', 'omit'},
    'evidence_basis': {'pixels', 'table_text', 'caption_context_only'},
    'scope': {'source_text', 'generated_field', 'image', 'metadata', 'filename'},
    'action': {'replace', 'delete', 'mask'},
    'location_status': {'located', 'uncertain', 'unavailable'},
    'claim_status': {'explicit', 'supported_summary', 'inference', 'uncertain'},
    'source_kind': {'author_text', 'cited_work', 'reviewer_comment', 'figure', 'table'},
}
GAP_TYPES = {'not_applicable', 'source_not_stated', 'extraction_missing', 'not_read', 'uncertain', 'source_conflict'}
INTEGER_FIELDS = {'order', 'start', 'end', 'offset', 'x0', 'y0', 'x1', 'y1', 'width', 'height'}
READING_FAILURE_CODES = {'embedded_object_review', 'native_chart_visual', 'body_container', 'scanned_page',
    'image_missing', 'formula_review', 'image_format_unreadable', 'pdf_vector_review',
    'pdf_table_review', 'external_images_missing', 'page_render_failed'}
# These diagnostics request review of an object; they do not establish that
# research content is missing. Real extraction failures keep the stronger gate.
READING_REVIEW_CODES = {'embedded_object_review', 'native_chart_visual', 'body_container',
                        'pdf_vector_review', 'pdf_table_review'}
SPLIT_HINT_PATH = '/segmentation/split_suggestions'
LEARNING_CONTENT_FIELDS = {
    'materials': ('description', 'key_fields', 'time_scope', 'comparison_objects', 'organization'),
    'steps': ('operation',),
    'outputs': ('content',),
    'formulas_parameters': ('expression_or_value',),
}


def _content_present(value):
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return any(_content_present(item) for item in value.values())
    if isinstance(value, list):
        return any(_content_present(item) for item in value)
    return value is not None


def _learning_content(rows, field):
    """Presence only: IDs, labels and references alone are not learned content."""
    if not isinstance(rows, list):
        rows = [rows]
    return any(isinstance(row, dict) and any(_content_present(row.get(key))
               for key in LEARNING_CONTENT_FIELDS[field]) for row in rows)


def _same_source_object(left, right):
    """Match a concrete object, never all objects sharing a paragraph/page."""
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False
    if left.get('object_ref') and left.get('part'):
        return (left['object_ref'] == right.get('object_ref') and left['part'] == right.get('part')
                and all(left[k] == right[k] for k in left.keys() & right.keys()))
    return (left.get('page') is not None and bool(left.get('bbox'))
            and left['page'] == right.get('page') and left['bbox'] == right.get('bbox'))


def _json(text, repairs=None):
    # Collect pairs before making a dict: normal JSON decoding would silently
    # discard the first value. Equal JSON values can collapse without choosing
    # a value; only reference arrays have additive semantics.
    merged, identical = {}, {}
    def pairs(entries):
        result = {}
        for key, value in entries:
            if key not in result:
                result[key] = value
                continue
            previous = result[key]
            canonical = lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False,
                                               separators=(',', ':'), allow_nan=False)
            if canonical(previous) == canonical(value):
                identical.setdefault(id(result), []).append(key)
                continue
            if key != 'source_refs' or not all(isinstance(v, list) and all(
                    isinstance(ref, dict) and not (ref.keys() - SOURCE_REF.keys()) for ref in v)
                    for v in (previous, value)):
                raise ValueError('duplicate JSON key: ' + key)
            # Keep existing indices; retain different locators/attribution.
            signature = lambda ref: json.dumps(ref, sort_keys=True, ensure_ascii=False)
            seen = {signature(ref) for ref in previous}
            combined = list(previous)
            for ref in value:
                if signature(ref) not in seen:
                    combined.append(ref)
                    seen.add(signature(ref))
            ambiguous = any(i >= len(previous) or signature(ref) != signature(previous[i])
                            for i, ref in enumerate(value))
            merged[id(result)] = merged.get(id(result), False) or ambiguous
            result[key] = combined
        return result
    result = json.loads(text, object_pairs_hook=pairs,
                        parse_constant=lambda value: (_ for _ in ()).throw(ValueError('non-finite JSON number')))
    locations, pointers = [], []
    def visit(value, path=''):
        if isinstance(value, dict):
            if repairs is not None:
                for key in identical.get(id(value), []):
                    repairs.append(dict(policy='collapse_identical_duplicate_key', path=path + '/' + _escape(key),
                                        research_values_changed=False, conflicting_values_overwritten=False))
            if id(value) in merged:
                locations.append((path + '/source_refs', merged[id(value)]))
            if isinstance(value.get('target_pointer'), str):
                pointers.append(value['target_pointer'])
            for key, child in value.items():
                visit(child, path + '/' + _escape(key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, path + '/' + str(index))
    visit(result)
    for path, ambiguous in locations:
        if ambiguous and any(p == path or p.startswith(path + '/') for p in pointers):
            raise ValueError('duplicate reference keys make target_pointer ambiguous')
        if repairs is not None:
            repairs.append(dict(policy='merge_duplicate_source_refs', path=path,
                                research_values_changed=False, conflicting_values_overwritten=False))
    return result


def parse_candidate(text, *, repairs=None):
    """Read one JSON object, plain, fenced, or in the MD's exact answer markers.

    Trailing commas, identical duplicates and additive reference arrays can be normalized.
    Conflicting keys, incomplete output and guessed research values are rejected.
    """
    if not isinstance(text, str):
        raise ValueError('candidate must be text')
    value = text.lstrip('\ufeff').strip()
    if START in value or END in value:
        if value.count(START) != 1 or value.count(END) != 1:
            raise ValueError('exactly one complete answer region required')
        first, last = value.index(START) + len(START), value.index(END)
        if first > last:
            raise ValueError('answer markers out of order')
        value = value[first:last].strip()
    if value.startswith('```'):
        match = re.fullmatch(r'```(?:json)?[ \t]*\r?\n(.*?)\r?\n```', value, flags=re.S | re.I)
        if not match:
            raise ValueError('one complete JSON code block required')
        value = match[1].strip()
    try:
        result = _json(value, repairs)
    except json.JSONDecodeError:
        fixed = re.sub(r'("(?:\\.|[^"\\])*")|,(?=\s*[}\]])',
                       lambda match: match.group(1) or '', value, flags=re.S)
        try:
            result = _json(fixed, repairs)
            if repairs is not None:repairs.append(dict(policy='trailing_comma_only',scalar_sequence_preserved=True))
        except json.JSONDecodeError:
            # Reuse the existing recovery that preserves every supplied key,
            # string, number and primitive in order. Schema/evidence still gate.
            from profile_json import loads_profile
            result = _json(json.dumps(loads_profile(value),ensure_ascii=False), repairs)
            if repairs is not None:repairs.append(dict(policy='existing_profile_json_scalar_preserving',scalar_sequence_preserved=True))
    if not isinstance(result, dict):
        raise ValueError('candidate JSON object required')
    return result


def load_template(path=DEFAULT_TEMPLATE):
    """Return the complete, unmodified MD and its blank JSON structure."""
    text = Path(path).read_text(encoding='utf-8-sig')
    skeleton = parse_candidate(text)
    if skeleton.get('schema_version') != VERSION or skeleton.get('record_kind') != 'blank_template':
        raise ValueError('unsupported preextract template')
    return text, skeleton


def _blank(value):
    if isinstance(value, dict):
        return {key: _blank(item) for key, item in value.items()}
    return [] if isinstance(value, list) else copy.deepcopy(value)


def _empty(value):
    if isinstance(value, dict):
        return all(_empty(item) for item in value.values())
    if isinstance(value, list):
        return not value or all(_empty(item) for item in value)
    return value is None or value == ''


def _escape(value):
    return str(value).replace('~', '~0').replace('/', '~1')


def _one_character_difference(left, right):
    if abs(len(left) - len(right)) > 1 or left == right:
        return False
    if len(left) == len(right):
        return sum(a != b for a, b in zip(left, right)) == 1
    shorter, longer = sorted((left, right), key=len)
    first = next((i for i, pair in enumerate(zip(shorter, longer)) if pair[0] != pair[1]), len(shorter))
    return shorter[first:] == longer[first + 1:]


def resolve_pointer(value, pointer):
    """Resolve RFC 6901; IDs and wildcards are not array indices."""
    if not isinstance(pointer, str) or (pointer and not pointer.startswith('/')):
        raise ValueError('JSON pointer required')
    if not pointer:
        return value
    for token in pointer[1:].split('/'):
        if re.search(r'~(?![01])', token):
            raise ValueError('invalid JSON pointer escape')
        token = token.replace('~1', '/').replace('~0', '~')
        if isinstance(value, list):
            if not re.fullmatch(r'0|[1-9][0-9]*', token):
                raise ValueError('numeric JSON array index required')
            try:
                value = value[int(token)]
            except IndexError:
                raise ValueError('JSON pointer index missing') from None
        elif isinstance(value, dict) and token in value:
            value = value[token]
        else:
            raise ValueError('JSON pointer does not resolve')
    return value


def is_preextract_artifact(source):
    """Recognize generated candidates/templates by their contract, not name."""
    path = Path(source)
    if path.suffix.lower() not in {'.md', '.json', '.txt'}:
        return False
    try:
        with path.open(encoding='utf-8-sig') as stream:
            head = stream.read(65536)
            if VERSION not in head or 'schema_version' not in head or 'record_kind' not in head:
                return False
            if START in head:
                return True
            if not head.lstrip().startswith(('{', '```')):
                return False
            data = parse_candidate(head + stream.read())
        return data.get('schema_version') == VERSION and data.get('record_kind') in {
            'blank_template', 'filled_candidate', 'partial_candidate'}
    except (OSError, UnicodeError, ValueError, TypeError):
        return False


def parse_saved_candidate(text, *, repairs=None):
    try:
        return parse_candidate(text, repairs=repairs)
    except (ValueError, TypeError) as exc:
        raise ValueError('模型回答格式校验未通过；原响应已保存，未自动重发') from exc


def validate_candidate(candidate, bundle, catalog, *, redaction_policy='strict', admission_policy='strict',
                       partition_material=None):
    """Validate a private candidate against actual material and the fixed MD.

    Errors use safe field locations and codes, without copying identity text.
    Missing optional fields are normalized to the template's empty values. All
    source facts come from ``bundle``; a model conflict remains an error.
    """
    if redaction_policy not in {'strict', 'best_effort'}:
        raise ValueError('unknown redaction policy')
    if admission_policy not in {'strict', 'learning'}:
        raise ValueError('unknown admission policy')
    learning = admission_policy == 'learning'
    best_effort = redaction_policy == 'best_effort'
    partition_pools = None
    if partition_material is not None:
        if (not learning or not isinstance(partition_material, dict)
                or partition_material.get('version') != 'preextract-material/1.0'
                or any(not bundle.get('source', {}).get(key) or
                       bundle['source'][key] != partition_material.get('source', {}).get(key)
                       for key in ('source_id', 'source_sha256'))):
            raise ValueError('分段与全文来源 identity 不一致')
        partition_pools = {key: {row['id']: row for row in partition_material.get(key, [])}
                           for key in ('blocks', 'sections', 'assets')}
        # A supplied neighboring context block already carries real text. Add
        # only its actual chapter/ancestry to the temporary validator index.
        # Never change the saved subset, plan or request bytes.
        bundle = dict(bundle, sections=copy.deepcopy(bundle.get('sections', [])))
        indexed = {row['id'] for row in bundle['sections']}
        for block in bundle.get('blocks', []):
            span = block.get('source_span') or {}
            original = partition_pools['blocks'].get(span.get('block_id', block['id']))
            if not original or original.get('section_id') != block.get('section_id'):
                continue
            if original.get('text', '')[span.get('start', 0):span.get('end', len(original.get('text', '')))] != block.get('text'):
                continue
            sid, seen = block.get('section_id'), set()
            while sid in partition_pools['sections'] and sid not in seen:
                seen.add(sid)
                row = partition_pools['sections'][sid]
                if sid not in indexed:
                    bundle['sections'].append(copy.deepcopy(row)); indexed.add(sid)
                sid = row.get('parent_id')
    _, skeleton = load_template()
    errors, warnings = [], []
    redaction_issues, executable_redactions, omissions = [], [], []
    split_issues, executable_split_points = [], []
    compatibility_report, pending_limits, wrapped_arrays = [], [], []

    def error(code, path, message):
        issue = dict(code=code, path=path, message=message)
        if best_effort and path.startswith('/redaction_plan'):
            warnings.append(issue)
            redaction_issues.append(issue)
        elif best_effort and (path == SPLIT_HINT_PATH or path.startswith(SPLIT_HINT_PATH + '/')):
            warnings.append(issue)
            split_issues.append(issue)
        elif learning and not (
                code in {'source_mismatch', 'source_identity', 'material_identity', 'material_version',
                         'image_version', 'image_evidence', 'asset_identity', 'learning_unusable', 'routing_missing'}
                or (code == 'routing_code' and path.startswith('/profile/routing_candidates'))
                or (code == 'source_reference' and path.endswith('/file_ref'))):
            # The workbench has exactly three admission gates: real source/image
            # identity and delivery integrity, traceable paragraphs, and at least
            # one usable learning chain. Other diagnostics remain limitations.
            warnings.append(dict(issue, code='learning_limit_' + code))
            pending_limits.append(('/profile/research_profile',
                '提取记录有局部格式、定位或说明问题；原始记录保留，相关细节需回看原文，不作为已核实依据。'))
        else:
            errors.append(issue)

    def warning(code, path, message):
        warnings.append(dict(code=code, path=path, message=message))

    def compatible(code, path, before, after, basis, limit=None, target=None):
        warning(code, path, basis)
        compatibility_report.append(dict(code=code, path=path, original=copy.deepcopy(before),
            normalized=copy.deepcopy(after), basis=basis))
        if limit:
            pending_limits.append((target or path, limit))

    def normalize(value, shape, path='', key=''):
        if isinstance(shape, dict):
            if not isinstance(value, dict):
                error('field_type', path, 'object required')
                if learning:
                    compatibility_report.append(dict(code='normalized_container', path=path,
                        original=copy.deepcopy(value), normalized=_blank(shape), basis='raw value retained; no semantic content invented'))
                return _blank(shape)
            result = copy.deepcopy(value)
            for name in value.keys() - shape.keys():
                error('unknown_field', path + '/' + _escape(name), 'field is absent from the fixed template')
            for name, child in shape.items():
                location = path + '/' + _escape(name)
                if name not in value:
                    result[name] = _blank(child)
                    warning('normalized_missing', location, 'missing field retained as empty template value')
                else:
                    result[name] = normalize(value[name], child, location, name)
            return result
        if isinstance(shape, list):
            item_shape = shape[0] if shape else {'source_refs': SOURCE_REF,
                'redactions': dict(x0=None, y0=None, x1=None, y1=None)}.get(key)
            if best_effort and item_shape is None and isinstance(value, str) and not value.strip():
                compatible('normalized_empty_array', path, value, [], 'empty string relation means no supplied identifier')
                return []
            if best_effort and ((isinstance(item_shape, dict) and isinstance(value, dict) and bool(value))
                               or (item_shape is None and isinstance(value, str))):
                compatible('normalized_single_item', path, value, [value], 'one supplied item wrapped without changing its content')
                wrapped_arrays.append(path)
                value = [value]
            if not isinstance(value, list):
                error('field_type', path, 'array required')
                if learning:
                    compatibility_report.append(dict(code='normalized_container', path=path,
                        original=copy.deepcopy(value), normalized=[], basis='raw value retained; no semantic content invented'))
                return []
            result = []
            for index, item in enumerate(value):
                location = path + '/' + str(index)
                if isinstance(item_shape, dict) and isinstance(item, dict) and _empty(item):
                    warning('normalized_placeholder', location, 'empty template item removed')
                    continue
                if item_shape is not None:
                    result.append(normalize(item, item_shape, location))
                elif not isinstance(item, str):
                    error('field_type', location, 'string array item required')
                    result.append(json.dumps(item, ensure_ascii=False) if learning else copy.deepcopy(item))
                else:
                    result.append(item)
            return result
        if value is None:
            return None
        if key in ('image_ref', 'metadata_selector'):
            return normalize(value, IMAGE_REF if key == 'image_ref' else METADATA_SELECTOR, path)
        if best_effort and key in INTEGER_FIELDS and isinstance(value, str) and re.fullmatch(r'-?(0|[1-9][0-9]*)', value.strip()):
            compatible('normalized_integer', path, value, int(value), 'integer field supplied as an exact decimal string')
            value = int(value)
        if best_effort and key == 'is_core' and isinstance(value, str) and value.strip().lower() in ('true', 'false'):
            converted = value.strip().lower() == 'true'
            compatible('normalized_boolean', path, value, converted, 'boolean field supplied as a literal string')
            value = converted
        if key in INTEGER_FIELDS:
            good = type(value) is int
        elif key == 'is_core':
            good = type(value) is bool
        else:
            good = isinstance(value, str)
        if not good:
            error('field_type', path, 'invalid scalar type')
            if learning:
                replacement = None if key in INTEGER_FIELDS or key == 'is_core' else json.dumps(value, ensure_ascii=False)
                compatible('normalized_scalar', path, value, replacement, 'exact supplied value serialized; no semantic content invented')
                return replacement
            return None if best_effort and path.startswith('/redaction_plan') else copy.deepcopy(value)
        choices = ENUMS.get(key)
        if key == 'type' and path.startswith('/gaps/'):
            choices = GAP_TYPES
        if key == 'role' and '/formulas_parameters/' in path:
            choices = {'method_requirement', 'author_choice', 'uncertain'}
        if choices and value not in choices:
            if best_effort and value.strip().lower() in choices:
                compatible('normalized_enum_spelling', path, value, value.strip().lower(), 'exact enum spelling normalized')
                return value.strip().lower()
            if best_effort and key == 'source_kind' and not path.startswith('/redaction_plan'):
                compatible('normalized_source_kind', path, value, None,
                    'unsupported source classification retained privately; location and work attribution unchanged',
                    '此处来源类别未确定；可按原文位置回读，不据此认定作者原创或引用归属。')
                return None
            if learning and not path.startswith('/redaction_plan'):
                replacement = 'uncertain' if 'uncertain' in choices else None
                compatible('normalized_unresolved_enum', path, value, replacement,
                           'unrecognized category retained privately; no author attribution or evidence level inferred',
                           '此处原分类为“' + value + '”，含义未确定；不据此认定原创、实证结果或图像已读。')
                return replacement
            error('field_enum', path, 'value is outside the documented enum')
        return value

    if not isinstance(candidate, dict):
        return dict(valid=False, errors=[dict(code='field_type', path='', message='candidate object required')], warnings=[], candidate=copy.deepcopy(candidate))
    candidate = copy.deepcopy(candidate)
    lifted = []
    if learning and isinstance(candidate.get('source'), dict):
        for key in ('profile', 'tree', 'assets', 'redaction_plan', 'gaps', 'segmentation', 'attachments'):
            if key in candidate['source'] and key not in candidate:
                candidate[key] = candidate['source'].pop(key)
                lifted.append(('/source/' + key, '/' + key))
                compatible('normalized_root_field', '/' + key, '/source/' + key, '/' + key,
                           'unique misplaced template section moved intact; conflicting roots are never merged')
    data = normalize(candidate, skeleton)
    if learning:
        if data.get('schema_version') != VERSION:
            compatible('normalized_schema_label', '/schema_version', data.get('schema_version'), VERSION,
                       'program serialization version assigned; raw model label retained')
            data['schema_version'] = VERSION
        if data.get('record_kind') not in ('filled_candidate', 'partial_candidate'):
            compatible('normalized_partial_label', '/record_kind', data.get('record_kind'), 'partial_candidate',
                       'unrecognized model label retained; completeness is not asserted')
            data['record_kind'] = 'partial_candidate'

    def rewrite_pointers(mappings):
        def walk(value):
            if isinstance(value, dict):
                pointer = value.get('target_pointer')
                if isinstance(pointer, str):
                    for old, new in mappings:
                        if pointer == old or pointer.startswith(old + '/'):
                            pointer = new + pointer[len(old):]
                            break
                    value['target_pointer'] = pointer
                for child in value.values():walk(child)
            elif isinstance(value, list):
                for child in value:walk(child)
        walk(data)
    if learning:
        def normalize_pointers(value):
            if isinstance(value, dict):
                pointer = value.get('target_pointer')
                # Only a complete field/index grammar, never eval or fuzzy lookup.
                if isinstance(pointer, str) and re.fullmatch(r'[A-Za-z_]\w*(?:(?:\.[A-Za-z_]\w*)|(?:\[(?:0|[1-9][0-9]*)\]))*', pointer):
                    converted = '/' + '/'.join(re.findall(r'[A-Za-z_]\w*|[0-9]+', pointer))
                    converted = next((new + converted[len(old):] for old, new in lifted
                                      if converted == old or converted.startswith(old + '/')), converted)
                    try:
                        resolve_pointer(data, converted)
                    except (ValueError, TypeError):
                        pass
                    else:
                        compatible('normalized_pointer', converted, pointer, converted,
                                   'field/index notation resolves to the same supplied value')
                        value['target_pointer'] = converted
                for child in value.values():normalize_pointers(child)
            elif isinstance(value, list):
                for child in value:normalize_pointers(child)
        normalize_pointers(data)
        rewrite_pointers(lifted)
    # Inner array paths already contain outer normalized indices.
    for path in sorted(wrapped_arrays, key=lambda p: p.count('/')):
        rewrite_pointers([(path, path + '/0')])

    def flush_limits():
        while pending_limits:
            pointer, limit = pending_limits.pop(0)
            identity = 'compat_' + hashlib.sha256((pointer + '\n' + limit).encode()).hexdigest()[:20]
            gap = _blank(skeleton['gaps'][0])
            gap.update(id=identity, type='uncertain', target_pointer=pointer,
                       description='程序兼容处理保留的局部限制', use_limit=limit,
                       claim_status='uncertain', source_refs=[])
            existing = next((g for g in data['gaps'] if g.get('id') == identity), None)
            if learning:
                while existing is not None and existing != gap:
                    identity += '_compat'
                    gap['id'] = identity
                    existing = next((g for g in data['gaps'] if g.get('id') == identity), None)
            if existing == gap:continue
            if existing is not None:
                error('duplicate_id', '/gaps', 'program limitation identity conflicts with a supplied record')
                continue
            data['gaps'].append(gap)
    flush_limits()
    for key in ('schema_version', 'record_kind', 'profile', 'tree', 'assets', 'redaction_plan', 'gaps', 'segmentation'):
        if key not in candidate:
            error('required_field', '/' + key, 'required answer section missing; absence is not an empty decision')
    if candidate.get('schema_version') != VERSION:
        error('schema_version', '/schema_version', 'unsupported or missing version')
    if candidate.get('record_kind') not in ('filled_candidate', 'partial_candidate'):
        error('record_kind', '/record_kind', 'filled or partial private candidate required')
    if not isinstance(bundle, dict) or bundle.get('version') != 'preextract-material/1.0':
        error('material_version', '/source', 'unsupported material bundle')
        bundle = bundle if isinstance(bundle, dict) else {}
    authoritative = bundle.get('source', {})
    if not isinstance(authoritative, dict):
        authoritative = {}
    source_conflicts = {}
    for key in skeleton['source']:
        if key == 'reading_scope':
            continue
        observed, expected = data['source'].get(key), authoritative.get(key)
        if observed is not None and observed != expected:
            if isinstance(observed, str) and not observed.strip():
                warning('normalized_source_fact', '/source/' + key, 'blank program fact filled from material')
            elif ((key == 'file' or (learning and key == 'text_file')) and isinstance(observed, str) and observed
                    and (observed == bundle.get('file_ref', 'main') or
                         (learning and observed == str(expected or '').replace('\\', '/').rsplit('/', 1)[-1]))
                    and isinstance(expected, str) and expected):
                # The request exposes the primary file alias, not its private
                # path. Resolve only this exact program-owned identity.
                compatible('normalized_source_reference', '/source/' + key, observed, expected,
                           'primary file alias resolved from the registered program material')
            else:
                error('source_mismatch', '/source/' + key, 'model source fact disagrees with program material')
                source_conflicts[key] = copy.deepcopy(observed)
    original_reading = data['source']['reading_scope']
    data['source'] = _blank(skeleton['source'])
    data['source'].update(copy.deepcopy(authoritative))
    data['source'].update(source_conflicts)
    if not data['source'].get('source_id') or not re.fullmatch(r'[0-9a-f]{64}', str(data['source'].get('source_sha256') or '')):
        error('source_identity', '/source', 'program source identity and SHA256 required')
    # Invalid containers/scalars or unknown structures cannot safely participate
    # in ID lookup. Keep their values and diagnostics instead of crashing.
    if any(item['code'] in {'field_type', 'unknown_field'} for item in errors):
        data['record_kind'] = 'partial_candidate'
        return dict(valid=False, errors=errors, warnings=warnings, candidate=data)

    def indexed(rows, name):
        result = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not isinstance(row.get('id'), str) or not row['id'] or row['id'] in result:
                error('material_identity', '/source', 'invalid or duplicate program ' + name + ' identity')
                continue
            result[row['id']] = row
        return result

    blocks = indexed(bundle.get('blocks', []), 'block')
    sections = indexed(bundle.get('sections', []), 'section')
    assets = indexed(bundle.get('assets', []), 'asset')

    def block_has_material(block):
        # These exact markers are emitted by native_docx for unresolved objects.
        # Their location remains useful, but the marker is not research content.
        text = block.get('text')
        return isinstance(text, str) and bool(text.strip()) and text.strip() not in {
            '[解析说明：此位置的原文图形需定向识别。]',
            '[解析说明：表格内图形需定向识别。]',
            '[解析说明：此位置存在未解析的正文对象。]'}

    def usable_source_ref(ref):
        if not isinstance(ref, dict):
            return False
        block_id, asset_id = ref.get('block_id'), ref.get('asset_id')
        raw_asset = assets.get(asset_id, {}) if isinstance(asset_id, str) else {}
        has_asset = bool(raw_asset) and (bool(str(raw_asset.get('text') or '').strip()) or
            all(raw_asset.get(key) for key in ('image_path', 'sha256', 'width', 'height')))
        if isinstance(block_id, str):
            if block_id not in blocks:
                return False
            # A placeholder's copied chapter ID must not make it sufficient
            # as the core unit's only evidence, including on revalidation.
            return block_has_material(blocks[block_id]) or has_asset
        section_id = ref.get('section_id')
        return has_asset or (isinstance(section_id, str) and section_id in sections and
            any(block_has_material(row) for row in blocks.values() if row.get('section_id') == section_id))

    if best_effort:
        # Identical repeated records carry no additional interpretation. Keep
        # conflicting records for the normal duplicate-ID error, never choose.
        unique, positions = [], {}
        for old_index, node in enumerate(data['tree']['nodes']):
            new_index = next((i for i, prior in enumerate(unique) if prior == node), len(unique))
            positions[old_index] = new_index
            if new_index == len(unique):
                unique.append(node)
            else:
                warning('normalized_identical_node', f'/tree/nodes/{old_index}', 'identical repeated chapter retained once')
        data['tree']['nodes'] = unique

        def remap_pointers(value):
            if isinstance(value, dict):
                pointer = value.get('target_pointer')
                match = re.match(r'^/tree/nodes/(0|[1-9][0-9]*)(/.*|$)', pointer) if isinstance(pointer, str) else None
                if match and int(match[1]) in positions:
                    value['target_pointer'] = '/tree/nodes/' + str(positions[int(match[1])]) + match[2]
                for child in value.values():
                    remap_pointers(child)
            elif isinstance(value, list):
                for child in value:
                    remap_pointers(child)
        remap_pointers(data)
        for index, node in enumerate(unique):
            raw = sections.get(node.get('id'))
            if raw is not None:
                for key in ('parent_id', 'order'):
                    if key in raw and node.get(key) != raw[key]:
                        node[key] = copy.deepcopy(raw[key])
                        warning('normalized_section_fact', f'/tree/nodes/{index}/{key}', 'chapter hierarchy or order taken from the supplied program index')
    if learning:
        existing_sections = {node.get('id') for node in data['tree']['nodes']}
        for ident, raw in sections.items():
            if ident not in existing_sections:
                node = _blank(skeleton['tree']['nodes'][0])
                node.update({key: copy.deepcopy(raw.get(key)) for key in ('id', 'title', 'parent_id', 'order')})
                node['source_refs'] = [dict(_blank(SOURCE_REF), file_ref=bundle.get('file_ref') or authoritative.get('source_id'), section_id=ident)]
                data['tree']['nodes'].append(node)
                warning('normalized_program_section', '/tree/nodes', 'missing chapter index restored from material; semantic function left empty')
        for index, unit in enumerate(data['profile']['learning_units']):
            if not unit['source_refs']:
                refs = [dict(_blank(SOURCE_REF), file_ref=bundle.get('file_ref') or authoritative.get('source_id'), asset_id=ident)
                        for ident in unit['asset_ids'] if ident in assets]
                if refs:
                    compatible('normalized_unit_evidence', f'/profile/learning_units/{index}/source_refs', [], refs,
                               'existing explicit asset relations reused as source locations; reading quality is not asserted')
                    unit['source_refs'] = refs
    files = {str(value) for value in (bundle.get('file_ref'), authoritative.get('source_id'),
             authoritative.get('file'), authoritative.get('text_file')) if value}
    files.update(str(row.get('file_ref')) for row in bundle.get('attachments', []) if isinstance(row, dict) and row.get('file_ref'))
    primary_files={str(v) for v in (bundle.get('file_ref','main'),authoritative.get('source_id'),
                   authoritative.get('file'),authoritative.get('text_file')) if v}
    attachment_files=files-primary_files
    all_read_refs = files | set(blocks) | set(sections) | set(assets)
    if learning:
        # Catalog is supplied request context, never evidence for a research
        # claim. Keep it out of `files` and of source_ref/usable_source_ref.
        if authoritative.get('catalog_sha256') and catalog.get('majors'):
            all_read_refs.add('catalog')
        object_rows = {}
        for ident, row in {**blocks, **assets}.items():
            position = row.get('position')
            if isinstance(position, dict) and isinstance(position.get('object_ref'), str):
                object_rows.setdefault(position['object_ref'], []).append((ident, position))
        object_aliases = {}
        for ref, rows in object_rows.items():
            positions = {json.dumps(position, sort_keys=True) for _, position in rows}
            if len(positions) == 1 and any(_same_source_object(rows[0][1], issue.get('position'))
                    for issue in bundle.get('issues', []) if isinstance(issue, dict)):
                object_aliases[ref] = [ident for ident, _ in rows]
        groups = {'blocks': list(blocks), 'text_blocks': list(blocks), 'text': list(blocks), 'sections': list(sections),
                  'assets': list(assets),
                  'tables': [key for key, row in assets.items() if row.get('kind') == 'table'],
                  'images': [key for key, row in assets.items() if row.get('kind') not in {'table', 'source_page'}],
                  'pages': [key for key, row in assets.items() if row.get('kind') == 'source_page']}
        page_numbers = {row.get('position', {}).get('page') for row in [*blocks.values(), *assets.values()]
                        if isinstance(row.get('position'), dict) and type(row['position'].get('page')) is int}
        page_images = {ident: row['position']['page'] for ident, row in assets.items()
                       if row.get('kind') == 'source_page' and isinstance(row.get('position'), dict)
                       and type(row['position'].get('page')) is int}
        # A deliberately omitted cover/acknowledgment page is not a missing
        # research image. Require the program's complete, unambiguous cleanup
        # proof; never infer an omission from a gap in page numbering.
        omitted_pages = set()
        cleanup = (bundle.get('cleanup') or {}).get('page_evidence') or {}
        nonresearch = {row['block'].get('id') for row in bundle.get('removed', [])
                       if isinstance(row, dict) and isinstance(row.get('block'), dict)
                       and row.get('reason') in {'bounded_cover_before_abstract', 'bounded_nonresearch_section'}}
        retained_ids = {row.get('source_block_id') for row in blocks.values()}
        cleanup_rows = [row for row in cleanup.get('pages', []) if isinstance(row, dict)]
        if best_effort and cleanup.get('method') == 'exact_source_text_or_pdf_position':
            for row in cleanup_rows:
                page, ids = row.get('page'), row.get('source_block_ids')
                if (type(page) is int and page > 0 and page not in page_numbers
                        and sum(other.get('page') == page for other in cleanup_rows) == 1
                        and row.get('action') == 'omit_nonresearch_page'
                        and row.get('uncertain_boxes') == [] and row.get('boxes')
                        and isinstance(ids, list) and ids
                        and all(isinstance(ident, str) and ident in nonresearch
                                and ident not in retained_ids for ident in ids)):
                    omitted_pages.add(f'page_{page:04d}')
        def expand_reading(ref, name):
            if ref in all_read_refs:return [ref]
            if name == 'unread_refs' and partition_pools:
                if any(ref in pool for pool in partition_pools.values()):return [ref]
                parts = ref.split(':')
                if parts[0] == bundle.get('file_ref', 'main'):parts = parts[1:]
                if len(parts) == 1 and '..' in parts[0]:
                    ends = parts[0].split('..')
                    pools = [list(pool) for pool in partition_pools.values()
                             if len(ends) == 2 and all(end in pool for end in ends)]
                    if len(pools) == 1:
                        first, last = map(pools[0].index, ends)
                        if first <= last:return pools[0][first:last + 1]
            if ref.strip() in all_read_refs:
                return [ref.strip()]
            if ref in object_aliases:return object_aliases[ref]
            if name == 'unread_refs' and ref in omitted_pages:
                note = '模型原未读声明：' + ref + '；程序已按明确边界移除该非研究页，未将此页作为已读内容或研究证据。'
                notes = original_reading.get('notes') or ''
                original_reading['notes'] = notes + ('\n' if notes else '') + note
                compatible('normalized_excluded_page_declaration', '/source/reading_scope/unread_refs', ref, [],
                    'one exact whole-page cleanup record resolves only to removed nonresearch blocks; original unread claim retained in notes')
                return []
            described_pages = re.fullmatch(r'(?:PDF\s+)?pages?\s+([0-9, \-]+)\s*(?:as\s+full-page\s+images|\(no page-image asset provided\)|的页图未提供)', ref, re.I)
            numbered_pages = re.fullmatch(r'page_(\d{4})(?:-page_(\d{4}))?(?:（未提供页图）)?', ref)
            if name == 'unread_refs' and numbered_pages:
                start, end = int(numbered_pages[1]), int(numbered_pages[2] or numbered_pages[1])
                if page_numbers and 1 <= start <= end <= max(page_numbers):
                    described_pages = re.fullmatch(r'(.*)', str(start) + '-' + str(end))
            if name == 'unread_refs' and described_pages and page_numbers:
                requested = set()
                for part in described_pages[1].split(','):
                    bounds = re.fullmatch(r'\s*(\d+)\s*(?:-\s*(\d+)\s*)?', part)
                    if not bounds:return [ref]
                    start, end = int(bounds[1]), int(bounds[2] or bounds[1])
                    if not 1 <= start <= end <= max(page_numbers):return [ref]
                    requested.update(range(start, end + 1))
                supplied = [ident for ident, page in page_images.items() if page in requested]
                if requested - set(page_images.values()):
                    note = '模型未读页图声明（部分整页图未提供，不等同于正文文本未读）：' + ref
                    notes = original_reading.get('notes') or ''
                    original_reading['notes'] = notes + ('\n' if notes else '') + note
                    compatible('normalized_unprovided_page_images', '/source/reading_scope/unread_refs', ref, supplied,
                        'only actual page images become references; the full original declaration is retained in notes',
                        note + '；相关版式与图形细节需回看原稿。', target='/source/reading_scope')
                return supplied
            parts = ref.split(':')
            if parts[0] == bundle.get('file_ref', 'main'):parts = parts[1:]
            if len(parts) == 1 and parts[0] in all_read_refs:return parts
            if parts in (['all_provided_blocks'], ['all_provided_text_blocks'], ['all_text_blocks']):return list(blocks)
            if len(parts) == 1 and '..' in parts[0]:
                ends = parts[0].split('..')
                # Bare ranges resolve only inside one actual primary namespace.
                pools = [list(pool) for pool in (blocks, sections, assets)
                         if len(ends) == 2 and all(end in pool for end in ends)]
                if len(pools) == 1:
                    start, end = map(pools[0].index, ends)
                    if start <= end:return pools[0][start:end + 1]
            if parts == ['assets', 'all_provided_images_and_tables']:
                return [ident for ident, row in assets.items() if row.get('kind') != 'source_page'] or [ref]
            if not parts or parts[0] not in groups:return [ref]
            pool = groups[parts[0]]
            if len(parts) == 1 or parts[1:] in (['all'], ['*']) or (parts[0] in {'blocks', 'text_blocks', 'text'} and parts[1:] == ['all_blocks']):return pool or [ref]
            if len(parts) != 2:return [ref]
            if parts[1] in pool:return [parts[1]]
            ends = parts[1].split('..')
            if len(ends) == 2 and all(pool.count(end) == 1 for end in ends):
                start, end = map(pool.index, ends)
                if start <= end:return pool[start:end + 1]
            return [ref]
        raw_unread = list(original_reading.get('unread_refs', []))
        broad_text_read = False
        explicit_reads = set()
        for ref in original_reading.get('read_refs', []):
            parts = ref.split(':')
            if parts[0] == bundle.get('file_ref', 'main'):parts = parts[1:]
            broad = (parts in (['all_provided_blocks'], ['all_provided_text_blocks'], ['all_text_blocks']) or
                     (parts and parts[0] in {'blocks', 'text_blocks', 'text'} and
                      (len(parts) == 1 or parts[1:] in (['all'], ['*'], ['all_blocks']))))
            if broad:broad_text_read = True
            else:explicit_reads.update(expand_reading(ref, 'read_refs'))
        for name in ('read_refs', 'unread_refs'):
            refs = original_reading.get(name, [])
            expanded = list(dict.fromkeys(item for ref in refs for item in expand_reading(ref, name)))
            if name == 'unread_refs' and partition_pools:
                outside = [ref for ref in expanded if ref not in all_read_refs
                           and any(ref in pool for pool in partition_pools.values())]
                if outside:
                    note = '本段未提供的全文材料（保留原未读声明，不作为本段证据）：' + '、'.join(refs)
                    original_reading['notes'] = (original_reading.get('notes') or '') + '\n' + note
                    compatible('normalized_partition_unread_scope', '/source/reading_scope/unread_refs',
                        refs, [ref for ref in expanded if ref not in outside],
                        'same-source whole-material index confirms out-of-part declarations only; no read or research evidence added')
                    expanded = [ref for ref in expanded if ref not in outside]
            if expanded != refs:
                compatible('normalized_reading_alias', '/source/reading_scope/' + name, refs, expanded,
                           'exact namespaces, ranges or uniquely located embedded objects resolved; remains a model reading declaration')
                original_reading[name] = expanded
        if broad_text_read:
            def mentioned(value):
                if isinstance(value, dict):
                    for child in value.values():yield from mentioned(child)
                elif isinstance(value, list):
                    for child in value:yield from mentioned(child)
                elif isinstance(value, str):yield value
            research_refs = set(mentioned([data['profile']['learning_units'], data['assets']['items']]))
            overlap = set(original_reading.get('read_refs', [])) & set(original_reading.get('unread_refs', []))
            removed, objects = set(), []
            for ref in raw_unread:
                ids = object_aliases.get(ref, [])
                if (not ids or not set(ids) & overlap or set(ids) & (explicit_reads | research_refs)
                        or ref in research_refs or any(ident not in blocks for ident in ids)):
                    continue
                if not all(blocks[ident].get('type') == 'paragraph' and
                           isinstance(blocks[ident].get('text'), str) and blocks[ident]['text'].strip() and
                           not block_has_material(blocks[ident]) for ident in ids):
                    continue
                if any(_same_source_object(row.get('position'), blocks[ident].get('position'))
                       for ident in ids for row in assets.values()):
                    continue
                removed.update(set(ids) & overlap);objects.append(ref)
            if removed:
                before = list(original_reading['read_refs'])
                original_reading['read_refs'] = [ref for ref in before if ref not in removed]
                note = '泛读声明仅涉及程序占位文字；下列原生图形对象仍未读，不能据此推断图像内容：' + '、'.join(objects)
                notes = original_reading.get('notes') or ''
                original_reading['notes'] = notes + ('\n' if notes else '') + note
                compatible('normalized_placeholder_reading_scope', '/source/reading_scope/read_refs', before,
                    original_reading['read_refs'],
                    'explicit unread object wins over broad text reading only for unique unused native placeholders; unread references and learning content retained')
    for name in ('read_refs', 'unread_refs'):
        invalid = []
        for ref in original_reading.get(name, []):
            if not isinstance(ref, str) or ref not in all_read_refs:
                error('source_reference', '/source/reading_scope/' + name, 'reading reference is absent from actual material')
                invalid.append(ref)
        if learning and invalid:
            note = '模型原始读取声明（未能对应实际材料，仅保留说明，不视为已读证据）：' + '；'.join(map(str, invalid))
            original_reading['notes'] = (original_reading.get('notes') or '') + '\n' + note
            compatible('normalized_unlocated_reading_declaration', '/source/reading_scope/' + name,
                       invalid, [], 'unlocated declaration retained as a limitation, never as evidence', note,
                       target='/source/reading_scope')
            original_reading[name] = [ref for ref in original_reading[name] if ref not in invalid]
    # Hashes and extraction diagnostics are tool facts. Read/unread/notes are
    # the model's bounded declarations and must survive as such, not become a
    # false independent assertion that submitting material proves it was read.
    tool_scope = authoritative.get('reading_scope') or {}
    data['source']['reading_scope'] = dict(
        read_refs=copy.deepcopy(original_reading.get('read_refs', [])),
        unread_refs=copy.deepcopy(original_reading.get('unread_refs', [])),
        extraction_issues=copy.deepcopy(tool_scope.get('extraction_issues', [])),
        notes=original_reading.get('notes'))
    if original_reading.get('read_refs') or original_reading.get('unread_refs') or original_reading.get('notes'):
        warning('reading_declaration', '/source/reading_scope', 'reading entries are model declarations, not independent verification')
    if original_reading.get('extraction_issues') != tool_scope.get('extraction_issues', []):
        warning('normalized_extraction_issues', '/source/reading_scope/extraction_issues', 'tool extraction diagnostics retained from program bundle')
    if set(original_reading.get('read_refs', [])) & set(original_reading.get('unread_refs', [])):
        error('reading_conflict', '/source/reading_scope', 'same material declared both read and unread')
        if learning:
            overlap = set(original_reading['read_refs']) & set(original_reading['unread_refs'])
            original_reading['read_refs'] = [ref for ref in original_reading['read_refs'] if ref not in overlap]
            data['source']['reading_scope']['read_refs'] = copy.deepcopy(original_reading['read_refs'])
            pending_limits.append(('/source/reading_scope', '原读取声明有冲突；冲突材料按未读保留，不作为已核实依据。'))

    if best_effort:
        primary_files = {str(v) for v in (bundle.get('file_ref'), authoritative.get('source_id'),
                         authoritative.get('file'), authoritative.get('text_file')) if v}
        pointer_moves = []
        def primary(ref):
            binding = ref.get('file_ref')
            return binding is None or (isinstance(binding, str) and binding in primary_files)
        def actual_block_ref(ref, section):
            block_id = ref.get('block_id')
            block = blocks.get(block_id) if isinstance(block_id, str) else None
            if not primary(ref) or not block or not block_has_material(block) or block.get('section_id') != section:
                return False
            if ref.get('section_id') not in (None, section) or (ref.get('asset_id') is not None and ref['asset_id'] not in assets):
                return False
            start, end = ref.get('start'), ref.get('end')
            return ((start is None and end is None) or
                    (type(start) is int and type(end) is int and 0 <= start < end <= len(block.get('text', ''))))
        def normalize_locations(value, path=''):
            if path.startswith(('/redaction_plan', '/segmentation')):
                return  # Those execution plans keep their existing exact validators.
            if isinstance(value, dict):
                refs = value.get('source_refs', [])
                # A real block in an image-only relation stays a real block.
                # Do not infer a picture from a placeholder or nearby caption.
                if isinstance(value.get('asset_ids'), list) and isinstance(refs, list):
                    kept = []
                    for index, ident in enumerate(value['asset_ids']):
                        old = path + '/asset_ids/' + str(index)
                        if isinstance(ident, str) and ident in blocks and ident not in assets:
                            block = blocks[ident]
                            ref = dict(SOURCE_REF, file_ref=bundle.get('file_ref') or authoritative.get('source_id'),
                                       block_id=ident, section_id=block.get('section_id'))
                            ref_index = next((i for i, r in enumerate(refs) if r == ref), len(refs))
                            if ref_index == len(refs):refs.append(ref)
                            pointer_moves.append((old, path + '/source_refs/' + str(ref_index) + '/block_id'))
                            compatible('normalized_block_relation', old, ident, ref,
                                'actual block moved out of image relation; no asset inferred',
                                '此处原填入图片关联的是正文位置，已保留正文引用；不据此宣称取得或读懂对应图形。',
                                target=path + '/asset_ids')
                        else:
                            pointer_moves.append((old, path + '/asset_ids/' + str(len(kept))))
                            kept.append(ident)
                    value['asset_ids'] = kept
                    if refs:value['source_refs'] = refs
                if isinstance(refs, list):
                    for index, ref in enumerate(refs):
                        if not isinstance(ref, dict) or not primary(ref):continue
                        location = path + '/source_refs/' + str(index)
                        for key, known in (('block_id', blocks), ('section_id', sections), ('asset_id', assets)):
                            ident = ref.get(key)
                            if isinstance(ident, str) and ident not in known and ident.strip() in known:
                                compatible('normalized_reference_space', location + '/' + key, ident, ident.strip(), 'exact supplied identity after whitespace removal')
                                ref[key] = ident.strip()
                        bad, sid = ref.get('block_id'), ref.get('section_id')
                        if not isinstance(bad, str) or bad in blocks:continue
                        hint = re.sub(r'\s+', '', ref.get('locator_hint') or '')
                        matches = []
                        if (sid in sections and re.fullmatch(r'block_[0-9a-f]{19,21}', bad)
                                and ref.get('start') is None and ref.get('end') is None):
                            for ident, block in blocks.items():
                                literal = re.sub(r'\s+', '', block.get('text', ''))
                                if (re.fullmatch(r'block_[0-9a-f]{20}', ident) and block.get('section_id') == sid
                                        and len(literal) >= 2 and hint in (literal, literal + '标题')
                                        and _one_character_difference(bad, ident)):
                                    matches.append(ident)
                        if len(matches) == 1:
                            ref['block_id'] = matches[0]
                            compatible('normalized_block_alias', location + '/block_id', bad, matches[0],
                                       'unique one-character identity difference, same file/section and exact literal locator')
                            continue
                        redundant = any(other is not ref and isinstance(other, dict) and actual_block_ref(other, sid) for other in refs)
                        node_metadata = path.startswith('/tree/nodes/') and value.get('is_core') is False
                        gap_metadata = path.startswith('/gaps/') and str(value.get('target_pointer') or '').startswith('/tree/')
                        if (sid in sections and ref.get('asset_id') is None
                                and ref.get('start') is None and ref.get('end') is None
                                and (redundant or node_metadata or gap_metadata)):
                            ref['block_id'] = None
                            compatible('normalized_reference_precision', location + '/block_id', bad, None,
                                'valid section retained; no invented block, range or cross-field fallback',
                                '此处一条正文编号无法确定，保留章节级定位及同项其他有效依据；不可把该条当作精确逐句引文。',
                                target=path + '/source_refs')
                for key, child in list(value.items()):normalize_locations(child, path + '/' + _escape(key))
            elif isinstance(value, list):
                for index, child in enumerate(value):normalize_locations(child, path + '/' + str(index))
        normalize_locations(data)
        rewrite_pointers(pointer_moves)
        flush_limits()

    def source_ref(ref, path):
        if not isinstance(ref, dict):
            error('source_reference', path, 'source reference object required')
            return
        located = False
        file_ref = ref.get('file_ref')
        if isinstance(file_ref,str) and file_ref in attachment_files and any(ref.get(k) is not None for k in ('block_id','section_id','asset_id','start','end')):
            error('source_reference',path+'/file_ref','attachment reference cannot use primary document coordinates')
        if file_ref is not None:
            located = True
            if not isinstance(file_ref, str) or file_ref not in files:
                error('source_reference', path + '/file_ref', 'unknown source file')
        for key, known in (('block_id', blocks), ('section_id', sections), ('asset_id', assets)):
            ident = ref.get(key)
            if ident is not None:
                located = True
                if not isinstance(ident, str) or ident not in known:
                    error('source_reference', path + '/' + key, 'unknown source location')
        if not located:
            warning('unlocated_evidence', path, 'reference contains no stable actual material location')
        block = blocks.get(ref.get('block_id')) if isinstance(ref.get('block_id'), str) else None
        if block and ref.get('section_id') and block.get('section_id') != ref['section_id']:
            if (learning and block_has_material(block) and ref['section_id'] in sections
                    and block.get('section_id') in sections and ref.get('asset_id') is None
                    and file_ref in {None, bundle.get('file_ref', 'main'), authoritative.get('source_id'),
                                     authoritative.get('file'), authoritative.get('text_file')}):
                compatible('normalized_block_section', path + '/section_id', ref['section_id'], block['section_id'],
                           'exact supplied block retained; redundant chapter identifier taken from the program index')
                ref['section_id'] = block['section_id']
            else:
                error('source_reference', path, 'block and section references disagree')
        start, end = ref.get('start'), ref.get('end')
        if start is not None or end is not None:
            if not block or type(start) is not int or type(end) is not int or not 0 <= start < end <= len(block.get('text', '')):
                if learning and block and not path.startswith(('/redaction_plan', '/segmentation')):
                    compatible('normalized_paragraph_range', path, dict(start=start, end=end), dict(start=None, end=None),
                        'actual paragraph retained; imprecise character offsets are not executable',
                        '此处保留真实段落级依据，字符范围不精确，不能作精确逐句引文。', target=path)
                    ref.update(start=None, end=None)
                else:
                    error('source_range', path, 'invalid Unicode code point range')

    def visit(value, path=''):
        if isinstance(value, dict):
            for key, item in value.items():
                location = path + '/' + _escape(key)
                if key == 'source_refs' and isinstance(item, list):
                    for i, ref in enumerate(item):
                        source_ref(ref, location + '/' + str(i))
                else:
                    visit(item, location)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                visit(item, path + '/' + str(i))
    # Normalize only an exact, unique literal in the explicitly selected block
    # before generic source-range validation; other evidence ranges stay strict.
    for i, row in enumerate(data['redaction_plan']):
        expected = row.get('expected_text')
        if row.get('scope') != 'source_text' or not isinstance(expected, str) or not expected:
            continue
        for ref in row.get('source_refs', []):
            raw = blocks.get(ref.get('block_id')) if isinstance(ref, dict) else None
            start, end = ref.get('start'), ref.get('end')
            if raw and raw.get('text', '').count(expected) == 1 and (
                    type(start) is not int or type(end) is not int
                    or not 0 <= start < end <= len(raw['text'])
                    or raw['text'][start:end] != expected):
                start = raw['text'].index(expected)
                ref.update(start=start, end=start + len(expected))
                warning('normalized_redaction_range', f'/redaction_plan/{i}',
                        'unique exact text matched within the explicitly referenced block')
    visit(data)

    units = data['profile']['learning_units']
    nodes = data['tree']['nodes']
    asset_rows = data['assets']['items']
    if learning:
        used = set()
        for index, unit in enumerate(units):
            ident = unit.get('id')
            if not isinstance(ident, str) or not ident.strip() or ident in used:
                replacement = 'unit_' + hashlib.sha256((str(index) + json.dumps(unit, ensure_ascii=False, sort_keys=True)).encode()).hexdigest()[:20]
                while replacement in used:replacement += '_unit'
                compatible('normalized_learning_id', f'/profile/learning_units/{index}/id', ident, replacement,
                           'program storage identity assigned; original content and original identifier retained')
                unit['id'] = replacement
            used.add(unit['id'])
    id_paths, id_sets = {}, {}

    def register(rows, path, kind):
        known = set()
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            ident, location = row.get('id'), path + '/' + str(i) + '/id'
            if not isinstance(ident, str) or not ident.strip():
                error('record_id', location, 'nonempty record ID required')
                continue
            if ident in id_paths:
                error('duplicate_id', location, 'record ID is already used')
            else:
                id_paths[ident] = location
            known.add(ident)
        id_sets.setdefault(kind, set()).update(known)
        return known

    unit_ids = register(units, '/profile/learning_units', 'unit')
    node_ids = register(nodes, '/tree/nodes', 'section')
    asset_ids = register(asset_rows, '/assets/items', 'asset')
    register(data['redaction_plan'], '/redaction_plan', 'redaction')
    register(data['gaps'], '/gaps', 'gap')
    for i, unit in enumerate(units):
        for key in ('materials', 'steps', 'outputs'):
            register(unit[key], f'/profile/learning_units/{i}/{key}', key)

    def references(values, known, path):
        for value in values:
            if not isinstance(value, str) or value not in known:
                error('record_reference', path, 'reference points to an absent record')

    def relations(value, path=''):
        if isinstance(value, dict):
            for key, item in value.items():
                location = path + '/' + _escape(key)
                known = {'unit_ids': unit_ids, 'upstream_unit_ids': unit_ids, 'unit_sequence': unit_ids,
                         'section_ids': node_ids, 'asset_ids': asset_ids,
                         'input_refs': id_sets.get('materials', set()) | id_sets.get('outputs', set()),
                         'output_refs': id_sets.get('outputs', set())}.get(key)
                if known is not None and isinstance(item, list):
                    unit_step = re.fullmatch(r'/profile/learning_units/(\d+)/steps/\d+', path)
                    if learning and key == 'output_refs' and unit_step:
                        # Collected samples can be described as a material and
                        # also be the output of a step. Preserve that explicit
                        # edge and all its content instead of inventing an O-ID.
                        own_materials = {row['id'] for row in units[int(unit_step[1])]['materials']}
                        extra = [ref for ref in item if ref not in known and ref in own_materials]
                        if extra:
                            compatible('compatible_material_output', location, item, item,
                                       'step output references an existing material in the same learning unit; record and edge retained')
                            known = known | own_materials
                    references(item, known, location)
                elif key in ('parent_id', 'to_section_id') and item is not None:
                    references([item], node_ids, location)
                elif key != 'source_refs':
                    relations(item, location)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                relations(item, path + '/' + str(i))
    relations(data)
    parents = {row['id']: row.get('parent_id') for row in nodes if isinstance(row.get('id'), str)}
    for ident in parents:
        seen, current = set(), ident
        while current in parents:
            if current in seen:
                error('section_cycle', '/tree/nodes', 'chapter parent cycle')
                break
            seen.add(current)
            current = parents[current]

    keep = learning or data['profile']['material_disposition']['decision'] == 'keep'
    if not data['profile']['material_disposition'].get('decision') or not data['profile']['material_disposition'].get('reason'):
        error('required_content', '/profile/material_disposition', 'material decision and reason required')
    if not data['profile'].get('material_type'):
        error('required_content', '/profile/material_type', 'material type or explicit limitation required')
    if keep:
        if not data['profile']['article'].get('title'):
            error('required_content', '/profile/article/title', 'retained article needs source title')
        if not units:
            error('required_content', '/profile/learning_units', 'retained paper has no learning units')
        if set(sections) != node_ids:
            error('section_coverage', '/tree/nodes', 'all actual source chapters must be represented exactly once')
        missing_assets = set(assets) - asset_ids
        missing_pages = {ident for ident in missing_assets if assets[ident].get('kind') == 'source_page'}
        if missing_assets - missing_pages or asset_ids - set(assets):
            error('asset_coverage', '/assets/items', 'all actual figures and tables need individual disposition')
        if missing_pages:
            warning('optional_page_disposition', '/assets/items',
                    'unused whole-page evidence has no publication disposition; original figures and tables remain required')
    for i, unit in enumerate(units):
        path = f'/profile/learning_units/{i}'
        for key in ('title', 'question', 'what_to_learn', 'work_attribution'):
            if not unit.get(key):
                error('required_content', path + '/' + key, 'learning unit required answer missing')
        if not unit['source_refs']:
            error('required_evidence', path + '/source_refs', 'learning unit has no actual source evidence')
        elif unit.get('is_core') is not False and not any(usable_source_ref(ref) for ref in unit['source_refs']):
            error('required_evidence', path + '/source_refs', 'core learning needs usable block, section or asset evidence beyond parser placeholders')
        if unit.get('is_core') is True and unit['method_evidence'].get('status') == 'documented' and not unit['steps']:
            error('required_content', path + '/steps', 'documented core process has no recorded steps')
    checked_images = set()

    def check_image(raw, path):
        if raw.get('id') in checked_images:
            return
        checked_images.add(raw.get('id'))
        try:
            from PIL import Image
            source = Path(raw['image_path'])
            if hashlib.sha256(source.read_bytes()).hexdigest() != raw['sha256']:
                raise ValueError('hash drift')
            with Image.open(source) as image:
                if image.size != (raw['width'], raw['height']):
                    raise ValueError('dimensions drift')
        except (OSError, KeyError, ValueError, TypeError):
            error('image_version', path, 'actual image is unreadable or changed')

    for i, row in enumerate(asset_rows):
        path, raw = f'/assets/items/{i}', assets.get(row.get('id')) if isinstance(row.get('id'), str) else None
        if raw is None:
            error('asset_identity', path, 'asset ID absent from program material')
            continue
        if row.get('source_image_id') is not None and row['source_image_id'] not in assets:
            error('asset_identity', path + '/source_image_id', 'source image identity absent from material')
        if row.get('storage') == 'image' or row.get('evidence_basis') == 'pixels':
            selected = assets.get(row.get('source_image_id') or row['id'], {})
            if not selected.get('image_path') or not selected.get('sha256') or not selected.get('width') or not selected.get('height'):
                error('image_evidence', path, 'actual readable image binding required')
            else:
                check_image(selected, path)
        if row.get('evidence_basis') == 'table_text' and not raw.get('text'):
            if (best_effort and raw.get('image_path') and raw.get('sha256') and raw.get('width') and raw.get('height')
                    and not {raw['id'], row.get('source_image_id') or raw['id']} & set(original_reading.get('unread_refs', []))):
                check_image(raw, path)
                limit = '此表仅取得原始图像，没有原生表格文字；已有解释保留，数值与单元格内容未经程序独立核验。'
                compatible('normalized_table_evidence', path + '/evidence_basis', 'table_text', None,
                           'actual image retained; unsupported table-text claim becomes unknown, not verified pixels', limit)
                row['evidence_basis'] = None
                if limit not in row['limits']:row['limits'].append(limit)
            else:
                if learning and row.get('storage') != 'image':
                    compatible('normalized_table_evidence', path + '/evidence_basis', 'table_text', None,
                        'unsupported native-text claim removed; supplied explanation retained',
                        '此表没有可核验的原生表格文字；只保留已有解释，空表不提供数值或方法依据。')
                    row['evidence_basis'] = None
                else:
                    error('table_evidence', path, 'table text evidence was not supplied')
        if row.get('storage') is None:
            if learning:
                mode = 'description' if any(_content_present(row.get(k)) for k in ('meaning', 'purpose', 'materials_needed', 'organization')) else 'omit'
                compatible('normalized_asset_storage', path + '/storage', None, mode,
                    'only supplied explanation retained; no image-publication decision invented',
                    '该图表未给出保留图片的决定；仅保留已有文字，未解释的像素内容不作学习依据。')
                row['storage'] = mode
            else:
                error('asset_disposition', path + '/storage', 'asset preservation decision unresolved')

    flush_limits()
    majors = {row['code']: row for row in catalog.get('majors', []) if isinstance(row, dict) and isinstance(row.get('code'), str)} if isinstance(catalog, dict) else {}
    chosen = set()
    for key in ('routing_candidates', 'additional_related_specialties'):
        routes = data['profile'][key]
        if key == 'routing_candidates' and len(routes) > 3:
            error('routing_count', '/profile/' + key, 'at most three ranked delivery specialties')
        retained_routes = []
        for i, route in enumerate(routes):
            code, path = route.get('code'), '/profile/' + key + '/' + str(i)
            if best_effort and isinstance(code, str) and code not in majors and re.fullmatch(r'\d{6}', code):
                matches = [row for catalog_code, row in majors.items()
                           if re.fullmatch(re.escape(code) + r'[A-Z]+', catalog_code)
                           and route.get('name') == row.get('name')]
                if len(matches) == 1:
                    replacement = matches[0]['code']
                    compatible('normalized_specialty_suffix', path + '/code', code, replacement,
                               'exact specialty name and numeric code identify one supplied catalog entry')
                    code = route['code'] = replacement
            if learning and key == 'additional_related_specialties' and (not isinstance(code, str) or code not in majors or code in chosen):
                note = '模型原相关专业说明：' + str(code or '') + ' ' + str(route.get('name') or '') + '；' + str(route.get('reason') or '') + '。此项仅作相关说明，不作为投递目标。'
                compatible('normalized_related_specialty', path, route, None,
                           'optional related specialty retained as text; actual delivery targets unchanged', note,
                           target='/profile/research_profile')
                continue
            retained_routes.append(route)
            if not isinstance(code, str) or code not in majors:
                error('routing_code', path + '/code', 'specialty code is not in the supplied catalog')
                continue
            if code in chosen:
                error('routing_duplicate', path, 'delivery and related specialties must be distinct')
            chosen.add(code)
            if route.get('name') != majors[code]['name']:
                route['name'] = majors[code]['name']
                warning('normalized_name', path + '/name', 'specialty name filled from authoritative code')
        if learning and key == 'additional_related_specialties':
            data['profile'][key] = retained_routes
    if keep and not data['profile']['routing_candidates']:
        error('routing_missing', '/profile/routing_candidates', 'no delivery specialty selected')

    for i, gap in enumerate(data['gaps']):
        pointer, path = gap.get('target_pointer'), f'/gaps/{i}'
        target = None
        if pointer is not None:
            try:
                target = resolve_pointer(data, pointer)
            except (ValueError, TypeError):
                if learning:
                    limit = (gap.get('use_limit') or '') + '\n原缺项定位未能精确对应：' + str(pointer)
                    compatible('normalized_gap_location', path + '/target_pointer', pointer, None,
                               'original limitation retained globally; exact location is not asserted')
                    gap.update(target_pointer=None, use_limit=limit)
                    pointer = None
                else:
                    error('target_pointer', path + '/target_pointer', 'gap pointer does not resolve')
                    continue
        critical = False
        if isinstance(pointer, str):
            if pointer.startswith('/profile/research_profile') or pointer in ('/profile/learning_units', '/assets/items'):
                critical = True
            match = re.match(r'^/profile/learning_units/(\d+)(?:/|$)', pointer)
            if match and int(match[1]) < len(units):
                critical = units[int(match[1])].get('is_core') is not False
            match = re.match(r'^/assets/items/(\d+)(?:/|$)', pointer)
            if match and int(match[1]) < len(asset_rows):
                row = asset_rows[int(match[1])]
                critical = row.get('storage') == 'image' or any(unit.get('is_core') is not False and unit.get('id') in row.get('unit_ids', []) for unit in units)
        bounded_local = (isinstance(gap.get('use_limit'), str) and bool(gap['use_limit'].strip())
                         and _content_present(target) and pointer not in
                         ('/profile/research_profile', '/profile/learning_units', '/assets/items'))
        content_field = re.fullmatch(r'/profile/learning_units/\d+/(materials|steps|outputs|formulas_parameters)(?:/\d+)?', pointer or '')
        if content_field:
            bounded_local = bounded_local and _learning_content(target, content_field[1])
        whole_unit = re.fullmatch(r'/profile/learning_units/(\d+)', pointer or '')
        if whole_unit and critical:
            unit = units[int(whole_unit[1])]
            bounded_local = (bounded_local and gap.get('type') != 'not_read'
                             and all(_learning_content(unit.get(key), key) for key in ('materials', 'steps', 'outputs')))
        whole_asset = re.fullmatch(r'/assets/items/(\d+)', pointer or '')
        if whole_asset and critical:
            row = asset_rows[int(whole_asset[1])]
            bounded_local = (bounded_local and gap.get('type') != 'not_read'
                             and any(_content_present(row.get(key)) for key in ('meaning', 'purpose')))
        if not pointer and gap.get('type') in {'extraction_missing', 'not_read'}:
            error('unscoped_gap', path, 'unavailable or conflicting evidence has no bounded affected field')
        elif critical and gap.get('type') in {'extraction_missing', 'not_read'} and not bounded_local:
            error('critical_gap', path, 'important learning evidence remains unavailable or conflicting')
        else:
            # A core unit/image label alone says nothing about the extent of a
            # local gap. Keep the supplied limit when the affected field still
            # has content; empty core targets and unbounded gaps remain errors.
            warning('reported_gap', path, 'explicit local limitation retained')

    # Verify executable redactions against the exact private originals.
    # An omitted/described image has no public pixels to mask. Preserve the raw
    # plan privately, while the executable candidate only retains needed masks.
    executable=[]
    for i,row in enumerate(data['redaction_plan']):
        image_id=(row.get('image_ref') or {}).get('asset_id')
        dispositions=[a for a in asset_rows if (a.get('source_image_id') or a.get('id'))==image_id]
        if row.get('scope')=='image' and dispositions and all(a.get('storage') in {'omit','description'} for a in dispositions):
            warning('redaction_image_not_published',f'/redaction_plan/{i}','private mask plan retained in raw candidate; image pixels are not published')
            continue
        executable.append(row)
    if not best_effort:
        data['redaction_plan']=executable
    replacements, spans = {}, {}
    for i, row in enumerate(data['redaction_plan']):
        path, scope, action = f'/redaction_plan/{i}', row.get('scope'), row.get('action')
        if best_effort and scope in {'metadata', 'filename'} and not row.get('target_pointer'):
            # The publisher always rebuilds filenames and metadata. No original
            # Office/PDF property is copied, regardless of a model's selector.
            omissions.append(dict(id=row.get('id'), scope=scope, status='private_' + scope + '_omitted'))
            warning('redaction_publication_policy', path, 'original filename or document metadata is not copied to the publication')
            continue
        prepared = copy.deepcopy(row)
        prepared['location_status'] = 'located'
        if row.get('location_status') != 'located':
            error('redaction_unlocated', path, 'redaction cannot execute without a verified location')
        entity = row.get('entity_key')
        if not isinstance(entity, str) or not entity:
            error('redaction_entity', path, 'redaction entity identity required')
        if action == 'replace':
            replacement = row.get('replacement')
            if not isinstance(replacement, str) or not replacement:
                error('redaction_replacement', path, 'explicit nonempty replacement required')
                if best_effort:
                    continue
            if isinstance(entity, str):
                if entity in replacements and replacements[entity] != replacement:
                    error('redaction_consistency', path, 'same entity has conflicting replacements')
                replacements[entity] = replacement
        if scope == 'image':
            if action != 'mask':
                error('redaction_action', path, 'image requires mask action')
                if best_effort:
                    continue
            ref = row.get('image_ref')
            raw = assets.get(ref.get('asset_id')) if isinstance(ref, dict) and isinstance(ref.get('asset_id'), str) else None
            if (not raw or not raw.get('sha256')
                    or any(type(raw.get(k)) is not int or raw[k] <= 0 for k in ('width', 'height'))
                    or any(ref.get(key) != raw.get(source_key) for key, source_key in (('rendition_sha256', 'sha256'), ('width', 'width'), ('height', 'height')))):
                error('image_version', path, 'redaction image hash or dimensions disagree with actual rendition')
                continue
            if not any(a.get('storage') == 'image' and (a.get('source_image_id') or a.get('id')) == ref['asset_id'] for a in asset_rows):
                error('redaction_image_selection', path, 'redaction targets an image not selected for publication')
                if best_effort:
                    omissions.append(dict(id=row.get('id'), scope=scope, status='private_image_omitted'))
                    continue
            check_image(raw, path)
            boxes = row.get('redactions', [])
            accepted_boxes = []
            if not boxes:
                error('redaction_box', path, 'mask has no rectangles')
            for box_index, box in enumerate(boxes):
                if not isinstance(box, dict) or set(box) != {'x0', 'y0', 'x1', 'y1'} or any(type(box.get(k)) is not int for k in ('x0', 'y0', 'x1', 'y1')):
                    error('redaction_box', path + '/redactions/' + str(box_index), 'rectangle requires four integer coordinates')
                elif not 0 <= box['x0'] < box['x1'] <= raw['width'] or not 0 <= box['y0'] < box['y1'] <= raw['height']:
                    error('redaction_box', path + '/redactions/' + str(box_index), 'rectangle exceeds actual image')
                else:
                    accepted_boxes.append(copy.deepcopy(box))
            if accepted_boxes:
                prepared['redactions'] = accepted_boxes
                executable_redactions.append(prepared)
            continue
        if action not in ('replace', 'delete'):
            error('redaction_action', path, 'text requires replace or delete action')
            if best_effort:
                continue
        expected = row.get('expected_text')
        if not isinstance(expected, str) or not expected:
            error('redaction_expected', path, 'exact original text required')
            continue
        if scope == 'source_text':
            accepted_refs = []
            if not row.get('source_refs'):
                error('redaction_location', path, 'source text redaction has no location')
            for ref_index, ref in enumerate(row.get('source_refs', [])):
                ref_path = path + '/source_refs/' + str(ref_index)
                raw = blocks.get(ref.get('block_id')) if isinstance(ref, dict) and isinstance(ref.get('block_id'), str) else None
                start, end = ref.get('start'), ref.get('end')
                if not raw or type(start) is not int or type(end) is not int or raw.get('text', '')[start:end] != expected or not 0 <= start < end <= len(raw.get('text', '')):
                    error('redaction_match', ref_path, 'exact source text and offsets do not match')
                    continue
                key = ('block', ref['block_id'])
                overlap = False
                for previous in spans.setdefault(key, []):
                    if max(start, previous[0]) < min(end, previous[1]) and (start, end, action, row.get('replacement')) != previous:
                        error('redaction_overlap', ref_path, 'overlapping redactions disagree')
                        overlap = True
                if best_effort and overlap:
                    continue
                spans[key].append((start, end, action, row.get('replacement')))
                accepted_refs.append(copy.deepcopy(ref))
            if accepted_refs:
                prepared['source_refs'] = accepted_refs
                executable_redactions.append(prepared)
        elif scope == 'generated_field' or (best_effort and scope in {'metadata', 'filename'} and row.get('target_pointer')):
            pointer = row.get('target_pointer')
            try:
                target = resolve_pointer(data, pointer)
                if not isinstance(pointer, str) or not pointer.startswith(('/profile/', '/tree/', '/assets/', '/gaps/', '/attachments/')):
                    raise ValueError('not a public generated field')
                if not isinstance(target, str) or target.count(expected) != 1:
                    raise ValueError('not one exact text match')
                start = target.index(expected)
                current = (start, start + len(expected), action, row.get('replacement'))
                overlap = False
                for previous in spans.setdefault(('pointer', pointer), []):
                    if max(start, previous[0]) < min(current[1], previous[1]) and current != previous:
                        error('redaction_overlap', path, 'generated field redactions overlap')
                        overlap = True
                if best_effort and overlap:
                    continue
                spans[('pointer', pointer)].append(current)
                prepared['scope'] = 'generated_field'
                executable_redactions.append(prepared)
            except (ValueError, TypeError):
                error('redaction_match', path, 'generated field pointer or exact text does not match')
        elif scope in ('metadata', 'filename'):
            selector = row.get('metadata_selector')
            if not isinstance(selector, dict) or selector.get('file_ref') not in files:
                error('redaction_metadata', path, 'metadata requires actual file selector')
            elif scope == 'filename':
                name = Path(str(authoritative.get('file') or '')).name
                if name.count(expected) != 1:
                    error('redaction_match', path, 'filename original does not match')
            else:
                matches = [entry for entry in bundle.get('metadata', []) if isinstance(entry, dict) and entry.get('selector') == selector]
                if len(matches) != 1 or not isinstance(matches[0].get('value'), str) or matches[0]['value'].count(expected) != 1:
                    error('redaction_metadata', path, 'metadata selector has no exact program value')
        else:
            error('redaction_scope', path, 'redaction scope missing or unsupported')

    for i, suggestion in enumerate(data['segmentation']['split_suggestions']):
        group_path = f'{SPLIT_HINT_PATH}/{i}'
        if any((issue['path'] == group_path or issue['path'].startswith(group_path + '/'))
               and not issue['path'].startswith(group_path + '/split_points/') for issue in split_issues):
            continue
        for j, point in enumerate(suggestion['split_points']):
            point_path = f'{group_path}/split_points/{j}'
            raw = blocks.get(point.get('block_id')) if isinstance(point.get('block_id'), str) else None
            offset = point.get('offset')
            if not raw or type(offset) is not int or not 0 <= offset <= len(raw.get('text', '')):
                error('segmentation_location', point_path, 'optional split point does not locate an actual block boundary')
                continue
            if not any(issue['path'] == point_path or issue['path'].startswith(point_path + '/') for issue in split_issues):
                executable_split_points.append(dict(block_id=point['block_id'], offset=offset))
    declared_pixels = {row.get('id') for row in asset_rows if row.get('evidence_basis') == 'pixels'}
    unread = set(original_reading.get('unread_refs', []))
    removed_positions = [row.get('block', {}).get('position') for row in bundle.get('removed', [])
                         if isinstance(row, dict) and row.get('reason') and isinstance(row.get('block'), dict)]
    for issue in bundle.get('issues', []):
        if isinstance(issue, dict) and (issue.get('critical') is True or issue.get('blocking') is True):
            error('material_gap', '/source/reading_scope', 'program reports blocking extraction issue')
        elif isinstance(issue, dict) and issue.get('code') in READING_FAILURE_CODES:
            position = issue.get('position')
            if (any(_same_source_object(position, removed) for removed in removed_positions)
                    and not any(_same_source_object(position, block.get('position')) for block in blocks.values())):
                warning('removed_material_issue', '/source/reading_scope',
                        'issue belongs to an object already excluded by deterministic cleaning; original diagnostic retained')
                continue
            pages = issue.get('page_evidence_ids')
            available = (isinstance(pages, list) and bool(pages) and all(
                isinstance(page, str) and page in assets and assets[page].get('image_path') for page in pages))
            if available:
                for page in pages:
                    check_image(assets[page], '/source/reading_scope')
            fallback = issue.get('page_evidence_status') == 'provided_full_document_fallback'
            resolved = (issue.get('code') != 'page_render_failed' and available and not fallback
                        and all(page in declared_pixels and page not in unread for page in pages))
            if resolved:
                warning('material_visual_evidence', '/source/reading_scope', 'native extraction issue has actual page images declared read; semantic limits remain candidate responsibility')
            elif available and issue.get('code') in READING_REVIEW_CODES:
                warning('material_review_limit', '/source/reading_scope',
                        'review diagnostic has real backup images; all-page reading is not required and object understanding is not asserted')
            else:
                error('material_gap', '/source/reading_scope', 'known extraction failure lacks actual page image reading evidence')
        else:
            warning('material_issue', '/source/reading_scope', 'program extraction issue retained in material')
    auxiliary_pages = {ident for ident, asset in assets.items() if asset.get('kind') == 'source_page'}
    bounded_unread = {ref.get(key) for gap in data['gaps'] if gap.get('target_pointer') and gap.get('use_limit')
                     for ref in gap.get('source_refs', []) for key in ('block_id', 'asset_id', 'section_id')}
    usable_units = []
    if learning and keep:
        primary_refs = {value for value in (bundle.get('file_ref', 'main'), authoritative.get('source_id'),
                        authoritative.get('file'), authoritative.get('text_file')) if value}
        main_unread = bool(unread & primary_refs)
        def ref_is_unread(ref):
            return (bool(unread & {ref.get(key) for key in ('file_ref', 'block_id', 'section_id', 'asset_id')})
                    or blocks.get(ref.get('block_id'), {}).get('section_id') in unread
                    or (main_unread and (not ref.get('file_ref') or ref['file_ref'] in primary_refs)))
        def contains_unread(value):
            if isinstance(value, dict):
                return (any(ref_is_unread(ref) for ref in value.get('source_refs', []))
                        or any(contains_unread(child) for key, child in value.items() if key != 'source_refs'))
            return isinstance(value, list) and any(contains_unread(child) for child in value)
        if unread:
            pending_limits.append(('/source/reading_scope/unread_refs',
                '模型明确标注部分材料未读；未读部分不能作为已核实依据，需要使用时回看原稿。'))
        for index, unit in enumerate(units):
            pointer = f'/profile/learning_units/{index}'
            if contains_unread(unit):
                pending_limits.append((pointer, '此单元引用的部分依据被模型标注为未读；已有步骤保留，相关内容需回读核实。'))
            declared_unread = any(g.get('type') == 'not_read' and g.get('target_pointer') in
                (pointer, '/profile/learning_units', '/profile') for g in data['gaps'])
            if (not declared_unread and _content_present(unit.get('question'))
                    and all(_learning_content(unit.get(key), key) for key in ('materials', 'steps', 'outputs'))
                    and any(usable_source_ref(ref) and not ref_is_unread(ref) for ref in unit['source_refs'])):
                usable_units.append(unit['id'])
        if not usable_units:
            error('learning_unusable', '/profile/learning_units', 'no traceable learning unit has a substantive question, materials, steps and output')
        else:
            retained_errors = []
            blocking_extraction = any(isinstance(issue, dict) and
                (issue.get('critical') is True or issue.get('blocking') is True) for issue in bundle.get('issues', []))
            for issue in errors:
                code, path = issue['code'], issue['path']
                local_content = code in {'required_content', 'required_evidence'} and path.startswith('/profile/learning_units/')
                if code in {'critical_gap', 'unscoped_gap'} or (code == 'material_gap' and not blocking_extraction) or local_content:
                    warning('learning_limit_' + code, path, issue['message'])
                    if code in {'critical_gap', 'unscoped_gap'}:
                        row = resolve_pointer(data, path)
                        if not _content_present(row.get('use_limit')):
                            row['use_limit'] = '此处材料未充分读取；仅借鉴已写明的方法与步骤，相关细节需回看原文。'
                    elif code == 'material_gap':
                        pending_limits.append(('/source/reading_scope',
                            '原稿存在公式或图形读取提示；可学习已记录的方法、步骤和产出，公式括号、参数及图纸细节须对照原文核实后使用。'))
                    else:
                        pending_limits.append((path, '此学习单元有局部字段或定位缺项；保留已提取内容，缺项处不能作为完整复现依据。'))
                else:
                    retained_errors.append(issue)
            errors[:] = retained_errors
            flush_limits()
    allow_partial = (data.get('record_kind') == 'partial_candidate' and not errors and
        ((learning and bool(usable_units)) or
         (best_effort and unread <= auxiliary_pages | bounded_unread
          and bool(data['gaps'] or redaction_issues or omissions or compatibility_report))))
    if data.get('record_kind') == 'partial_candidate':
        if allow_partial:
            warning('model_declared_partial', '/record_kind', 'model partial declaration retained; program found only bounded noncritical limitations or best-effort privacy issues')
        else:
            error('candidate_incomplete', '/record_kind', 'partial candidate requires resolution before publication')
    flush_limits()
    if errors:
        data['record_kind'] = 'partial_candidate'
    return dict(valid=not errors, errors=errors, warnings=warnings, candidate=data,
        compatibility_report=compatibility_report,
        executable_redactions=executable_redactions,
        executable_split_points=executable_split_points,
        segmentation_warnings=[dict(row, execution='skipped_optional_hint') for row in split_issues],
        admission=dict(allow_partial=allow_partial, redaction_policy=redaction_policy,
                       policy=admission_policy, usable_unit_ids=usable_units,
                       gate_policy='three-required-checks/1' if learning else 'strict'),
        redaction_report=dict(policy=redaction_policy, requested=len(data['redaction_plan']),
            executable=len(executable_redactions), omissions=omissions, warnings=redaction_issues))
