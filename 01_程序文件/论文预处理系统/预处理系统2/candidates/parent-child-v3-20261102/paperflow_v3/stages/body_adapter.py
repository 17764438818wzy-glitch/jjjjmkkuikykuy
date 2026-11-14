"""Deterministic candidate adapter for existing, completely saved body replies.

Callers bind the reviewed legacy business modules before calling this module.
No controller, credentials, connection, request runner or upload entry is used.
The upstream bridge must build ``body-postprocess-input/1`` from its verified
handoff and pass the independently trusted seven-field folder identity.
``task_run_id`` retains the upstream historical name; it is a folder FlowRun.

Input references are {path, sha256}. ``plan`` is the frozen full planner output;
``plan_identity`` is the upstream paperflow-body-input-plan/1 receipt. Each
response row supplies identity, part_id, request/state/events/response refs,
images_work, and request_images (a checked checkpoint ref or None).
Receipt bytes are checked here; registered-receipt admission belongs upstream.
Counter is caller-owned and must be the reviewed offline production counter.
The enclosing folder stage must pin body-result.json, READY, manifest and every
public package member in its independently persisted receipt. Local replay checks
prove current derivation, not an authenticated history if every output is resealed.
"""
from collections import Counter, defaultdict
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat


IDENTITY_KEYS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
                 'parent_run_id', 'task_run_id')
MERGE_LIMIT = ('各段按已冻结范围由程序合并，未进行全篇模型统合；跨段关系、全局结论和语义一致性未独立核实。')


class BodyValidationError(ValueError):
    """A deterministic invalid profile with a persisted private report."""
    def __init__(self, reason, diagnostic):
        self.diagnostic = diagnostic
        super().__init__(reason)


def _require(good, code):
    if not good:
        raise ValueError(code)


def _digest(value, *, sort=True):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=sort,
                                    separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _read(reference, *, json_value=False):
    _require(isinstance(reference, dict) and isinstance(reference.get('path'), str)
             and re.fullmatch('[a-f0-9]{64}', str(reference.get('sha256', ''))), 'invalid_file_reference')
    try:
        raw = Path(reference['path']).read_bytes()
    except OSError as exc:
        raise ValueError('input_missing') from exc
    _require(hashlib.sha256(raw).hexdigest() == reference['sha256'], 'input_changed')
    if json_value:
        try:
            return json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise ValueError('invalid_input_json') from exc
    return raw


def _reference(path):
    return {'path': str(Path(path).resolve()), 'sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest()}


def _save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    return _reference(path)


def _identity(actual, expected):
    _require(isinstance(actual, dict) and isinstance(expected, dict)
             and all(isinstance(expected.get(k), str) and expected[k] for k in IDENTITY_KEYS)
             and all(actual.get(k) == expected[k] for k in IDENTITY_KEYS), 'folder_identity_mismatch')


def frozen_plan_evidence(work, plan_identity):
    """Capture existing upstream plan files, without plan creation or recovery.

    Pass the returned value as evidence['plan']. Identity/coverage are checked
    against the trusted upstream plan_identity again when consuming it.
    """
    work = Path(work).resolve()
    mode = plan_identity.get('mode')
    _require(mode in {'single', 'partitioned'}, 'invalid_plan_mode')
    paths = ({'single': work / 'private/llm-frozen-single-plan.json'} if mode == 'single' else {
        'metadata': work / 'private/llm-frozen-plan-metadata.json',
        'partition_plan': work / 'model/partition-plan.json',
        'partition_state': work / 'model/partition-state.json',
        'partition_material': work / 'model/partition-material.json'})
    if mode == 'partitioned':
        for index, part in enumerate(plan_identity.get('parts', []), 1):
            ident = f'part-{index:03d}'
            _require(part.get('id') == ident, 'invalid_part_identity')
            paths[ident + '.request'] = work / 'model' / ident / 'planned-request.json'
            paths[ident + '.material'] = work / 'model' / ident / 'material.json'
    try:
        references = {key: _reference(path) for key, path in paths.items()}
    except OSError as exc:
        raise ValueError('frozen_plan_files_missing') from exc
    descriptor = dict(schema='body-frozen-plan-files/1', mode=mode, work=str(work), files=references)
    _load_plan(descriptor, plan_identity)
    return descriptor


def _load_plan(reference, receipt, original=None):
    if reference.get('schema') != 'body-frozen-plan-files/1':
        return _read(reference, json_value=True)
    mode, files, work = reference.get('mode'), reference.get('files'), Path(reference.get('work', '')).resolve()
    _require(mode == receipt.get('mode') and isinstance(files, dict), 'frozen_plan_mode_mismatch')
    for item in files.values():
        _require(Path(item['path']).resolve().is_relative_to(work), 'frozen_plan_path_outside_work')
    values = {key: _read(item, json_value=True) for key, item in files.items()}
    if mode == 'single':
        value = values.get('single', {})
        _require(set(values) == {'single'} and value.get('schema') == 'paperflow-raw-frozen-single-plan/1'
                 and value.get('plan_sha256') == receipt.get('plan_sha256'), 'frozen_single_plan_mismatch')
        plan = value.get('plan')
    else:
        metadata, frozen, state = (values.get(k, {}) for k in ('metadata', 'partition_plan', 'partition_state'))
        rows, extras = frozen.get('parts'), metadata.get('part_extras')
        _require(metadata.get('schema') == 'paperflow-raw-frozen-plan-metadata/1'
                 and metadata.get('plan_sha256') == receipt.get('plan_sha256')
                 and isinstance(metadata.get('top'), dict) and 'parts' not in metadata['top']
                 and frozen.get('schema') == state.get('schema') == 'preextract-partition-run/1'
                 and state.get('status') == 'parts_pending' and not state.get('recoveries')
                 and state.get('plan_sha256') == files['partition_plan']['sha256']
                 and frozen.get('material_sha256') == files['partition_material']['sha256']
                 and isinstance(rows, list) and isinstance(extras, list)
                 and len(rows) == len(extras) == receipt.get('part_count'), 'frozen_partition_state_mismatch')
        if original is not None:
            _require(frozen.get('source_material_sha256') == _digest(original), 'partition_original_changed')
        requests = state.get('requests')
        _require(isinstance(requests, list) and len(requests) == len(rows), 'partition_requests_incomplete')
        fields = {'id', 'ranges', 'asset_ids', 'capacity', 'body', 'bundle'}
        parts = []
        expected_keys = {'metadata', 'partition_plan', 'partition_state', 'partition_material'}
        for index, (row, extra, request_row) in enumerate(zip(rows, extras, requests), 1):
            ident = f'part-{index:03d}'
            body, bundle = values.get(ident + '.request'), values.get(ident + '.material')
            expected_keys.update((ident + '.request', ident + '.material'))
            _require(row.get('id') == extra.get('id') == request_row.get('id') == ident
                     and isinstance(extra.get('extra'), dict) and not (fields & set(extra['extra']))
                     and isinstance(body, dict) and isinstance(bundle, dict)
                     and row.get('material_sha256') == files[ident + '.material']['sha256']
                     and row.get('request_sha256') == request_row.get('request_sha256') == _digest(body, sort=False),
                     'frozen_partition_part_mismatch')
            parts.append({**extra['extra'], 'id': ident, 'ranges': row.get('ranges'),
                          'asset_ids': row.get('asset_ids'), 'capacity': row.get('capacity'), 'body': body, 'bundle': bundle})
        _require(set(values) == expected_keys, 'unexpected_frozen_plan_files')
        plan = {**metadata['top'], 'parts': parts}
        _require(plan.get('coverage') == frozen.get('coverage'), 'frozen_coverage_mismatch')
    _require(isinstance(plan, dict) and _digest(plan) == receipt.get('plan_sha256'), 'reconstructed_plan_mismatch')
    return plan


def _formula_gaps(reading, blocks):
    # Reproduce the real partitioner's deterministic ambiguity calculation;
    # never trust a gap merely because the frozen plan repeats it.
    groups = {}
    for formula in reading.get('formulas', []):
        key = (json.dumps(formula.get('position') or {}, sort_keys=True), str(formula.get('latex') or ''))
        groups.setdefault(key, []).append(formula)
    gaps = []
    for (encoded, latex), formulas in groups.items():
        position = json.loads(encoded)
        if not position or not latex:
            continue
        marker = '$' + latex + '$'
        occurrences = [(bid, match.start(), match.end()) for bid, block in blocks.items()
                       if block.get('position') and all(position.get(k) == v for k, v in block['position'].items())
                       for match in re.finditer(re.escape(marker), block.get('text', ''))]
        if occurrences and len(occurrences) != len(formulas):
            gaps.append({'code': 'formula_position_unresolved', 'position': position,
                         'message': '同一段中有重复公式标记，无法确定原生公式对应哪一处；文字继续保留。'})
    return gaps


def _partition_source(part, reading, blocks, formula_gaps):
    """Match the partitioner's exact local issue and source projection."""
    subset = part['bundle']
    available = {(row.get('source_span') or {'block_id': row['id']})['block_id']
                 for row in subset.get('blocks', [])}
    _require(available <= set(blocks), 'part_issue_source_mismatch')
    ranges, owned = part['ranges'], set(part['asset_ids'])
    # make() may add short immediately adjacent context and locations for
    # images owned by this part; unrelated blocks cannot justify extra issues.
    included = {span['block_id'] for span in ranges}
    _require(included <= set(blocks), 'part_issue_source_mismatch')
    order = list(blocks)
    indices = {bid: index for index, bid in enumerate(order)}
    allowed = set(included)
    for bid in included:
        for index in (indices[bid] - 1, indices[bid] + 1):
            if 0 <= index < len(order) and len(blocks[order[index]].get('text', '')) <= 800:
                allowed.add(order[index])
    for asset in reading.get('assets', []):
        if asset.get('id') in owned and asset.get('image_path'):
            allowed.update(asset.get('block_ids', []))
    _require(available <= allowed, 'part_context_outside_scope')
    def local(issue):
        pos = issue.get('position') or {}
        return (any(all(pos.get(k) == v for k, v in (blocks[bid].get('position') or {}).items())
                    for bid in available) if pos else
                bool(ranges and ranges[0]['block_id'] == next(iter(blocks), None)))
    issues = [copy.deepcopy(issue) for issue in reading.get('issues', [])
              if local(issue) or set(issue.get('page_evidence_ids', [])) & owned]
    issues.extend(copy.deepcopy(gap) for gap in formula_gaps if local(gap))
    _require(subset.get('issues', []) == issues, 'part_issues_mismatch')
    source = copy.deepcopy(reading['source'])
    if source.get('reading_scope') is not None:
        source['reading_scope']['extraction_issues'] = list(dict.fromkeys(issue.get('message', '') for issue in issues))
    return source


def _plan(plan, receipt, source_sha, reading_sha, reading):
    _require(isinstance(plan, dict) and plan.get('schema') == 'preextract-input-plan/1', 'invalid_frozen_plan')
    parts, mode = plan.get('parts'), plan.get('mode')
    _require(isinstance(parts, list) and parts and mode in {'single', 'partitioned'}
             and ((mode == 'single' and len(parts) == 1) or (mode == 'partitioned' and len(parts) > 1)),
             'invalid_plan_parts')
    identities = []
    ranges, assets = [], []
    source_blocks = {b['id']: b for b in reading['blocks']}
    _require(len(source_blocks) == len(reading['blocks']), 'duplicate_source_blocks')
    formula_gaps = _formula_gaps(reading, source_blocks) if mode == 'partitioned' else []
    if mode == 'partitioned':
        _require(plan.get('partition_gaps', []) == formula_gaps, 'partition_gaps_mismatch')
    for index, part in enumerate(parts, 1):
        ident = f'part-{index:03d}'
        _require(isinstance(part, dict) and (mode == 'single' or part.get('id') == ident)
                 and isinstance(part.get('bundle'), dict) and isinstance(part.get('body'), dict)
                 and isinstance(part.get('ranges'), list) and isinstance(part.get('asset_ids'), list), 'invalid_plan_part')
        identities.append(dict(id=ident, request_sha256=_digest(part['body'], sort=False),
            material_sha256=_digest(part['bundle']), scope_sha256=_digest({k: part[k] for k in ('ranges', 'asset_ids')})))
        subset = part['bundle']
        expected_source = (_partition_source(part, reading, source_blocks, formula_gaps)
                           if mode == 'partitioned' else reading.get('source'))
        _require(subset.get('source') == expected_source, 'part_source_mismatch')
        primaries = []
        for block in subset.get('blocks', []):
            span = block.get('source_span') or dict(block_id=block['id'], start=0, end=len(block['text']))
            original = source_blocks.get(span.get('block_id'))
            _require(original is not None and type(span.get('start')) is int and type(span.get('end')) is int
                     and 0 <= span['start'] <= span['end'] <= len(original['text'])
                     and block.get('section_id') == original.get('section_id')
                     and block.get('text') == original['text'][span['start']:span['end']], 'part_content_mismatch')
            if block.get('reading_role', 'primary') == 'primary':
                primaries.append(span)
        _require(primaries == part['ranges'], 'part_ranges_mismatch')
        ranges.extend(part['ranges']); assets.extend(part['asset_ids'])
    coverage = plan.get('coverage')
    _require(isinstance(coverage, dict) and coverage.get('complete') is True
             and coverage.get('ranges') == ranges and Counter(coverage.get('asset_ids', [])) == Counter(assets)
             and len(assets) == len(set(assets)), 'incomplete_plan_coverage')
    intervals = defaultdict(list)
    for span in ranges:
        intervals[span['block_id']].append((span['start'], span['end']))
    _require(set(intervals) == set(source_blocks), 'uncovered_source_blocks')
    for bid, spans in intervals.items():
        cursor = 0
        for start, end in sorted(spans):
            _require(start == cursor and end >= start, 'overlapping_or_missing_ranges')
            cursor = end
        _require(cursor == len(source_blocks[bid]['text']), 'uncovered_source_characters')
    _require(coverage.get('block_count') == len(source_blocks)
             and coverage.get('character_count') == sum(len(b['text']) for b in source_blocks.values()), 'coverage_count_mismatch')
    _require(set(assets) == {a['id'] for a in reading.get('assets', [])}, 'uncovered_source_assets')
    expected = dict(schema='paperflow-body-input-plan/1', source_sha256=source_sha, reading_sha256=reading_sha,
        mode=mode, part_count=len(parts), parts=identities, coverage_sha256=_digest(coverage), plan_sha256=_digest(plan))
    _require(receipt == expected, 'plan_identity_mismatch')


def _verify_named_source(evidence, original, reading, expected_identity):
    """Validate the frozen preprocessor's named copy against the receipt."""
    from paperflow_prefect.stage3_handoff import read_receipt
    from paperflow_prefect.intake_handoff import verify_received_file
    from paperflow_prefect.named_delivery import _safe_directory, _valid_name
    try:
        named = evidence['named_source']
        work = _no_reparse(named['work']).resolve(strict=True)
        _require(evidence['plan'].get('schema') == 'body-frozen-plan-files/1'
                 and Path(evidence['material']['path']).resolve(strict=True).parent == work,
                 'named_source_work_mismatch')
        proof_path = _no_reparse(named['proof']['path']).resolve(strict=True)
        _require(proof_path == work / 'named-input-proof.json', 'named_proof_path_mismatch')
        proof = _read(named['proof'], json_value=True)
        _read(evidence['receipt'])
        receipt, paper = read_receipt(evidence['receipt']['path'])
        _require(isinstance(paper, dict) and isinstance(paper.get('body'), dict), 'named_receipt_mismatch')
        row = paper['body']
        _require(all(receipt.get(k) == expected_identity[k] for k in ('group_id', 'paper_id', 'receipt_id'))
                 and verify_received_file(row) is True, 'named_receipt_mismatch')
        source = _no_reparse(evidence['source']['path']).resolve(strict=True)
        target = _no_reparse(proof['delivery_path']).resolve(strict=True)
        _safe_directory(proof_path)
        _safe_directory(source)
        _safe_directory(target)
        hashes = original.get('file_hashes') or {}
        digest = evidence['source']['sha256']
        _require(proof.get('schema') == 'named-delivery-input/1'
                 and proof.get('paper_id') == expected_identity['paper_id']
                 and proof.get('file_id') == row['file_id'] and proof.get('kind') == 'body'
                 and Path(row['path']).resolve() == source
                 and Path(proof['source_path']).resolve() == source
                 and row['sha256'] == proof.get('source_sha256') == digest
                 and row['bytes'] == proof.get('bytes') == source.stat().st_size == target.stat().st_size
                 and target.parent == work / 'named-input' and target.name == proof.get('display_name')
                 and _valid_name(target.name, source.name)
                 and all(Path(bundle['source']['file']).resolve() == target
                         and bundle['source']['source_sha256'] == digest for bundle in (original, reading))
                 and hashes.get(str(proof_path)) == named['proof']['sha256']
                 and hashes.get(str(source)) == hashes.get(str(target)) == digest,
                 'named_source_binding_mismatch')
        _read(evidence['source'])
        _read({'path': str(target), 'sha256': digest})
        _read(named['proof'])
    except (KeyError, TypeError, OSError) as exc:
        raise ValueError('invalid_named_source') from exc


def _held_images(evidence, reading):
    """A frozen image adaptation may omit pixels, never source-byte checks."""
    reference = evidence.get('image_report')
    if reference is None:
        return {}
    work = (Path(evidence['plan']['work']).resolve() if evidence['plan'].get('schema') == 'body-frozen-plan-files/1'
            else Path(evidence['material']['path']).resolve().parent)
    _require(Path(reference['path']).resolve() == work / 'private/request-images.json', 'image_report_path_mismatch')
    report = _read(reference, json_value=True)
    _require(report.get('schema') == 'preextract-request-images/1'
             and report.get('material_sha256') == _digest(reading), 'image_report_material_mismatch')
    assets = {row['id']: row for row in reading.get('assets', [])}
    held = {}
    for row in report.get('images', []):
        if row.get('held') is not True:
            continue
        asset = assets.get(row.get('id'))
        _require(asset is not None and asset.get('image_path') and row['id'] not in held
                 and row.get('source_sha256') == asset.get('sha256'), 'held_image_source_mismatch')
        _read({'path': asset['image_path'], 'sha256': asset['sha256']})
        held[row['id']] = asset['sha256']
    return held


def _bound_inputs(evidence, expected_identity):
    """Verify actual files and completed requests before optional content projection."""
    _require(isinstance(evidence, dict) and evidence.get('schema') == 'body-postprocess-input/1', 'invalid_body_input')
    _identity(evidence.get('identity'), expected_identity)
    _read(evidence['receipt'])
    _read(evidence['source'])
    original = _read(evidence['material'], json_value=True)
    reading = _read(evidence['reading_material'], json_value=True)
    plan = _load_plan(evidence['plan'], evidence['plan_identity'], original)
    receipt = evidence['plan_identity']
    _require(_digest(plan) == receipt.get('plan_sha256')
             and receipt.get('source_sha256') == evidence['source']['sha256']
             and receipt.get('reading_sha256') == evidence['reading_material']['sha256']
             and receipt.get('mode') == plan.get('mode')
             and receipt.get('part_count') == len(plan.get('parts', [])), 'plan_source_identity_mismatch')
    from preextract_images import _verified_image
    if any(Path(bundle['source']['file']).resolve() != Path(evidence['source']['path']).resolve()
           for bundle in (original, reading)):
        _verify_named_source(evidence, original, reading, expected_identity)
    held = _held_images(evidence, reading)
    for bundle in (original, reading):
        _require(bundle.get('version') == 'preextract-material/1.0'
                 and bundle.get('source', {}).get('source_sha256') == evidence['source']['sha256'], 'source_material_mismatch')
        for field in ('text', 'index'):
            path, sha = bundle['source'].get(field + '_file'), bundle['source'].get(field + '_sha256')
            if path is not None or sha is not None:
                _read({'path': path, 'sha256': sha})
        for asset in bundle.get('assets', []):
            if asset.get('image_path'):
                if held.get(asset['id']) == asset.get('sha256'):
                    _read({'path': asset['image_path'], 'sha256': asset['sha256']})
                else:
                    _verified_image(asset)
    rows = evidence.get('responses')
    _require(isinstance(rows, list) and len(rows) == len(plan['parts']), 'missing_or_extra_responses')
    from preextract_scheduler_adapter import request_observation
    verified = []
    for index, (row, part) in enumerate(zip(rows, plan['parts']), 1):
        _identity(row.get('identity'), expected_identity)
        _require(row.get('part_id') == f'part-{index:03d}', 'response_part_mismatch')
        request = _read(row['request'], json_value=True)
        state = _read(row['state'], json_value=True)
        raw = _read(row['response']).decode('utf8')
        _read(row['events'])
        directory = Path(row['request']['path']).resolve().parent
        for key, name in [('request', 'request.json'), ('state', 'request-state.json'),
                          ('events', 'response-events.jsonl'), ('response', 'response.txt')]:
            _require(Path(row[key]['path']).resolve() == directory / name, 'response_evidence_directory_mismatch')
        request_sha = _digest(part['body'], sort=False)
        _require(receipt['parts'][index - 1].get('request_sha256') == request_sha,
                 'plan_request_identity_mismatch')
        phase, next_step, observation = request_observation(directory)
        _require(request == part['body'] and _digest(request, sort=False) == request_sha
                 and state.get('status') == 'response_complete' and state.get('finish_reason') == 'stop'
                 and state.get('request_sha256') == request_sha
                 and state.get('response_sha256') == row['response']['sha256']
                 and phase == 'response_complete' and next_step == 'postprocess'
                 and observation.get('request_sha256') == request_sha
                 and observation.get('response_sha256') == row['response']['sha256'], 'raw_response_not_complete')
        images_work = Path(row['images_work']).resolve()
        checkpoint = images_work / 'private/request-images.json'
        checkpoint_ref = row.get('request_images')
        if checkpoint.exists():
            _require(isinstance(checkpoint_ref, dict) and Path(checkpoint_ref['path']).resolve() == checkpoint,
                     'unbound_image_checkpoint')
            _read(checkpoint_ref)
        else:
            _require(checkpoint_ref is None, 'image_checkpoint_missing')
        verified.append((raw, part['bundle']))
    ranking = evidence.get('whole_paper_ranking')
    if isinstance(ranking, dict) and ranking.get('response'):
        _read(ranking['response'])
    return plan, original, reading, verified


def _verified_whole_table(asset, subset, original, blocks):
    """Verify the native row projection, then retain its bound complete table."""
    if asset == original:
        return original
    from knowledge_tables import table_data
    from preextract_partition import _fragment as legacy_fragment
    from paperflow_prefect.body_segment_plan import _fragment as current_fragment
    _require(asset.get('kind') == 'table' and not asset.get('image_path'), 'table_projection_mismatch')
    structure = original.get('structure') or {}
    grid = structure.get('grid')
    _require(isinstance(grid, list) and grid and isinstance(grid[0], list) and grid[0],
             'table_projection_source_invalid')
    native = table_data(dict(structure, columns=len(grid[0])), original['id'])
    lines = ['\t'.join(row) for row in native['rows']]
    joined = '\n'.join(lines)
    fragments, selected_rows = [], set()
    for row in subset.get('blocks', []):
        span = row.get('source_span') or {}
        if span.get('block_id') not in original.get('block_ids', []):
            continue
        block = blocks.get(span['block_id'])
        if not row.get('table_context'):
            # The planner adds whole adjacent context after row projection.
            # Only that exact native form may lack table_context; a primary
            # or partial fragment cannot silently drop its table rows.
            _require(block is not None and row.get('reading_role') == 'context'
                     and span.get('start') == 0 and span.get('end') == len(block.get('text', ''))
                     and any(row == fragment(block, span, 'context')
                             for fragment in (current_fragment, legacy_fragment)),
                     'table_projection_fragment_missing')
            continue
        _require(block is not None and block.get('text') in (joined, joined + '\n'),
                 'table_projection_source_invalid')
        cursor, spans = 0, []
        for index, line in enumerate(lines):
            end = cursor + len(line) + int(index < len(lines) - 1 or block['text'] == joined + '\n')
            spans.append((cursor, end))
            cursor = end
        _require(span['start'] in {a for a, _ in spans} and span['end'] in {z for _, z in spans},
                 'table_projection_row_boundary')
        selected = [i for i, (a, z) in enumerate(spans) if a < span['end'] and z > span['start']]
        role = row.get('reading_role', 'primary')
        _require(role in {'primary', 'context'}, 'table_projection_mismatch')
        context = dict(original_rows=selected,
            header='\n'.join(lines[i] for i in structure.get('header_rows', [])),
            units=structure.get('units'), notes=structure.get('notes'))
        # Both existing planners own complete fragment forms. Compare whole
        # objects, including exact position markers; do not splice or drop fields.
        expected_fragments = []
        for fragment in (current_fragment, legacy_fragment):
            expected = fragment(block, span, role)
            expected['table_context'] = context
            expected_fragments.append(expected)
        _require(any(row == expected for expected in expected_fragments), 'table_projection_fragment_mismatch')
        fragments.append(row)
        selected_rows.update(selected)
    _require(fragments, 'table_projection_fragment_missing')
    expected = copy.deepcopy(original)
    projected = expected['structure']
    headers = structure.get('header_rows', [])
    selected = sorted(set(headers) | selected_rows)
    rows = [grid[i] for i in selected]
    used = sorted({i for row in rows for i in row if i is not None})
    indexes = {value: index for index, value in enumerate(used)}
    projected.update(cells=[structure['cells'][i] for i in used],
        grid=[[None if i is None else indexes[i] for i in row] for row in rows],
        source_row_indices=selected, source_header_rows=headers,
        header_rows=[selected.index(i) for i in headers])
    expected['block_ids'] = [row['id'] for row in fragments]
    _require(asset == expected, 'table_projection_mismatch')
    return original


def _project_inputs(evidence, bound):
    """Best-effort rich projection; it cannot veto already bound source/profile storage."""
    plan, original, reading, raw_rows = bound
    if (evidence['plan'].get('schema') == 'body-frozen-plan-files/1'
            and evidence.get('image_report') is not None):
        # The input receipt binds pre-adaptation reading bytes, while the frozen
        # plan owns the native sent renditions (including tile IDs). Restore the
        # existing checkpoint before checking coverage; never regenerate images.
        from preextract_images import restore_prepared_images
        from paperflow_prefect.body_clean_handoff import add_image_gaps
        projection = reading
        reading = restore_prepared_images(projection, evidence['plan']['work'])
        report = _read(evidence['image_report'], json_value=True)
        reading, _ = add_image_gaps(reading, projection, report)
        frozen = (_read(evidence['plan']['files']['partition_material'], json_value=True)
                  if plan['mode'] == 'partitioned' else plan['parts'][0]['bundle'])
        _require(_digest(reading) == _digest(frozen), 'restored_reading_plan_mismatch')
    _plan(plan, evidence['plan_identity'], evidence['source']['sha256'], evidence['reading_material']['sha256'], reading)
    from preextract_images import restore_request_images
    from preextract_image_alias import AliasContext
    aliases, verified = AliasContext(), []
    for row, part, (raw, _) in zip(evidence['responses'], plan['parts'], raw_rows):
        request = part['body']
        images_work = Path(row['images_work']).resolve()
        source_assets = {a['id']: a for a in reading.get('assets', [])}
        image_root = (Path(evidence['plan']['work']).resolve()
                      if evidence['plan'].get('schema') == 'body-frozen-plan-files/1' else images_work)
        for asset in part['bundle'].get('assets', []):
            source_asset = source_assets.get(asset['id'])
            _require(source_asset is not None, 'image_origin_mismatch')
            if asset.get('image_path') and asset.get('sha256') != source_asset.get('sha256'):
                adaptation = asset.get('image_adaptation', {})
                _require(adaptation.get('source_sha256') == source_asset.get('sha256')
                         and adaptation.get('source_width') == source_asset.get('width')
                         and adaptation.get('source_height') == source_asset.get('height')
                         and Path(asset['image_path']).resolve().is_relative_to(image_root / 'private/request-images'),
                         'image_origin_mismatch')
        subset = restore_request_images(part['bundle'], images_work, request, alias_context=aliases,
                                        original_bundle=original)
        verified.append((raw, subset))
    # Packaging uses the exact verified sent renditions, never freshly rendered images.
    restored = copy.deepcopy(reading)
    source_assets = {a['id']: a for a in reading.get('assets', [])}
    source_blocks = {b['id']: b for b in reading.get('blocks', [])}
    sent_assets = {}
    for _, subset in verified:
        for asset in subset.get('assets', []):
            source_asset = source_assets[asset['id']]
            if source_asset.get('kind') == 'table' and not source_asset.get('image_path'):
                asset = _verified_whole_table(asset, subset, source_asset, source_blocks)
            if asset['id'] in sent_assets:
                _require(sent_assets[asset['id']] == asset, 'conflicting_sent_image')
            sent_assets[asset['id']] = asset
    _require(set(sent_assets) == {a['id'] for a in reading.get('assets', [])}, 'sent_asset_set_mismatch')
    restored['assets'] = [sent_assets[a['id']] for a in reading.get('assets', [])]
    return plan, restored, verified, aliases


def _inputs(evidence, expected_identity):
    return _project_inputs(evidence, _bound_inputs(evidence, expected_identity))


def _typed_reading_aliases(candidate, bundle):
    """Resolve only a typed exact asset already present once in this material."""
    candidate = copy.deepcopy(candidate)
    source = candidate.get('source')
    scope = source.get('reading_scope') if isinstance(source, dict) else None
    prefix = str(bundle.get('file_ref') or 'main') + ':asset:'
    counts = Counter(row['id'] for row in bundle.get('assets', [])
                     if isinstance(row, dict) and isinstance(row.get('id'), str))
    other_ids = {'main'} | {row['id'] for group in ('blocks', 'sections') for row in bundle.get(group, [])
                           if isinstance(row, dict) and isinstance(row.get('id'), str)}
    other_ids.update(value for value in (bundle.get('file_ref'),
                     *(bundle.get('source', {}).get(key) for key in ('source_id', 'file', 'text_file')))
                     if isinstance(value, str))
    changes = []
    if not isinstance(scope, dict):
        return candidate, changes
    for field in ('read_refs', 'unread_refs'):
        original = scope.get(field)
        if not isinstance(original, list):
            continue
        normalized = []
        for item in original:
            if (isinstance(item, str) and item.startswith(prefix)
                    and counts[item[len(prefix):]] == 1 and item[len(prefix):] not in other_ids):
                normalized.append(item[len(prefix):])
            else:
                normalized.append(item)
        if normalized != original:
            changes.append(dict(code='normalized_typed_reading_alias',
                path='/source/reading_scope/' + field, original=copy.deepcopy(original),
                normalized=copy.deepcopy(normalized),
                basis='exact typed asset IDs matched uniquely in the supplied material; declaration remains a model claim'))
            scope[field] = normalized
    return candidate, changes


def _reading_limits(report, bundle, typed_aliases=()):
    """Keep unasserted reading scope visible in both diagnostics and RAG profile."""
    candidate = report['candidate']
    scope = candidate['source']['reading_scope']
    unlocated_unread = any(row.get('code') == 'normalized_unlocated_reading_declaration'
                           and row.get('path') == '/source/reading_scope/unread_refs'
                           and row.get('original')
                           for row in report.get('compatibility_report', []))
    if unlocated_unread:
        limit = '模型声明部分材料未读，但部分位置无法与材料包明确对应；不能据此认定相关材料已读，需回看原始回答和原稿。'
        limits = candidate['profile']['migration_boundary']['global_limits']
        if limit not in limits:
            limits.append(limit)
        report['warnings'].append(dict(code='unlocated_unread_declaration',
            path='/source/reading_scope/unread_refs',
            message='an original unread declaration cannot be located; no reading or location is inferred'))
    reads = set(scope['read_refs'])
    source = bundle['source']
    files = {'main', bundle.get('file_ref')} | {source.get(k) for k in ('source_id', 'file', 'text_file')}
    files.discard(None)
    missing = [block['id'] for block in bundle['blocks']
               if block.get('reading_role', 'primary') == 'primary'
               and not ({block['id'], block.get('section_id')} | files) & reads]
    if missing:
        limit = '部分正文块没有明确的已读声明；未声明部分的阅读情况和具体细节需要回看原文核实。'
        limits = candidate['profile']['migration_boundary']['global_limits']
        if limit not in limits:
            limits.append(limit)
        notes = scope.get('notes') or ''
        if limit not in notes:
            scope['notes'] = notes + ('\n' if notes else '') + limit
        report['warnings'].append(dict(code='unconfirmed_reading_scope', path='/source/reading_scope/read_refs',
            message='some primary blocks have no explicit model read declaration; no reading is inferred'))
    if typed_aliases:
        report['compatibility_report'].extend(copy.deepcopy(typed_aliases))
        report['warnings'].extend(dict(code=row['code'], path=row['path'], message=row['basis'])
                                  for row in typed_aliases)


def _constrain_ambiguous_reading(report, bundle, original_scope, *, catalog):
    """Never turn a typed declaration into an ambiguous untyped read claim."""
    groups = [{row['id'] for row in bundle.get(key, [])
               if isinstance(row, dict) and isinstance(row.get('id'), str)}
              for key in ('blocks', 'sections', 'assets')]
    source = bundle['source']
    files = {'main'} | {value for value in (bundle.get('file_ref'), source.get('source_id'),
                                           source.get('file'), source.get('text_file'))
                         if isinstance(value, str) and value}
    groups.append(files)
    if source.get('catalog_sha256') and isinstance(catalog, dict) and catalog.get('majors'):
        groups.append({'catalog'})
    identifiers = set().union(*groups)
    ambiguous = {value for value in identifiers if sum(value in group for group in groups) > 1}
    scope = report['candidate']['source']['reading_scope']
    before = scope['read_refs']
    removed = [value for value in before if value in ambiguous]
    if not removed:
        return
    scope['read_refs'] = [value for value in before if value not in ambiguous]
    raw = original_scope.get('read_refs', []) if isinstance(original_scope, dict) else []
    raw = [value for value in raw if isinstance(value, str)] if isinstance(raw, list) else []
    note = '模型原读取声明存在同名材料，无法安全转换为无类型已读依据：' + '、'.join(raw)
    notes = scope.get('notes') or ''
    if note not in notes:
        scope['notes'] = notes + ('\n' if notes else '') + note
    report['warnings'].append(dict(code='ambiguous_reading_namespace', path='/source/reading_scope/read_refs',
        message='colliding material IDs are retained only as declarations, not untyped read evidence'))
    report['compatibility_report'].append(dict(code='unconfirmed_ambiguous_reading_declaration',
        path='/source/reading_scope/read_refs', original=copy.deepcopy(raw), normalized=copy.deepcopy(scope['read_refs']),
        basis='normalized IDs shared by different material namespaces cannot establish a specific reading claim'))


def _no_reparse(path):
    """Inspect lexical ancestors before resolve; junctions must not disappear."""
    path = Path(os.path.abspath(path))
    for current in (path, *path.parents):
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        _require(not stat.S_ISLNK(info.st_mode)
                 and not (getattr(info, 'st_file_attributes', 0) & 0x400), 'output_reparse_point')
    return path


def _output_boundary(evidence, output_dir):
    output = _no_reparse(output_dir).resolve()
    # These are exact body/input directories, not shared receipt-store or library roots.
    protected = {Path(evidence[key]['path']).resolve().parent
                 for key in ('source', 'material', 'reading_material')}
    plan_ref = evidence['plan']
    if plan_ref.get('schema') == 'body-frozen-plan-files/1':
        protected.add(Path(plan_ref['work']).resolve())
    else:
        protected.add(Path(plan_ref['path']).resolve().parent)
    for row in evidence['responses']:
        model = Path(row['request']['path']).resolve().parent
        protected.update((model, Path(row['images_work']).resolve()))
        if model.name == 'model':
            protected.add(model.parent)
        elif model.parent.name == 'model':
            protected.add(model.parent.parent)
    receipt = _read(evidence['receipt'], json_value=True)
    _require(isinstance(receipt, dict) and receipt.get('schema') == 'paperflow-receipt/1'
             and isinstance(receipt.get('group_root'), str) and bool(receipt['group_root'])
             and Path(receipt['group_root']).is_absolute()
             and receipt.get('group_id') == evidence['identity']['group_id']
             and receipt.get('paper_id') == evidence['identity']['paper_id'], 'invalid_receipt_paths')
    def add_group(value):
        if isinstance(value.get('group_root'), str) and value['group_root']:
            protected.add(Path(value['group_root']).resolve())
        for item in value.get('items', []):
            for key in ('path', 'source_path'):
                if isinstance(item.get(key), str) and item[key]:
                    protected.add(Path(item[key]).resolve().parent)
    add_group(receipt)
    delivery = receipt.get('delivery_ref')
    if delivery is not None:
        delivery_value = _read(delivery, json_value=True)
        protected.add(Path(delivery['path']).resolve().parent)
        if isinstance(delivery_value, dict):
            add_group(delivery_value)
    handoff = receipt.get('handoff_ref')
    if isinstance(handoff, dict) and handoff.get('path'):
        handoff_value = _read(handoff, json_value=True)
        _require(isinstance(handoff_value, dict), 'invalid_handoff_paths')
        add_group(handoff_value)
    _require(all(not (output == root or output.is_relative_to(root) or root.is_relative_to(output))
                 for root in protected), 'protected_output_overlap')
    # Cached files must also be real descendants, never junctions or symlinks.
    if output.exists():
        pending = [output]
        while pending:
            directory = pending.pop()
            for child in directory.iterdir():
                _no_reparse(child)
                if child.is_dir(): pending.append(child)
    return output


def _derive(plan, bundle, verified, aliases, catalog, *, ranking_raw=None, on_invalid=None):
    """Replay only deterministic parsing/validation/assembly; no writes or calls."""
    from preextract_contract import validate_candidate
    from preextract_partition_run import partition_validation_errors
    from .raw_publication import parse_response
    candidates, repairs, artifacts = [], [], {}
    original_routing = None
    for index, (raw, subset) in enumerate(verified, 1):
        syntax_repairs = []
        candidate = parse_response(raw, repairs=syntax_repairs)
        profile = candidate.get('profile')
        if 'profile' not in candidate and isinstance(candidate.get('source'), dict):
            profile = candidate['source'].get('profile')
        if isinstance(profile, dict):
            routes = profile.get('routing_candidates')
            if isinstance(routes, list):
                # The model's list is already in relevance order. Keep its
                # full original evidence; the existing package accepts at most
                # three delivery targets. Never change the saved raw response.
                if plan['mode'] == 'single':
                    original_routing = copy.deepcopy(routes)
                profile['routing_candidates'] = routes[:3]
        raw_source = candidate.get('source')
        original_scope = copy.deepcopy(raw_source.get('reading_scope')) if isinstance(raw_source, dict) else None
        candidate, typed_aliases = _typed_reading_aliases(candidate, subset)
        report = validate_candidate(candidate, subset, catalog, redaction_policy='best_effort',
            admission_policy='learning', partition_material=bundle if plan['mode'] == 'partitioned' else None,
            alias_context=aliases)
        # The native learning validator owns locator diagnostics and its
        # verified neighboring-chapter context. Do not recheck the smaller
        # frozen subset with a second admission policy.
        admitted = report.get('valid') is True
        if (not admitted and plan['mode'] == 'partitioned' and report.get('errors')
                and not partition_validation_errors(report)):
            # Reuse the native partition boundary. Keep the original invalid
            # part diagnostic; only the assembled whole can satisfy these
            # content/routing requirements. No values or routes are invented.
            admitted = True
            report['partition_admission'] = {
                'status': 'whole_paper_requirements_deferred',
                'deferred_errors': copy.deepcopy(report['errors'])}
        if admitted:
            _constrain_ambiguous_reading(report, subset, original_scope, catalog=catalog)
            _reading_limits(report, subset, typed_aliases)
        artifacts[f'part_{index}_validation'] = report
        if not admitted and on_invalid is not None:
            reference = on_invalid(f'part_{index}_validation', report)
            raise BodyValidationError('candidate_validation_failed', reference)
        _require(admitted, 'candidate_validation_failed')
        candidates.append(report['candidate']); repairs.append(syntax_repairs)
    limitations = []
    if plan['mode'] == 'partitioned':
        # Each response is already bound to this source SHA and frozen plan.
        # Reuse the native merger's retained assertions and limitations; model
        # title/description variants are not a second source-identity gate.
        from preextract_partition import assemble_parts
        candidate, assembly = assemble_parts(candidates, [v[1] for v in verified], bundle)
        assembly['mode'] = 'program_merge'
        limitations.append(MERGE_LIMIT)
        candidate['profile']['migration_boundary']['global_limits'].append(MERGE_LIMIT)
        ranking = parse_response(ranking_raw)
        from preextract_research_type import has_research_type, resolve_research_type
        if has_research_type(candidates) or 'research_type' in ranking:
            candidate['profile']['research_profile']['research_type'] = resolve_research_type(
                candidates, final=ranking.get('research_type'), authoritative='research_type' in ranking)
        routes = ranking.get('routing_candidates')
        _require(isinstance(routes, list), 'whole_paper_ranking_invalid')
        selected = copy.deepcopy(routes[:3])
        # Keep the model's unresolved unit references verbatim. The native
        # learning validator below records their limitations; this adapter
        # must not add a second admission rule or guess a part/unit mapping.
        candidate['profile']['routing_candidates'] = selected
        chosen = {row.get('code') for row in selected if isinstance(row, dict)}
        candidate['profile']['additional_related_specialties'] = [
            row for row in candidate['profile'].get('additional_related_specialties', [])
            if row.get('code') not in chosen]
        assembly['routing_candidates_original'] = copy.deepcopy(routes)
        assembly['whole_paper_ranking'] = True
    else:
        candidate, assembly = candidates[0], {'mode': 'single_saved_response',
                                              'routing_candidates_original': original_routing}
    validation = validate_candidate(candidate, bundle, catalog, redaction_policy='best_effort',
                                     admission_policy='learning', alias_context=aliases)
    if validation.get('valid') is True:
        _constrain_ambiguous_reading(validation, bundle, candidate['source']['reading_scope'], catalog=catalog)
        _reading_limits(validation, bundle)
    if validation.get('valid') is not True and on_invalid is not None:
        reference = on_invalid('validation', validation)
        raise BodyValidationError('assembled_candidate_validation_failed', reference)
    _require(validation.get('valid') is True, 'assembled_candidate_validation_failed')
    artifacts.update(validation=validation, candidate=validation['candidate'], bundle=bundle, assembly=assembly)
    return artifacts, repairs, limitations


def _artifact_path(output, key):
    names = {'validation': 'validation.json', 'candidate': 'candidate.json',
             'bundle': 'validated-bundle.json', 'assembly': 'assembly.json'}
    if key not in names:
        match = re.fullmatch(r'part_(\d+)_validation', key)
        _require(match is not None, 'unexpected_private_artifact')
        return output / 'private' / f'part-{int(match.group(1)):03d}-validation.json'
    return output / 'private' / names[key]


def _package_report(result, packet, artifacts):
    """Check report facts against sealed files and freshly derived source data."""
    report = result.get('validation')
    _require(isinstance(report, dict), 'invalid_publication_report')
    candidate = artifacts['candidate']
    source = artifacts['bundle']['source']
    _require(all(result['task'].get(key) == source.get(key) for key in ('source_id', 'source_sha256')),
             'publication_source_identity_changed')
    from preextract_publish import _text_edits, _frontmatter_edits
    blocks = copy.deepcopy(artifacts['bundle']['blocks'])
    _, offsets, _ = _text_edits(copy.deepcopy(candidate), blocks, artifacts['bundle'].get('metadata', []),
                                plan=artifacts['validation']['executable_redactions'], best_effort=True)
    _frontmatter_edits(blocks, artifacts['bundle']['blocks'], offsets)
    image_blocks = {}
    if not any(block['text'].strip() for block in blocks):
        image_blocks = {page['block_id']: page['asset_id']
                        for page in packet['profile']['body_representation']['pages']}
    expected_body = '\n\n'.join(
        f'图像正文 {image_blocks[block["id"]]}：未取得文字层，正文见对应原始页面图像。'
        if block['id'] in image_blocks else block['text'] for block in blocks)
    actual_body = ''.join(row['content'] for row in packet['chunks'] if row['kind'] == 'body')
    _require(actual_body == expected_body, 'public_body_not_derived_from_input')
    _require(report.get('status') == 'publication_ready' and report.get('body_exact_reassembly') is True
             and report.get('manifest_sha256') == packet['manifest_sha256']
             and report.get('learning_units') == len(packet['profile'].get('learning_units', []))
             and report.get('learning_chunks') == sum(row['kind'] == 'learning' for row in packet['chunks'])
             and report.get('model_record_kind') == candidate.get('record_kind')
             and report.get('source_unchanged') is True
             and report.get('admission', {}).get('policy') == 'learning', 'publication_report_changed')


def process_body(evidence, output_dir, *, expected_identity, catalog, paperflow_root, counter):
    """Return a verified body package only; never declare the folder waiting RAG.

    A directory is claimed exclusively once. A failed/unfinished directory stays
    for diagnosis and cannot mint another internal task. Completed reuse checks
    all inputs and the actual sealed package. The coordinator must durably bind
    body-result.json's hash before advancing its existing folder stage.
    """
    evidence = copy.deepcopy(evidence)
    _identity(evidence.get('identity'), expected_identity)
    fingerprint = _digest(dict(evidence=evidence, catalog=catalog, paperflow_root=str(Path(paperflow_root).resolve())))
    bound = _bound_inputs(evidence, expected_identity)
    projection_error = None
    try:
        plan, bundle, verified, aliases = _project_inputs(evidence, bound)
    except ValueError as exc:
        # The trusted source and response bytes are already bound. Coverage,
        # image aliases and locator projection do not decide storage admission.
        projection_error = exc
        plan, _, bundle, verified = bound
        aliases = None
    ranking_raw = None
    if plan['mode'] == 'partitioned':
        ranking = evidence.get('whole_paper_ranking')
        if (not isinstance(ranking, dict)
                or ranking.get('coverage') != [part['id'] for part in plan['parts']]):
                projection_error = projection_error or ValueError('whole_paper_ranking_missing')
        else:
            ranking_raw = _read(ranking['response']).decode('utf8')
    output = _output_boundary(evidence, output_dir)
    from paperflow.package import validate_package
    from .raw_publication import raw_publication
    result_path = output / 'body-result.json'
    if output.exists():
        _require(result_path.is_file(), 'unregistered_output_preserved')
        result = json.loads(result_path.read_text(encoding='utf8'))
        _require(result.get('schema') == 'body-postprocess-result/1'
                 and result.get('status') == 'body_package_verified', 'invalid_completed_result')
        _require(result.get('input_fingerprint') == fingerprint, 'registered_input_changed')
        _identity(result.get('identity'), expected_identity)
        identity_record = json.loads((output / 'input-identity.json').read_text(encoding='utf8'))
        _require(identity_record == {'identity': expected_identity, 'input_fingerprint': fingerprint}, 'output_identity_changed')
        if result.get('admission_policy') == 'raw_retention':
            return raw_publication(evidence, output, identity=expected_identity, fingerprint=fingerprint,
                                   bundle=bundle, catalog=catalog, counter=counter, reuse=True)
        if projection_error is not None:
            raise projection_error
        artifacts, repairs, limitations = _derive(plan, bundle, verified, aliases, catalog, ranking_raw=ranking_raw)
        references = result.get('private_artifacts')
        _require(isinstance(references, dict) and set(references) == set(artifacts), 'private_artifact_set_changed')
        for key, value in artifacts.items():
            reference = references[key]
            _require(isinstance(reference, dict) and set(reference) == {'path', 'sha256'}
                     and reference['path'] == str(_artifact_path(output, key)), 'private_artifact_path_changed')
            # Recreate the expected bytes from current original replies, not from
            # the untrusted result's replacement hash or replacement candidate.
            raw = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).replace('\n', os.linesep).encode('utf8')
            _require(reference['sha256'] == hashlib.sha256(raw).hexdigest()
                     and _read(reference) == raw, 'private_artifact_not_derived_from_input')
        _require(result.get('assembly') == artifacts['assembly'] and result.get('syntax_repairs') == repairs
                 and result.get('limitations') == limitations
                 and result.get('whole_paper_semantic_synthesis') is False
                 and result.get('model_requests_added') == 0 and result.get('folder_stage_advanced') is False
                 and result.get('original_responses_retained') is True
                 and result.get('redaction_policy') == 'best_effort' and result.get('admission_policy') == 'learning',
                 'completed_claims_changed')
        _require(Path(result['package_path']).resolve() == output / 'publication', 'registered_output_changed')
        packet = validate_package(result['package_path'], result['task'], catalog, strict_seal=True)
        _require(packet['manifest_sha256'] == result['manifest_sha256'], 'sealed_package_changed')
        _package_report(result, packet, artifacts)
        return result
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise ValueError('output_claimed_by_another_worker') from exc
    _save(output / 'input-identity.json', {'identity': expected_identity, 'input_fingerprint': fingerprint})
    try:
        if projection_error is not None:
            raise projection_error
        artifacts, repairs, limitations = _derive(plan, bundle, verified, aliases, catalog, ranking_raw=ranking_raw,
            on_invalid=lambda key, report: _save(_artifact_path(output, key), report))
        candidate, validation, assembly = (artifacts[k] for k in ('candidate', 'validation', 'assembly'))
        private = {key: _save(_artifact_path(output, key), value) for key, value in artifacts.items()}
        from preextract_publish import prepare_publication
        prepared = prepare_publication(candidate, bundle, output / 'publication', paperflow_root=paperflow_root,
            catalog=catalog, counter=counter, redaction_policy='best_effort',
            candidate_validation=validation, admission_policy='learning')
        packet = validate_package(prepared['package_path'], prepared['task'], catalog, strict_seal=True)
        _require(packet['manifest_sha256'] == prepared['validation']['manifest_sha256']
                 and prepared['validation']['body_exact_reassembly'] is True, 'final_package_invalid')
        result = dict(schema='body-postprocess-result/1', status='body_package_verified',
            identity=copy.deepcopy(expected_identity), input_fingerprint=fingerprint,
            package_path=prepared['package_path'], task=prepared['task'], manifest_sha256=packet['manifest_sha256'],
            validation=prepared['validation'], private_artifacts=private, assembly=assembly,
            limitations=limitations, whole_paper_semantic_synthesis=False, model_requests_added=0,
            syntax_repairs=repairs, redaction_policy='best_effort', admission_policy='learning',
            folder_stage_advanced=False, original_responses_retained=True)
        _package_report(result, packet, artifacts)
        _save(result_path, result)
        return result
    except BaseException as exc:
        _save(output / 'diagnostic.json', dict(status='blocked', identity=expected_identity,
            input_fingerprint=fingerprint, error_type=type(exc).__name__, reason=str(exc),
            model_requests_added=0, original_responses_retained=True, new_identity_on_retry_forbidden=True))
        raise
