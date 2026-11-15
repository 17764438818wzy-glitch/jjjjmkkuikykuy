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
    for index, part in enumerate(parts, 1):
        ident = f'part-{index:03d}'
        _require(isinstance(part, dict) and (mode == 'single' or part.get('id') == ident)
                 and isinstance(part.get('bundle'), dict) and isinstance(part.get('body'), dict)
                 and isinstance(part.get('ranges'), list) and isinstance(part.get('asset_ids'), list), 'invalid_plan_part')
        identities.append(dict(id=ident, request_sha256=_digest(part['body'], sort=False),
            material_sha256=_digest(part['bundle']), scope_sha256=_digest({k: part[k] for k in ('ranges', 'asset_ids')})))
        subset = part['bundle']
        _require(subset.get('source') == reading.get('source'), 'part_source_mismatch')
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


def _inputs(evidence, expected_identity):
    _require(isinstance(evidence, dict) and evidence.get('schema') == 'body-postprocess-input/1', 'invalid_body_input')
    _identity(evidence.get('identity'), expected_identity)
    _read(evidence['receipt'])
    _read(evidence['source'])
    original = _read(evidence['material'], json_value=True)
    reading = _read(evidence['reading_material'], json_value=True)
    plan = _load_plan(evidence['plan'], evidence['plan_identity'], original)
    from preextract_images import _verified_image
    for bundle in (original, reading):
        _require(bundle.get('version') == 'preextract-material/1.0'
                 and bundle.get('source', {}).get('source_sha256') == evidence['source']['sha256']
                 and Path(bundle['source']['file']).resolve() == Path(evidence['source']['path']).resolve(), 'source_material_mismatch')
        for field in ('text', 'index'):
            path, sha = bundle['source'].get(field + '_file'), bundle['source'].get(field + '_sha256')
            if path is not None or sha is not None:
                _read({'path': path, 'sha256': sha})
        for asset in bundle.get('assets', []):
            if asset.get('image_path'):
                _verified_image(asset)
    _plan(plan, evidence['plan_identity'], evidence['source']['sha256'], evidence['reading_material']['sha256'], reading)
    rows = evidence.get('responses')
    _require(isinstance(rows, list) and len(rows) == len(plan['parts']), 'missing_or_extra_responses')
    from preextract_scheduler_adapter import request_observation
    from preextract_images import restore_request_images
    from preextract_image_alias import AliasContext
    aliases = AliasContext()
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
    sent_assets = {}
    for _, subset in verified:
        for asset in subset.get('assets', []):
            if asset['id'] in sent_assets:
                _require(sent_assets[asset['id']] == asset, 'conflicting_sent_image')
            sent_assets[asset['id']] = asset
    _require(set(sent_assets) == {a['id'] for a in reading.get('assets', [])}, 'sent_asset_set_mismatch')
    restored['assets'] = [sent_assets[a['id']] for a in reading.get('assets', [])]
    return plan, restored, verified, aliases


def _source_refs(value, bundle):
    """Learning admission may soften locators; this adapter must not invent them."""
    blocks = {b['id']: b for b in bundle['blocks']}
    sections = {s['id'] for s in bundle['sections']}
    assets = {a['id'] for a in bundle.get('assets', [])}
    files = {'main', bundle.get('file_ref')} | {bundle['source'].get(k) for k in ('source_id', 'file', 'text_file')}
    scope = value.get('source', {}).get('reading_scope', {})
    yes, no = scope.get('read_refs'), scope.get('unread_refs')
    _require(isinstance(yes, list) and isinstance(no, list)
             and all(isinstance(v, str) for v in yes + no), 'body_reading_incomplete')
    known = set(blocks) | sections | assets | (files - {None})
    _require(set(yes + no) <= known, 'unknown_reading_ref')
    for block in bundle['blocks']:
        if block.get('reading_role', 'primary') != 'primary':
            continue
        refs = {block['id'], block.get('section_id')} | (files - {None})
        _require(bool(refs & set(yes)) and not (refs & set(no)), 'body_reading_incomplete')
    def walk(node):
        if isinstance(node, list):
            for child in node: walk(child)
        elif isinstance(node, dict):
            refs = node.get('source_refs', [])
            _require(isinstance(refs, list), 'invalid_source_refs')
            for ref in refs:
                _require(isinstance(ref, dict) and ref.get('file_ref') in files - {None}, 'invalid_file_ref')
                bid, sid, aid = (ref.get(k) for k in ('block_id', 'section_id', 'asset_id'))
                _require((bid is None or bid in blocks) and (sid is None or sid in sections)
                         and (aid is None or aid in assets), 'unknown_source_ref')
                _require(not (bid and sid) or blocks[bid].get('section_id') == sid, 'conflicting_source_ref')
                start, end = ref.get('start'), ref.get('end')
                if start is not None or end is not None:
                    _require(bid in blocks and type(start) is int and type(end) is int
                             and 0 <= start <= end <= len(blocks[bid]['text']), 'source_ref_out_of_bounds')
            for child in node.values(): walk(child)
    walk(value)


def _merge_conflicts(candidates):
    def consistent(values):
        values = [v for v in values if v not in (None, '', [], {})]
        _require(not values or all(v == values[0] for v in values), 'cross_part_conflict')
    for field in ('material_type',):
        consistent([c['profile'].get(field) for c in candidates])
    for group, field in [('material_disposition', 'decision'), ('article', 'title')]:
        consistent([c['profile'].get(group, {}).get(field) for c in candidates])
    for group, key in [('tree', 'nodes'), ('assets', 'items')]:
        shared = defaultdict(list)
        for candidate in candidates:
            for row in candidate.get(group, {}).get(key, []):
                shared[row.get('id')].append(row)
        for rows in shared.values():
            for field in ('parent_id', 'order', 'kind', 'storage', 'evidence_basis', 'status', 'decision'):
                consistent([r.get(field) for r in rows])


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


def _derive(plan, bundle, verified, aliases, catalog, *, on_invalid=None):
    """Replay only deterministic parsing/validation/assembly; no writes or calls."""
    from preextract_contract import parse_saved_candidate, validate_candidate
    candidates, repairs, artifacts = [], [], {}
    for index, (raw, subset) in enumerate(verified, 1):
        syntax_repairs = []
        candidate = parse_saved_candidate(raw, repairs=syntax_repairs)
        _source_refs(candidate, subset)
        report = validate_candidate(candidate, subset, catalog, redaction_policy='best_effort',
            admission_policy='learning', partition_material=bundle if plan['mode'] == 'partitioned' else None,
            alias_context=aliases)
        artifacts[f'part_{index}_validation'] = report
        if report.get('valid') is not True and on_invalid is not None:
            reference = on_invalid(f'part_{index}_validation', report)
            raise BodyValidationError('candidate_validation_failed', reference)
        _require(report.get('valid') is True, 'candidate_validation_failed')
        candidates.append(report['candidate']); repairs.append(syntax_repairs)
    limitations = []
    if plan['mode'] == 'partitioned':
        _merge_conflicts(candidates)
        from preextract_partition import assemble_parts
        candidate, assembly = assemble_parts(candidates, [v[1] for v in verified], bundle)
        assembly['mode'] = 'program_merge'
        limitations.append(MERGE_LIMIT)
        candidate['profile']['migration_boundary']['global_limits'].append(MERGE_LIMIT)
    else:
        candidate, assembly = candidates[0], {'mode': 'single_saved_response'}
    _source_refs(candidate, bundle)
    validation = validate_candidate(candidate, bundle, catalog, redaction_policy='best_effort',
                                     admission_policy='learning', alias_context=aliases)
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
    expected_body = '\n\n'.join(block['text'] for block in blocks)
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
    plan, bundle, verified, aliases = _inputs(evidence, expected_identity)
    output = _output_boundary(evidence, output_dir)
    from paperflow.package import validate_package
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
        artifacts, repairs, limitations = _derive(plan, bundle, verified, aliases, catalog)
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
        artifacts, repairs, limitations = _derive(plan, bundle, verified, aliases, catalog,
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
        # Recheck all upstream bytes and image renditions after packaging.
        _inputs(evidence, expected_identity)
        result = dict(schema='body-postprocess-result/1', status='body_package_verified',
            identity=copy.deepcopy(expected_identity), input_fingerprint=fingerprint,
            package_path=prepared['package_path'], task=prepared['task'], manifest_sha256=packet['manifest_sha256'],
            validation=prepared['validation'], private_artifacts=private, assembly=assembly,
            limitations=limitations, whole_paper_semantic_synthesis=False, model_requests_added=0,
            syntax_repairs=repairs, redaction_policy='best_effort', admission_policy='learning',
            folder_stage_advanced=False, original_responses_retained=True)
        _save(result_path, result)
        return result
    except BaseException as exc:
        _save(output / 'diagnostic.json', dict(status='blocked', identity=expected_identity,
            input_fingerprint=fingerprint, error_type=type(exc).__name__, reason=str(exc),
            model_requests_added=0, original_responses_retained=True, new_identity_on_retry_forbidden=True))
        raise
