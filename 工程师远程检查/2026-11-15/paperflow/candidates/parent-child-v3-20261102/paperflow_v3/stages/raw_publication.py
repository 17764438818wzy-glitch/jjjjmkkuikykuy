"""Retain full replies and their original profiles without inventing a merge."""
import copy
import hashlib
import json
from pathlib import Path

FORMAT_STATUS = 'unstructured_retained'


class UnstructuredResponse(ValueError):
    """A saved reply has no reliable structured projection."""


def parse_response(raw, *, repairs=None):
    from preextract_contract import parse_saved_candidate
    try:
        return parse_saved_candidate(raw, repairs=repairs)
    except (ValueError, TypeError) as exc:
        raise UnstructuredResponse(str(exc)) from exc


def retention_routes(packet):
    """Use a storage namespace, never a claimed academic specialty."""
    if packet['profile'].get('format_status') != FORMAT_STATUS:
        return packet
    if packet['routes']:
        raise ValueError('unstructured retention must not claim a specialty')
    from paperflow_qdrant.identity import stable_id
    route = {'code': '__unstructured_retention__', 'name': '未整理原文',
        'purpose': 'unstructured_retention',
        'paper_dataset_id': stable_id('paperflow-unstructured-retention', 'body'),
        'image_dataset_id': stable_id('paperflow-unstructured-retention', 'images')}
    return {**packet, 'routes': [route], 'pending_routes': []}


def _contents(evidence):
    rows = [('original_source', 'original/source' + Path(evidence['source']['path']).suffix, evidence['source'])]
    rows.extend(('llm_response', f'model-responses/part-{index:03d}.txt', row['response'])
                for index, row in enumerate(evidence['responses'], 1))
    if evidence.get('whole_paper_ranking'):
        rows.append(('llm_ranking', 'model-responses/whole-paper-ranking.txt', evidence['whole_paper_ranking']['response']))
    rows.extend(('llm_attachment', f'model-responses/attachment-{index:03d}.txt', reference)
                for index, reference in enumerate(evidence.get('retained_attachment_responses', []), 1))
    return rows


def _article(bundle):
    text = '\n\n'.join(row['text'] for row in bundle['blocks'])
    if text.strip():
        return text
    # This is a pointer to real pages, not fabricated OCR.
    images = [row['id'] for row in bundle.get('assets', []) if row.get('image_path')]
    if not images:
        raise ValueError('original body has neither text nor verified page images')
    return '\n\n'.join('原始正文页：' + ident + '（无文字层，内容见原始图像）' for ident in images)


def _profiles(evidence):
    """Keep original profile values per saved response, without inventing a merge."""
    from .body_adapter import _read
    result = []
    for index, row in enumerate(evidence['responses'], 1):
        try:
            candidate = parse_response(_read(row['response']).decode('utf8'))
        except UnstructuredResponse:
            continue
        candidates = [(candidate.get('profile'), '/profile', '')]
        if isinstance(candidate.get('source'), dict):
            candidates.append((candidate['source'].get('profile'), '/source/profile', '-source'))
        for profile, location, suffix in candidates:
            if not isinstance(profile, dict) or not profile:
                continue
            result.append({'part_id': row['part_id'], 'file': f'model-profiles/part-{index:03d}{suffix}.json',
                'response_sha256': row['response']['sha256'], 'profile_location': location,
                'profile': copy.deepcopy(profile)})
    return result


def raw_publication(evidence, output, *, identity, fingerprint, bundle, catalog, counter, reuse=False):
    from .body_adapter import _read, _save, _require, _held_images
    from paperflow.package import inside, seal_package, validate_package
    from paperflow.rag import RagPublisher
    from preextract_publish import _Budget

    output = Path(output)
    root = output / 'publication'
    expected = []
    raw_files = {}
    for role, name, reference in _contents(evidence):
        raw = _read(reference)
        expected.append({'role': role, 'file': name, 'sha256': reference['sha256'], 'bytes': len(raw)})
        raw_files[name] = raw
    profiles = _profiles(evidence)
    # The adaptation report belongs to the frozen reading material. The
    # publication bundle may already contain validated resized/tiled images.
    held = _held_images(evidence, _read(evidence['reading_material'], json_value=True))
    unavailable = [{'id': ident, 'source_sha256': sha} for ident, sha in held.items()]
    for row in profiles:
        raw = json.dumps(row['profile'], ensure_ascii=False, indent=2, allow_nan=False).encode('utf8')
        expected.append({'role': 'llm_profile', 'file': row['file'],
                         'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)})
        raw_files[row['file']] = raw
    article = _article(bundle)
    if reuse:
        result = json.loads((output / 'body-result.json').read_bytes())
        _require(result.get('identity') == identity and result.get('input_fingerprint') == fingerprint
                 and result.get('admission_policy') == 'raw_retention'
                 and Path(result['package_path']).resolve() == root.resolve(), 'raw_retention_identity_changed')
        packet = validate_package(root, result['task'], catalog, strict_seal=True)
        _require(result.get('manifest_sha256') == packet['manifest_sha256']
                 and result.get('model_requests_added') == 0
                 and result.get('original_responses_retained') is True, 'raw_retention_result_changed')
        _require(packet['profile'].get('format_status') == FORMAT_STATUS
                 and packet['profile'].get('retained_content') == expected
                 and packet['profile'].get('learning_units') == []
                 and packet['profile'].get('routing_candidates') == []
                 and packet['tree']['nodes'] == [], 'raw_retention_fields_changed')
        _require(packet['profile'].get('retained_profiles', []) == profiles,
                 'retained_profile_projection_changed')
        _require(packet['profile'].get('unavailable_images', []) == unavailable, 'held_image_projection_changed')
        for name, raw in raw_files.items():
            _require(inside(root, name).read_bytes() == raw, 'retained_source_changed')
        _require(''.join(row['content'] for row in packet['chunks'] if row['kind'] == 'body') == article,
                 'raw_body_changed')
        for item in expected:
            if item['role'].startswith('llm_'):
                text = raw_files[item['file']].decode('utf8')
                if text.strip():
                    _require(''.join(row['content'] for row in packet['chunks']
                        if row.get('retained_file') == item['file']) == text, 'raw_answer_projection_changed')
        return result

    root.mkdir()
    for name, raw in raw_files.items():
        path = inside(root, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as stream:
            stream.write(raw)
    source = bundle['source']
    task = {'task_id': identity['task_run_id'], 'source_id': source['source_id'],
            'source_sha256': source['source_sha256'], 'revision': 1}
    ident = {key: task[key] for key in ('task_id', 'source_id', 'source_sha256')}
    profile = {**ident, 'format_status': FORMAT_STATUS, 'structure_applicable': False,
        'structure_parse_status': 'not_available', 'article': {}, 'learning_units': [],
        'routing_candidates': [], 'retained_content': expected,
        'retention_policy': 'original-and-full-answer/1'}
    if profiles:
        profile.update(profile_status='retained', retained_profiles=profiles)
    if unavailable:
        profile['unavailable_images'] = unavailable
    tree = {**ident, 'nodes': []}
    assets = []
    from preextract_images import _verified_image
    for index, row in enumerate(bundle.get('assets', []), 1):
        if not row.get('image_path'):
            continue
        if row['id'] in held:
            continue
        path = _verified_image(row)
        raw = path.read_bytes()
        name = 'images/' + hashlib.sha256(raw).hexdigest() + path.suffix.lower()
        target = inside(root, name)
        if not target.exists():
            target.parent.mkdir(exist_ok=True)
            target.write_bytes(raw)
        assets.append({'id': row['id'], 'kind': row.get('kind', 'image'), 'storage': 'image',
            'file': name, 'sha256': hashlib.sha256(raw).hexdigest(), 'section_id': None,
            'source_location': {'asset_id': row['id']}})

    budget = _Budget(counter)
    provisional = {'manifest_sha256': 'f' * 64, 'profile': profile, 'tree': tree, 'assets': assets}
    def render(rid, text):
        return RagPublisher.render(task, provisional, (rid, text, None)) + ' reserve' * 64
    chunks = []
    def append_chunks(text, kind, prefix, retained_file=None):
        if not text.strip():
            return
        parts = budget.split(text, lambda index, part: render(prefix + f'{index:06d}', part))
        for index, part in enumerate(parts):
            row = {'id': prefix + f'{index:06d}', 'kind': kind, 'content': part,
                   'section_ids': [], 'asset_ids': []}
            if retained_file:
                row['retained_file'] = retained_file
            chunks.append(row)
        _require(''.join(parts) == text, 'retention_split_changed_content')
    append_chunks(article, 'body', 'body_')
    for index, item in enumerate(expected):
        if item['role'].startswith('llm_'):
            append_chunks(raw_files[item['file']].decode('utf8'), 'llm_raw', f'raw_{index:03d}_', item['file'])
    (root / 'article.redacted.md').write_text(article, encoding='utf8', newline='')
    for name, value in [('profile.json', profile), ('tree.json', tree),
                        ('assets.json', {**ident, 'items': assets}), ('chunks.json', {**ident, 'chunks': chunks})]:
        _save(root / name, value)
    seal_package(root, task, 1)
    packet = validate_package(root, task, catalog, strict_seal=True)
    checks = []
    for role, records in RagPublisher(None).records(packet).items():
        for record in records:
            text = RagPublisher.render(task, packet, record)
            _require(budget.fits(text), 'retained record exceeds embedding budget')
            checks.append({'record_id': record[0], 'role': role, **budget.count(text)})
    report = {'status': 'publication_ready', 'body_exact_reassembly': True,
        'manifest_sha256': packet['manifest_sha256'], 'learning_units': 0, 'learning_chunks': 0,
        'source_unchanged': True, 'admission': {'policy': 'raw_retention'},
        'embedding': {'admission_verified': True, 'counter_identity': getattr(counter, 'identity', None), 'checks': checks},
        'format_status': FORMAT_STATUS}
    result = {'schema': 'body-postprocess-result/1', 'status': 'body_package_verified',
        'identity': copy.deepcopy(identity), 'input_fingerprint': fingerprint,
        'package_path': str(root), 'task': task, 'manifest_sha256': packet['manifest_sha256'],
        'validation': report, 'private_artifacts': {}, 'assembly': {'mode': 'raw_retention'},
        'limitations': ['结构字段未采用；原论文、全部原回答完整保留。'],
        'whole_paper_semantic_synthesis': False, 'model_requests_added': 0, 'syntax_repairs': [],
        'redaction_policy': 'not_applied_to_retained_originals', 'admission_policy': 'raw_retention',
        'folder_stage_advanced': False, 'original_responses_retained': True}
    _save(output / 'body-result.json', result)
    # Apply the same readback checks as completed reuse before returning success.
    return raw_publication(evidence, output, identity=identity, fingerprint=fingerprint, bundle=bundle,
                           catalog=catalog, counter=counter, reuse=True)
