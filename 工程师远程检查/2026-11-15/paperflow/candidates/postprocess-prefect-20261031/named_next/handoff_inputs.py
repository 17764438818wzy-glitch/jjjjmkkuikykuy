"""Read a completed upstream folder into body/attachment adapter inputs.

No work is dispatched and no upstream file or queue row is written. Callers
must first obtain ``verified`` from the actual upstream FlowRun binding and
obtain admissions from a consistent control-database snapshot. The injected
functions are the selected upstream's existing completion, frozen-plan and
request-observation routines, plus its actual attachment work-path resolver.
"""
import copy
import hashlib
import json
from pathlib import Path
import re


IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
            'parent_run_id', 'task_run_id')


def _require(condition, detail):
    if not condition:
        raise ValueError(detail)


def _json_hash(value, *, sort=False):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=sort, separators=(',', ':'),
                     allow_nan=False).encode('utf8')
    return hashlib.sha256(raw).hexdigest()


def _ref(path, expected=None):
    path = Path(path)
    _require(path.is_absolute(), 'evidence path is not absolute')
    try:
        path = path.resolve(strict=True)
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    except OSError as exc:
        raise ValueError('required evidence is missing') from exc
    if expected is not None:
        _require(isinstance(expected, str) and re.fullmatch('[a-f0-9]{64}', expected)
                 and expected == digest, 'evidence bytes changed')
    return {'path': str(path), 'sha256': digest}


def _saved_response(row, observe_request):
    model = Path(row['model_dir']).resolve(strict=True)
    refs = {name: _ref(model / filename) for name, filename in (
        ('request', 'request.json'), ('state', 'request-state.json'),
        ('events', 'response-events.jsonl'), ('response', 'response.txt'))}
    try:
        request = json.loads(Path(refs['request']['path']).read_bytes())
        state = json.loads(Path(refs['state']['path']).read_bytes())
        phase, step, proof = observe_request(model)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError('saved request evidence is not readable') from exc
    _require(isinstance(request, dict) and isinstance(state, dict) and isinstance(proof, dict)
             and row['request_sha256'] == _json_hash(request)
             and row['response_sha256'] == refs['response']['sha256']
             and state.get('request_sha256') == row['request_sha256']
             and state.get('response_sha256') == row['response_sha256']
             and state.get('status') == 'response_complete' and state.get('finish_reason') == 'stop'
             and phase == 'response_complete' and step == 'postprocess'
             and proof.get('request_sha256') == row['request_sha256']
             and proof.get('response_sha256') == row['response_sha256'],
             'saved response does not match its admitted request')
    return refs


def build_inputs(*, identity, verified, admissions, manifest_sha256, expected_components,
                 inspect_folder, observe_request, frozen_plan, attachment_work):
    """Create adapter inputs only after every expected component is verified."""
    _require(isinstance(identity, dict) and set(identity) == set(IDENTITY)
             and all(isinstance(identity[k], str) and identity[k] for k in IDENTITY),
             'folder identity is incomplete')
    _require(isinstance(verified, dict) and isinstance(verified.get('manifest'), dict)
             and isinstance(verified.get('paper'), dict) and isinstance(admissions, list),
             'verified upstream handoff is unavailable')
    manifest, paper = verified['manifest'], verified['paper']
    _require(_json_hash(manifest, sort=True) == manifest_sha256, 'frozen manifest identity changed')
    _require(all(manifest.get(k) == identity[k] for k in IDENTITY[:5])
             and paper.get('paper_id') == identity['paper_id']
             and paper.get('body') == manifest.get('body')
             and paper.get('attachments') == manifest.get('attachments'), 'handoff identity changed')
    for reference in (manifest['receipt'], manifest['material'], manifest['reading_material'],
                      manifest['body'], *manifest['attachments']):
        _ref(reference['path'], reference['sha256'])

    components = expected_components(manifest, identity['task_run_id'])
    components_by_id = {c['component_id']: c for c in components}
    papers_by_id = {row['file_id']: row for row in paper['attachments']}
    _require(len(papers_by_id) == len(paper['attachments']), 'duplicate attachment identity')
    body_work = Path(manifest['body_work']).resolve(strict=True)
    body_mode = manifest['capacity']['plan_identity']['mode']
    verified_responses, seen_models = {}, set()

    def verify_row(row):
        component = components_by_id[row['component_id']]
        if component['kind'] == 'body':
            work = body_work
            model = work / 'model'
            if body_mode == 'partitioned':
                model = model / component['part_id']
        else:
            work = Path(attachment_work(paper, papers_by_id[component['file_id']])).resolve(strict=True)
            model = work / 'model'
        _require(Path(row['work_dir']).resolve(strict=True) == work
                 and Path(row['model_dir']).resolve(strict=True) == model.resolve(strict=True),
                 'admitted work directory does not belong to this component')
        _require(model not in seen_models, 'multiple components share a model history')
        seen_models.add(model)
        verified_responses[component['component_id']] = _saved_response(row, observe_request)

    status = inspect_folder(manifest, identity['task_run_id'], admissions, verify_row)
    _require(isinstance(status, dict) and status.get('status') == 'raw_complete'
             and len(verified_responses) == len(components), 'folder LLM responses are incomplete')
    responses, attachments = [], []
    for component in components:
        refs = verified_responses[component['component_id']]
        row = next(r for r in admissions if r['component_id'] == component['component_id'])
        if component['kind'] == 'body':
            model = Path(row['model_dir']).resolve(strict=True)
            checkpoint = model / 'private' / 'request-images.json'
            responses.append({'identity': copy.deepcopy(identity), 'part_id': component['part_id'],
                **refs, 'images_work': str(model), 'request_images': _ref(checkpoint) if checkpoint.exists() else None})
        else:
            work = Path(row['work_dir']).resolve(strict=True)
            attachments.append({'schema': 'attachment-postprocess-input/1', 'ticket': copy.deepcopy(identity),
                'receipt_ref': copy.deepcopy(manifest['receipt']), 'attachment_id': component['file_id'],
                'work_dir': str(work), 'model_refs': {'evidence': _ref(work / 'evidence.json'),
                    'request': refs['request'], 'request_state': refs['state'], 'response': refs['response'],
                    'response_events': refs['events']}})
    body = {'schema': 'body-postprocess-input/1', 'identity': copy.deepcopy(identity),
            'receipt': copy.deepcopy(manifest['receipt']),
            'source': {'path': manifest['body']['path'], 'sha256': manifest['body']['sha256']},
            'material': copy.deepcopy(manifest['material']), 'reading_material': copy.deepcopy(manifest['reading_material']),
            'plan_identity': copy.deepcopy(manifest['capacity']['plan_identity']),
            'plan': frozen_plan(body_work, manifest['capacity']['plan_identity']), 'responses': responses}
    # Preserve the exact frozen preprocessor proof only when this material was
    # made from its named working copy. The registered delivery remains source.
    material = json.loads(Path(manifest['material']['path']).read_bytes())
    material_source = (material.get('source') or {}).get('file')
    if material_source and Path(material_source).resolve() != Path(manifest['body']['path']).resolve():
        proof = body_work / 'named-input-proof.json'
        frozen_hash = (material.get('file_hashes') or {}).get(str(proof))
        _require(isinstance(frozen_hash, str), 'named proof is not bound by frozen material')
        body['named_source'] = {'work': str(body_work), 'proof': _ref(proof, frozen_hash)}
    return {'identity': copy.deepcopy(identity), 'manifest_sha256': manifest_sha256,
            'body': body, 'attachments': attachments}
