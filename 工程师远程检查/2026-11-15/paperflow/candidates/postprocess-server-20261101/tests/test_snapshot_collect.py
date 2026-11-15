"""Only synthetic, retained inputs; no service calls or real material."""
import copy
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
from functools import partial
from unittest.mock import patch

import pytest

HERE = Path(__file__).resolve().parents[1]
UPSTREAM = HERE.parent / 'postprocess-prefect-20261031'
sys.path[:0] = [str(HERE), str(UPSTREAM / 'tests'), str(UPSTREAM)]
from test_synthetic_mainline_integration import synthetic_folder, compact
from handoff_inputs import build_inputs
from folder_completion import expected_components, inspect_folder
from preextract_scheduler_adapter import request_observation
from body_adapter import frozen_plan_evidence


def collect(inputs, root):
    module = importlib.import_module('snapshot_collect')
    return module.collect_inputs(inputs, allowed_roots=[root])


def fixture(root, parts=1, *, rich=False, delivered=False):
    from test_attachment_adapter import fixture as attachment_fixture
    with patch('test_synthetic_mainline_integration.attachment_fixture',
               partial(attachment_fixture, rich=rich, delivered=delivered)):
        identity, manifest, paper, admissions, _, work = synthetic_folder(root, parts)
    return build_inputs(identity=identity, verified={'manifest': manifest, 'paper': paper},
        admissions=admissions, manifest_sha256=compact(manifest, sort=True),
        expected_components=expected_components, inspect_folder=inspect_folder,
        observe_request=request_observation, frozen_plan=frozen_plan_evidence,
        attachment_work=lambda *a: work)


def rewrite_ref(ref, mutate):
    path = Path(ref['path']); value = json.loads(path.read_bytes()); mutate(value)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')
    ref['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()


def bind_catalog(inputs, path, value=None):
    """Synthetic producer metadata; the trusted path is supplied separately."""
    if value is not None:
        path.write_text(json.dumps(value), encoding='utf8')
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    def pin(material):
        material['source']['catalog_sha256'] = sha
        material.setdefault('file_hashes', {})[str(path)] = sha
    rewrite_ref(inputs['body']['material'], pin)
    return sha


def test_external_catalog_is_one_exact_file_and_inputs_are_preserved(tmp_path):
    inside = tmp_path / 'inside'
    inputs = fixture(inside)
    catalog = tmp_path / 'catalog.json'
    bind_catalog(inputs, catalog, {'synthetic': 1})
    original = copy.deepcopy(inputs)
    module = importlib.import_module('snapshot_collect')
    with pytest.raises(ValueError, match='outside'):
        module.collect_inputs(inputs, allowed_roots=[inside])
    result = module.collect_inputs(inputs, allowed_roots=[inside],
        trusted_catalog_path=catalog, defer_binary_hash=True)
    assert result['inputs'] == original == inputs
    assert result['catalog_evidence'] == dict(path=str(catalog),
        sha256=hashlib.sha256(catalog.read_bytes()).hexdigest(), value={'synthetic': 1})
    assert str(catalog) in {entry['path'] for entry in result['files']}


@pytest.mark.parametrize('change', ['missing_material_sha', 'material_sha', 'entry_sha',
    'missing_entry', 'bytes', 'configured_path', 'input_self_authorization', 'sibling'])
def test_catalog_authorization_rejects_unproven_or_wider_access(tmp_path, change):
    inside = tmp_path / 'inside'
    inputs = fixture(inside)
    catalog = tmp_path / 'catalog.json'
    bind_catalog(inputs, catalog, {'synthetic': 1})
    configured = catalog
    if change == 'bytes': catalog.write_bytes(catalog.read_bytes() + b' ')
    elif change == 'configured_path':
        configured = tmp_path / 'another-catalog.json'
        configured.write_bytes(catalog.read_bytes())
    elif change == 'input_self_authorization':
        inputs['trusted_catalog_path'] = str(catalog)
        configured = None
    elif change == 'sibling':
        sibling = tmp_path / 'unapproved.json'
        sibling.write_bytes(b'{"private":true}')
        rewrite_ref(inputs['body']['material'], lambda v: v['file_hashes'].update({
            str(sibling): hashlib.sha256(sibling.read_bytes()).hexdigest()}))
    else:
        def damage(v):
            if change == 'missing_material_sha': v['source'].pop('catalog_sha256')
            elif change == 'material_sha': v['source']['catalog_sha256'] = '0' * 64
            elif change == 'entry_sha': v['file_hashes'][str(catalog)] = '0' * 64
            else: v['file_hashes'].pop(str(catalog))
        rewrite_ref(inputs['body']['material'], damage)
    module = importlib.import_module('snapshot_collect')
    with pytest.raises(ValueError):
        module.collect_inputs(inputs, allowed_roots=[inside],
            trusted_catalog_path=configured, defer_binary_hash=True)


def test_exact_catalog_permission_does_not_authorize_a_source_reference(tmp_path):
    inside = tmp_path / 'inside'
    inputs = fixture(inside)
    catalog = tmp_path / 'catalog.json'
    sha = bind_catalog(inputs, catalog, {'synthetic': 1})
    inputs['body']['source'] = dict(path=str(catalog), sha256=sha)
    module = importlib.import_module('snapshot_collect')
    with pytest.raises(ValueError, match='outside'):
        module.collect_inputs(inputs, allowed_roots=[inside], trusted_catalog_path=catalog)


@pytest.mark.parametrize('kind', ['hardlink', 'symlink'])
def test_exact_catalog_exception_keeps_link_rejection(tmp_path, kind):
    inside = tmp_path / 'inside'
    inputs = fixture(inside)
    catalog = tmp_path / 'catalog.json'
    bind_catalog(inputs, catalog, {'synthetic': 1})
    alias = tmp_path / 'alias.json'
    if kind == 'hardlink':
        os.link(catalog, alias)
    else:
        catalog.rename(alias)
        try: catalog.symlink_to(alias)
        except OSError: pytest.skip('symlink unavailable')
    module = importlib.import_module('snapshot_collect')
    with pytest.raises(ValueError, match='link|reparse'):
        module.collect_inputs(inputs, allowed_roots=[inside], trusted_catalog_path=catalog)


def test_catalog_is_hashed_again_at_final_recheck_even_with_binary_hash_deferred(tmp_path):
    inside = tmp_path / 'inside'
    inputs = fixture(inside)
    catalog = tmp_path / 'catalog.json'
    bind_catalog(inputs, catalog, {'synthetic': 1})
    module = importlib.import_module('snapshot_collect')
    response = module._Collector.response
    def change_after_read(self, *args, **kwargs):
        value = response(self, *args, **kwargs)
        catalog.write_bytes(catalog.read_bytes() + b' ')
        return value
    with patch.object(module._Collector, 'response', change_after_read):
        with pytest.raises(ValueError, match='changed'):
            module.collect_inputs(inputs, allowed_roots=[inside],
                trusted_catalog_path=catalog, defer_binary_hash=True)


@pytest.mark.parametrize('parts', [1, 2])
def test_complete_closure_preserves_inputs_identity_and_every_file_byte(tmp_path, parts):
    inputs = fixture(tmp_path, parts)
    original = copy.deepcopy(inputs)
    before = {str(p): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    result = collect(inputs, tmp_path)
    assert result['status'] == 'complete'
    assert result['identity'] == inputs['identity']
    assert result['manifest_sha256'] == inputs['manifest_sha256']
    assert result['inputs'] == inputs == original
    found = {r['path']: r for r in result['files']}
    assert len(found) == len(result['files'])
    for row in inputs['body']['responses']:
        for key in ('request', 'state', 'events', 'response'):
            assert row[key]['path'] in found
    for key in ('material', 'reading_material', 'source', 'receipt'):
        assert inputs['body'][key]['path'] in found
    assert all(before[p] == Path(p).read_bytes() for p in before)
    assert all(r['sha256'] == hashlib.sha256(before[r['path']]).hexdigest()
               and r['bytes'] == len(before[r['path']]) for r in found.values())


@pytest.mark.parametrize('change', ['source', 'response', 'missing'])
def test_tampered_or_missing_evidence_fails_closed(tmp_path, change):
    inputs = fixture(tmp_path)
    ref = inputs['body']['source'] if change == 'source' else inputs['body']['responses'][0]['response']
    path = Path(ref['path'])
    if change == 'missing': path.unlink()
    else: path.write_bytes(b'tampered')
    with pytest.raises(ValueError): collect(inputs, tmp_path)


def test_binary_hash_can_be_deferred_to_the_server_without_claiming_local_verification(tmp_path):
    inputs = fixture(tmp_path)
    source = Path(inputs['body']['source']['path'])
    old = source.read_bytes()
    assert old
    source.write_bytes(bytes([old[0] ^ 1]) + old[1:])
    module = importlib.import_module('snapshot_collect')
    result = module.collect_inputs(inputs, allowed_roots=[tmp_path], defer_binary_hash=True)
    assert result['evidence_scope'] == 'supported_dependency_graph_binary_hash_deferred'
    entry = next(row for row in result['files'] if row['path'] == str(source))
    assert entry['sha256'] == inputs['body']['source']['sha256']
    with pytest.raises(ValueError):
        module.collect_inputs(inputs, allowed_roots=[tmp_path])


def test_metadata_budget_rejects_oversize_before_parsing(tmp_path):
    inputs = fixture(tmp_path)
    module = importlib.import_module('snapshot_collect')
    with pytest.raises(ValueError, match='metadata.*budget'):
        module.collect_inputs(inputs, allowed_roots=[tmp_path], defer_binary_hash=True,
                              max_json_file_bytes=1, max_total_json_bytes=100)


def test_unrecognized_material_version_fails_closed(tmp_path):
    inputs = fixture(tmp_path)
    rewrite_ref(inputs['body']['material'], lambda v: v.update(version='future-material/2'))
    with pytest.raises(ValueError, match='unsupported'): collect(inputs, tmp_path)


def test_nested_request_text_material_reference_is_required(tmp_path):
    inputs = fixture(tmp_path)
    outside = tmp_path / 'never-present.txt'
    def mutate(v):
        text = json.loads(v['messages'][1]['content'][0]['text'])
        text['material']['source'].update(text_file=str(outside), text_sha256='0' * 64)
        v['messages'][1]['content'][0]['text'] = json.dumps(text)
    rewrite_ref(inputs['body']['responses'][0]['request'], mutate)
    with pytest.raises(ValueError): collect(inputs, tmp_path)


def test_actual_material_view_hash_only_source_is_request_evidence_not_a_file_ref(tmp_path):
    from preextract_request import material_view
    module = importlib.import_module('snapshot_collect')
    bundle = dict(source=dict(file=str(tmp_path / 'not-opened.pdf'), source_sha256='1' * 64,
        text_file=str(tmp_path / 'not-opened.txt'), text_sha256='2' * 64,
        index_file=str(tmp_path / 'not-opened.json'), index_sha256='3' * 64),
        blocks=[], sections=[], assets=[], issues=[], attachments=[], metadata=[])
    view = material_view(bundle)
    assert all(key not in view['source'] for key in ('file', 'text_file', 'index_file'))
    assert view['source']['text_sha256'] == '2' * 64
    assert view['source']['index_sha256'] == '3' * 64
    request = dict(messages=[dict(role='system', content='synthetic'),
        dict(role='user', content=[dict(type='text', text=json.dumps({'material': view}))])])
    original = copy.deepcopy(request)
    collector = module._Collector([tmp_path])
    collector.request(request, 'body.plan.request', body=True)
    assert collector.files == {}
    assert request == original


@pytest.mark.parametrize('field', ['text', 'index'])
@pytest.mark.parametrize('change', ['valid', 'missing_sha', 'invalid_sha', 'tampered', 'outside'])
def test_stripped_projection_with_explicit_path_still_requires_valid_bound_file(tmp_path, field, change):
    inside = tmp_path / 'inside'; inside.mkdir()
    path = (tmp_path if change == 'outside' else inside) / ('synthetic.' + field)
    path.write_bytes(b'synthetic file evidence')
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    source = {field + '_file': str(path)}
    if change != 'missing_sha': source[field + '_sha256'] = 'invalid' if change == 'invalid_sha' else sha
    if change == 'tampered': path.write_bytes(b'tampered file evidence')
    projection = dict(source=source, assets=[], attachments=[])
    module = importlib.import_module('snapshot_collect')
    collector = module._Collector([inside])
    if change == 'valid':
        collector.material(projection, 'body.request.projection', projection=True)
        assert {entry['path'] for entry in collector.files.values()} == {str(path)}
    else:
        with pytest.raises(ValueError):
            collector.material(projection, 'body.request.projection', projection=True)


def test_full_private_material_hash_without_text_path_is_still_rejected(tmp_path):
    path = tmp_path / 'synthetic.pdf'; path.write_bytes(b'synthetic source')
    bundle = dict(version='preextract-material/1.0', source=dict(file=str(path),
        source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), text_sha256='2' * 64),
        assets=[], attachments=[])
    module = importlib.import_module('snapshot_collect')
    with pytest.raises(ValueError):
        module._Collector([tmp_path]).material(bundle, 'body.material')


@pytest.mark.parametrize('change', ['valid_absent_id', 'wrong_response_id', 'wrong_receipt_id', 'wrong_explicit_id'])
def test_raw_single_plan_uses_original_receipt_part_identity_without_rewriting_plan(tmp_path, change):
    inputs = fixture(tmp_path)
    ref = inputs['body']['plan']['files']['single']
    def raw_single(saved):
        if change == 'wrong_explicit_id': saved['plan']['parts'][0]['id'] = 'part-002'
        else: saved['plan']['parts'][0].pop('id')
        saved['plan_sha256'] = compact(saved['plan'], sort=True)
    rewrite_ref(ref, raw_single)
    saved = json.loads(Path(ref['path']).read_bytes())
    inputs['body']['plan_identity']['plan_sha256'] = saved['plan_sha256']
    if change == 'wrong_response_id': inputs['body']['responses'][0]['part_id'] = 'part-002'
    if change == 'wrong_receipt_id': inputs['body']['plan_identity']['parts'][0]['id'] = 'part-002'
    original = copy.deepcopy(inputs)
    original_plan = Path(ref['path']).read_bytes()
    if change == 'valid_absent_id':
        assert collect(inputs, tmp_path)['status'] == 'complete'
    else:
        with pytest.raises(ValueError): collect(inputs, tmp_path)
    assert inputs == original
    assert Path(ref['path']).read_bytes() == original_plan


@pytest.mark.parametrize('change', ['response_id', 'receipt_id', 'raw_id_missing'])
def test_partitioned_plan_never_infers_or_reassigns_part_identity(tmp_path, change):
    inputs = fixture(tmp_path, parts=2)
    if change == 'response_id': inputs['body']['responses'][1]['part_id'] = 'part-001'
    elif change == 'receipt_id': inputs['body']['plan_identity']['parts'][1]['id'] = 'part-001'
    else:
        refs = inputs['body']['plan']['files']
        rewrite_ref(refs['partition_plan'], lambda v: v['parts'][1].pop('id'))
        rewrite_ref(refs['partition_state'], lambda v: v.update(plan_sha256=refs['partition_plan']['sha256']))
    with pytest.raises(ValueError): collect(inputs, tmp_path)


def test_cross_root_rejected_before_read_even_with_valid_hash(tmp_path):
    inputs = fixture(tmp_path / 'inside')
    outside = tmp_path / 'secret.txt'; outside.write_bytes(b'secret')
    inputs['body']['source'] = {'path': str(outside), 'sha256': hashlib.sha256(b'secret').hexdigest()}
    with pytest.raises(ValueError, match='outside'): collect(inputs, tmp_path / 'inside')


def test_opened_evidence_redirected_outside_root_is_not_read(tmp_path):
    inputs = fixture(tmp_path / 'inside')
    with patch('source_handle._opened_path', return_value=str(tmp_path / 'outside-secret')):
        with pytest.raises(ValueError, match='opened source'):
            collect(inputs, tmp_path / 'inside')


def test_model_response_paths_do_not_trigger_reads(tmp_path):
    inputs = fixture(tmp_path)
    row = inputs['body']['responses'][0]
    rewrite_ref(row['response'], lambda v: v.update(arbitrary_path=r'Z:\secret\private.json'))
    # Response digest must also remain bound to the saved state.
    rewrite_ref(row['state'], lambda v: v.update(response_sha256=row['response']['sha256']))
    result = collect(inputs, tmp_path)
    assert not any(r['path'].startswith('Z:') for r in result['files'])


@pytest.mark.parametrize('kind', ['hardlink', 'directory-link'])
def test_links_and_directory_reparse_points_are_rejected(tmp_path, kind):
    inputs = fixture(tmp_path / 'inside')
    ref = inputs['body']['source']; path = Path(ref['path'])
    if kind == 'hardlink':
        os.link(path, path.with_name('hardlink.bin'))
    else:
        link = tmp_path / 'linked'
        try: link.symlink_to(path.parent, target_is_directory=True)
        except OSError: pytest.skip('symlink unavailable')
        ref['path'] = str(link / path.name)
    with pytest.raises(ValueError, match='link|reparse'): collect(inputs, tmp_path)


@pytest.mark.parametrize('delivered', [False, True])
def test_attachment_sidecars_and_bound_images_are_collected(tmp_path, delivered):
    inputs = fixture(tmp_path, rich=True, delivered=delivered)
    result = collect(inputs, tmp_path)
    roles = {role for row in result['files'] for role in row['roles']}
    assert {'attachment.sidecar', 'attachment.sidecar.image'} <= roles
    if delivered: assert 'receipt.delivery' in roles


def test_missing_attachment_image_fails_closed(tmp_path):
    inputs = fixture(tmp_path, rich=True)
    receipt = json.loads(Path(inputs['body']['receipt']['path']).read_bytes())
    sidecar = receipt['inventory']['attachments'][0]['evidence_records'][0]
    evidence = json.loads(Path(sidecar['path']).read_bytes())
    Path(evidence['images'][0]['path']).unlink()
    with pytest.raises(ValueError): collect(inputs, tmp_path)


def test_source_text_index_images_and_retained_hashes_are_collected(tmp_path):
    inputs = fixture(tmp_path)
    files = {}
    for key in ('text', 'index', 'image', 'retained'):
        path = tmp_path / (key + '.bin'); path.write_bytes(key.encode())
        files[key] = dict(path=str(path), sha256=hashlib.sha256(key.encode()).hexdigest())
    def change(value):
        for key in ('text', 'index'):
            value['source'][key + '_file'] = files[key]['path']
            value['source'][key + '_sha256'] = files[key]['sha256']
        value['assets'].append(dict(id='synthetic-image', image_path=files['image']['path'], sha256=files['image']['sha256']))
        value['file_hashes'] = {files['retained']['path']: files['retained']['sha256']}
    rewrite_ref(inputs['body']['material'], change)
    result = collect(inputs, tmp_path)
    assert {ref['path'] for ref in files.values()} <= {ref['path'] for ref in result['files']}


def test_unbound_existing_request_image_checkpoint_fails_closed(tmp_path):
    inputs = fixture(tmp_path)
    checkpoint = Path(inputs['body']['responses'][0]['images_work']) / 'private/request-images.json'
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text('{"schema":"preextract-request-images/1","images":[]}')
    with pytest.raises(ValueError, match='checkpoint'): collect(inputs, tmp_path)


def test_missing_frozen_segment_cannot_report_complete(tmp_path):
    inputs = fixture(tmp_path, 2)
    Path(inputs['body']['plan']['files']['part-002.material']['path']).unlink()
    with pytest.raises(ValueError): collect(inputs, tmp_path)


def test_unsupported_historical_sidecar_is_explicitly_blocked(tmp_path):
    inputs = fixture(tmp_path, rich=True)
    receipt_ref = inputs['body']['receipt']
    receipt = json.loads(Path(receipt_ref['path']).read_bytes())
    sidecar = receipt['inventory']['attachments'][0]['evidence_records'][0]
    rewrite_ref(sidecar, lambda v: v['text_samples'][0]['provenance'].update(kind='historical_reuse', record_ref=r'Z:\secret'))
    rewrite_ref(receipt_ref, lambda v: v['inventory']['attachments'][0]['evidence_records'].__setitem__(0, sidecar))
    inputs['attachments'][0]['receipt_ref'] = copy.deepcopy(receipt_ref)
    with pytest.raises(ValueError, match='unsupported historical'): collect(inputs, tmp_path)


def update_single_plan(inputs, mutate):
    body = inputs['body']; ref = body['plan']['files']['single']
    wrapper = json.loads(Path(ref['path']).read_bytes()); plan = wrapper['plan']
    mutate(plan)
    wrapper['plan_sha256'] = compact(plan, sort=True)
    rewrite_ref(ref, lambda v: v.update(wrapper))
    body['plan_identity']['plan_sha256'] = wrapper['plan_sha256']
    body['plan_identity']['reading_sha256'] = body['reading_material']['sha256']
    part = plan['parts'][0]
    body['plan_identity']['parts'][0].update(request_sha256=compact(part['body']),
                                           material_sha256=compact(part['bundle'], sort=True))
    row = body['responses'][0]
    rewrite_ref(row['request'], lambda v: (v.clear(), v.update(part['body'])))
    rewrite_ref(row['state'], lambda v: v.update(request_sha256=compact(part['body'])))


def test_actual_pathless_material_view_inside_request_text_is_supported(tmp_path):
    from preextract_request import material_view
    inputs = fixture(tmp_path)
    def mutate(plan):
        part = plan['parts'][0]
        part['body']['messages'][1]['content'][0]['text'] = json.dumps({'material': material_view(part['bundle'])})
    update_single_plan(inputs, mutate)
    assert collect(inputs, tmp_path)['status'] == 'complete'


def named_fixture(tmp_path):
    inputs = fixture(tmp_path)
    body = inputs['body']; work = Path(body['plan']['work'])
    target = work / 'named-input/Synthetic.docx'; target.parent.mkdir()
    target.write_bytes(Path(body['source']['path']).read_bytes())
    receipt = json.loads(Path(body['receipt']['path']).read_bytes())
    proof = dict(schema='named-delivery-input/1', paper_id=inputs['identity']['paper_id'],
        file_id=receipt['inventory']['body']['file_id'], kind='body',
        source_path=body['source']['path'], source_sha256=body['source']['sha256'],
        delivery_path=str(target), bytes=target.stat().st_size, display_name=target.name)
    path = work / 'named-input-proof.json'; path.write_text(json.dumps(proof), encoding='utf8')
    proof_ref = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    body['named_source'] = dict(work=str(work), proof=proof_ref)
    def change_material(value):
        value['source']['file'] = str(target)
        value['file_hashes'] = {str(path): proof_ref['sha256'], str(target): body['source']['sha256'],
                                body['source']['path']: body['source']['sha256']}
    for key in ('material', 'reading_material'): rewrite_ref(body[key], change_material)
    def change_plan(plan):
        part = plan['parts'][0]; change_material(part['bundle'])
        part['body']['messages'][1]['content'][0]['text'] = json.dumps({'material': part['bundle']})
    update_single_plan(inputs, change_plan)
    return inputs, path, target


def test_named_source_proof_and_copy_are_collected(tmp_path):
    inputs, path, target = named_fixture(tmp_path)
    result = collect(inputs, tmp_path)
    assert {str(path), str(target)} <= {r['path'] for r in result['files']}


def test_named_material_without_bound_proof_is_rejected(tmp_path):
    inputs, _, _ = named_fixture(tmp_path)
    inputs['body'].pop('named_source')
    with pytest.raises(ValueError, match='named'): collect(inputs, tmp_path)


def test_named_proof_of_another_source_is_rejected(tmp_path):
    inputs, _, _ = named_fixture(tmp_path)
    rewrite_ref(inputs['body']['named_source']['proof'], lambda v: v.update(source_sha256='0' * 64))
    with pytest.raises(ValueError): collect(inputs, tmp_path)


def test_unknown_attachment_request_projection_is_rejected(tmp_path):
    inputs = fixture(tmp_path)
    refs = inputs['attachments'][0]['model_refs']
    def mutate(value):
        value['messages'][1]['content'][0]['text'] = '{"schema":"future-attachment-input/2"}'
    rewrite_ref(refs['request'], mutate)
    request = json.loads(Path(refs['request']['path']).read_bytes())
    rewrite_ref(refs['request_state'], lambda value: value.update(request_sha256=compact(request)))
    with pytest.raises(ValueError, match='unsupported attachment request'): collect(inputs, tmp_path)


def test_checkpoint_with_unrecognized_binding_fails_closed(tmp_path):
    inputs = fixture(tmp_path)
    row = inputs['body']['responses'][0]
    checkpoint = Path(row['images_work']) / 'private/request-images.json'
    checkpoint.parent.mkdir(exist_ok=True)
    checkpoint.write_text(json.dumps(dict(schema='preextract-request-images/1',
        material_sha256='0' * 64, images=[dict(id='unknown-image', binding={})])), encoding='utf8')
    row['request_images'] = dict(path=str(checkpoint), sha256=hashlib.sha256(checkpoint.read_bytes()).hexdigest())
    with pytest.raises(ValueError, match='checkpoint'): collect(inputs, tmp_path)


def test_checkpoint_sent_rendition_is_collected(tmp_path):
    inputs = fixture(tmp_path)
    body = inputs['body']; row = body['responses'][0]
    original = tmp_path / 'source-image.png'; original.write_bytes(b'synthetic original image bytes')
    sent = Path(row['images_work']) / 'private/request-images/sent.png'; sent.parent.mkdir(parents=True)
    sent.write_bytes(b'synthetic sent image bytes')
    asset = dict(id='image-1', image_path=str(original), sha256=hashlib.sha256(original.read_bytes()).hexdigest(), width=2, height=2)
    binding = dict(image_path=str(sent), sha256=hashlib.sha256(sent.read_bytes()).hexdigest(), width=1, height=1)
    for key in ('material', 'reading_material'):
        rewrite_ref(body[key], lambda v: v['assets'].append(asset))
    part_bundle = []
    def mutate(plan):
        part = plan['parts'][0]; part['bundle']['assets'].append(asset); part['asset_ids'].append(asset['id'])
        plan['coverage']['asset_ids'].append(asset['id'])
        part['body']['messages'][1]['content'][0]['text'] = json.dumps({'material': part['bundle']})
        part_bundle.append(part['bundle'])
    update_single_plan(inputs, mutate)
    checkpoint = sent.parent.parent / 'request-images.json'
    checkpoint.write_text(json.dumps(dict(schema='preextract-request-images/1',
        material_sha256=compact(part_bundle[0], sort=True), images=[dict(id=asset['id'], adjusted=True,
            source_sha256=asset['sha256'], source_width=2, source_height=2, binding=binding)])), encoding='utf8')
    row['request_images'] = dict(path=str(checkpoint), sha256=hashlib.sha256(checkpoint.read_bytes()).hexdigest())
    result = collect(inputs, tmp_path)
    assert {str(original), str(sent), str(checkpoint)} <= {f['path'] for f in result['files']}


def current_fixture(root, *, delivered=False, include_hash=False, mutate_record=None, origin_path=None):
    """Preserve a real-shaped current origin distinct from the registered copy."""
    inputs = fixture(root, rich=True, delivered=delivered)
    receipt_ref = inputs['body']['receipt']
    receipt = json.loads(Path(receipt_ref['path']).read_bytes())
    row = receipt['inventory']['attachments'][0]
    ref = row['evidence_records'][0]
    record = json.loads(Path(ref['path']).read_bytes())
    origin = origin_path or root / 'classification-origin.json'
    origin.write_bytes(Path(receipt['handoff_ref']['path']).read_bytes())
    provenance = dict(kind='current_read', record_ref=str(origin), run_ref=None,
        entry_ref=record['source']['file_id'],
        record_sha256=receipt['handoff_ref']['sha256'] if include_hash else None)
    for item in [record['archive_listing'], *record['text_samples'], *record['images'], record['decision_basis']]:
        if item is not None:
            item['provenance'] = copy.deepcopy(provenance)
    if mutate_record is not None:
        mutate_record(record, row)
    Path(ref['path']).write_text(json.dumps(record, ensure_ascii=False), encoding='utf8')
    ref['sha256'] = hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()
    if delivered:
        delivery_ref = receipt['delivery_ref']
        delivery_path = Path(delivery_ref['path'])
        value = json.loads(delivery_path.read_bytes())
        for entry in value['evidence_records']:
            if delivery_path.parent / entry['path'] == Path(ref['path']):
                entry['sha256'] = ref['sha256']
        value['delivery_id'] = hashlib.sha256(json.dumps({k:v for k,v in value.items() if k != 'delivery_id'},
            ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')).hexdigest()
        delivery_path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')
        delivery_ref.update(sha256=hashlib.sha256(delivery_path.read_bytes()).hexdigest(),
                            delivery_id=value['delivery_id'])
    Path(receipt_ref['path']).write_text(json.dumps(receipt, ensure_ascii=False), encoding='utf8')
    receipt_ref['sha256'] = hashlib.sha256(Path(receipt_ref['path']).read_bytes()).hexdigest()
    for attachment in inputs['attachments']:
        attachment['receipt_ref'] = copy.deepcopy(receipt_ref)
    return inputs, origin, row, record


@pytest.mark.parametrize('delivered', [False, True])
@pytest.mark.parametrize('include_hash', [False, True])
def test_current_read_refs_bind_upstream_entry_and_original_handoff_hash(tmp_path, delivered, include_hash):
    inputs, origin, row, record = current_fixture(tmp_path, delivered=delivered, include_hash=include_hash)
    assert row['file_id'] != row['upstream_file_id'] == record['source']['file_id']
    assert str(origin) != row['handoff_ref']['path']
    before_inputs = copy.deepcopy(inputs)
    before = {str(p): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    result = collect(inputs, tmp_path)
    found = next(entry for entry in result['files'] if entry['path'] == str(origin))
    assert found['sha256'] == row['handoff_ref']['sha256']
    assert 'attachment.sidecar.current_handoff' in found['roles']
    assert result['inputs'] == inputs == before_inputs
    assert all(Path(path).read_bytes() == raw for path, raw in before.items())


@pytest.mark.parametrize('decision_present', [False, True])
def test_not_archive_accepts_null_provenance_without_inventing_current_evidence(tmp_path, decision_present):
    def inactive(record, row):
        record['archive_listing'] = dict(evidence_id=None, status='not_archive', total_entries=None,
            listed_entries=0, entries=[], reason=None, provenance=None)
        record['read_scope']['directory'] = dict(status='not_applicable', reason=None)
        if not decision_present:
            record['decision_basis'] = None
    inputs, origin, _, _ = current_fixture(tmp_path, mutate_record=inactive)
    assert collect(inputs, tmp_path)['status'] == 'complete'
    assert origin.exists()


@pytest.mark.parametrize('change', ['registered_id', 'another_entry', 'record_hash', 'run_ref', 'source_binding',
                                   'registered_upstream_id', 'original_path'])
def test_current_read_mismatches_are_rejected_without_historical_reinterpretation(tmp_path, change):
    def mutate(record, row):
        origin = record['text_samples'][0]['provenance']
        if change == 'registered_id':
            origin['entry_ref'] = row['file_id']
        elif change == 'another_entry':
            origin['entry_ref'] = 'not-this-upstream-file'
        elif change == 'record_hash':
            origin['record_sha256'] = '0' * 64
        elif change == 'run_ref':
            origin['run_ref'] = 'unreviewed-run-record'
        elif change == 'registered_upstream_id':
            row['upstream_file_id'] = row['file_id']
        elif change == 'original_path':
            # A fabricated source alias must not replace the original handoff
            # row even when the actual delivered copy still hashes correctly.
            row['source_path'] = str(tmp_path / 'another-original.zip')
            row['delivery_ref'] = {'synthetic': True}
        else:
            record['source']['source_sha256'] = '0' * 64
            row['source_binding']['source_sha256'] = '0' * 64
    inputs, _, _, _ = current_fixture(tmp_path, mutate_record=mutate)
    with pytest.raises(ValueError):
        collect(inputs, tmp_path)


def test_current_origin_byte_tamper_is_rejected_even_when_json_content_is_equal(tmp_path):
    inputs, origin, _, _ = current_fixture(tmp_path)
    origin.write_bytes(origin.read_bytes() + b' ')
    with pytest.raises(ValueError, match='bytes changed'):
        collect(inputs, tmp_path)


def test_current_origin_outside_allowed_roots_is_not_opened_even_with_matching_hash(tmp_path):
    inside = tmp_path / 'inside'; inside.mkdir()
    outside = tmp_path / 'outside-handoff.json'
    inputs, _, _, _ = current_fixture(inside, origin_path=outside)
    opened = Path.open
    accesses = []
    def guard(path, *args, **kwargs):
        if path == outside:
            accesses.append(path)
            pytest.fail('untrusted origin must be rejected before opening')
        return opened(path, *args, **kwargs)
    with patch.object(Path, 'open', guard):
        with pytest.raises(ValueError, match='outside'):
            collect(inputs, inside)
    assert accesses == []


def test_null_provenance_is_rejected_for_available_archive_evidence(tmp_path):
    inputs, _, _, _ = current_fixture(tmp_path,
        mutate_record=lambda record, row: record['archive_listing'].update(provenance=None))
    with pytest.raises(ValueError):
        collect(inputs, tmp_path)


def test_no_content_or_decision_provenance_does_not_collect_unreferenced_origin(tmp_path):
    def no_content(record, row):
        record['archive_listing'] = dict(evidence_id=None, status='not_archive', total_entries=None,
            listed_entries=0, entries=[], reason=None, provenance=None)
        record['read_scope']['directory'] = dict(status='not_applicable', reason=None)
        record['text_samples'], record['images'], record['decision_basis'] = [], [], None
        for field in ('text', 'images'):
            record['read_scope'][field] = dict(status='not_read', reason=None)
    inputs, origin, _, _ = current_fixture(tmp_path, mutate_record=no_content)
    result = collect(inputs, tmp_path)
    assert result['status'] == 'complete'
    assert str(origin) not in {row['path'] for row in result['files']}


def test_current_handoff_metadata_is_still_hashed_under_deferred_binary_policy(tmp_path):
    inputs, origin, _, _ = current_fixture(tmp_path)
    module = importlib.import_module('snapshot_collect')
    result = module.collect_inputs(inputs, allowed_roots=[tmp_path], defer_binary_hash=True)
    assert str(origin) in {row['path'] for row in result['files']}
    origin.write_bytes(origin.read_bytes() + b' ')
    with pytest.raises(ValueError, match='bytes changed'):
        module.collect_inputs(inputs, allowed_roots=[tmp_path], defer_binary_hash=True)


def test_current_handoff_origin_symlink_is_rejected(tmp_path):
    inputs, origin, _, _ = current_fixture(tmp_path)
    actual = origin.with_name('same-content.json')
    origin.rename(actual)
    try:
        origin.symlink_to(actual)
    except OSError:
        pytest.skip('symlink unavailable')
    with pytest.raises(ValueError, match='link|reparse'):
        collect(inputs, tmp_path)


def test_current_provenance_uses_registered_delivery_bytes_without_reopening_original_papers(tmp_path):
    inputs, origin, row, _ = current_fixture(tmp_path, delivered=True)
    handoff = json.loads(Path(row['handoff_ref']['path']).read_bytes())
    originals = [handoff['body'], *handoff['attachments']]
    for original in originals:
        Path(original['path']).unlink()  # only temporary synthetic originals
    result = collect(inputs, tmp_path)
    assert result['status'] == 'complete'
    assert str(origin) in {entry['path'] for entry in result['files']}
    assert not any(entry['path'] in {item['path'] for item in originals} for entry in result['files'])
