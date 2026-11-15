import copy
import json

import pytest

from paperflow_prefect.llm_queue.folder_completion import expected_components
from paperflow_prefect.records import identity as fingerprint, sha_file


@pytest.fixture
def server_handoff(tmp_path):
    identity = {'task_id': 'task', 'group_id': 'group', 'paper_id': 'paper',
                'receipt_id': 'a' * 64, 'task_run_id': 'child'}
    body = {'file_id': 'body', 'sha256': 'b' * 64}
    attachment = {'file_id': 'attachment', 'sha256': 'c' * 64}
    paper = {'paper_id': 'paper', 'group_id': 'group', 'body': body, 'attachments': [attachment]}
    plan = {'schema': 'paperflow-body-input-plan/1', 'mode': 'single', 'source_sha256': body['sha256'],
            'reading_sha256': 'd' * 64, 'plan_sha256': 'e' * 64, 'coverage_sha256': 'f' * 64,
            'parts': [{'id': 'part-001', 'request_sha256': '1' * 64}], 'part_count': 1}
    manifest = {**{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                'body': body, 'attachments': [attachment], 'reading_material': {'sha256': 'd' * 64},
                'capacity': {'status': 'planned', 'mode': 'single', 'parts': 1, 'plan_identity': plan}}
    manifest_path = tmp_path / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest), encoding='utf8')
    handoff = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': identity,
               'validation_status': 'passed', 'manifest_sha256': fingerprint(manifest),
               'body': {'package_path': 'pkg', 'manifest_sha256': '2' * 64},
               'attachments': [{'attachment_id': 'attachment'}]}
    body_result = {'schema': 'body-postprocess-result/1', 'status': 'body_package_verified',
                   'identity': identity, 'package_path': 'pkg', 'manifest_sha256': '2' * 64,
                   'task': {'source_sha256': body['sha256']}, 'validation': {}}
    body_path = tmp_path / 'body-result.json'
    body_path.write_text(json.dumps(body_result), encoding='utf8')
    attachment_path = tmp_path / 'attachment-result.json'
    attachment_path.write_text('{}', encoding='utf8')

    def row(kind, data):
        return {'kind': kind, 'task_id': identity['task_id'], 'folder_id': identity['group_id'], 'data': data}

    parts = expected_components(manifest, identity['task_run_id'])
    completed = []
    raw = []
    for index, part in enumerate(parts):
        request_hash = part.get('request_sha256', '3' * 64)
        data = {'component_id': part['component_id'], 'request_id': 'request-' + str(index),
                'kind': part['kind'], 'request_sha256': request_hash, 'response_sha256': '4' * 64,
                'response_path': str(tmp_path / ('response-' + str(index)))}
        raw.append(row('llm_response_saved', data))
        completed.append({key: data[key] for key in ('component_id', 'request_sha256', 'response_sha256')})
    records = [row('postprocessing_complete', handoff),
               row('stage_work_inventory', {'identity': identity,
                   'files': [str(manifest_path), str(body_path), str(attachment_path)]}),
               row('postprocessing_component_complete', {'component_id': 'body', 'result_path': str(body_path),
                                                          'result_sha256': sha_file(body_path)}),
               row('postprocessing_component_complete', {'component_id': 'attachment:attachment',
                    'result_path': str(attachment_path), 'result_sha256': sha_file(attachment_path)}),
               row('llm_folder_complete', {'status': 'raw_complete', 'manifest_sha256': fingerprint(manifest),
                                            'components': completed, 'whole_paper_ranking': None}),
               *raw,
               row('preprocessing_complete', {**{key: identity[key] for key in
                    ('task_id', 'group_id', 'paper_id', 'receipt_id')}, 'status': 'preprocessing_complete',
                    'manifest_path': str(manifest_path), 'manifest_sha256': sha_file(manifest_path)})]
    return identity, paper, records, handoff, body_result


def test_real_stage_receipts_are_bound_before_delivery(server_handoff):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff

    identity, paper, records, handoff, body_result = server_handoff
    result = read_complete_handoff(identity, paper, records)
    assert result['handoff'] == handoff
    assert result['body_result'] == body_result


@pytest.mark.parametrize('missing', ['preprocessing_complete', 'llm_response_saved',
                                    'llm_folder_complete', 'postprocessing_component_complete',
                                    'postprocessing_complete', 'stage_work_inventory'])
def test_missing_server_stage_evidence_blocks_delivery(server_handoff, missing):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff

    identity, paper, records, _, _ = server_handoff
    records = [row for row in records if row['kind'] != missing]
    with pytest.raises(ValueError):
        read_complete_handoff(identity, paper, records)


def test_mismatched_raw_identity_or_another_paper_is_not_accepted(server_handoff):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff

    identity, paper, records, _, _ = server_handoff
    altered = copy.deepcopy(records)
    next(row for row in altered if row['kind'] == 'llm_response_saved')['data']['response_sha256'] = '5' * 64
    with pytest.raises(ValueError):
        read_complete_handoff(identity, paper, altered)
    altered = copy.deepcopy(records)
    next(row for row in altered if row['kind'] == 'postprocessing_complete')['folder_id'] = 'other'
    with pytest.raises(ValueError):
        read_complete_handoff(identity, paper, altered)


def test_unresolved_model_request_blocks_delivery_even_if_older_stage_was_complete(server_handoff):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff

    identity, paper, records, _, _ = server_handoff
    unknown = {'task_id': identity['task_id'], 'folder_id': identity['group_id'], 'kind': 'llm_request_unknown',
               'data': {'component_id': 'extra-part', 'request_id': 'extra-request'}}
    with pytest.raises(ValueError, match='unknown'):
        read_complete_handoff(identity, paper, [unknown, *records])


def test_partitioned_paper_requires_one_ranking_covering_every_part(server_handoff):
    from paperflow_v3.delivery.server_handoff import read_complete_handoff

    identity, paper, records, _, _ = server_handoff
    before = next(row['data'] for row in records if row['kind'] == 'preprocessing_complete')
    path = __import__('pathlib').Path(before['manifest_path'])
    manifest = json.loads(path.read_text(encoding='utf8'))
    capacity = manifest['capacity']
    capacity['mode'] = capacity['plan_identity']['mode'] = 'partitioned'
    capacity['parts'] = capacity['plan_identity']['part_count'] = 2
    capacity['plan_identity']['parts'].append({'id': 'part-002', 'request_sha256': '6' * 64})
    path.write_text(json.dumps(manifest), encoding='utf8')
    before['manifest_sha256'] = sha_file(path)
    parts = expected_components(manifest, identity['task_run_id'])
    saved = [row['data'] for row in records if row['kind'] == 'llm_response_saved']
    new_part = {'component_id': parts[1]['component_id'], 'request_id': 'second-request', 'kind': 'body',
                'request_sha256': '6' * 64, 'response_sha256': '7' * 64, 'response_path': 'response-second'}
    final = {'component_id': 'whole-ranking', 'request_id': 'ranking-request', 'kind': 'body',
             'request_sha256': '8' * 64, 'response_sha256': '9' * 64, 'response_path': 'response-ranking'}
    row = lambda kind, data: {'task_id': identity['task_id'], 'folder_id': identity['group_id'],
                              'kind': kind, 'data': data}
    records[0:0] = [row('llm_response_saved', new_part), row('llm_response_saved', final)]
    ranking = {'status': 'raw_complete', **{key: final[key] for key in
               ('component_id', 'request_id', 'request_sha256', 'response_sha256', 'response_path')},
               'coverage': ['part-001', 'part-002'],
               'part_responses': [{'part_id': 'part-001', 'response_sha256': saved[0]['response_sha256']},
                                  {'part_id': 'part-002', 'response_sha256': new_part['response_sha256']}]}
    records.insert(0, row('llm_whole_ranking_complete', ranking))
    llm = next(row['data'] for row in records if row['kind'] == 'llm_folder_complete')
    llm['components'].append({key: new_part[key] for key in ('component_id', 'request_sha256', 'response_sha256')})
    llm['whole_paper_ranking'] = {key: final[key] for key in
                                  ('request_id', 'kind', 'request_sha256', 'response_sha256', 'response_path')}
    llm['manifest_sha256'] = fingerprint(manifest)
    next(row['data'] for row in records if row['kind'] == 'postprocessing_complete')['manifest_sha256'] = fingerprint(manifest)
    assert read_complete_handoff(identity, paper, records)['handoff']['manifest_sha256'] == fingerprint(manifest)
    ranking['coverage'] = ['part-001']
    with pytest.raises(ValueError, match='ranking'):
        read_complete_handoff(identity, paper, records)
