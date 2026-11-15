"""Read this paper's completed processing evidence from its child FlowRun."""

from paperflow_prefect.llm_queue.folder_completion import expected_components
from paperflow_prefect.records import identity as fingerprint, read_json, sha_file
from paperflow_v3.control.stage_admission import unsettled_requests
from paperflow_v3.stages.gateway_admission import verify_receipt

from .handoff import require_complete_handoff


def read_complete_handoff(identity, paper, records):
    """Require the upstream server receipts and their original identities."""
    rows = [row for row in records if row.get('task_id') == identity['task_id']
            and row.get('folder_id') == identity['group_id']]

    def latest(kind):
        return next((row['data'] for row in rows if row.get('kind') == kind), None)

    before = latest('preprocessing_complete')
    if (not before or before.get('status') != 'preprocessing_complete'
            or any(before.get(key) != identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
            or sha_file(before['manifest_path']) != before.get('manifest_sha256')):
        raise ValueError('server preprocessing evidence is incomplete')
    manifest = read_json(before['manifest_path'])
    if (any(manifest.get(key) != identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
            or manifest.get('body') != paper['body'] or manifest.get('attachments') != paper['attachments']):
        raise ValueError('preprocessing evidence belongs to another paper')

    components = expected_components(manifest, identity['task_run_id'])
    llm = latest('llm_folder_complete')
    if not llm or llm.get('status') != 'raw_complete' or llm.get('manifest_sha256') != fingerprint(manifest):
        raise ValueError('server LLM completion evidence is incomplete')
    saved = {}
    for row in rows:
        kind = row.get('kind')
        if kind not in {'llm_request_intent', 'llm_request_not_sent', 'llm_request_failed',
                        'llm_request_unknown', 'llm_response_saved'}:
            continue
        value = row['data']
        if kind != 'llm_response_saved':
            continue
        key = value['component_id']
        evidence = {name: value.get(name) for name in
                    ('component_id', 'request_id', 'kind', 'request_sha256', 'response_sha256', 'response_path')}
        if key in saved and saved[key] != evidence:
            raise ValueError('server LLM response identity changed')
        saved[key] = evidence
    if unsettled_requests(rows, 'llm'):
        raise ValueError('a previous model request outcome is unknown')
    for row in rows:
        if row.get('kind') == 'llm_gateway_not_dispatched':
            # Metadata suffices for display; delivery reads the original bytes
            # and exact folder binding before accepting a no-dispatch outcome.
            verify_receipt(row['data'], identity=identity)
    summaries = {item['component_id']: item for item in llm.get('components', [])}
    if (len(summaries) != len(llm.get('components', []))
            or set(summaries) != {part['component_id'] for part in components}):
        raise ValueError('server LLM completion is missing a required component')
    for part in components:
        value = saved.get(part['component_id']) or {}
        summary = summaries[part['component_id']]
        if (value.get('kind') != part['kind'] or not value.get('request_id')
                or not value.get('request_sha256') or not value.get('response_sha256')
                or not value.get('response_path')
                or any(summary.get(key) != value.get(key) for key in ('request_sha256', 'response_sha256'))
                or (part['kind'] == 'body' and value['request_sha256'] != part['request_sha256'])):
            raise ValueError('server LM request and response evidence does not match')
    if manifest['capacity']['plan_identity']['mode'] == 'partitioned':
        ranking = latest('llm_whole_ranking_complete') or {}
        part_rows = [part for part in components if part['kind'] == 'body']
        final = saved.get(ranking.get('component_id')) or {}
        if (ranking.get('status') != 'raw_complete' or not final
                or not all(ranking.get(key) for key in ('request_id', 'request_sha256', 'response_sha256', 'response_path'))
                or ranking.get('coverage') != [part['part_id'] for part in part_rows]
                or ranking.get('part_responses') != [{'part_id': part['part_id'],
                    'response_sha256': saved[part['component_id']]['response_sha256']} for part in part_rows]
                or any(ranking.get(key) != final.get(key) for key in
                       ('request_id', 'request_sha256', 'response_sha256', 'response_path'))
                or llm.get('whole_paper_ranking') != {key: final[key] for key in
                       ('request_id', 'kind', 'request_sha256', 'response_sha256', 'response_path')}):
            raise ValueError('server whole-paper ranking evidence is incomplete')

    handoff = latest('postprocessing_complete')
    if not handoff or handoff.get('manifest_sha256') != fingerprint(manifest):
        raise ValueError('server postprocessing evidence is incomplete')
    require_complete_handoff(paper, identity, handoff)
    inventory = latest('stage_work_inventory')
    if (not inventory or inventory.get('identity') != identity or not isinstance(inventory.get('files'), list)
            or not inventory['files']):
        raise ValueError('server paper work inventory is incomplete')
    results = {}
    for row in rows:
        if row.get('kind') in {'postprocessing_component_started', 'postprocessing_component_not_started',
                               'postprocessing_component_failed', 'postprocessing_component_complete',
                               'postprocessing_component_waiting'}:
            data = row['data']
            results.setdefault(data['component_id'], (row['kind'], data))
    required = ['body', *('attachment:' + item['file_id'] for item in paper['attachments'])]
    if any(key not in results or results[key][0] != 'postprocessing_component_complete' for key in required):
        raise ValueError('server postprocessing component evidence is incomplete')
    for key in required:
        data = results[key][1]
        if sha_file(data['result_path']) != data['result_sha256']:
            raise ValueError('saved postprocessing component changed')
    body = read_json(results['body'][1]['result_path'])
    if (body.get('schema') != 'body-postprocess-result/1' or body.get('status') != 'body_package_verified'
            or body.get('identity') != identity or body.get('package_path') != handoff['body']['package_path']
            or body.get('manifest_sha256') != handoff['body']['manifest_sha256']):
        raise ValueError('saved body package does not match server handoff')
    return {'handoff': handoff, 'body_result': body, 'manifest': manifest,
            'stage_work_files': inventory['files']}
