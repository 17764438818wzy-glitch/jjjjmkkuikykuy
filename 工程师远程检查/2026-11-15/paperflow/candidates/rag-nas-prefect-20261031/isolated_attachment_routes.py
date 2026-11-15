"""Bind attachment publication to verified body and test-only destinations."""


def attachment_route(paper_id, profile_sha256, plan, body, folder):
    routes = plan['routes']
    codes = [route['code'] for route in routes]
    binding = body['binding']
    if (plan.get('schema') != 'paperflow-isolated-test-route-plan/1' or not plan.get('attachments_required')
            or not body.get('body_verified') or binding['run_id'] != plan['run_id']
            or binding['manifest_sha256'] != plan['manifest_sha256']
            or binding['targets'] != {route['code']: route['paper_dataset_id'] for route in routes}
            or set(body['targets']) != set(codes)
            or folder.get('schema') != 'paperflow-isolated-files-folder/1' or folder.get('status') != 'verified'
            or folder['binding']['run_id'] != plan['run_id']
            or folder['binding']['journal_id'] != binding['journal_id']
            or not folder.get('folder_id') or not all(route.get('image_dataset_id') for route in routes)):
        raise ValueError('attachment test destinations or completed body do not match')
    parents = []
    for route in routes:
        code = route['code']
        target = body['targets'][code]
        doc = target['body']
        if (not target.get('verified') or not target.get('retrieval_verified')
                or doc['dataset_id'] != route['paper_dataset_id']
                or doc['identity']['paperflow_source_sha256'] != binding['source_sha256']
                or doc['identity']['paperflow_manifest_sha256'] != plan['manifest_sha256']
                or doc['identity']['paperflow_specialty'] != code):
            raise ValueError('attachment parent body is not verified')
        parents.append({'code': code, 'dataset_id': doc['dataset_id'],
                        'document_id': doc['document_id'], 'identity': doc['identity']})
    destinations = [{'code': row['code'], 'dataset_id': row['image_dataset_id']} for row in routes]
    destinations[0]['parent_id'] = folder['folder_id']
    return {'paper_id': paper_id, 'body_source_sha256': binding['source_sha256'],
            'profile_sha256': profile_sha256, 'routing_codes': codes,
            'primary': destinations[0], 'references': destinations[1:],
            'history_reviewed': True, 'parent_targets': parents}
