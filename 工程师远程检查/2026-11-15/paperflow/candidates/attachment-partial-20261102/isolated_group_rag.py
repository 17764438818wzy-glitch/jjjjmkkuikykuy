"""Complete RAG for one paper group, including all required attachments."""
import hashlib
from pathlib import Path

from paperflow_prefect.records import read_json, sha_file, write_json

from folder_native_body import publish_body
from folder_native_attachment import publish_attachment
from isolated_attachment_routes import attachment_route
from group_stage import FIELDS


def publish_group(stage, copy_receipt_path, prepared, packet, plan, folder, attachments, profile_sha256,
                  raw, journal, admit, require_runtime, config, budget, work_dir, *, failed_attachments=()):
    ticket = {key: stage[key] for key in FIELDS}
    copied = read_json(copy_receipt_path)
    if prepared['manifest_sha256'] != packet['manifest_sha256']:
        raise ValueError('packet differs from the bound postprocessing manifest')
    profile_entries = [row for row in packet['manifest']['files']
                       if (row if isinstance(row, str) else row.get('path')) == 'profile.json']
    sealed_profile = (packet['manifest'].get('file_hashes', {}).get('profile.json')
                      if len(profile_entries) == 1 and isinstance(profile_entries[0], str)
                      else profile_entries[0].get('sha256') if len(profile_entries) == 1 else None)
    if sealed_profile != profile_sha256:
        raise ValueError('attachment profile identity differs from the sealed manifest')
    if any(copied[key] != ticket[key] for key in ('group_id', 'paper_id', 'receipt_id')):
        raise ValueError('test copy belongs to a different folder ticket')
    bodies = [row for row in copied['files'] if row['kind'] == 'body']
    expected = {row['file_id']: row['sha256'] for row in copied['files'] if row['kind'] == 'attachment'}
    test_files = {row['file_id']: row['path'] for row in copied['files'] if row['kind'] == 'attachment'}
    supplied = {item['attachment_id']: item['source_sha256'] for item in attachments}
    failed = {item['attachment_id']: item['source_sha256'] for item in failed_attachments}
    if (len(bodies) != 1 or bodies[0]['sha256'] != prepared['task']['source_sha256']
            or len(supplied) != len(attachments) or len(failed) != len(failed_attachments)
            or set(supplied) & set(failed) or supplied | failed != expected
            or any(item['status'] != 'failed' for item in failed_attachments)
            or any(item['paper_id'] != ticket['paper_id'] for item in attachments)
            or bool(attachments) != bool(plan.get('attachments_required'))):
        raise ValueError('body or attachments differ from the group test copy')
    work = Path(work_dir)
    work.mkdir(parents=True, exist_ok=True)
    path = work / 'rag-group.json'
    binding = {'ticket': ticket, 'copy_receipt_sha256': sha_file(copy_receipt_path),
               'manifest_sha256': packet['manifest_sha256'], 'run_id': plan['run_id'],
               'journal_id': journal.identity(), 'profile_sha256': profile_sha256,
               'postprocess': {key: stage[key] for key in ('claim_id', 'postprocess_job_id',
                   'postprocess_receipt_sha256', 'upstream_manifest_sha256', 'source_generation')},
               'attachments': {item['attachment_id']: {'source_sha256': item['source_sha256'],
                   'md_sha256': item['md_sha256'], 'delivery_version': item['delivery_version']}
                   for item in attachments}}
    if failed_attachments:
        binding['failed_attachments'] = list(failed_attachments)
    state = read_json(path) if path.is_file() else {
        'schema': 'paperflow-isolated-group-rag/1', 'binding': binding, 'receipt_path': str(path)}
    if state.get('schema') != 'paperflow-isolated-group-rag/1' or state.get('binding') != binding:
        raise ValueError('group RAG receipt belongs to a different test')
    state.update(status='pending', group_count=0)
    write_json(path, state)
    body_path = work / 'body.json'
    body = publish_body(prepared, packet, plan, raw, journal, admit, require_runtime, config, body_path)
    completed = []
    if attachments:
        route = attachment_route(ticket['paper_id'], profile_sha256, plan, body, folder)
        for item in attachments:
            item = dict(item, source_path=test_files[item['attachment_id']])
            name = hashlib.sha256(item['attachment_id'].encode('utf8')).hexdigest() + '.json'
            attachment_path = work / 'attachments' / name
            result = publish_attachment(item, route, raw, journal, admit, require_runtime,
                                        config.native_pipeline_id, budget, attachment_path)
            if result.get('status') != 'verified':
                raise ValueError('attachment was not completely verified')
            completed.append({'attachment_id': item['attachment_id'], 'source_sha256': item['source_sha256'],
                              'md_sha256': item['md_sha256'], 'receipt_path': str(attachment_path),
                              'receipt_sha256': sha_file(attachment_path)})
    state.update(status='verified', publication_status='partial' if failed else 'complete',
                 group_count=1, body_receipt_path=str(body_path),
                 body_receipt_sha256=sha_file(body_path), attachments=completed)
    write_json(path, state)
    return state
