"""One paper's real Prefect-bound RAG, archive and cleanup entry point."""

import copy
from pathlib import Path
from urllib.parse import urlencode
from prefect.utilities.asyncutils import run_coro_as_sync

from paperflow_prefect import legacy
from paperflow_prefect.records import read_json, sha_file
from paperflow_prefect.unified import UnifiedBackend
from paperflow_prefect.unified_attachments import receipt_root_for
from paperflow_v3.control.admission import admitted_sync_slot
from paperflow_v3.control.receipts import read_receipts_sync, read_registration, save_folder_completion
from paperflow_v3.control.workspace import folder_settings

from .archive import save_archive
from .cleanup import cleanup_paper
from .owned_work import owned_work_files
from .server_handoff import read_complete_handoff
from .progress import FolderProgress


def _latest(records, identity, kind):
    return next((copy.deepcopy(row['data']) for row in records
                 if row.get('task_id') == identity['task_id'] and row.get('folder_id') == identity['group_id']
                 and row.get('kind') == kind), None)


def _rag_complete(paper, identity, packet, targets, attachments):
    return (bool(packet['routes'])
        and all((targets.get(route['code']) or {}).get('body_delivery_state') == 'complete'
                and (targets.get(route['code']) or {}).get('formal_route') == route
                and (targets.get(route['code']) or {}).get('ticket_identity') == identity
                for route in packet['routes'])
        and all((attachments.get(row['file_id']) or {}).get('delivery_state') == 'complete'
                and (attachments.get(row['file_id']) or {}).get('ticket_identity') == identity
                for row in paper['attachments']))


def _require_queueable_file_state(state, stage, retry_failed):
    statuses = [row.get('status') for row in (state.get('files') or {}).values()]
    if any(status in ('in_flight', 'unknown') for status in statuses):
        raise ValueError(stage + ' outcome unknown; reconciliation required')
    if 'failed' in statuses and not retry_failed:
        raise ValueError(stage + ' file requires a user-selected retry')


def _registration(identity, registered, settings, first_step_work_root):
    group = identity['group_id']
    task = identity['task_id']
    if Path(group).name != group or group in ('.', '..') or Path(task).name != task or task in ('.', '..'):
        raise ValueError('invalid paper work identity')
    base = Path(first_step_work_root).resolve()
    configured = [settings.path('data_root')]
    if settings.values.get('classification_root'):
        configured.append(settings.path('classification_root'))
    if task not in base.parts or not any(base.is_relative_to(path) for path in configured):
        raise ValueError('first-step work root must be task-owned and inside configured work storage')
    root = base / group
    receipt = root / 'registration' / 'receipts' / (identity['receipt_id'] + '.json')
    if (registered.get('status') != 'passed' or registered.get('delivery_complete') is not True
            or registered.get('registration_status') != 'registered'
            or any(registered.get(key) != identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
            or Path(registered['receipt_path']).resolve() != receipt.resolve()
            or sha_file(receipt) != registered.get('receipt_sha256')):
        raise ValueError('paper registration or owned first-step root does not match')
    saved = run_coro_as_sync(read_registration(task, group))
    if not saved or any(saved.get(key) != registered.get(key) for key in
            ('task_id', 'group_id', 'paper_id', 'receipt_id', 'receipt_path', 'receipt_sha256', 'paper')):
        raise ValueError('paper registration is not saved on the Prefect server')
    return root


def _work_paths(records, identity, handoff, work_root):
    paths = [handoff['body']['package_path']]
    for row in records:
        if row.get('task_id') != identity['task_id'] or row.get('folder_id') != identity['group_id']:
            continue
        data = row.get('data') or {}
        if row.get('kind') == 'preprocessing_complete':
            paths.append(data['manifest_path'])
        elif row.get('kind') == 'llm_response_saved':
            paths.append(data['response_path'])
        elif row.get('kind') == 'postprocessing_component_complete':
            paths.append(data['result_path'])
    if any(not Path(path).resolve().is_relative_to(work_root) for path in paths):
        raise ValueError('paper processing output is outside its owned work root')


def _original_folder(client, roots, packet, settings):
    from .attachment_delivery import ensure_original_folder

    expected_model = settings.values.get('rag_embedding_model_id')
    if not expected_model:
        raise ValueError('formal attachment embedding model is not configured')
    for route in packet['routes']:
        result = client.request('GET', '/datasets?' + urlencode(dict(id=route['image_dataset_id'], page=1, page_size=100)))
        rows = result if isinstance(result, list) else result.get('datasets', [])
        matches = [row for row in rows if row.get('id') == route['image_dataset_id']]
        name = '【' + route['code'] + '】' + route['name'] + '｜资料库'
        if len(matches) != 1 or matches[0].get('name') != name or matches[0].get('embedding_model') != expected_model:
            raise ValueError('formal attachment dataset or embedding model does not match')
    root = client.request('GET', '/files?page=1&page_size=1').get('parent_folder', {})
    if root.get('name') != '/' or root.get('type') != 'folder' or root.get('id') != root.get('parent_id'):
        raise ValueError('RAG Files root is not uniquely verified')
    first = packet['routes'][0]
    name = '【' + first['code'] + '】' + first['name'] + '｜附件原件'
    folders = {ensure_original_folder(client, root['id'], first['code'], name, receipt_root)
               for receipt_root in roots.values()}
    if len(folders) != 1:
        raise ValueError('attachment original folders do not match')
    return folders.pop()


def _cleanup_root(path, configured):
    path = Path(path).absolute()
    owner = next((root for root in configured if path.is_relative_to(root)), None)
    if owner is None:
        raise ValueError('cleanup work root is outside configured storage')
    current = path
    while current != owner:
        if current.is_symlink() or current.is_junction():
            raise ValueError('cleanup work root or ancestor is redirected')
        current = current.parent
    return path.resolve()


def _delivered_cleanup_inputs(identity, registered, settings, first_step_work_root, progress, records, cleanup):
    """Bind this folder's delivery proofs and local file scope, without processing outputs."""
    if not {'rag_complete', 'nas'}.issubset(progress.remote_complete):
        raise ValueError('cleanup requires this folder RAG and NAS completion receipts')
    paper = registered['paper']
    if (any(registered.get(key) != identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
            or paper.get('paper_id') != identity['paper_id'] or paper.get('group_id') != identity['group_id']):
        raise ValueError('cleanup file list belongs to another folder')
    task, group = identity['task_id'], identity['group_id']
    if any(Path(name).name != name or name in ('.', '..') for name in (task, group)):
        raise ValueError('invalid cleanup folder identity')
    base = Path(first_step_work_root).absolute()
    configured = [settings.path('data_root').resolve()]
    if settings.values.get('classification_root'):
        configured.append(settings.path('classification_root').resolve())
    if task not in base.parts or not any(base.is_relative_to(root) for root in configured):
        raise ValueError('cleanup first-step root is outside this task')
    roots = tuple(_cleanup_root(path, configured) for path in
        (base / group, settings.path('data_root').resolve() / 'paperflow-v3' / task / group))
    source, nas_root = Path(paper['person_dir']).resolve(), settings.path('nas_destination').resolve()
    if (source.is_relative_to(nas_root) or nas_root.is_relative_to(source)
            or any(root.is_relative_to(protected) or protected.is_relative_to(root)
                   for root in roots for protected in (source, nas_root))
            or roots[0].is_relative_to(roots[1]) or roots[1].is_relative_to(roots[0])):
        raise ValueError('cleanup local roots overlap source or NAS archive')
    archive = _latest(records, identity, 'nas') or cleanup.get('archive', {})
    if archive.get('status') != 'complete' or archive.get('identity') != identity:
        raise ValueError('cleanup NAS recovery evidence is incomplete or unrelated')
    archive_root = nas_root / task / group
    if any(not Path(row.get('destination', '')).resolve().is_relative_to(archive_root)
           for row in archive.get('files', {}).values()):
        raise ValueError('cleanup archive destination belongs to another folder')
    for row in paper['archive_files']:
        original = Path(row.get('source_path') or row['path']).resolve()
        copied = Path(row['path']).resolve()
        if (not original.is_relative_to(source)
                or not any(copied.is_relative_to(root) for root in (*roots, source))):
            raise ValueError('cleanup source or copy is outside this folder')
    if cleanup:
        if cleanup.get('identity') != identity or cleanup.get('archive') != archive:
            raise ValueError('cleanup frozen recovery state changed')
        files = list(cleanup['files'])
        if any(not any(Path(path).resolve().is_relative_to(root) for root in (*roots, source)) for path in files):
            raise ValueError('cleanup frozen file list is outside this folder')
        return {'routes': cleanup['routes']}, cleanup['body_targets'], cleanup['attachment_receipts'], archive, files
    # Persisted delivery state owns its backend; do not reconstruct an article package.
    prefix = 'rag-qdrant' if _latest(records, identity, 'rag-qdrant-body') is not None else 'rag'
    targets = _latest(records, identity, prefix + '-body') or {}
    attachments = _latest(records, identity, prefix + '-attachments') or {}
    routes = [copy.deepcopy(row.get('formal_route')) for row in targets.values()]
    if not routes or any(not isinstance(route, dict) for route in routes):
        raise ValueError('cleanup RAG recovery evidence is incomplete')
    files = owned_work_files(roots, source, nas_root)
    return {'routes': routes}, targets, attachments, archive, files


def deliver_paper(*, identity, registered, settings, admission, limits, first_step_work_root,
                  retry_failed=False, cleanup_only=False):
    """Run after W2 from a worker thread; all callbacks bind to Prefect automatically.

    ``settings`` is the original configured Settings, and ``first_step_work_root``
    is the actual parent passed to classify_and_register, not a source directory.
    """
    if not limits.get('cleanup') or set(limits['cleanup']) & set(limits['nas']):
        raise ValueError('cleanup capacity is required and must be independent from NAS')
    records = read_receipts_sync(identity['task_run_id'])
    progress = FolderProgress(settings, identity, records, save_folder_completion)
    records = progress.records
    paper = registered['paper']
    handoff = _latest(records, identity, 'postprocessing_complete')
    cleanup = _latest(records, identity, 'cleanup') or {}
    save_cleanup = progress.writer('cleanup')
    save_stage = progress.writer('delivery_stage')
    check_cleanup_running = getattr(admission, 'cleanup_running_sync', admission.running_sync)

    def mark_stage(stage):
        if not save_stage({'identity': copy.deepcopy(identity), 'stage': stage}):
            raise ValueError('local delivery state was not saved')

    def complete_stage(kind, data):
        # Cached results can still need their original completion receipt.
        # A published receipt is a read; a missing receipt requires admission.
        if kind not in progress.value['published'] and kind not in progress.remote_complete:
            if hasattr(admission, 'admit_sync') and not admission.admit_sync():
                raise InterruptedError('大工单已暂停，完成回执等待归队')
        progress.complete(kind, data)

    archive = _latest(records, identity, 'nas') or {}
    if cleanup_only or cleanup or archive.get('status') == 'complete' or 'nas' in progress.remote_complete:
        packet, targets, attachments, archive, files = _delivered_cleanup_inputs(
            identity, registered, settings, first_step_work_root, progress, records, cleanup)
        if cleanup.get('status') == 'complete':
            complete_stage('cleanup', cleanup)
            return {'status': 'complete', 'cleanup_status': 'complete'}
        _require_queueable_file_state(cleanup, 'cleanup', retry_failed)
        mark_stage('waiting_cleanup')
        with admitted_sync_slot(admission, limits['cleanup']):
            mark_stage('cleanup_in_progress')
            cleanup_paper(paper, identity, None, packet, targets,
                attachments, archive, files, cleanup,
                save_cleanup, check_cleanup_running, retry_failed=retry_failed)
        complete_stage('cleanup', cleanup)
        return {'status': 'complete', 'cleanup_status': 'complete'}

    legacy.bind(settings)
    from paperflow.package import validate_package
    from preextract_pipeline import offline_counter
    from preextract_publish import _Budget
    from .attachments import publish_attachments
    from .publication import publish_body_targets
    from .transport import ServerEffects, TaskCheckedClient

    first_root = _registration(identity, registered, settings, first_step_work_root)
    scoped = folder_settings(settings, identity['task_id'], identity['group_id'], paper)
    work_root = scoped.path('data_root')
    evidence = read_complete_handoff(identity, paper, records)
    handoff, body = evidence['handoff'], evidence['body_result']
    _work_paths(records, identity, handoff, work_root)
    if any(not Path(path).resolve().is_relative_to(work_root) for path in evidence['stage_work_files']):
        raise ValueError('server paper work inventory is outside its owned root')
    nas_root = settings.path('nas_destination')
    owned_work_files((first_root, work_root), paper['person_dir'], nas_root)

    catalog = read_json(settings.path('catalog_path'))
    prepared = {'task': body['task'], 'validation': body['validation'], 'catalog': catalog}
    packet = validate_package(body['package_path'], body['task'], catalog, strict_seal=True)
    from paperflow_v3.stages.raw_publication import retention_routes
    packet = retention_routes(packet)
    targets = _latest(records, identity, 'rag-body') or {}
    attachments = _latest(records, identity, 'rag-attachments') or {}
    from .backend import progress_kinds, select_backend
    backend_name=select_backend(settings.values.get('rag_backend','ragflow'),records,targets,attachments)
    old_complete = ('rag_complete' in progress.remote_complete
                    and _rag_complete(paper, identity, packet, targets, attachments))
    body_kind, attachments_kind, binding_kind = 'rag-body', 'rag-attachments', 'rag-backend'
    if backend_name == 'qdrant' and not old_complete:
        body_kind, attachments_kind, binding_kind = progress_kinds(records, targets, attachments)
        targets = _latest(records, identity, body_kind) or {}
        attachments = _latest(records, identity, attachments_kind) or {}
    archive = _latest(records, identity, 'nas') or {}
    _require_queueable_file_state(archive, 'archive', retry_failed)
    if 'rag_complete' in progress.remote_complete and not _rag_complete(paper, identity, packet, targets, attachments):
        raise ValueError('completed RAG folder is missing local recovery state; do not resend')
    if 'nas' in progress.remote_complete and archive.get('status') != 'complete':
        raise ValueError('completed NAS folder is missing local recovery state; do not copy again')

    def save_event(event_id, kind, data):
        return progress.save(kind, data, event_id)

    effects = ServerEffects(identity, records, save_event, retry_failed=retry_failed)
    if backend_name != 'qdrant' or old_complete:
        if any(value['status'] == 'rag_effect_intent' for value in effects.states.values()):
            raise ValueError('a previous RAG request outcome is unknown; reconciliation required')
        if not retry_failed and any(value['status'] == 'rag_effect_failed' for value in effects.states.values()):
            raise ValueError('a failed RAG request requires a user-selected retry')
    if not _rag_complete(paper, identity, packet, targets, attachments):
        mark_stage('waiting_rag_upload')
        with admitted_sync_slot(admission, limits['rag']):
            mark_stage('rag_in_progress')
            # Admission is per folder. Pause stops new folders; this admitted folder finishes.
            backend = UnifiedBackend(scoped, invoke=None)
            if backend_name=='qdrant':
                from .backend import qdrant_api_key
                from paperflow_qdrant.delivery import deliver_folder
                from paperflow_qdrant.identity import stable_id
                binding={'backend':'qdrant','folder_id':stable_id(identity['task_id'],identity['group_id'],
                    prepared['task']['source_sha256'],packet['manifest_sha256']),
                    'gateway_url':scoped['qdrant_gateway_url']}
                previous=_latest(records,identity,binding_kind)
                if previous and previous!=binding: raise ValueError('folder RAG binding changed')
                if not previous: progress.save(binding_kind,binding)
                counter=offline_counter(backend.runtime,scoped.path('release_root')/'core') if paper['attachments'] else None
                try:
                    targets,attachments=deliver_folder(paper=paper,identity=identity,task=prepared['task'],
                        packet=packet,handoff=handoff,budget=_Budget(counter) if counter else None,
                        base_url=scoped['qdrant_gateway_url'],api_key=qdrant_api_key(scoped),
                        check_running=admission.running_sync)
                finally:
                    if counter: counter.close()
                progress.save(body_kind,targets)
                progress.save(attachments_kind,attachments)
            else:
                client = TaskCheckedClient(legacy.rag_client(scoped), admission.running_sync, effects)
                roots = {row['file_id']: receipt_root_for(backend, paper, row) for row in paper['attachments']}
                if any(not Path(root).resolve().is_relative_to(work_root) for root in roots.values()):
                    raise ValueError('historical attachment receipts require manual review')
                publish_body_targets(paper, identity, handoff, prepared, packet, client, targets,
                                     progress.writer('rag-body'), admission.running_sync, retry_failed=retry_failed)
                if paper['attachments'] and any(attachments.get(row['file_id'], {}).get('delivery_state') != 'complete'
                                                for row in paper['attachments']):
                    folder = _original_folder(client, roots, packet, scoped)
                    counter = offline_counter(backend.runtime, scoped.path('release_root') / 'core')
                    try:
                        publish_attachments(paper, identity, handoff, packet, targets, client, roots, _Budget(counter),
                            folder, True, attachments, progress.writer('rag-attachments'), admission.running_sync,
                            retry_failed=retry_failed)
                    finally:
                        counter.close()
    if (backend_name != 'qdrant' or old_complete) and any(
            value['status'] in ('rag_effect_intent', 'rag_effect_failed') for value in effects.states.values()):
        raise ValueError('a RAG request is failed or unknown; archive and cleanup are blocked')
    if not _rag_complete(paper, identity, packet, targets, attachments):
        raise ValueError('RAG targets are not complete; archive and cleanup are blocked')
    complete_stage('rag_complete', {'identity': identity, 'status': 'complete',
                                     'body_targets': targets, 'attachment_receipts': attachments})
    if archive.get('status') != 'complete' or archive.get('identity') != identity:
        mark_stage('waiting_nas_archive')
        with admitted_sync_slot(admission, limits['nas']):
            mark_stage('nas_in_progress')
            save_archive(paper, identity, handoff, nas_root / identity['task_id'] / identity['group_id'], archive,
                         progress.writer('nas'), admission.running_sync, retry_failed=retry_failed)
    if archive.get('status') != 'complete' or archive.get('identity') != identity:
        raise ValueError('archive is not complete; cleanup is blocked')
    complete_stage('nas', archive)
    extra = owned_work_files((first_root, work_root), paper['person_dir'], nas_root)
    if not {Path(path).resolve() for path in evidence['stage_work_files']}.issubset(
            {Path(path).resolve() for path in extra}):
        raise ValueError('server paper work inventory is incomplete locally')
    mark_stage('waiting_cleanup')
    with admitted_sync_slot(admission, limits['cleanup']):
        mark_stage('cleanup_in_progress')
        cleanup_paper(paper, identity, handoff, packet, targets, attachments, archive, extra, cleanup,
                      save_cleanup, check_cleanup_running, retry_failed=retry_failed)
    complete_stage('cleanup', cleanup)
    return {'status': 'complete', 'cleanup_status': 'complete'}
