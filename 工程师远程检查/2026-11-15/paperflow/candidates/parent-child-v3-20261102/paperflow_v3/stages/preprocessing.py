"""Prepare one already-registered folder without the old intake queue."""
from pathlib import Path

from paperflow_prefect.attachment_queue import settings_for_verified_receipt
from paperflow_prefect.backend import Backend
from paperflow_prefect.body_template import bind_work_template
from paperflow_prefect.local_preprocessing import (
    _available_material, _capacity_and_projection, _file_ref, _verified_aux,
)
from paperflow_prefect.llm_queue.segment_plan_contract import describe_plan
from paperflow_prefect.records import Records, read_json, sha_file, write_json
from paperflow_prefect.unified import UnifiedBackend


def _first_step_cached_docx(registered, first_step_work_root):
    if first_step_work_root is None:
        return None
    group, receipt = registered['group_id'], registered['receipt_id']
    if Path(group).name != group or group in ('.', '..'):
        return None
    root = Path(first_step_work_root).resolve() / group
    expected = root / 'registration' / 'receipts' / (receipt + '.json')
    if Path(registered['receipt_path']).resolve() != expected.resolve():
        return None
    from material_conversion import ConversionPool
    pool = ConversionPool(root / 'conversion' / 'office' / 'private' / 'material-conversion')
    source_sha = registered['paper']['body']['sha256']
    for engine in ('libreoffice', 'wps'):
        for report_path in sorted((pool.root / 'cache' / engine).glob('*/identity.json')):
            try:
                report = read_json(report_path)
                if report.get('engine') != engine or report.get('source_sha256') != source_sha:
                    continue
                cached = pool._cached(engine, report_path.parent.name, 'docx')
                if cached is not None:
                    return cached
            except (OSError, ValueError, AttributeError):
                continue
    return None


def prepare_registered_folder(settings, registered, *, attempt_id, first_step_work_root=None):
    """Use the existing document parser and handoff format for one folder.

    ``registered`` is the already-verified H2.1 record supplied by the parent
    flow. Stage admission and server-side receipt persistence are its caller's
    responsibility; this function never claims a queue or starts another run.
    """
    backend = UnifiedBackend(settings_for_verified_receipt(settings, registered), invoke=None)
    paper = backend.receive(registered['receipt_path'])
    if paper != registered['paper'] or not paper or not paper.get('body'):
        raise ValueError('登记回执与论文正文不一致')

    handoff_dir = backend.paper_root(paper) / 'local-preprocessing'
    manifest_path = handoff_dir / (registered['receipt_id'] + '-handoff.json')
    if manifest_path.exists():
        saved = read_json(manifest_path)
        if (any(saved.get(key) != registered[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id'))
                or saved.get('attempt_id') != attempt_id or saved.get('body') != paper['body']
                or saved.get('attachments') != paper['attachments']
                or saved.get('receipt') != {'path': registered['receipt_path'],
                                            'sha256': registered['receipt_sha256']}):
            raise ValueError('已有前处理交接与论文身份不一致')
        return {'status': 'preprocessing_complete', 'task_id': registered['task_id'],
                'group_id': registered['group_id'], 'paper_id': registered['paper_id'],
                'receipt_id': registered['receipt_id'], 'attempt_id': attempt_id,
                'manifest_path': str(manifest_path.resolve()), 'manifest_sha256': sha_file(manifest_path)}

    cached_docx = _first_step_cached_docx(registered, first_step_work_root)
    result = (Backend.prepare(backend, paper, preconverted_docx=cached_docx)
              if cached_docx is not None else Backend.prepare(backend, paper))
    material_path = Path(result['material']).resolve(strict=True)
    work = material_path.parent
    material = read_json(material_path)
    prior_profile = Records(work).cached('profile')
    model_dir = work / 'model'
    history = ('complete' if prior_profile is not None else
               'existing' if model_dir.is_dir() and any(model_dir.iterdir()) else 'not_started')
    available, integrity_gaps = _available_material(material, paper)
    template_ref = bind_work_template(work, backend.settings.path('template_path'))
    projection, gaps, capacity, plan = _capacity_and_projection(available, work, backend)
    gaps = integrity_gaps + gaps
    projection_path = handoff_dir / (registered['receipt_id'] + '-reading.json')
    if projection_path.exists():
        if read_json(projection_path) != projection:
            raise ValueError('已有前处理阅读材料与当前材料不一致')
    else:
        write_json(projection_path, projection)
    projection_ref = _file_ref(projection_path)
    if capacity.get('status') == 'planned':
        capacity = dict(capacity, plan_identity=describe_plan(
            plan, source_sha256=paper['body']['sha256'], reading_sha256=projection_ref['sha256']))
    manifest = {
        'schema': 'paperflow-local-preprocessing-handoff/1',
        **{key: registered[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
        # This is the existing handoff format's consumer marker. No old queue
        # or consumer is started or consulted by this adapter.
        'consumer_id': 'prefect-local-preprocessing', 'attempt_id': attempt_id,
        'receipt': {'path': registered['receipt_path'], 'sha256': registered['receipt_sha256']},
        'revision': paper['revision'], 'body': paper['body'], 'attachments': paper['attachments'],
        'body_work': str(work), 'profile_history': history, 'completed_profile': prior_profile,
        'template_ref': template_ref,
        'material': _file_ref(material_path), 'reading_material': projection_ref,
        'body_text': _verified_aux(work / 'private' / 'body.txt', material, gaps, '正文文本'),
        'index': _verified_aux(work / 'private' / 'index.json', material, gaps, '正文索引'),
        'gaps': gaps, 'capacity': capacity, 'planning_context': {},
    }
    image_report = work / 'private/request-images.json'
    if image_report.is_file():
        manifest['image_report'] = _file_ref(image_report)
    write_json(manifest_path, manifest)
    return {'status': 'preprocessing_complete', 'task_id': registered['task_id'],
            'group_id': registered['group_id'], 'paper_id': registered['paper_id'],
            'receipt_id': registered['receipt_id'], 'attempt_id': attempt_id,
            'manifest_path': str(manifest_path.resolve()), 'manifest_sha256': sha_file(manifest_path)}
