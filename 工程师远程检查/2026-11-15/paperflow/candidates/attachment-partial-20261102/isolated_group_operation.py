"""Run the RAG or NAS part of one already prepared isolated paper group."""
from pathlib import Path

from isolated_archive import archive_test_copy
from isolated_cleanup import cleanup_test_copy
from isolated_group_rag import publish_group
from task_effect_admission import task_effect_admission


def execute_group_stage(stage, job, context):
    if 'reuse_receipts' in context:
        receipts = context['reuse_receipts']
        if stage == 'rag':
            return {'rag_receipt_path': receipts['rag'], 'copy_receipt_path': receipts['copy'],
                    'reused_isolated_test': True}
        if stage == 'nas':
            return {'archive_receipt_path': receipts['archive'], 'cleanup_receipt_path': receipts['cleanup'],
                    'reused_isolated_test': True}
        raise ValueError('unknown folder stage')
    copy_path = Path(context['copy_receipt_path'])
    work = Path(context['work_root']) / job['group_id']
    def admit(*_):
        return task_effect_admission(context['database'], job['task_id'], job['run_generation'], started=True)

    if stage == 'rag':
        counter = None
        if context['attachments']:
            from preextract_pipeline import offline_counter
            from preextract_publish import _Budget
            counter = offline_counter(context['runtime'], context['paperflow_root'])
        try:
            rag = publish_group(job, copy_path, context['prepared'], context['packet'], context['plan'],
                                context['folder'], context['attachments'], context['profile_sha256'], context['raw'],
                                context['journal'], admit, context['require_runtime'], context['config'],
                                _Budget(counter) if counter else None, work / 'rag',
                                failed_attachments=context.get('failed_attachments', []))
        finally:
            if counter:
                counter.close()
        return {'rag_receipt_path': rag['receipt_path'], 'copy_receipt_path': str(copy_path)}
    if stage != 'nas' or Path(job['copy_receipt_path']) != copy_path or not job['rag_receipt_path']:
        raise ValueError('NAS stage has no matching completed group RAG')
    archive_work = work / 'archive'
    nas_dir = Path(context['nas_root']) / job['group_id']
    def archive_admission():
        with admit() as permit:
            permit.check_current()
        return True
    archive_test_copy(copy_path, archive_work, nas_dir, admit=archive_admission)
    cleanup_path = work / 'cleanup.json'
    cleanup_test_copy(copy_path, job['rag_receipt_path'], archive_work, context['test_root'], nas_dir,
                      cleanup_path, admit=admit)
    return {'archive_receipt_path': str(archive_work / 'archive-complete.json'),
            'cleanup_receipt_path': str(cleanup_path)}
