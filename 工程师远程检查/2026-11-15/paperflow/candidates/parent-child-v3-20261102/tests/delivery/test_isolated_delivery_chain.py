"""Real delivery functions with synthetic RAG, files and a temporary Prefect server."""

import asyncio
from pathlib import Path
import shutil
from uuid import uuid4

from prefect import flow
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness

from paperflow_v3.control.receipts import read_receipts, save_receipt_sync
from paperflow_v3.delivery.archive import save_archive
from paperflow_v3.delivery.attachments import publish_attachments
from paperflow_v3.delivery.cleanup import cleanup_paper
from paperflow_v3.delivery.publication import publish_body_targets
from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient
from preextract_publish import _Budget
from test_attachment_ingest import handoff as attachment_handoff
from test_attachment_publish import Counter, FakeStore


def test_synthetic_paper_flows_through_formal_publishers_archive_and_cleanup(tmp_path):
    source, copies, work, nas = [tmp_path / name for name in ('source', 'copies', 'work', 'nas')]
    for directory in (source, copies, work, nas):
        directory.mkdir()
    item = attachment_handoff(source)
    (source / 'body.docx').write_bytes(b'synthetic body')
    (source / 'trash.txt').write_bytes(b'keep this unrelated file')
    shutil.copyfile(source / 'body.docx', copies / 'body.docx')
    shutil.copyfile(item['source_path'], copies / 'attachment.txt')
    paper = {'paper_id': item['paper_id'], 'group_id': 'group-1', 'person_dir': str(source),
             'excluded': ['trash.txt'], 'body': {'file_id': 'body', 'sha256': 'b' * 64},
             'attachments': [{'file_id': item['attachment_id'], 'sha256': item['source_sha256']}],
             'archive_files': [
                 {'file_id': 'body', 'kind': 'body', 'source_path': str(source / 'body.docx'),
                  'path': str(copies / 'body.docx'), 'relative': '正文/body.docx'},
                 {'file_id': item['attachment_id'], 'kind': 'attachment', 'source_path': item['source_path'],
                  'path': str(copies / 'attachment.txt'), 'relative': '附件/attachment.txt'},
             ]}
    identity = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': item['paper_id'], 'receipt_id': 'r',
                'attempt_id': 'a', 'parent_run_id': 'p', 'task_run_id': 'pending'}
    handoff = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': dict(identity),
               'validation_status': 'passed', 'body': {'package_path': 'pkg', 'manifest_sha256': 'd' * 64},
               'attachments': [item]}
    routes = [{'code': '081301', 'name': '化学工程与工艺', 'paper_dataset_id': 'body-1', 'image_dataset_id': 'ds-main'},
              {'code': '080206', 'name': '过程装备与控制工程', 'paper_dataset_id': 'body-2', 'image_dataset_id': 'ds-ref'}]
    packet = {'routes': routes, 'manifest_sha256': 'd' * 64,
              'manifest': {'files': [{'path': 'profile.json', 'sha256': 'c' * 64}]},
              'root': str(work), 'profile': {'article': {'title': 'Synthetic paper'},
                                             'routing_candidates': [{'code': row['code']} for row in routes]},
              'tree': {'nodes': []}, 'chunks': [{'id': 'c1', 'content': 'Synthetic text.', 'kind': 'body'}],
              'assets': []}
    prepared = {'task': {'task_id': 'task-1', 'source_sha256': 'b' * 64, 'revision': 1},
                'validation': {'embedding': {'admission_verified': True}}, 'catalog': {'majors': list(routes)}}
    class JsonRagStore(FakeStore):
        def request(self, method, endpoint, body=None):
            result = super().request(method, endpoint, body)
            if method == 'PATCH' and isinstance(result, dict):
                return {'id': result['id']}
            return result

    client, body_targets, attachments, archive, cleanup = JsonRagStore(), {}, {}, {}, {}

    @flow(persist_result=False)
    async def child():
        child_id = FlowRunContext.get().flow_run.id
        identity['task_run_id'] = str(child_id)
        handoff['identity'] = dict(identity)

        def save(kind):
            def persist(data):
                return save_receipt_sync(task_id=identity['task_id'], folder_id=identity['group_id'],
                                         child_flow_run_id=child_id, event_id=uuid4(), kind=kind, data=data)
            return persist

        def save_event(event_id, kind, data):
            return save_receipt_sync(task_id=identity['task_id'], folder_id=identity['group_id'],
                                     child_flow_run_id=child_id, event_id=event_id, kind=kind, data=data)

        events = await read_receipts(child_id)
        guarded = TaskCheckedClient(client, lambda: True, ServerEffects(identity, events, save_event))

        await asyncio.to_thread(publish_body_targets, paper, identity, handoff, prepared, packet, guarded,
                                body_targets, save('rag-body'), lambda: True)
        await asyncio.to_thread(publish_attachments, paper, identity, handoff, packet, body_targets, guarded,
                                work / 'attachment-receipts', _Budget(Counter()), 'folder-main', True,
                                attachments, save('rag-attachments'), lambda: True)
        await asyncio.to_thread(save_archive, paper, identity, handoff, nas / 'paper-1', archive,
                                save('nas'), lambda: True)
        extra_files = [str(p) for p in work.rglob('*') if p.is_file()] + [item['md_path']]
        await asyncio.to_thread(cleanup_paper, paper, identity, handoff, packet, body_targets, attachments,
                                archive, extra_files, cleanup, save('cleanup'), lambda: True)
        return child_id

    async def exercise():
        child_id = await child()
        records = await read_receipts(child_id)
        assert {row['kind'] for row in records} >= {
            'rag-body', 'rag-attachments', 'nas', 'cleanup', 'rag_effect_intent', 'rag_effect_returned'}
        assert any(row['kind'] == 'cleanup' and row['data']['status'] == 'complete' for row in records)
        assert cleanup['status'] == 'complete'
        assert not (source / 'body.docx').exists() and not Path(item['source_path']).exists()
        assert not (copies / 'body.docx').exists() and not (copies / 'attachment.txt').exists()
        assert not Path(item['md_path']).exists() and not any(path.is_file() for path in work.rglob('*'))
        assert (source / 'trash.txt').is_file()
        assert (nas / 'paper-1/正文/body.docx').read_bytes() == b'synthetic body'
        assert (nas / 'paper-1/附件/attachment.txt').read_bytes() == b'print(1)'
        assert len(client.files) == 1

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(exercise())
