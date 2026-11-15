"""Synthetic local files and explicitly synthetic upstream RAG receipts."""
from pathlib import Path
import tempfile
import zipfile
from paperflow_prefect.records import Records, identity, write_json, sha_file
from paperflow_prefect.settings import Settings
from paperflow_prefect.legacy import bind
from paperflow_prefect.intake import inspect_person, object_fingerprint
from paperflow_prefect.archive import archive_originals


class Fixture:
    def __init__(self):
        bind(Settings.load())
        self.temp = tempfile.TemporaryDirectory(prefix='cleanup-fixture-')
        self.root = Path(self.temp.name).resolve()
        self.library = self.root / 'new-library'
        self.person = self.library / 'school/person'
        self.data = self.root / 'records'
        self.work = self.data / 'revisions/original'
        self.nas = self.root / 'nas-fixture'
        for name in ('正文', '附件', '垃圾材料'):
            (self.person / name).mkdir(parents=True)
        self.body = self.person / '正文/paper.txt'
        self.body.write_text('Synthetic paper for isolated acceptance.\n', encoding='utf8')
        self.attachment = self.person / '附件/source.zip'
        with zipfile.ZipFile(self.attachment, 'w') as archive:
            archive.writestr('README.txt', 'A complete original synthetic archive')
            archive.writestr('data.csv', 'x,y\n1,2\n')
        self.garbage = self.person / '垃圾材料/report.txt'
        self.garbage.write_bytes(b'keep garbage unchanged')
        self.paper = inspect_person(self.person, self.library)
        for row in self.paper['archive_files']:
            row['object_fingerprint'] = object_fingerprint(Path(row['path']).stat())
        self.work.mkdir(parents=True)
        write_json(self.work / 'inventory.json', self.paper)
        self.records = Records(self.work)
        self.records.complete('intake', {'paper_id': self.paper['paper_id']}, [self.work / 'inventory.json'])
        self.record_rag()
        result = archive_originals(self.paper, self.work / 'archive', self.nas, reserve_bytes=0)
        self.records.complete('nas', result, [self.work / 'archive/nas-receipt.json'])
        self.saved = self.data / 'model-answer-package-request-receipt.bin'
        self.saved.write_bytes(b'preserve model answers, sealed packages, request and delivery records')
        self.protected = {str(p): sha_file(p) for p in (self.garbage, self.saved)}

    def record_rag(self):
        paper = self.paper
        context = {'paper_id': paper['paper_id'], 'revision': paper['revision'],
                   'inventory_sha256': identity(paper['archive_files']),
                   'source_sha256': paper['body']['sha256']}
        receipt = {'status': 'complete', 'stage': 'verified', 'task_id': 'fixture-task',
                   'manifest_sha256': 'a' * 64, 'targets': {'fixture': {'body': {
                       'dataset_id': 'fixture-dataset', 'document_id': 'fixture-body',
                       'identity': {'paperflow_source_sha256': paper['body']['sha256']}}}},
                   'fixture': True}
        path = self.work / 'body-delivery.json'
        write_json(path, receipt)
        self.records.complete('rag-body', {**context, **receipt, 'status': 'verified', 'receipt': str(path)}, [path])
        attachment_receipt = {'status': 'verified', 'attachment_id': paper['attachments'][0]['file_id'],
                              'paper_id': paper['paper_id'], 'dataset_id': 'fixture-dataset',
                              'parent_body_document_id': 'fixture-body', 'document_id': 'fixture-attachment',
                              'fixture': True}
        attachment_path = self.work / 'attachment-delivery.json'
        write_json(attachment_path, attachment_receipt)
        self.records.complete('rag-attachments', {**context, 'status': 'verified', 'attachments': 1,
                              'targets': [attachment_receipt]}, [attachment_path])

    def manager(self, enabled=True):
        from paperflow_prefect.cleanup import CleanupCoordinator
        return CleanupCoordinator(self.data, self.library, self.nas, enabled=enabled)

    def assert_protected(self, case):
        case.assertEqual({p: sha_file(p) for p in self.protected}, self.protected)

    def close(self):
        self.temp.cleanup()


class Crash(BaseException):
    pass
