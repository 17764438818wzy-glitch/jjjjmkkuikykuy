"""Real unified receipt/backend/admission with synthetic upstream RAG only."""
from pathlib import Path
import tempfile
from nas_handoff_fixture import fixture, build
from paperflow_prefect.settings import Settings
from paperflow_prefect.legacy import bind
from paperflow_prefect.records import Records, read_json, sha_file, write_json
from paperflow_prefect.unified import UnifiedBackend
from paperflow_prefect.stage5_handoff import Stage5Receiver


class Fixture:
    def __init__(self):
        bind(Settings.load())
        self.temp=tempfile.TemporaryDirectory(prefix='unified-acceptance-')
        self.root=Path(self.temp.name).resolve();self.f=fixture(self.root)
        self.library=self.root/'library';self.person=self.library/'person'
        self.data=self.root/'data';self.nas=self.root/'nas'
        self.values=dict(Settings.load().values,execution_enabled=True,local_cleanup_enabled=True,
            library_root=str(self.library),data_root=str(self.data),nas_destination=str(self.nas),
            acceptance_isolation_root=str(self.root),nas_reserve_bytes=0)
        self.backend=UnifiedBackend(Settings(self.values))
        self.receipt_path=self.f['intake_receipt_path']
        self.original=self.backend.receive(self.receipt_path)
        self.receiver=Stage5Receiver(self.data,self.library,self.nas,enabled=True,isolation_root=self.root,reserve_bytes=0)
        accepted=self.receiver.receive(build(self.f));self.work=Path(accepted['work'])
        path=self.backend.paper_root(self.original)/'stage5-binding.json';write_json(path,accepted)
        Records(self.backend.paper_root(self.original)).complete('stage5-binding',accepted,[path])
        self.archive=self.receiver.archive(self.work)
        self.paper=read_json(self.work/'inventory.json')
        self.body=Path(self.paper['body']['path']);self.attachment=Path(self.paper['attachments'][0]['path'])
        self.protected={p:sha_file(p) for p in [self.receipt_path,*self.f['attachment_receipt_paths']]}
    def manager(self,enabled=True):
        from paperflow_prefect.cleanup import CleanupCoordinator
        return CleanupCoordinator(self.data,self.library,self.nas,enabled=enabled)
    def assert_protected(self,case):case.assertEqual({p:sha_file(p) for p in self.protected},self.protected)
    def close(self):self.temp.cleanup()
