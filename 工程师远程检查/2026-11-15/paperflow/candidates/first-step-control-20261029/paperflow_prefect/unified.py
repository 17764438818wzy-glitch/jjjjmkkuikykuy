"""Connect the existing H2.1, publication and Stage5Receiver contracts."""
import asyncio
from pathlib import Path
import threading

from .backend import Backend
from .records import Records, identity, read_json, sha_file, write_json
from .stage3_handoff import read_receipt
from .workflow import needs_generated_title, prepare_and_generate_title

CONTRACT = 'paperflow-unified-five/1'


def received_completely(receipt):
    return (receipt.get('state') == '已接收' and not receipt.get('unresolved')
            and all(row.get('state') == '已接收' for row in receipt.get('items', [])))


async def execute_unified(paper, receipt, call, *, resume_rag=False, resume_stage5=False):
    """Preserve independent upstream results and join only an entire received paper."""
    title_generation = None
    if resume_stage5:
        stages = ('archive', 'cleanup', 'finish')
    else:
        if not resume_rag:
            needs_title = needs_generated_title(paper)
            title_ready = asyncio.Event()
            if not needs_title: title_ready.set()
            async def body_branch():
                nonlocal title_generation
                if paper.get('body') is None: return
                try:
                    title_generation = await prepare_and_generate_title(paper, call)
                finally:
                    title_ready.set()
                for stage in ('profile', 'package', 'publish_body'):
                    await call(stage, paper)
            async def attachment_branch():
                await title_ready.wait()
                gate = asyncio.Semaphore(20)
                async def identify(row):
                    async with gate: return await call('attachment', paper, row)
                results = await asyncio.gather(*(identify(row) for row in paper['attachments']), return_exceptions=True)
                if any(isinstance(result, BaseException) for result in results):
                    raise RuntimeError('附件识别存在未完成项')
            outcomes = await asyncio.gather(body_branch(), attachment_branch(), return_exceptions=True)
            failures = {name: type(value).__name__ for name, value in zip(('body', 'attachments'), outcomes)
                        if isinstance(value, BaseException)}
            if failures:
                result = dict(status='incomplete', paper_id=paper['paper_id'], failures=failures)
                if title_generation is not None: result['title_generation'] = title_generation
                return result
        if not received_completely(receipt) or paper.get('body') is None:
            return dict(status='incomplete', paper_id=paper['paper_id'],
                        failures={'intake': 'whole_paper_not_received'})
        stages = (('recover_body',) if resume_rag else ()) + ('publish_attachments', 'archive', 'cleanup', 'finish')
    for stage in stages:
        try: result = await call(stage, paper)
        except Exception as exc:
            return dict(status='incomplete', paper_id=paper['paper_id'], failures={stage: type(exc).__name__})
    if title_generation is not None and isinstance(result, dict):
        result = dict(result, title_generation=title_generation)
    return result


class UnifiedBackend(Backend):
    """Latest body and attachment adapters remain inherited and unchanged."""
    def paper_root(self, paper):
        # Receipt/evidence updates have their own stage records; model work stays
        # at the original identity selected by Backend.retained_work.
        return self.root / 'unified-papers' / paper['paper_id'] / paper['revision'] / identity(paper)[:24]

    def receive(self, receipt_path):
        self.settings.require_execution()
        receipt, paper = read_receipt(receipt_path)
        if Path(receipt['library_root']).resolve() != self.settings.path('library_root'):
            raise ValueError('接收回执必须属于当前已配置的输入资料库')
        if paper is None: return None
        root = self.paper_root(paper)
        binding = dict(schema=CONTRACT, receipt_path=str(Path(receipt_path).resolve()),
                       receipt_sha256=sha_file(receipt_path), receipt_id=receipt['receipt_id'])
        for name, value in (('received-inventory.json', paper), ('received.json', binding)):
            path = root / name
            if path.exists() and read_json(path) != value: raise ValueError('原接收身份或绑定发生改变')
            if not path.exists(): write_json(path, value)
        records = Records(root)
        if records.cached('intake') is None:
            records.complete('intake', dict(paper_id=paper['paper_id'], revision=paper['revision'],
                receipt_id=receipt['receipt_id'], attachment_count=len(paper['attachments'])),
                [root / 'received-inventory.json', root / 'received.json'])
        self.stops.setdefault(paper['paper_id'], threading.Event())
        return paper

    def receipt_for(self, paper):
        binding = read_json(self.paper_root(paper) / 'received.json')
        if sha_file(binding['receipt_path']) != binding['receipt_sha256']:
            raise ValueError('已绑定的接收回执发生改变')
        receipt, current = read_receipt(binding['receipt_path'])
        if current != paper or receipt['receipt_id'] != binding['receipt_id']:
            raise ValueError('接收回执不属于本篇')
        return receipt

    def confirm_history(self, paper, row):
        from .attachments import has_request_history
        from .request_admission import confirm_first_request, bind_work
        work = self.body_work(paper) if row['kind'] == 'body' else self.attachment_work(paper, row)
        if has_request_history(work) or (work / 'first-request.json').exists(): return
        if row.get('history') is None: raise ValueError('统一入口缺少逐项历史核对记录')
        roots = list(dict.fromkeys([str(self.root), *self.settings.values.get('history_roots', [])]))
        self.root.mkdir(parents=True, exist_ok=True)
        confirm_first_request(paper, row, work, history_roots=roots)
        bind_work(self.root, paper, row, row['kind'], work)

    def prepare(self, paper):
        work = self.body_work(paper)
        if Records(work).cached('profile') is None: self.confirm_history(paper, paper['body'])
        return super().prepare(paper)

    def attachment(self, paper, row):
        work = self.attachment_work(paper, row)
        if Records(work).cached('identify') is None: self.confirm_history(paper, row)
        return super().attachment(paper, row)

    def publish_body(self, paper):
        from .intake import verify_inventory
        from . import legacy
        from .rag_status import observe_client
        from .recovery import recover_publication
        from preextract_publish import deliver_publication
        self.settings.require_execution(); verify_inventory(paper)
        work = self.body_work(paper); path = work / 'delivery-receipt.json'
        prepared = read_json(self.prepared_path(paper))
        if prepared['task']['source_sha256'] != paper['body']['sha256']:
            raise ValueError('沿用的封包与正文来源不符')
        from .unified_guards import StopAwareClient
        client = observe_client(StopAwareClient(legacy.rag_client(self.settings),self.stops[paper['paper_id']]))
        result = (recover_publication(prepared, receipt_path=path, client=client) if path.exists() else
                  deliver_publication(prepared, config=None, receipt_path=path, client=client))
        if result.get('status') != 'complete' or result.get('stage') != 'verified':
            raise ValueError('正文入库尚未完整核验')
        summary = dict(status='verified', paper_id=paper['paper_id'], revision=paper['revision'],
                       targets=result['targets'], receipt=str(path), task_id=prepared['task']['task_id'],
                       manifest_sha256=result['manifest_sha256'])
        Records(self.paper_root(paper)).complete('rag-body', summary, [path, self.prepared_path(paper)])
        return summary

    def prepared_path(self, paper):
        work = self.body_work(paper)
        paths = [p for p in (work / 'prepared-publication.json', work / 'private/prepared-publication.json') if p.is_file()]
        if len(paths) > 1 and sha_file(paths[0]) != sha_file(paths[1]):
            raise ValueError('存在两个不同的原封包记录，不能重新封包')
        return paths[0] if paths else work / 'prepared-publication.json'

    def package(self, paper):
        path = self.prepared_path(paper)
        if not path.is_file(): return super().package(paper)
        from paperflow.package import validate_package
        prepared = read_json(path)
        packet = validate_package(prepared['package_path'], prepared['task'], prepared['catalog'], strict_seal=True)
        if (prepared['task']['source_sha256'] != paper['body']['sha256'] or
                packet['manifest_sha256'] != prepared['validation']['manifest_sha256']):
            raise ValueError('原封包与正文来源不符')
        return dict(package_path=prepared['package_path'], task_id=prepared['task']['task_id'],
                    manifest_sha256=packet['manifest_sha256'], reused=True)

    def recover_body(self, paper):
        if not (self.body_work(paper) / 'delivery-receipt.json').is_file():
            raise ValueError('恢复缺少原入库回执，不能推测首次上传')
        return self.publish_body(paper)

    def publish_attachments(self, paper):
        self.settings.require_execution()
        root = self.paper_root(paper)
        if not paper['attachments']:
            result = dict(status='verified', attachments=0, receipt_paths=[], targets=[])
            Records(root).complete('rag-attachments', result)
            return result
        from .unified_attachments import publish_cards
        result = publish_cards(self, paper)
        Records(root).complete('rag-attachments', result, result['receipt_paths'])
        return result

    def receiver(self, paper):
        from .stage5_handoff import Stage5Receiver
        return Stage5Receiver(self.root, self.settings.path('library_root'), self.settings.path('nas_destination'),
            enabled=True, reserve_bytes=self.settings['nas_reserve_bytes'],
            isolation_root=self.settings.values.get('acceptance_isolation_root'))

    def stage5_work(self, paper):
        binding = Records(self.paper_root(paper)).cached('stage5-binding')
        if not binding: raise ValueError('尚未完成整篇第五步准入')
        from .stage5_handoff import admitted_handoff
        frozen = admitted_handoff(binding['work'])
        if frozen['handoff']['paper'] != paper: raise ValueError('第五步绑定不属于当前接收版本')
        return Path(binding['work'])

    def archive(self, paper):
        self.settings.require_execution()
        records = Records(self.paper_root(paper)); receiver = self.receiver(paper)
        if records.cached('stage5-binding'):
            return receiver.archive(self.stage5_work(paper))
        receipt = self.receipt_for(paper)
        if not received_completely(receipt): raise ValueError('整组尚未接收完成')
        body = records.cached('rag-body'); attachments = records.cached('rag-attachments')
        if not body or not attachments: raise ValueError('整篇入库回执不完整')
        from .nas_handoff import build_nas_handoff
        binding = read_json(self.paper_root(paper) / 'received.json')
        handoff = build_nas_handoff(binding['receipt_path'], body_receipt_path=body['receipt'],
            body_prepared_path=self.prepared_path(paper),
            attachment_receipt_paths=attachments['receipt_paths'], native_settings=self.settings,
            retained_evidence_paths=self.retained_evidence(paper))
        if handoff['status'] != 'ready': raise ValueError('整篇入库证据不足：' + ','.join(handoff['reasons']))
        accepted = receiver.receive(handoff)
        if accepted['status'] != 'ready': raise ValueError('第五步未接收整篇')
        path = self.paper_root(paper) / 'stage5-binding.json'; write_json(path, accepted)
        records.complete('stage5-binding', accepted, [path])
        return receiver.archive(accepted['work'])

    def retained_evidence(self, paper):
        paths = []
        works = [self.body_work(paper), *(self.attachment_work(paper, row) for row in paper['attachments'])]
        for work in works:
            for name in ('material.json', 'candidate.json', 'validation.json', 'description.json', 'first-request.json'):
                if (work / name).is_file(): paths.append(work / name)
            paths.extend(p for p in work.glob('model/*') if p.is_file() and p.suffix in {'.json', '.jsonl', '.txt', '.md'})
        return paths

    def cleanup(self, paper):
        self.settings.require_execution()
        if self.settings.values.get('local_cleanup_enabled') is not True:
            return dict(status='cleanup_pending', removed_files=0, reason='local_cleanup_disabled')
        return self.receiver(paper).cleanup(self.stage5_work(paper),stop=self.stops[paper['paper_id']])

    def finish(self, paper):
        self.settings.require_execution()
        if self.settings.values.get('local_cleanup_enabled') is not True:
            return dict(status='incomplete', paper_id=paper['paper_id'], archive_status='verified',
                        failures={'cleanup':'local_cleanup_disabled'}, contract_version=CONTRACT)
        from .stage5_handoff import finish_stage5
        result = finish_stage5(self.stage5_work(paper), self.receiver(paper).coordinator)
        return dict(result, contract_version=CONTRACT)
