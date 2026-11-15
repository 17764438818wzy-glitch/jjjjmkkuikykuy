"""Five-step business adapters. Only constructed after the local execution guard."""
from pathlib import Path
from contextlib import nullcontext
import threading
from . import body, legacy
from .attachments import identify_attachment
from .archive import archive_originals
from .delivery import admit_descriptions, description_content, publish_description
from .intake import inspect_person, verify_inventory
from .records import Records, identity, read_json, sha_file, write_json
from .resources import LocalPhase
from .rag_status import observe_client


class Backend:
    def __init__(self, settings, invoke=None, local_scope=nullcontext):
        settings.require_execution()
        legacy.bind(settings)
        self.settings = settings
        self.catalog = read_json(settings.path('catalog_path'))
        self.runtime = read_json(settings.path('runtime_config'))
        self.root = settings.path('data_root')
        self.model_signature = identity(read_json(settings.path('model_config')))
        self.body_signature = identity({
            'adapter': 'body-adapter/1', 'model': self.model_signature,
            'template': sha_file(settings.path('template_path')),
            'catalog': sha_file(settings.path('catalog_path')),
            'source': sha_file(settings.path('source_manifest')),
            'max_pages': settings['max_pages'],
            'runtime': sha_file(settings.path('runtime_config'))})[:24]
        self.invoke = invoke
        self.local_scope = local_scope
        self.stops = {}

    def intake(self, directory):
        self.settings.require_execution()
        paper = inspect_person(directory, self.settings.path('library_root'))
        self.stops.setdefault(paper['paper_id'], threading.Event())
        root = self.paper_root(paper)
        path = root / 'inventory.json'
        if path.is_file() and read_json(path) != paper: raise ValueError('原冻结清单与当前输入不符')
        write_json(path, paper)
        Records(root).complete('intake', {'paper_id': paper['paper_id'], 'revision': paper['revision'],
            'attachment_count': len(paper['attachments']), 'excluded_count': len(paper['excluded'])}, [path])
        return paper

    def paper_root(self, paper):
        return self.root / 'papers' / paper['paper_id'] / 'revisions' / paper['revision'] / self.body_signature

    def body_work(self, paper):
        # Attachment changes never invalidate a completed body model result.
        key = identity([paper['body']['sha256'], paper['body']['relative'], self.body_signature])[:24]
        return self.root / 'papers' / paper['paper_id'] / 'body' / key

    def attachment_work(self, paper, row):
        key = identity(['attachment-adapter/1', self.model_signature,
                        sha_file(self.settings.path('source_manifest'))])[:24]
        return self.root / 'papers' / paper['paper_id'] / 'attachments' / row['file_id'] / key

    def _cached_stage(self, root, stage, operation, artifacts=lambda r: ()):
        self.settings.require_execution()
        records = Records(root)
        cached = records.cached(stage)
        if cached is not None: return cached
        records.started(stage)
        try:
            result = operation()
            records.complete(stage, result, artifacts(result))
            return result
        except BaseException as exc:
            records.failed(stage, exc)
            raise

    def _invoke(self, kind, paper, phase):
        if self.invoke is None: raise RuntimeError('缺少 Prefect 模型请求调度接线')
        def invoke(request, connection, directory, **kw):
            stop = self.stops[paper['paper_id']]
            if stop.is_set(): raise InterruptedError('此篇已取消，停止发起新请求')
            kw['stop'] = stop
            with phase.suspended():
                return self.invoke(kind, request, connection, directory, **kw)
        return invoke

    def prepare(self, paper):
        self.settings.require_execution()
        work = self.body_work(paper)
        if sha_file(paper['body']['path']) != paper['body']['sha256']: raise ValueError('正文改变')
        # Native preparation verifies its complete cache including retained images.
        body.prepare_body(paper['body']['path'], work, self.settings.path('catalog_path'),
                          max_pages=self.settings['max_pages'])
        result = {'material': str(work / 'material.json'), 'paper_id': paper['paper_id']}
        Records(work).complete('prepare', result, [work / 'material.json'])
        return result

    def profile(self, paper):
        work = self.body_work(paper)
        if sha_file(paper['body']['path']) != paper['body']['sha256']: raise ValueError('正文改变')
        def operation():
            with LocalPhase(self.local_scope) as phase:
                return body.extract_profile(work, self.settings.path('template_path'), self.catalog,
                    legacy.connection(self.settings), self._invoke('body', paper, phase))
        return self._cached_stage(work, 'profile', operation,
            lambda r: [r['candidate'], r['validation'], r['bundle']])

    def package(self, paper):
        self.settings.require_execution()
        work = self.body_work(paper)
        prepared = body.package_body(work, self.runtime, self.catalog, self.settings.path('release_root') / 'core')
        result = {'package_path': prepared['package_path'], 'task_id': prepared['task']['task_id'],
                  'manifest_sha256': prepared['validation']['manifest_sha256']}
        Records(work).complete('package', result, [work / 'prepared-publication.json'])
        return result

    def attachment(self, paper, row):
        self.settings.require_execution()
        if sha_file(row['path']) != row['sha256']: raise ValueError('附件内容已改变')
        work = self.attachment_work(paper, row)
        def operation():
            with LocalPhase(self.local_scope) as phase:
                return identify_attachment(row, paper['paper_id'], work, legacy.connection(self.settings),
                                           self._invoke('attachment', paper, phase))
        return self._cached_stage(work, 'identify', operation, lambda r: [work / 'description.json'])

    def descriptions(self, paper):
        result = []
        for row in paper['attachments']:
            description = Records(self.attachment_work(paper, row)).cached('identify')
            if description is None: raise ValueError('仍有附件尚未完成简单识别')
            result.append(description)
        return result

    def admit_attachments(self, paper):
        descriptions = self.descriptions(paper)
        return self._cached_stage(self.paper_root(paper), 'attachment-admission', lambda:
            admit_descriptions(descriptions, self.runtime, self.settings.path('release_root') / 'core'))

    def publish_body(self, paper):
        self.settings.require_execution()
        verify_inventory(paper)
        from preextract_publish import deliver_publication
        from .recovery import recover_publication
        work = self.body_work(paper)
        receipt = work / 'delivery-receipt.json'
        prepared = read_json(work / 'prepared-publication.json')
        if prepared['task']['source_sha256'] != paper['body']['sha256']:
            raise ValueError('原封包与当前论文来源不同')
        client = observe_client(legacy.rag_client(self.settings))
        if receipt.is_file():
            result = recover_publication(prepared, receipt_path=receipt, client=client)
        else:
            result = deliver_publication(prepared, config=None, receipt_path=receipt, client=client)
        if result.get('status') != 'complete' or result.get('stage') != 'verified':
            raise ValueError('正文 RAG 核验尚未完成')
        summary = {'status': 'verified', 'paper_id': paper['paper_id'], 'targets': result['targets'],
            'task_id': prepared['task']['task_id'], 'manifest_sha256': result['manifest_sha256'],
            'receipt': str(receipt)}
        Records(self.paper_root(paper)).complete('rag-body', summary)
        return summary

    def load_rag_resume(self, inventory_path):
        """Read a persisted admission; never rediscover, prepare, profile or package."""
        self.settings.require_execution()
        path = Path(inventory_path).resolve()
        if not path.is_relative_to(self.root.resolve()) or path.name != 'inventory.json':
            raise ValueError('恢复清单必须来自当前记录区')
        paper = read_json(path)
        if path != (self.paper_root(paper) / 'inventory.json').resolve():
            raise ValueError('原记录与当前配置身份不一致；不会重新生成画像或封包')
        records = Records(self.paper_root(paper))
        if not records.status('intake').get('artifacts', {}).get(str(path)):
            raise ValueError('原冻结清单缺少接收阶段的哈希凭据')
        intake = records.cached('intake')
        if (intake is None or intake.get('paper_id') != paper['paper_id']
                or intake.get('revision') != paper['revision']
                or intake.get('attachment_count') != len(paper['attachments'])):
            raise ValueError('原冻结清单与接收记录不一致')
        verify_inventory(paper)
        work = self.body_work(paper)
        prepared = read_json(work / 'prepared-publication.json')
        if prepared['task']['source_sha256'] != paper['body']['sha256']:
            raise ValueError('原封包来源改变')
        if not (work / 'delivery-receipt.json').is_file():
            raise FileNotFoundError('缺少原入库回执；恢复入口不能推测首次提交')
        descriptions = self.descriptions(paper)
        if descriptions:
            admissions = Records(self.paper_root(paper)).cached('attachment-admission')
            if admissions is None: raise ValueError('缺少原附件准入记录')
            for row, description in zip(paper['attachments'], descriptions):
                if (description['paper_id'] != paper['paper_id'] or description['attachment_id'] != row['file_id']
                        or description['source_sha256'] != row['sha256']
                        or admissions.get(row['file_id'], {}).get('verified') is not True
                        or admissions[row['file_id']]['content_sha256'] != identity(description_content(description))):
                    raise ValueError('原附件说明、来源或准入身份不一致')
        self.stops.setdefault(paper['paper_id'], threading.Event())
        return paper

    def recover_body(self, paper):
        if not (self.body_work(paper) / 'delivery-receipt.json').is_file():
            raise FileNotFoundError('缺少原入库回执')
        return self.publish_body(paper)

    def publish_attachments(self, paper):
        self.settings.require_execution()
        verify_inventory(paper)
        root = self.paper_root(paper)
        parent = Records(root).cached('rag-body')
        admissions = Records(root).cached('attachment-admission')
        if paper['attachments'] == [] and admissions is None: admissions = {}
        if parent is None or admissions is None: raise ValueError('正文关联或附件嵌入准入未完成')
        descriptions = self.descriptions(paper)
        client = observe_client(legacy.rag_client(self.settings)) if descriptions else None
        delivered = []
        for description in descriptions:
            admission = admissions[description['attachment_id']]
            if not admission['verified'] or admission['content_sha256'] != identity(description_content(description)):
                raise ValueError('附件说明与嵌入准入结果不符')
            for code, target in parent['targets'].items():
                obj = target['body']
                key = identity([description, obj['dataset_id'], obj['document_id']])
                receipt = self.attachment_work(paper, next(r for r in paper['attachments']
                    if r['file_id'] == description['attachment_id'])) / 'delivery' / (key + '.json')
                delivered.append(publish_description(description, obj['dataset_id'], obj['document_id'], client, receipt,
                                                     parent_identity=obj['identity']))
        result = {'status': 'verified', 'attachments': len(descriptions), 'targets': delivered}
        Records(root).complete('rag-attachments', result)
        return result

    def archive(self, paper):
        self.settings.require_execution()
        root = self.paper_root(paper)
        for stage in ('rag-body', 'rag-attachments'):
            result = Records(root).cached(stage)
            if not result or result.get('status') != 'verified': raise ValueError('RAG 尚未全部核验')
        result = archive_originals(paper, root / 'archive', self.settings.path('nas_destination'),
                                  reserve_bytes=self.settings['nas_reserve_bytes'])
        Records(root).complete('nas', result, [root / 'archive/nas-receipt.json'])
        return result

    def finish(self, paper):
        self.settings.require_execution()
        verify_inventory(paper)
        records = Records(self.paper_root(paper))
        results = {name: records.cached(name) for name in ('rag-body', 'rag-attachments', 'nas')}
        if any(not row or row.get('status') != 'verified' for row in results.values()):
            raise ValueError('RAG、NAS 和记录尚未全部完成')
        result = {'status': 'complete', 'paper_id': paper['paper_id'], 'revision': paper['revision'],
            'body': results['rag-body'], 'attachments': results['rag-attachments'],
            'nas': results['nas'], 'local_folder_retained': True,
            'local_person_folder': paper['person_dir']}
        path = self.paper_root(paper) / 'completed.json'
        write_json(path, result)
        records.complete('complete', {'status': 'complete', 'record': str(path), 'local_folder_retained': True}, [path])
        return {'status': 'complete', 'paper_id': paper['paper_id'], 'record': str(path), 'local_folder_retained': True}
