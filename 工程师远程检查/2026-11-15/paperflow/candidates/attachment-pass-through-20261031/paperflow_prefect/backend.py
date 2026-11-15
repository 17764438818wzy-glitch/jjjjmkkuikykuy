"""Five-step business adapters. Only constructed after the local execution guard."""
from pathlib import Path
from contextlib import nullcontext
import threading
from . import body, legacy
from .attachments import AttachmentError, has_request_history, identify_attachment, validate_description, verify_cached_description
from .request_admission import retained_work, bind_work, require_model_history
from .archive import archive_originals
from .delivery import admit_descriptions, description_content, publish_description
from .intake import inspect_person, verify_inventory
from .records import Records, identity, read_json, sha_file, write_json
from .resources import LocalPhase


class Backend:
    def __init__(self, settings, invoke=None, local_scope=nullcontext, title_local_scope=None):
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
        self.title_local_scope = title_local_scope or local_scope
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
        return retained_work(self.root, paper, paper['body'], 'body', self.root / 'papers' / paper['paper_id'] / 'body' / key)

    def attachment_work(self, paper, row):
        key = identity(['attachment-adapter/1', self.model_signature,
                        sha_file(self.settings.path('source_manifest'))])[:24]
        return retained_work(self.root, paper, row, 'attachment', self.root / 'papers' / paper['paper_id'] / 'attachments' / row['file_id'] / key)

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
        if Records(work).cached('profile') is not None:
            if not (work / 'material.json').is_file(): raise ValueError('成功画像缺少原正文材料记录')
            return {'material': str(work / 'material.json'), 'paper_id': paper['paper_id']}
        # Native preparation verifies its complete cache including retained images.
        work.mkdir(parents=True, exist_ok=True)
        bind_work(self.root, paper, paper['body'], 'body', work)
        source = paper['body']['path']
        named_bound = (work / 'named-input-proof.json').is_file()
        fresh = not any((work / name).exists() for name in ('material.json', 'model', 'private', 'publication'))
        interrupted_named_prepare = named_bound and not any((work / name).exists() for name in ('material.json', 'model', 'publication'))
        if fresh or interrupted_named_prepare:
            from .named_delivery import materialize_named_input, resolved_delivery_row
            naming_row = resolved_delivery_row(paper, paper['body'], self.root)
            named = materialize_named_input(paper, naming_row, work)
            if named is not None: source = named
        body.prepare_body(source, work, self.settings.path('catalog_path'),
                          max_pages=self.settings['max_pages'])
        result = {'material': str(work / 'material.json'), 'paper_id': paper['paper_id']}
        Records(work).complete('prepare', result, [work / 'material.json'])
        return result

    def generate_title(self, paper):
        """Name a new untitled body once without replaying its original profile."""
        self.settings.require_execution()
        row = paper.get('body') or {}
        if not row or row.get('display_name'):
            return {'status': 'not_needed'}
        work = self.body_work(paper)
        from .named_delivery import (body_request_started, mark_title_not_applied,
                                     resolved_delivery_row, stage_generated_profile_name)
        if body_request_started(work):
            return {'status': 'existing_body_request_preserved'}
        existing = resolved_delivery_row(paper, row, self.root)
        if existing.get('display_name'):
            if existing.get('title_origin') == 'model_generated':
                if not stage_generated_profile_name(paper, work, self.root):
                    mark_title_not_applied(paper, self.root)
                    return {'status': 'available_after_profile_started', 'origin': 'model_generated'}
            return {'status': 'available', 'origin': existing.get('title_origin', 'source')}
        if self.invoke is None:
            raise RuntimeError('缺少 Prefect 模型请求调度接线')
        from title_llm_supplement import request_title
        from .resources import DeadlineStop
        target = {'paper_id': paper['paper_id'], 'body_file_id': row['file_id'],
                  'group_id': paper.get('group_id'), 'source_sha256': row['sha256'],
                  'original_path': row['path'], 'material_path': str(work / 'material.json'),
                  'text_path': str(work / 'private/body.txt')}
        stop = DeadlineStop(self.stops[paper['paper_id']], 60)
        with LocalPhase(getattr(self, 'title_local_scope', self.local_scope)) as phase:
            connection = dict(legacy.connection(self.settings))
            connection['timeout'] = min(float(connection.get('timeout', 60)), 60)
            def invoke(kind, body, conn, directory):
                if stop.is_set(): raise InterruptedError('题目请求已停止')
                # Release local capacity before HTTP. Do not reacquire it after a
                # successful reply: a later slot timeout must not discard a saved title.
                phase.__exit__(None, None, None)
                return self.invoke(kind, body, conn, directory, stop=stop, admission_timeout=5)
            result = request_title(target, connection, self.root / 'title-requests', invoke, lock_timeout=30)
        if not stage_generated_profile_name(paper, work, self.root):
            mark_title_not_applied(paper, self.root)
            return {'status': 'generated_after_profile_started', 'origin': result['origin'], 'title': result['title']}
        return {'status': 'generated', 'origin': result['origin'], 'title': result['title']}

    def profile(self, paper):
        work = self.body_work(paper)
        if sha_file(paper['body']['path']) != paper['body']['sha256']: raise ValueError('正文改变')
        def operation():
            require_model_history(paper, paper['body'], work)
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
            require_model_history(paper, row, work)
            work.mkdir(parents=True, exist_ok=True)
            bind_work(self.root, paper, row, 'attachment', work)
            input_source = None; naming_row = row
            upstream_only = (paper.get('attachment_input_policy') == 'upstream_only'
                             or row.get('delivery_ref') is not None)
            if not upstream_only and not any((work / name).exists() for name in ('evidence.json', 'model', 'description.json', 'cards')):
                from .named_delivery import materialize_named_input, resolved_delivery_row
                naming_row = resolved_delivery_row(paper, row, self.root)
                input_source = materialize_named_input(paper, naming_row, work)
            with LocalPhase(self.local_scope) as phase:
                kwargs = {'input_source': input_source} if input_source is not None else {}
                if upstream_only: kwargs['upstream_only'] = True
                return identify_attachment(naming_row, paper['paper_id'], work, lambda: legacy.connection(self.settings),
                                           self._invoke('attachment', paper, phase), **kwargs)
        description = self._cached_stage(work, 'identify', operation, lambda r: [work / 'description.json'])
        verify_cached_description(row, paper['paper_id'], work, description)
        # A cached identification still needs its independently versioned local card.
        card = self.attachment_card(paper, row, description)
        if card.get('current_evidence_status') == 'unavailable':
            return dict(description, current_evidence_status='unavailable')
        return description

    def attachment_card(self, paper, row, description=None):
        from .attachment_card import render_attachment_card
        from .attachment_input import load_received_evidence, card_evidence
        work = self.attachment_work(paper, row)
        description = description or Records(work).cached('identify')
        if description is None: raise ValueError('附件尚未取得完整 LLM 识别结果')
        try:
            loaded = load_received_evidence(row)
        except AttachmentError as exc:
            if exc.details.get('code') != 'evidence_changed' or row.get('delivery_ref') is None:
                raise
            cached = Records(work).cached('attachment-card')
            if not cached or not has_request_history(work):
                raise
            verify_cached_description(row, paper['paper_id'], work, description)
            inputs = cached.get('inputs') or {}
            identity_facts = inputs.get('identity') or {}
            if (cached.get('paper_id') != paper['paper_id'] or cached.get('attachment_id') != row['file_id']
                    or cached.get('source_sha256') != row['sha256']
                    or identity_facts.get('paper_id') != paper['paper_id']
                    or identity_facts.get('attachment_id') != row['file_id']
                    or identity_facts.get('source_sha256') != row['sha256']
                    or inputs.get('description_sha256') != identity(description)
                    or cached.get('version') != identity(inputs)
                    or Path(cached.get('original', '')).resolve() != Path(row['path']).resolve()
                    or not Path(cached.get('path', '')).resolve().is_relative_to((work/'cards').resolve())
                    or sha_file(cached['path']) != cached.get('md_sha256')):
                raise
            return dict(cached, current_evidence_status='unavailable')
        if loaded:
            record, handoff, report = loaded
            evidence = card_evidence(record, report)
            if (work / 'evidence.json').is_file():
                seen = read_json(work / 'evidence.json')
                evidence['identification_input'] = {'refs': seen.get('refs', []),
                    'image_count': len(seen.get('images', [])), 'read_scope': seen.get('read_scope'),
                    'limitations': seen.get('limitations', []), 'sha256': sha_file(work / 'evidence.json')}
        elif (work / 'evidence.json').is_file():
            evidence = read_json(work / 'evidence.json')
        else:
            evidence = {'schema': 'attachment-evidence/1', 'source_sha256': row['sha256'],
                'limitations': ['历史识别证据未留存，沿用已经核验的原 LLM 说明'],
                'archive_listing': {'status': 'unavailable', 'total_entries': None,
                    'entries': [], 'reason': '历史目录未留存'}}
        result = render_attachment_card(row, paper['paper_id'], description, evidence, work / 'cards')
        records = Records(work)
        if records.cached('attachment-card') != result:
            records.complete('attachment-card', result, [result['path']])
        return result

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
        from preextract_publish import deliver_publication, reconcile_publication
        work = self.body_work(paper)
        receipt = work / 'delivery-receipt.json'
        prepared = read_json(work / 'prepared-publication.json')
        client = legacy.rag_client(self.settings)
        if receipt.is_file() and read_json(receipt).get('status') != 'complete':
            # Reconcile exact prior writes only. Missing or ambiguous writes remain unresolved.
            result = reconcile_publication(prepared, receipt_path=receipt, client=client)
        else:
            result = deliver_publication(prepared, config=None, receipt_path=receipt, client=client)
        if result.get('status') != 'complete' or result.get('stage') != 'verified':
            raise ValueError('正文 RAG 核验尚未完成')
        summary = {'status': 'verified', 'paper_id': paper['paper_id'], 'targets': result['targets'],
            'task_id': prepared['task']['task_id'], 'manifest_sha256': result['manifest_sha256'],
            'receipt': str(receipt)}
        Records(self.paper_root(paper)).complete('rag-body', summary)
        return summary

    def publish_attachments(self, paper):
        self.settings.require_execution()
        verify_inventory(paper)
        root = self.paper_root(paper)
        parent = Records(root).cached('rag-body')
        admissions = Records(root).cached('attachment-admission')
        if parent is None or admissions is None: raise ValueError('正文关联或附件嵌入准入未完成')
        descriptions = self.descriptions(paper)
        client = legacy.rag_client(self.settings) if descriptions else None
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
