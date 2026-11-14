"""Continuously hand off passed education packages from a live preprocessing run.

The GUI owns its batch manifest. This worker reads it and keeps separate,
durable receipts so an in-progress export is never rewritten underneath it.
"""
from __future__ import annotations

import argparse
import json
import msvcrt
import os
import re
import sys
import time
import uuid
from pathlib import Path

from engine import digest, safe_component
from evidence import package_fingerprint
from education_image_receipts import eligible_for_auxiliary_index, image_receipt_key
from education_direct_tree import bind_tree_chunks, extract_tree
from education_method_graph import build_method_graph, merge_method_graphs
from knowledge_package import knowledge_files
from pilot_package_copy import prepare_pilot_copy
from ragflow_client import upload_name

ROOT = Path(__file__).resolve().parent
MAX_ACTIVE = 3
MAX_UPLOADS_PER_TICK = 10
POLL_SECONDS = 15


def recovery_poll_delay(queue_failure_streak: int) -> int:
    """Ambiguous remote progress text must not delay unrelated papers."""
    return POLL_SECONDS


def poll_delay(parse_waiting: int) -> int:
    """Poll quickly while an uploaded primary is waiting for a free slot."""
    return 5 if parse_waiting > 0 else POLL_SECONDS


class ArrearageRecoveryHalted(RuntimeError):
    """Stop new submissions when the embedding account fails again."""


def classify_parse_failure(progress_msg: str) -> str:
    message = (progress_msg or '').strip()
    if 'Arrearage' in message:
        return 'embedding_arrearage'
    if re.fullmatch(r'\d+ tasks? are ahead in the queue\.\.\.', message):
        return 'outcome_unresolved'
    if '"education_processing_issue":"input_invalid"' in message:
        return 'input_invalid'
    if '"education_processing_issue":"annotation_invalid"' in message:
        return 'annotation_invalid'
    return 'other_failure'


def education_remote_name(path: Path) -> str:
    """Keep the stable document ID and content hash within RAGFlow's byte limit."""
    path = Path(path)
    ordinary = upload_name(path)
    if len(ordinary.encode('utf-8')) <= 255:
        return ordinary
    metadata = json.loads(path.with_name('metadata.json').read_text(encoding='utf-8'))
    suffix = '_' + safe_component(metadata['id']) + '_' + digest(path)[:16] + '.md'
    budget = 255 - len(suffix.encode('utf-8'))
    title = safe_component(metadata.get('title') or '待确认题目')
    if budget < 8:
        raise ValueError('论文远端文件名身份后缀过长')
    prefix = ''
    for character in title:
        if len((prefix + character).encode('utf-8')) > budget:
            break
        prefix += character
    return prefix.rstrip(' ._') + suffix


def ready_items(batch: dict, pilot_ids: set[str], receipts: dict) -> list[dict]:
    seen = set(pilot_ids)
    result = []
    for item in batch.get('items') or []:
        paper_id = item.get('paper_id')
        if (item.get('status') != 'pass' or item.get('category') != '教育教学类'
                or not re.fullmatch(r'paper_[0-9a-f]{24}', paper_id or '')
                or not item.get('package_path') or paper_id in seen):
            continue
        seen.add(paper_id)
        result.append(item)
    return result


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    for attempt in range(10):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.2)


def exact_document(client, dataset_id: str, filename: str) -> dict | None:
    inventories = getattr(client, '_education_name_inventory', None)
    if inventories is None:
        inventories = {}
        client._education_name_inventory = inventories
    if dataset_id not in inventories:
        inventory = {}
        for page in range(1, 1001):
            payload = client.request('GET', f'/datasets/{dataset_id}/documents?page={page}&page_size=100')
            docs = payload.get('docs') if isinstance(payload, dict) else None
            if not isinstance(docs, list):
                raise ValueError('远端文档清单格式不正确')
            for doc in docs:
                inventory.setdefault(doc.get('name'), []).append(doc)
            if len(docs) < 100:
                break
        else:
            raise ValueError('远端文档清单超过分页上限')
        inventories[dataset_id] = inventory
    rows = inventories[dataset_id].get(filename, [])
    if len(rows) > 1:
        raise ValueError('远端存在同名重复文档：' + filename)
    return rows[0] if rows else None


def checked_copy(item: dict, batch_dir: Path, copies: Path) -> tuple[Path, dict]:
    source = Path(item['package_path']).resolve()
    if not source.is_relative_to(Path(batch_dir).resolve() / 'packages') or source.name != item['id']:
        raise ValueError('批次知识包路径不符：' + item['id'])
    knowledge_files(source)
    original = json.loads((source / 'manifest.json').read_text(encoding='utf-8'))
    if (original.get('document_id') != item['id'] or original.get('paper_id') != item['paper_id']
            or digest(source / 'ingest.md') != item.get('reviewed_hash')
            or package_fingerprint(source) != item.get('reviewed_package_hash')):
        raise ValueError('源知识包身份或内容与预处理回执不一致：' + item['id'])
    destination = copies / item['id']
    if not destination.exists():
        copies.mkdir(parents=True, exist_ok=True)
        temporary = copies / ('.' + item['id'] + '.' + uuid.uuid4().hex + '.tmp')
        prepare_pilot_copy(source, temporary)
        os.replace(temporary, destination)
    knowledge_files(destination, require_stable_paper_id=True)
    manifest = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('document_id') != item['id'] or manifest.get('paper_id') != item['paper_id']:
        raise ValueError('规范副本身份不一致：' + item['id'])
    return destination, manifest


class Stream:
    def __init__(self, batch_path: Path, private_dir: Path, client, deployment: dict, pilot_ids: set[str]):
        self.batch_path = batch_path.resolve()
        self.batch_dir = self.batch_path.parent
        self.private_dir = private_dir.resolve()
        self.copies = self.private_dir / 'canonical_packages'
        self.state_path = self.private_dir / 'receipts.json'
        self.state = json.loads(self.state_path.read_text(encoding='utf-8')) if self.state_path.exists() else {
            'batch_id': self.batch_dir.name, 'primary': {}, 'images': {}, 'errors': {}}
        if self.state['batch_id'] != self.batch_dir.name:
            raise ValueError('导入回执批次编号不匹配')
        self.client = client
        self.deployment = deployment
        self.pilot_ids = pilot_ids
        self.paper_dataset = deployment['paper_dataset_id']
        self.image_dataset = deployment['image_dataset_id']
        self.pipeline = deployment.get('full_ingest_pipeline_id', deployment['pipeline_id'])
        self.arrearage_pipeline = deployment.get('arrearage_retry_pipeline_id', self.pipeline)
        self.max_active = deployment.get('full_ingest_max_active',
                                         2 if deployment.get('full_ingest_pipeline_id') else MAX_ACTIVE)
        if not isinstance(self.max_active, int) or not 1 <= self.max_active <= MAX_ACTIVE:
            raise ValueError('教育解析并发上限超出已验证范围')
        self.native_graph_cleanup_dir = self.private_dir / 'native_graph_cleanup'
        native_ledger_path = self.native_graph_cleanup_dir / 'receipts.json'
        self.native_graph_cleanup_ledger = (
            json.loads(native_ledger_path.read_text(encoding='utf-8'))
            if native_ledger_path.exists() else {'dataset_id': self.paper_dataset, 'items': {}})
        if self.native_graph_cleanup_ledger.get('dataset_id') != self.paper_dataset:
            raise ValueError('旧 Graph 清理回执知识库身份不符')
        plan_path = self.private_dir / 'arrearage_retry_plan.json'
        if plan_path.exists():
            plan = json.loads(plan_path.read_text(encoding='utf-8'))
            if (plan.get('batch_id') != self.batch_dir.name
                    or plan.get('dataset_id') != self.paper_dataset
                    or not isinstance(plan.get('items'), list)):
                raise ValueError('欠费重试计划与批次或知识库不匹配')
            self.arrearage_recovery_plan = plan['items']
        else:
            self.arrearage_recovery_plan = []

    def save(self):
        write_json(self.state_path, self.state)

    def direct_tree(self, item: dict, package: Path):
        """Persist a source-linked chapter tree without an LLM or compiler call."""
        paper_id = item['paper_id']
        receipt = self.state['primary'][paper_id]
        remote_id = receipt['ragflow_document_id']
        ingest = (package / 'ingest.md').read_text(encoding='utf-8')
        destination = self.private_dir / 'direct_trees' / (paper_id + '.json')
        expected = extract_tree(ingest, paper_id, remote_id)
        if destination.exists():
            existing = json.loads(destination.read_text(encoding='utf-8'))
            if (existing.get('paper_id') != paper_id or existing.get('source_doc_id') != remote_id
                    or existing.get('ingest_sha256') != expected['ingest_sha256']):
                raise ValueError('目录 Tree 文件与当前论文身份或正文不一致：' + paper_id)
            receipt['tree_status'] = 'needs_review' if existing.get('issues') else 'done'
            receipt['tree_heading_count'] = len(existing.get('nodes') or [])
            self.save()
            return
        docs = self.client.documents(self.paper_dataset, document_id=remote_id)
        if (len(docs) != 1 or docs[0].get('id') != remote_id
                or docs[0].get('name') != receipt['upload_name']
                or docs[0].get('run') != 'DONE'
                or (docs[0].get('meta_fields') or {}).get('education_paper_id') != paper_id
                or (docs[0].get('meta_fields') or {}).get('education_admission') != '入库'):
            raise ValueError('目录 Tree 的远端文档尚未通过身份和入库校验：' + paper_id)
        chunks = []
        for page in range(1, 101):
            payload = self.client.request('GET',
                f'/datasets/{self.paper_dataset}/documents/{remote_id}/chunks?page={page}&page_size=100')
            batch = payload.get('chunks') or []
            chunks.extend(batch)
            if len(chunks) >= payload.get('total', 0):
                break
            if not batch:
                raise ValueError('目录 Tree 来源切片分页中断：' + paper_id)
        else:
            raise ValueError('目录 Tree 来源切片分页超限：' + paper_id)
        tree = bind_tree_chunks(expected, chunks)
        write_json(destination, tree)
        receipt['tree_status'] = 'needs_review' if tree['issues'] else 'done'
        receipt['tree_heading_count'] = len(tree['nodes'])
        receipt['tree_unlinked_count'] = tree['unlinked_headings']
        self.save()

    def method_graph(self, item: dict, package: Path):
        """Persist only executed methods with located body chunk IDs."""
        paper_id = item['paper_id']
        receipt = self.state['primary'][paper_id]
        remote_id = receipt['ragflow_document_id']
        source_hash = digest(package / 'ingest.md')
        destination = self.private_dir / 'method_graphs' / (paper_id + '.json')
        if destination.exists():
            existing = json.loads(destination.read_text(encoding='utf-8'))
            if (existing.get('paper', {}).get('id') != paper_id
                    or existing.get('paper', {}).get('source_doc_id') != remote_id
                    or existing.get('ingest_sha256') != source_hash):
                raise ValueError('方法 Graph 文件与当前论文身份或正文不一致：' + paper_id)
            receipt['method_graph_status'] = ('needs_review' if existing['unlinked_methods'] else 'done')
            receipt['method_graph_edge_count'] = len(existing['edges'])
            self.save()
            return False
        docs = self.client.documents(self.paper_dataset, document_id=remote_id)
        if (len(docs) != 1 or docs[0].get('id') != remote_id
                or docs[0].get('name') != receipt['upload_name']
                or docs[0].get('run') != 'DONE'
                or (docs[0].get('meta_fields') or {}).get('education_paper_id') != paper_id):
            raise ValueError('方法 Graph 的远端文档身份或解析状态不一致：' + paper_id)
        metadata = docs[0].get('meta_fields') or {}
        if metadata.get('education_admission') != '入库':
            receipt['method_graph_status'] = 'skipped'
            self.save()
            return False
        chunks = []
        for page in range(1, 101):
            payload = self.client.request('GET',
                f'/datasets/{self.paper_dataset}/documents/{remote_id}/chunks?page={page}&page_size=100')
            batch = payload.get('chunks') or []
            chunks.extend(batch)
            if len(chunks) >= payload.get('total', 0):
                break
            if not batch:
                raise ValueError('方法 Graph 来源切片分页中断：' + paper_id)
        else:
            raise ValueError('方法 Graph 来源切片分页超限：' + paper_id)
        graph = build_method_graph(paper_id, remote_id, metadata, chunks)
        graph['ingest_sha256'] = source_hash
        write_json(destination, graph)
        receipt['method_graph_status'] = 'needs_review' if graph['unlinked_methods'] else 'done'
        receipt['method_graph_edge_count'] = len(graph['edges'])
        receipt['method_graph_unlinked_count'] = len(graph['unlinked_methods'])
        self.save()
        return True

    def rebuild_method_index(self):
        graphs = [json.loads(path.read_text(encoding='utf-8'))
                  for path in sorted((self.private_dir / 'method_graphs').glob('paper_*.json'))]
        index = merge_method_graphs(graphs)
        write_json(self.private_dir / 'method_index.json', index)
        return len(index['edges'])

    def native_graph_cleanup_pending(self, paper_id: str, receipt: dict) -> bool:
        ledger = getattr(self, 'native_graph_cleanup_ledger', None)
        return bool(ledger is not None
                    and self.paper_dataset == 'ecf88dc4b82b11f1a8846d29005ec004'
                    and receipt.get('status') == 'done'
                    and receipt.get('admission') == '入库'
                    and receipt.get('method_graph_status') in ('done', 'needs_review')
                    and receipt.get('native_graph_cleanup_status') not in
                    ('deleted', 'reconciled', 'already_absent', 'not_required', 'needs_review')
                    and ledger['items'].get(paper_id, {}).get('status') != 'done')

    def cleanup_native_graph(self, item: dict) -> None:
        paper_id = item['paper_id']
        receipt = self.state['primary'][paper_id]
        if not self.native_graph_cleanup_pending(paper_id, receipt):
            return
        remote_id = receipt['ragflow_document_id']
        try:
            docs = self.client.documents(self.paper_dataset, document_id=remote_id)
            if (len(docs) != 1 or docs[0].get('id') != remote_id
                    or docs[0].get('name') != receipt['upload_name']
                    or docs[0].get('run') != 'DONE'
                    or (docs[0].get('meta_fields') or {}).get('education_paper_id') != paper_id
                    or (docs[0].get('meta_fields') or {}).get('education_admission') != '入库'):
                raise ValueError('旧 Graph 清理前远端论文身份不符：' + paper_id)
            if docs[0].get('pipeline_id') == self.pipeline:
                outcome = 'not_required'
            else:
                from education_native_graph_cleanup import cleanup_one
                outcome = cleanup_one(
                    self.client, self.paper_dataset, self.pipeline, paper_id,
                    receipt, self.native_graph_cleanup_ledger,
                    run_dir=self.private_dir, work_dir=self.native_graph_cleanup_dir)
            if outcome == 'method_missing':
                raise ValueError('旧 Graph 清理缺少方法 artifact：' + paper_id)
            receipt['native_graph_cleanup_status'] = outcome
            self.save()
            if outcome in ('deleted', 'reconciled'):
                print('NATIVE_GRAPH_REMOVED', paper_id, outcome, flush=True)
        except Exception as exc:
            receipt['native_graph_cleanup_status'] = 'needs_review'
            self.state['errors']['native_graph:' + paper_id] = str(exc)[:350]
            self.save()
            print('NATIVE_GRAPH_REVIEW', paper_id, str(exc)[:180], flush=True)

    def primary(self, item: dict, package: Path, manifest: dict):
        paper_id = item['paper_id']
        filename = education_remote_name(package / 'ingest.md')
        receipts = self.state['primary']
        receipt = receipts.get(paper_id)
        if (receipt and receipt.get('status') == 'unknown'
                and not receipt.get('ragflow_document_id')
                and receipt.get('upload_name') != filename
                and len(receipt.get('upload_name', '').encode('utf-8')) > 255
                and 'File name must be 255 bytes or less' in self.state['errors'].get(paper_id, '')):
            if exact_document(self.client, self.paper_dataset, receipt['upload_name']):
                raise ValueError('超长文件名意外出现在远端，需人工核对：' + paper_id)
            del receipts[paper_id]
            receipt = None
            self.save()
        if receipt and (receipt['document_id'] != item['id'] or receipt['upload_name'] != filename):
            raise ValueError('主文档回执身份或内容不一致：' + paper_id)
        if receipt and receipt['status'] in ('done', 'fail', 'cancel', 'unknown'):
            return
        if receipt is None:
            existing = exact_document(self.client, self.paper_dataset, filename)
            if existing is None:
                receipts[paper_id] = {'document_id': item['id'], 'upload_name': filename,
                                      'status': 'upload_intent'}
                self.save()
                try:
                    remote_id = self.client.upload(self.paper_dataset, package / 'ingest.md', remote_name=filename)
                except Exception:
                    receipts[paper_id]['status'] = 'unknown'
                    self.save()
                    raise
                existing = {'id': remote_id, 'name': filename}
                self.client._education_name_inventory[self.paper_dataset].setdefault(filename, []).append(existing)
            receipts[paper_id] = {'document_id': item['id'], 'upload_name': filename,
                                  'ragflow_document_id': existing['id'], 'status': 'uploaded'}
            self.save()
            print('UPLOADED', item['id'], paper_id, existing['id'], flush=True)
        elif receipt['status'] == 'upload_intent':
            existing = exact_document(self.client, self.paper_dataset, filename)
            if existing is None:
                receipt['status'] = 'unknown'
                self.save()
                raise RuntimeError('上次上传回执不确定，已阻止重复提交：' + paper_id)
            receipt.update(ragflow_document_id=existing['id'], status='uploaded')
            self.save()

        receipt = receipts[paper_id]
        if receipt['status'] != 'uploaded':
            return
        active = sum(row['status'] in ('submitted', 'parse_intent') for row in receipts.values())
        if active >= self.max_active:
            return
        remote_id = receipt['ragflow_document_id']
        docs = self.client.documents(self.paper_dataset, document_id=remote_id)
        if len(docs) != 1 or docs[0].get('name') != filename:
            raise ValueError('主文档远端身份不一致：' + paper_id)
        doc = docs[0]
        if doc.get('pipeline_id') != self.pipeline:
            self.client.request('PATCH', f'/datasets/{self.paper_dataset}/documents/{remote_id}',
                                {'pipeline_id': self.pipeline})
            doc = self.client.documents(self.paper_dataset, document_id=remote_id)[0]
            if doc.get('pipeline_id') != self.pipeline:
                raise ValueError('教育 Pipeline 绑定未生效：' + paper_id)
        if doc['run'] in ('DONE', 'FAIL', 'CANCEL'):
            receipt['status'] = doc['run'].lower()
            self.save()
            return
        if doc['run'] == 'RUNNING':
            receipt['status'] = 'submitted'
            self.save()
            return
        receipt['status'] = 'parse_intent'
        receipt['parse_requested_at'] = time.time()
        self.save()
        self.client.parse(self.paper_dataset, remote_id)
        receipt['status'] = 'submitted'
        self.save()
        print('PARSE_SUBMITTED', item['id'], paper_id, remote_id, flush=True)

    def poll_primary(self):
        for paper_id, receipt in list(self.state['primary'].items()):
            if receipt['status'] not in ('submitted', 'parse_intent'):
                continue
            docs = self.client.documents(self.paper_dataset, document_id=receipt['ragflow_document_id'])
            if len(docs) != 1 or docs[0].get('name') != receipt['upload_name']:
                raise ValueError('解析中文档的远端身份不一致：' + paper_id)
            doc = docs[0]
            if doc['run'] in ('DONE', 'FAIL', 'CANCEL'):
                receipt['status'] = doc['run'].lower()
                receipt['admission'] = (doc.get('meta_fields') or {}).get('education_admission')
                receipt['chunk_count'] = doc.get('chunk_count')
                if doc['run'] == 'FAIL':
                    receipt['failure_kind'] = classify_parse_failure(doc.get('progress_msg') or '')
                    receipt['failure_detail'] = (doc.get('progress_msg') or '')[-600:]
                    receipt['process_duration'] = doc.get('process_duration')
                    self.state['queue_failure_streak'] = 0
                else:
                    receipt.pop('failure_kind', None)
                    receipt.pop('failure_detail', None)
                    self.state['queue_failure_streak'] = 0
                self.save()
                print('PARSE_END', receipt['document_id'], paper_id, doc['run'],
                      receipt['admission'], receipt['chunk_count'], flush=True)
                if receipt.get('failure_kind') == 'embedding_arrearage':
                    raise ArrearageRecoveryHalted('向量模型账户返回 Arrearage，已暂停导入')
            elif receipt['status'] == 'parse_intent' and doc['run'] == 'RUNNING':
                receipt['status'] = 'submitted'
                self.save()
            elif (receipt['status'] == 'parse_intent' and doc['run'] == 'UNSTART'
                  and time.time() - receipt.get('parse_requested_at', 0) > 180):
                receipt['status'] = 'unknown'
                self.state['errors'][paper_id] = '解析提交结果不确定，需核对远端状态后再处理'
                self.save()
                print('PARSE_UNCERTAIN', paper_id, flush=True)

    def retry_arrearage(self):
        if getattr(self,'private_dir',None) and (self.private_dir/'queue_owner.json').exists():
            raise RuntimeError('Batch is owned by the universal ingest queue')
        """Retry only audited embedding failures on their existing remote IDs."""
        plan = getattr(self, 'arrearage_recovery_plan', [])
        for row in plan:
            paper_id = row['paper_id']
            receipt = self.state['primary'].get(paper_id)
            if (not receipt or receipt.get('document_id') != row['source_document_id']
                    or receipt.get('ragflow_document_id') != row['ragflow_document_id']):
                raise ValueError('欠费重试计划与主文档回执身份不一致：' + paper_id)
            if (receipt.get('retry_attempts', 0) < 1 or receipt['status'] != 'fail'
                    or receipt.get('recovery_non_arrearage_checked')):
                continue
            docs = self.client.documents(self.paper_dataset, document_id=row['ragflow_document_id'])
            expected_pipeline = receipt.get('retry_pipeline_id') or getattr(
                self, 'arrearage_pipeline', self.pipeline)
            if (len(docs) != 1 or docs[0].get('id') != row['ragflow_document_id']
                    or docs[0].get('name') != receipt['upload_name']
                    or docs[0].get('pipeline_id') != expected_pipeline):
                raise ValueError('欠费重试远端文档身份或 Pipeline 不一致：' + paper_id)
            doc = docs[0]
            if doc['run'] == 'FAIL' and 'Arrearage' in (doc.get('progress_msg') or ''):
                raise ArrearageRecoveryHalted('向量模型账户再次返回 Arrearage，已暂停导入')
            if doc['run'] == 'FAIL':
                receipt['recovery_non_arrearage_checked'] = True
                self.save()
            elif doc['run'] == 'RUNNING':
                receipt['status'] = 'submitted'
                self.save()
            elif doc['run'] == 'DONE':
                receipt.update(status='done', admission=(doc.get('meta_fields') or {}).get('education_admission'),
                               chunk_count=doc.get('chunk_count'))
                self.save()

        active = sum(r['status'] in ('submitted', 'parse_intent')
                     for r in self.state['primary'].values())
        for row in plan:
            if active >= self.max_active:
                break
            paper_id = row['paper_id']
            receipt = self.state['primary'][paper_id]
            if receipt['status'] in ('done', 'submitted', 'parse_intent'):
                continue
            if receipt.get('retry_attempts', 0) >= 1:
                continue
            if receipt['status'] != 'fail':
                raise ValueError('欠费重试文档状态异常：' + paper_id)
            docs = self.client.documents(self.paper_dataset, document_id=row['ragflow_document_id'])
            if (len(docs) != 1 or docs[0].get('id') != row['ragflow_document_id']
                    or docs[0].get('name') != receipt['upload_name']):
                raise ValueError('欠费重试远端文档身份不一致：' + paper_id)
            doc = docs[0]
            if doc['run'] == 'DONE':
                receipt.update(status='done', admission=(doc.get('meta_fields') or {}).get('education_admission'),
                               chunk_count=doc.get('chunk_count'))
                self.save()
                continue
            if doc['run'] == 'RUNNING':
                receipt['status'] = 'submitted'
                self.save()
                continue
            old_pipeline = getattr(self, 'arrearage_pipeline', self.pipeline)
            target_pipeline = receipt.get('retry_pipeline_id') or self.pipeline
            charged_failure = doc['run'] == 'FAIL' and 'Arrearage' in (doc.get('progress_msg') or '')
            rebound_unstart = (doc['run'] == 'UNSTART'
                               and receipt.get('retry_pipeline_id') == target_pipeline
                               and doc.get('pipeline_id') == target_pipeline
                               and receipt.get('failure_kind') in (None, 'embedding_arrearage'))
            if not charged_failure and not rebound_unstart:
                raise ValueError('计划中的文档并非当前欠费失败或已核验的绑定重置，拒绝重试：' + paper_id)
            if doc.get('pipeline_id') not in (old_pipeline, target_pipeline):
                raise ValueError('欠费重试远端 Pipeline 不属于已核验版本：' + paper_id)
            if not receipt.get('retry_pipeline_id'):
                receipt['retry_pipeline_id'] = target_pipeline
                self.save()
            if doc.get('pipeline_id') != target_pipeline:
                self.client.request('PATCH',
                    f'/datasets/{self.paper_dataset}/documents/{row["ragflow_document_id"]}',
                    {'pipeline_id': target_pipeline})
                rebound = self.client.documents(self.paper_dataset,
                                                document_id=row['ragflow_document_id'])
                if (len(rebound) != 1 or rebound[0].get('id') != row['ragflow_document_id']
                        or rebound[0].get('name') != receipt['upload_name']
                        or rebound[0].get('pipeline_id') != target_pipeline
                        or rebound[0].get('run') not in ('FAIL', 'UNSTART')):
                    raise ValueError('欠费重试快速 Pipeline 绑定未生效：' + paper_id)
            receipt.update(status='parse_intent', parse_requested_at=time.time(),
                           retry_reason='embedding_arrearage_recharge', retry_attempts=1)
            self.save()
            self.client.parse(self.paper_dataset, row['ragflow_document_id'])
            receipt['status'] = 'submitted'
            self.save()
            active += 1
            print('ARREARAGE_RETRY_SUBMITTED', receipt['document_id'], paper_id,
                  row['ragflow_document_id'], flush=True)

    def recover_arrearage_tick(self):
        if getattr(self,'private_dir',None) and (self.private_dir/'queue_owner.json').exists():
            raise RuntimeError('Batch is owned by the universal ingest queue')
        """Recover one audited arrearage failure without uploading new papers."""
        self.poll_primary()
        plan = self.arrearage_recovery_plan

        def pending_rows():
            return [row for row in plan
                    if (receipt := self.state['primary'].get(row['paper_id']))
                    and receipt.get('status') == 'fail'
                    and receipt.get('failure_kind') == 'embedding_arrearage'
                    and not receipt.get('retry_attempts')]

        active = sum(receipt['status'] in ('submitted', 'parse_intent')
                     for receipt in self.state['primary'].values())
        pending = pending_rows()
        if not active and pending:
            self.arrearage_recovery_plan = pending[:1]
            try:
                self.retry_arrearage()
            finally:
                self.arrearage_recovery_plan = plan
        active = sum(receipt['status'] in ('submitted', 'parse_intent')
                     for receipt in self.state['primary'].values())
        remaining = len(pending_rows())
        return not active and not remaining, {'remaining': remaining, 'active': active}

    def images(self, item: dict, package: Path, manifest: dict):
        paper_id = item['paper_id']
        receipt = self.state['primary'].get(paper_id)
        if not receipt or receipt['status'] != 'done':
            return
        docs = self.client.documents(self.paper_dataset, document_id=receipt['ragflow_document_id'])
        if len(docs) != 1 or not eligible_for_auxiliary_index(docs[0], paper_id):
            return
        metadata = json.loads((package / 'metadata.json').read_text(encoding='utf-8'))
        resources = {row['local_path']: row for row in metadata.get('resources') or []}
        for row in manifest['files']:
            relative = row['path']
            role = row.get('role')
            if role not in ('image', 'image_projection', 'paper_assets'):
                continue
            if role == 'image':
                resource = resources.get(relative)
                if (not resource or resource.get('privacy_status') != 'approved'
                        or resource.get('sha256') != row['sha256']):
                    raise ValueError('图片隐私或哈希未通过：' + relative)
            key = image_receipt_key(paper_id, relative)
            filename = paper_id + '_' + Path(relative).name
            image_receipt = self.state['images'].get(key)
            if image_receipt and image_receipt.get('sha256') != row['sha256']:
                raise ValueError('图片回执哈希不一致：' + key)
            if image_receipt and image_receipt['status'] == 'unknown':
                self.state['errors']['image:' + key] = '图片上传结果不确定，需核对远端后再处理'
                self.save()
                return False
            if image_receipt is None:
                existing = exact_document(self.client, self.image_dataset, filename)
                if existing is None:
                    self.state['images'][key] = {'name': filename, 'sha256': row['sha256'],
                                                 'role': role, 'status': 'upload_intent'}
                    self.save()
                    try:
                        remote_id = self.client.upload(self.image_dataset, package / relative,
                                                       remote_name=filename)
                    except Exception:
                        self.state['images'][key]['status'] = 'unknown'
                        self.save()
                        raise
                else:
                    remote_id = existing['id']
                if existing is None:
                    self.client._education_name_inventory[self.image_dataset].setdefault(filename, []).append(
                        {'id': remote_id, 'name': filename})
                self.state['images'][key] = {'name': filename, 'sha256': row['sha256'],
                                             'role': role, 'ragflow_document_id': remote_id,
                                             'status': 'uploaded'}
                self.save()
                print('IMAGE_UPLOADED', key, remote_id, flush=True)
            image_receipt = self.state['images'][key]
            if image_receipt['status'] == 'upload_intent':
                existing = exact_document(self.client, self.image_dataset, filename)
                if existing is None:
                    image_receipt['status'] = 'unknown'
                    self.save()
                    raise RuntimeError('图片上传回执不确定：' + key)
                image_receipt.update(ragflow_document_id=existing['id'], status='uploaded')
                self.save()
            if role in ('image_projection', 'paper_assets') and image_receipt['status'] == 'uploaded':
                remote_id = image_receipt['ragflow_document_id']
                image_doc = self.client.documents(self.image_dataset, document_id=remote_id)[0]
                if image_doc['chunk_method'] != 'naive':
                    self.client.request('PATCH', f'/datasets/{self.image_dataset}/documents/{remote_id}',
                                        {'chunk_method': 'naive'})
                    image_doc = self.client.documents(self.image_dataset, document_id=remote_id)[0]
                if image_doc['run'] == 'UNSTART':
                    image_receipt['status'] = 'parse_intent'
                    image_receipt['parse_requested_at'] = time.time()
                    self.save()
                    self.client.parse(self.image_dataset, remote_id)
                    image_receipt['status'] = 'submitted'
                    self.save()
                elif image_doc['run'] in ('RUNNING', 'DONE'):
                    image_receipt['status'] = image_doc['run'].lower()
                    self.save()
        return True

    def poll_images(self):
        for key, receipt in list(self.state['images'].items()):
            if receipt['status'] not in ('submitted', 'parse_intent', 'running'):
                continue
            docs = self.client.documents(self.image_dataset, document_id=receipt['ragflow_document_id'])
            if len(docs) != 1 or docs[0].get('name') != receipt['name']:
                raise ValueError('图片文档远端身份不一致：' + key)
            run = docs[0]['run']
            if run in ('DONE', 'FAIL', 'CANCEL'):
                receipt['status'] = run.lower()
                if run != 'DONE':
                    self.state['errors']['image:' + key] = '图片材料解析状态：' + run
                self.save()
                print('IMAGE_PARSE_END', key, run, flush=True)
            elif run == 'RUNNING' and receipt['status'] == 'parse_intent':
                receipt['status'] = 'submitted'
                self.save()
            elif (run == 'UNSTART' and receipt['status'] == 'parse_intent'
                  and time.time() - receipt.get('parse_requested_at', 0) > 180):
                receipt['status'] = 'unknown'
                self.state['errors']['image:' + key] = '图片解析提交结果不确定，需核对远端状态'
                self.save()

    def tick(self) -> tuple[bool, dict]:
        if getattr(self,'private_dir',None) and (self.private_dir/'queue_owner.json').exists():
            raise RuntimeError('Batch is owned by the universal ingest queue')
        batch = json.loads(self.batch_path.read_text(encoding='utf-8'))
        if batch.get('run_id') != self.batch_dir.name:
            raise ValueError('批次清单编号不一致')
        self.poll_primary()
        items = ready_items(batch, self.pilot_ids, self.state['primary'])
        new_uploads = 0
        for item in items:
            paper_id = item['paper_id']
            receipt = self.state['primary'].get(paper_id)
            recoverable_oversize = bool(
                receipt and receipt.get('status') == 'unknown'
                and not receipt.get('ragflow_document_id')
                and len(receipt.get('upload_name', '').encode('utf-8')) > 255
                and 'File name must be 255 bytes or less' in self.state['errors'].get(paper_id, ''))
            if receipt and receipt['status'] in ('fail', 'cancel', 'unknown') and not recoverable_oversize:
                continue
            if receipt and receipt['status'] in ('done', 'submitted', 'parse_intent'):
                continue
            if (receipt and receipt['status'] == 'uploaded'
                    and sum(row['status'] in ('submitted', 'parse_intent')
                            for row in self.state['primary'].values()) >= self.max_active):
                continue
            if receipt is None and new_uploads >= MAX_UPLOADS_PER_TICK:
                break
            try:
                package, manifest = checked_copy(item, self.batch_dir, self.copies)
                self.primary(item, package, manifest)
                if receipt is None and paper_id in self.state['primary']:
                    new_uploads += 1
                self.state['errors'].pop(paper_id, None)
            except (ValueError, OSError) as exc:
                self.state['errors'][paper_id] = str(exc)[:350]
                self.save()
                print('ITEM_ERROR', item['id'], paper_id, str(exc)[:180], flush=True)
            except Exception as exc:
                self.state['errors'][paper_id] = str(exc)[:350]
                self.save()
                print('API_ERROR', item['id'], paper_id, str(exc)[:180], flush=True)
                break

        self.poll_images()
        new_trees = 0
        new_method_graphs = 0
        new_method_files = 0
        for item in items:
            paper_id = item['paper_id']
            receipt = self.state['primary'].get(paper_id)
            if not receipt or receipt['status'] != 'done':
                continue
            if (paper_id in self.state.get('images_complete', {})
                    and receipt.get('tree_status') in ('done', 'needs_review')
                    and receipt.get('method_graph_status') in ('done', 'needs_review', 'skipped')
                    and not self.native_graph_cleanup_pending(paper_id, receipt)):
                continue
            if (paper_id in self.state.get('images_complete', {})
                    and new_trees >= 4 and new_method_graphs >= 4):
                continue
            try:
                package, manifest = checked_copy(item, self.batch_dir, self.copies)
                if paper_id not in self.state.get('images_complete', {}):
                    if self.images(item, package, manifest) is True:
                        self.state.setdefault('images_complete', {})[paper_id] = True
                        self.save()
                if receipt.get('tree_status') not in ('done', 'needs_review') and new_trees < 4:
                    self.direct_tree(item, package)
                    new_trees += 1
                if (receipt.get('method_graph_status') not in ('done', 'needs_review', 'skipped')
                        and new_method_graphs < 4):
                    new_method_files += bool(self.method_graph(item, package))
                    new_method_graphs += 1
                self.cleanup_native_graph(item)
                self.state['errors'].pop(paper_id, None)
            except (ValueError, OSError) as exc:
                self.state['errors'][paper_id] = str(exc)[:350]
                self.save()
                print('ITEM_ERROR', item['id'], paper_id, str(exc)[:180], flush=True)
            except Exception as exc:
                self.state['errors'][paper_id] = str(exc)[:350]
                self.save()
                print('API_ERROR', item['id'], paper_id, str(exc)[:180], flush=True)
                break
        if new_method_files:
            self.rebuild_method_index()
        # Newly prepared papers take available parse slots before legacy
        # arrearage retries, so old structure compilation cannot starve v5.
        self.retry_arrearage()
        terminal = batch.get('status') in ('completed', 'failed', 'cancelled')
        pending = [item for item in items if self.state['primary'].get(item['paper_id'], {}).get('status')
                   not in ('done', 'fail', 'cancel', 'unknown')]
        summary = {'batch_status': batch.get('status'), 'processed_sources': batch.get('processed_sources'),
                   'passed_education': len(items) + len(self.pilot_ids),
                   'uploaded': len(self.state['primary']),
                   'done': sum(row['status'] == 'done' for row in self.state['primary'].values()),
                   'parse_fail': sum(row['status'] == 'fail' for row in self.state['primary'].values()),
                   'parse_waiting': sum(self.state['primary'].get(item['paper_id'], {}).get('status') == 'uploaded'
                                        for item in items),
                   'images': len(self.state['images']), 'errors': len(self.state['errors'])}
        self.state['summary'] = summary
        self.save()
        return terminal and not pending, summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_id')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--recover-arrearage-only', action='store_true')
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    batch_path = ROOT / 'data' / 'runs' / args.run_id / 'manifest.json'
    private_dir = ROOT / 'data' / 'private' / 'education_full_ingest' / args.run_id
    private_dir.mkdir(parents=True, exist_ok=True)
    with (private_dir / 'worker.lock').open('a+b') as lock:
        lock.seek(0)
        lock.write(b'0')
        lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            raise RuntimeError('该批次已经有一个导入进程') from None
        if (private_dir/'queue_owner.json').exists():
            raise RuntimeError('Batch is owned by the universal ingest queue')
        pilot_dir = ROOT / 'data' / 'private' / 'education_pilot'
        sys.path.insert(0, str(pilot_dir))
        from deploy_pilot import client, state as deployment
        pilot_ids = {row['stable_paper_id'] for row in json.loads(
            (pilot_dir / 'selection.json').read_text(encoding='utf-8'))}
        datasets = {row['id']: row for row in client.datasets()}
        if datasets[deployment['paper_dataset_id']]['name'] != '教育论文研究范例·八篇试验 md':
            raise ValueError('目标教育 md 知识库名称与用户指定的不一致')
        if deployment['paper_dataset_id'] == '01885874b4e311f1807c3d9794ef0978':
            raise ValueError('拒绝导入财会知识库')
        stream = Stream(batch_path, private_dir, client, deployment, pilot_ids)
        if args.recover_arrearage_only:
            stream.max_active = 1
        while True:
            try:
                if args.recover_arrearage_only:
                    finished, summary = stream.recover_arrearage_tick()
                    print('RECOVERY_PROGRESS', json.dumps(summary, ensure_ascii=False), flush=True)
                else:
                    finished, summary = stream.tick()
                    print('PROGRESS', json.dumps(summary, ensure_ascii=False), flush=True)
                if args.once or finished:
                    break
                delay = poll_delay(summary.get('parse_waiting', 0))
                time.sleep(delay)
            except ArrearageRecoveryHalted as exc:
                print('WORKER_HALTED_ARREARAGE', str(exc), flush=True)
                raise SystemExit(2) from None
            except Exception as exc:
                print('WORKER_ERROR', str(exc)[:300], flush=True)
                if args.once or args.recover_arrearage_only:
                    raise
                time.sleep(60)


if __name__ == '__main__':
    main()
