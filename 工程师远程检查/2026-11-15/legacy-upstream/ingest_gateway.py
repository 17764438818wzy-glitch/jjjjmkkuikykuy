"""RAGFlow writes and conservative reconciliation for uncertain acknowledgements."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Callable, Literal
import tempfile
from urllib.parse import quote
from urllib.request import Request
from evidence import package_fingerprint
from upload_tracking import service_key
from ingest_execution import current_execution, ExecutionUnconfirmed


class SourceChanged(ValueError):
    """A queued file no longer has the bytes approved at enqueue time."""


class IdentityMismatch(ValueError):
    """The remote document does not match the queued destination identity."""


class ParsePreparationFailed(RuntimeError):
    """A preflight failed before the parse POST; no parse write was attempted."""

    def __init__(self, cause):
        super().__init__('Parse preparation failed before submission: '+type(cause).__name__)
        self.status_code=getattr(cause,'status_code',None)


def _download_for_reconciliation(client, dataset_id, document):
    """Read exact uploaded bytes using the client's validated origin/opener."""
    from ragflow_client import RagflowClient
    if not isinstance(client,RagflowClient):
        raise IdentityMismatch('Remote byte verification is unavailable for this client')
    endpoint=f'/api/v1/datasets/{quote(dataset_id,safe="")}/documents/{quote(document["id"],safe="")}'
    request=Request(client.base_url+endpoint,method='GET',
                    headers={'Authorization':'Bearer '+client.api_key})
    with client.opener.open(request,timeout=20) as response:
        return response.read()


class RagflowGateway:
    def __init__(self, client_factory: Callable,
                 remote_content_reader: Callable | None = None,
                 baseline_recorder: Callable | None = None,
                 before_parse: Callable | None = None):
        self.client_factory = client_factory
        self.remote_content_reader = remote_content_reader or _download_for_reconciliation
        self.baseline_recorder = baseline_recorder
        self.before_parse = before_parse

    def verify_execution(self, task, remote):
        if (remote.get('name') != task.get('remote_name')
                or remote.get('pipeline_id') != task.get('pipeline_id')
                or (task.get('policy_key') == 'native_naive' and
                    (task.get('pipeline_id') or remote.get('chunk_method') != 'naive'))
                or not current_execution(task, remote)):
            raise ExecutionUnconfirmed('Remote execution does not belong to this parse attempt')

    def _client(self,task):
        client=self.client_factory()
        if task.get('service_key') and service_key(client.base_url)!=task['service_key']:
            raise IdentityMismatch('Queued task belongs to a different RAGFlow service')
        return client

    def lookup(self, task: dict) -> list[dict]:
        client = self._client(task)
        remote_id = task.get('remote_id')
        rows = client.documents(task['dataset_id'], document_id=remote_id) if remote_id else (
            client.documents(task['dataset_id'], name=task['remote_name']))
        return [row for row in rows
                if row.get('name') == task['remote_name']
                and (not remote_id or row.get('id') == remote_id)]

    def upload(self, task: dict) -> str:
        if task.get('package_path'):
            try:
                package_hash=package_fingerprint(Path(task['package_path']))
            except (OSError,ValueError,KeyError) as exc:
                raise SourceChanged('Queued package is missing or invalid') from exc
            if package_hash!=task.get('package_sha256'):
                raise SourceChanged('Queued approved package changed')
        path = Path(task['path'])
        try:
            content=path.read_bytes()
            actual=hashlib.sha256(content).hexdigest()
        except OSError as exc:
            raise SourceChanged('Queued source file is missing or unreadable') from exc
        if actual != task['content_sha256'].lower():
            raise SourceChanged('Queued source file hash changed')
        client = self._client(task)
        if hasattr(client,'upload_bytes'):
            return client.upload_bytes(task['dataset_id'],task['remote_name'],content)
        # Compatibility for clients without a bytes API: upload a private snapshot.
        with tempfile.TemporaryDirectory(prefix='ragflow-checked-') as directory:
            snapshot=Path(directory)/path.name
            snapshot.write_bytes(content)
            return client.upload(task['dataset_id'],snapshot,remote_name=task['remote_name'])

    def parse(self, task: dict) -> None:
        try:
            if self.before_parse:
                self.before_parse(task)
            client, remote = self._prepare_parse(task)
            if self.baseline_recorder:
                self.baseline_recorder(task, remote)
        except (IdentityMismatch, SourceChanged):
            raise
        except Exception as exc:
            raise ParsePreparationFailed(exc) from exc
        # Only failures after this boundary can represent an uncertain parse write.
        client.parse(task['dataset_id'], task['remote_id'])

    def _prepare_parse(self, task: dict):
        remote_id = task.get('remote_id')
        if not remote_id:
            raise IdentityMismatch('Parse task has no remote document ID')
        client = self._client(task)
        docs = client.documents(task['dataset_id'], document_id=remote_id)
        if len(docs) != 1 or docs[0].get('id') != remote_id or docs[0].get('name') != task['remote_name']:
            raise IdentityMismatch('Remote document identity does not match parse task')
        expected = task.get('pipeline_id')
        native = task.get('policy_key') == 'native_naive'
        if native and (expected or docs[0].get('pipeline_id') or docs[0].get('chunk_method') != 'naive'):
            raise IdentityMismatch('Native parser destination changed')
        if not expected and not native:
            raise IdentityMismatch('Parse task has no Pipeline ID')
        if docs[0].get('run')=='RUNNING':
            raise IdentityMismatch('Source is already executing remotely')
        if task.get('execution_attempt'):
            if docs[0].get('run')=='RUNNING':
                raise IdentityMismatch('Routed source is already executing remotely')
            allowed={expected,task.get('requested_pipeline_id'),task.get('execution_previous_pipeline_id'),None}
            if docs[0].get('pipeline_id') not in allowed:
                raise IdentityMismatch('Remote Pipeline changed outside the verified routes')
        if docs[0].get('pipeline_id') != expected:
            dataset = quote(task['dataset_id'], safe='')
            document = quote(remote_id, safe='')
            client.request('PATCH', f'/datasets/{dataset}/documents/{document}',
                           {'pipeline_id': expected})
            rebound = client.documents(task['dataset_id'], document_id=remote_id)
            if (len(rebound) != 1 or rebound[0].get('pipeline_id') != expected
                    or rebound[0].get('id')!=remote_id or rebound[0].get('name')!=task['remote_name']):
                raise IdentityMismatch('Remote Pipeline binding was not confirmed')
            docs = rebound
        if task.get('policy_key')=='education' and task.get('role')=='paper_primary':
            metadata=docs[0].get('meta_fields') or {}
            if metadata.get('education_paper_id') not in (None, task['source_id']):
                raise IdentityMismatch('Education metadata belongs to another paper')
            kept={key:value for key,value in metadata.items()
                  if not key.startswith(('education_','artifact_'))}
            if kept!=metadata:
                # Old string metadata otherwise overwrites the new generated values.
                dataset=quote(task['dataset_id'],safe='');document=quote(remote_id,safe='')
                client.request('PATCH',f'/datasets/{dataset}/documents/{document}',{'meta_fields':kept})
                refreshed=client.documents(task['dataset_id'],document_id=remote_id)
                if (len(refreshed)!=1 or any(refreshed[0].get(k)!=docs[0].get(k)
                        for k in ('id','name','pipeline_id','process_begin_at','run'))
                        or (refreshed[0].get('meta_fields') or {})!=kept):
                    raise IdentityMismatch('Education metadata reset was not confirmed')
                docs=refreshed
        return client,docs[0]


def reconcile_write(task: dict, operation: Literal['upload', 'parse'],
                    gateway: RagflowGateway) -> str:
    if operation not in ('upload', 'parse'):
        raise ValueError('Unknown RAGFlow write operation')
    try:
        docs = gateway.lookup(task)
    except Exception:
        return 'uncertain'
    if len(docs) != 1:
        return 'uncertain'
    doc = docs[0]
    if operation == 'parse':
        if not task.get('remote_id') or doc.get('pipeline_id') != task.get('pipeline_id'):
            return 'uncertain'
        if (task.get('policy_key') == 'native_naive' and
                (task.get('pipeline_id') or doc.get('chunk_method') != 'naive')):
            return 'uncertain'
        return 'confirmed' if (current_execution(task, doc)
            and doc.get('run') in ('RUNNING', 'DONE', 'FAIL', 'CANCEL')) else 'uncertain'
    if task.get('remote_id'):
        return 'confirmed'
    if gateway.remote_content_reader is None:
        return 'uncertain'
    try:
        client = gateway.client_factory()
        data = gateway.remote_content_reader(client, task['dataset_id'], doc)
        if not isinstance(data, bytes):
            return 'uncertain'
        actual = hashlib.sha256(data).hexdigest()
    except Exception:
        return 'uncertain'
    return 'confirmed' if actual == task['content_sha256'].lower() else 'uncertain'
