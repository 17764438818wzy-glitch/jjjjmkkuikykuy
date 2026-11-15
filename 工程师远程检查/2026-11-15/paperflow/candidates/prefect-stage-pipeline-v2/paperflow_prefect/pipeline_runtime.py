"""Fresh-process stage context; no import of legacy flows or batch registry."""
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import os
from pathlib import Path
import uuid
import psutil
from .pipeline_contracts import ArtifactRef, StageEnvelope, StageResult, operation_key
from .pipeline_ledger import StageLedger, EvidenceChanged
from .records import read_json, write_json, sha_file, identity
from .settings import Settings

active_context=ContextVar('pipeline_stage_context',default=None)


def artifact(path):
    path=Path(path).resolve()
    return ArtifactRef(str(path),sha_file(path))


def verify_ref(ref):
    if not Path(ref.path).is_absolute() or sha_file(ref.path)!=ref.sha256:
        raise EvidenceChanged('artifact changed')


@asynccontextmanager
async def native_scope(name):
    from prefect.concurrency.asyncio import concurrency, rate_limit
    if name=='model_start':
        await rate_limit('pipeline-model-start',occupy=1,strict=True)
        yield
    else:
        names=['pipeline-model-total','pipeline-'+name] if name in ('body-http','attachment-http') else ['pipeline-'+name]
        async with concurrency(names,occupy=1,strict=True): yield


@dataclass
class StageContext:
    envelope: StageEnvelope
    ledger: StageLedger
    connection: dict | None
    scope: object
    fence: object
    settings: Settings | None=None
    backend: object=None
    paper: dict | None=None
    row: dict | None=None
    config_path: Path | None=None
    config_sha256: str | None=None
    owner: dict | None=None
    recovery_authorizer: object=None

    @classmethod
    def for_transport(cls,envelope,ledger,connection,scope,fence):
        """Transport embedding seam; caller supplies its already-claimed ledger."""
        return cls(envelope,ledger,connection,scope,fence)


def load_stage_context(config_path: Path,envelope: StageEnvelope) -> StageContext:
    from .stage3_handoff import read_receipt
    from .unified import UnifiedBackend
    config_path=Path(config_path).resolve()
    expected=os.environ.get('PAPERFLOW_PIPELINE_CONFIG_SHA256')
    if not expected or sha_file(config_path)!=expected: raise EvidenceChanged('worker configuration binding changed')
    settings=Settings.load(config_path); settings.require_execution()
    pipeline=settings.values['pipeline']
    binding_refs=tuple(ArtifactRef.from_dict(value) for value in pipeline['binding_refs'])
    if pipeline.get('model_connection_ref'):
        binding_refs += (ArtifactRef.from_dict(pipeline['model_connection_ref']),)
    required={settings.path(key) for key in ('model_config','runtime_config','template_path','catalog_path','source_manifest')}
    if not required <= {Path(ref.path).resolve() for ref in binding_refs}:
        raise EvidenceChanged('worker must bind all native configuration files')
    for ref in binding_refs: verify_ref(ref)
    verify_ref(envelope.receipt_ref)
    for ref in envelope.input_refs: verify_ref(ref)
    receipt,paper=read_receipt(envelope.receipt_ref.path)
    if not paper or paper['paper_id']!=envelope.paper_id: raise EvidenceChanged('receipt paper mismatch')
    if Path(receipt['library_root']).resolve()!=settings.path('library_root'): raise EvidenceChanged('receipt library mismatch')
    row=paper['body'] if envelope.item_id is None else next((r for r in paper['attachments'] if r['file_id']==envelope.item_id),None)
    if envelope.stage.value in ('profile','prepare','package','rag_body') and envelope.item_id is not None:
        raise EvidenceChanged('body stage cannot select an attachment')
    source_sha=row['sha256'] if row else paper['body']['sha256'] if paper.get('body') else identity(paper)
    if operation_key(dict(paper_id=envelope.paper_id,source_sha256=source_sha,stage=envelope.stage.value,
            item_id=envelope.item_id,part_id=envelope.part_id,input_refs=envelope.input_refs,
            contract_version=envelope.contract_version))!=envelope.operation_key:
        raise EvidenceChanged('source or operation binding changed')
    if envelope.item_id is not None and row is None: raise EvidenceChanged('unknown attachment')
    def fence():
        if sha_file(config_path)!=expected: raise EvidenceChanged('configuration changed')
        for ref in binding_refs: verify_ref(ref)
        verify_ref(envelope.receipt_ref)
        for ref in envelope.input_refs: verify_ref(ref)
        current=read_json(pipeline['control_path'])
        if current.get('generation')!=envelope.control_generation or current.get('allowed') is not True:
            raise InterruptedError('control fence closed')
        if row and sha_file(row['path'])!=row['sha256']: raise EvidenceChanged('source bytes changed')
    fence()
    backend=UnifiedBackend(settings)
    bound=backend.receive(envelope.receipt_ref.path)
    if bound!=paper: raise EvidenceChanged('receipt binding changed')
    return StageContext(envelope,StageLedger(Path(pipeline['ledger_root'])),None,native_scope,fence,
                        settings,backend,paper,row,config_path,expected)


def current_context(envelope):
    ctx=active_context.get()
    if ctx is None or ctx.envelope!=envelope: raise EvidenceChanged('missing owning stage context')
    return ctx


def configured_path():
    value=os.environ.get('PAPERFLOW_PIPELINE_CONFIG')
    if not value: raise ValueError('explicit worker configuration required')
    return Path(value)


def task_owner(ctx):
    try:
        from prefect.runtime import task_run
        task_id=task_run.id
    except ImportError: task_id=None
    return dict(instance_id=ctx.settings.values['pipeline']['instance_id'],pid=os.getpid(),
                process_create_time=psutil.Process().create_time(),task_run_id=task_id or 'direct-'+uuid.uuid4().hex)


async def execute_stage(envelope: StageEnvelope) -> StageResult:
    from .pipeline_execution import execute_stage as execute
    return await execute(envelope)
