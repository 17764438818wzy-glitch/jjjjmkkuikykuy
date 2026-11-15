"""Pure, explicit full-input admission. No config loading, network, or persistence.

Counts always describe the complete intended vector input, before token truncation.
Pipeline cleaning is the source-inspected transformation; token budgets remain
independent because the deployed model and middleware use different tokenizers.
"""
from dataclasses import asdict, dataclass
import hashlib
import json
import re


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


@dataclass(frozen=True)
class AdmissionPolicy:
    model_budget: int
    model_sha256: str
    libllama_sha256: str
    cl100k_sha256: str
    path_fingerprint: str
    renderer_fingerprint: str
    physical_capacity: int = 2048
    pipeline_limit: int = 2038
    adapter_limit: int = 8191
    special_tokens: str = 'add_special=true;parse_special=true'
    schema: str = 'paperflow-embedding-admission/1'
    counter_runtime_sha256: str | None = None

    def __post_init__(self):
        if (type(self.model_budget) is not int or not 1 <= self.model_budget <= self.physical_capacity
                or self.physical_capacity != 2048 or self.pipeline_limit != 2038
                or self.adapter_limit != 8191 or self.special_tokens != 'add_special=true;parse_special=true'
                or any(not x for x in (self.model_sha256, self.libllama_sha256, self.cl100k_sha256,
                                      self.path_fingerprint, self.renderer_fingerprint))):
            raise ValueError('explicit verified embedding policy required')

    @property
    def fingerprint(self):
        return digest(asdict(self))


@dataclass(frozen=True)
class EmbeddingInput:
    specialty_code: str
    role: str
    record_id: str
    record_type: str
    path: str
    text: str


def evaluate(inputs, policy, counter, *, observed_path, observed_renderer):
    """Return auditable evidence; unknown counters or paths never mean pass."""
    result = dict(schema='paperflow-embedding-admission/1', rule_fingerprint=None,
                  counter_verified=False, path_verified=False,
                  endpoint_probe_status='not_run', admission_verified=False, records=[], issues=[])
    result['rules']=None
    if not isinstance(policy, AdmissionPolicy):
        result['issues'].append(dict(category='embedding_policy_unverified'))
        return result
    result['rule_fingerprint'] = policy.fingerprint
    result['rules']={key:getattr(policy,key) for key in ('model_budget','physical_capacity',
        'pipeline_limit','adapter_limit','model_sha256','libllama_sha256','cl100k_sha256','special_tokens')}
    result['path_verified'] = (observed_path == policy.path_fingerprint and
                               observed_renderer == policy.renderer_fingerprint)
    expected = {key: getattr(policy, key) for key in ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}
    result['counter_verified'] = counter is not None and getattr(counter, 'identity', None) == expected
    if not result['path_verified']:
        result['issues'].append(dict(category='embedding_path_unverified'))
    if not result['counter_verified']:
        result['issues'].append(dict(category='embedding_counter_unverified'))
    inputs = list(inputs)
    if not inputs:
        result['issues'].append(dict(category='embedding_inputs_unverified'))
    if result['issues']:
        return result
    texts = []
    for row in inputs:
        if (row.path not in ('native_content', 'native_title', 'add_chunk', 'add_chunk_title')
                or not all((row.specialty_code, row.role, row.record_id, row.record_type))
                or not isinstance(row.text, str)):
            result['path_verified'] = False
            result['issues'].append(dict(category='embedding_path_unverified', record_id=row.record_id))
            return result
        texts.append(re.sub(r'</?(table|td|caption|tr|th)( [^<>]{0,12})?>', ' ', row.text).strip()
                     if row.path == 'native_content' else row.text)
    try:
        counts = counter.count_many(texts)
        if hasattr(counts, 'counter_epoch'):
            result['counter_epoch'] = counts.counter_epoch
        if len(counts) != len(inputs):
            raise ValueError('counter result cardinality mismatch')
        if any(type(c.get(k)) is not int or c[k] < 0 for c in counts
               for k in ('model_tokens', 'cl100k_tokens')):
            raise ValueError('counter returned invalid count')
    except Exception as exc:
        from .scheduling_policy import ActionDeferred
        if isinstance(exc, ActionDeferred): raise
        result['counter_verified'] = False
        result['issues'].append(dict(category='embedding_counter_unverified', error_type=type(exc).__name__))
        return result
    for row, text, count in zip(inputs, texts, counts):
        limit = policy.pipeline_limit if row.path == 'native_content' else policy.adapter_limit
        evidence = dict(specialty_code=row.specialty_code, role=row.role, record_id=row.record_id,
                        record_type=row.record_type, path=row.path, input_sha256=hashlib.sha256(row.text.encode()).hexdigest(),
                        counted_sha256=hashlib.sha256(text.encode()).hexdigest(), cleaning_changed=text != row.text,
                        model_tokens=count['model_tokens'], model_budget=policy.model_budget,
                        cl100k_tokens=count['cl100k_tokens'], cl100k_limit=limit, issues=[])
        if count['model_tokens'] > policy.model_budget:
            evidence['issues'].append('embedding_model_length')
        if count['cl100k_tokens'] > limit:
            evidence['issues'].append('embedding_pipeline_length' if row.path == 'native_content' else 'embedding_adapter_length')
        result['records'].append(evidence)
    result['admission_verified'] = not any(row['issues'] for row in result['records'])
    return result


def qualification_summary(manifest_sha256,evidence):
    """Bounded durable fact; no full text, records array, request or credentials.

    This attests local input qualification only, never remote write termination.
    The evidence digest binds the complete per-record counts without sending
    that potentially large array through action-result IPC.
    """
    evidence=evidence or {}
    records=evidence.get('records',[])
    rules=evidence.get('rules')
    rule_keys=('model_budget','physical_capacity','pipeline_limit','adapter_limit',
               'model_sha256','libllama_sha256','cl100k_sha256','special_tokens')
    result=dict(schema='paperflow-embedding-qualification/1',scope='local_full_input',
        manifest_sha256=manifest_sha256,rule_fingerprint=evidence.get('rule_fingerprint'),
        counter_verified=evidence.get('counter_verified') is True,
        path_verified=evidence.get('path_verified') is True,
        admission_verified=evidence.get('admission_verified') is True,
        endpoint_probe_status='not_run',rules={k:rules[k] for k in rule_keys if k in rules} if rules else None,
        record_count=len(records),record_evidence_sha256=digest(records),
        max_model_tokens=max((r['model_tokens'] for r in records),default=None),
        max_cl100k_tokens=max((r['cl100k_tokens'] for r in records),default=None))
    if isinstance(evidence.get('counter_epoch'), str):
        result['counter_epoch'] = evidence['counter_epoch']
    if evidence.get('reason')=='mapping_pending':
        result.update(admission_verified=False,reason='mapping_pending')
    return result


def final_inputs(task, packet, *, native):
    """Reuse the production publisher's projections and exact identity prefixes."""
    from .rag import RagPublisher
    publisher = RagPublisher(None)
    result = []
    for route in packet.get('admission_routes', packet['routes']):
        for role, records in publisher.records(packet).items():
            if not records:
                continue
            for record in records:
                rid = record[0]
                kind = ('profile' if rid == '__profile' or rid.startswith('__profile.p') else
                        'tree' if rid == '__tree' or rid.startswith('__tree.p') else
                        'asset' if rid.startswith('asset_') else 'body')
                result.append(EmbeddingInput(route['code'], role, rid, kind,
                    'native_content' if native and role == 'body' else 'add_chunk', publisher.render(task, packet, record)))
            # Native Pipeline embeds from_upstream.name independently of its content.
            # Add Chunk embeds the same document name through its title path.
            _, name = publisher.document_names(task, packet, route['code'], role)
            result.append(EmbeddingInput(route['code'], role, '__title', 'document_title',
                'native_title' if native and role == 'body' else 'add_chunk_title', name))
    return result


def repair_route(record):
    """Only generated content has an existing LLM repair owner.

    Program projections, title/source mappings, and unknown configuration need
    explicit structural work; they must never consume a generic LLM correction.
    """
    return {'body': 'round3_chunk', 'asset': 'round3_assets'}.get(record['record_type'])


class AdmissionRejected(ValueError):
    def __init__(self, evidence):
        self.evidence = evidence
        super().__init__('embedding admission rejected: ' + str(evidence.get('rule_fingerprint')))


class PackageAdmission:
    """Same injectable boundary for package preparation and remote delivery."""
    def __init__(self, policy, counter, *, native, observed_path, observed_renderer):
        self.policy, self.counter, self.native = policy, counter, native
        self.observed_path, self.observed_renderer = observed_path, observed_renderer
        self._memo = None

    def memo_key(self,task,packet):
        return digest(dict(inputs=[asdict(row) for row in final_inputs(task,packet,native=self.native)],
            manifest_sha256=packet['manifest_sha256'],policy=self.policy.fingerprint if self.policy else None,
            observed_path=self.observed_path,observed_renderer=self.observed_renderer,
            current_renderer=renderer_fingerprint(),counter_identity=getattr(self.counter,'identity',None)))

    def check(self, task, packet):
        key=self.memo_key(task,packet)
        if self._memo is not None and self._memo[0]==key:
            return self._memo[1]
        result = evaluate(final_inputs(task, packet, native=self.native), self.policy, self.counter,
                          observed_path=self.observed_path, observed_renderer=self.observed_renderer)
        if not result['admission_verified']:
            raise AdmissionRejected(result)
        self._memo=(key,result)
        return result

    def close(self):
        if self.counter is not None and hasattr(self.counter, 'close'):
            self.counter.close()


def renderer_fingerprint():
    from pathlib import Path
    root = Path(__file__).parent
    return digest({name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                   for name in ('rag.py', 'native_delivery.py', 'embedding_admission.py',
                                'embedding_counter.py', 'embedding_counter_worker.py',
                                'embedding_service.py', 'embedding_recovery.py', 'private_files.py',
                                'structure_split.py', 'structure_plan.py')})


def path_fingerprint(config, evidence):
    """Local binding only; live deployment identity remains an independent gate."""
    return digest(dict(native_pipeline_id=config.native_pipeline_id,
                       path_evidence=evidence, content_field='text',
                       generated_questions=False, generated_summary=False))


def admission_from_config(config):
    """Explicit runtime wiring without any implicit config or credential lookup."""
    settings = getattr(config, 'embedding_admission', None)
    if not settings:
        return PackageAdmission(None, None, native=bool(config.native_pipeline_id),
                                observed_path=None, observed_renderer=None)
    try:
        policy = AdmissionPolicy(model_budget=config.embedding_model_budget, **settings['policy'])
        if (settings['path_evidence'].get('generated_questions') is not False
                or settings['path_evidence'].get('generated_summary') is not False):
            raise ValueError('unknown vector input selection')
        observed_path = path_fingerprint(config, settings['path_evidence'])
        observed_renderer = renderer_fingerprint()
    except (KeyError, TypeError, ValueError):
        return PackageAdmission(None, None, native=bool(config.native_pipeline_id),
                                observed_path=None, observed_renderer=None)
    return ConfiguredAdmission(policy, settings.get('counter'), native=bool(config.native_pipeline_id),
                               observed_path=observed_path, observed_renderer=observed_renderer)


class ConfiguredAdmission(PackageAdmission):
    """Managed exact counter only. Unattached/unavailable services fail closed.

    Worker qualification runs outside resource actions. A prepared local seal
    may consume its identical full-input result; manager registration separately
    checks the service's live authoritative cache without starting/waiting.
    """
    def __init__(self, policy, settings, **options):
        super().__init__(policy, None, **options)
        self.settings = settings
        self.prepared_only = False

    def memo_key(self,task,packet):
        from pathlib import Path
        from .embedding_counter import CL100K_CACHE
        states={}
        try:
            paths=[Path(self.settings['model_path']),Path(self.settings['library_path']),
                   Path(self.settings['cache_dir'])/CL100K_CACHE]
            for path in paths:
                stat=path.stat()
                states[str(path.resolve())]=(stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns)
        except (OSError,KeyError,TypeError):
            states={'artifacts':'missing'}
        return digest(dict(inputs=super().memo_key(task,packet),settings=self.settings,artifact_states=states))

    def validate_prepared(self, evidence):
        """Check the live generation only; safe inside an existing local action.

        Managed adapters implement this boundary. Pure injected test/audit
        counters retain their explicit non-service contract.
        """
        if hasattr(self.counter, 'validate_epoch'):
            self.counter.validate_epoch(evidence.get('counter_epoch'))
        return evidence

    def check(self, task, packet):
        from .embedding_service import CountsPending
        self.observed_renderer=renderer_fingerprint()
        if self.observed_renderer!=self.policy.renderer_fingerprint or self.observed_path!=self.policy.path_fingerprint:
            raise AdmissionRejected(evaluate([],self.policy,None,observed_path=self.observed_path,
                                             observed_renderer=self.observed_renderer))
        key=self.memo_key(task,packet)
        if packet.get('structure_plan') is not None:
            from .structure_plan import validate_plan
            # A sealed plan fixes the original mapping, not today's admission.
            # Preserve its identity/SHA/reassembly checks; qualify the complete
            # current rendered inputs below under the current policy and epoch.
            validate_plan(task,packet,packet['structure_plan'])
        if self.prepared_only:
            if self._memo is None or self._memo[0]!=key: raise CountsPending()
            return self.validate_prepared(self._memo[1])
        result=PackageAdmission(self.policy, self.counter, native=self.native,
            observed_path=self.observed_path, observed_renderer=self.observed_renderer).check(task, packet)
        self.validate_prepared(result)
        self._memo=(key,result)
        return result


def bind_worker_counter(admission, client, root):
    if isinstance(admission,ConfiguredAdmission) and client is not None:
        from .embedding_service import PipeCounter
        admission.counter=PipeCounter(client,root,admission.policy)
    return admission


def bind_manager_counter(admission, manager, config):
    if isinstance(admission,ConfiguredAdmission):
        from .embedding_service import CachedCounter,install
        endpoint=install(manager,config)
        if endpoint is not None:admission.counter=CachedCounter(endpoint.broker)
    return admission
