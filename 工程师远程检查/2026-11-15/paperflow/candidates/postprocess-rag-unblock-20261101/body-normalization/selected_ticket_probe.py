"""Read-only selection evidence for one explicitly named running parent task.

This is an operator tool, not a dispatcher or an execution reservation. Only
the candidate evidence file is created; upstream SQLite connections are read
only, Prefect access is GET-only, and no claim/model/transport API is called.
The evidence must be freshly revalidated before any separately authorized test.
Neither a successful graph check nor a recorded identity is a processing grant.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from types import ModuleType
from urllib.parse import parse_qs, urlparse
from uuid import UUID

sys.dont_write_bytecode = True

from snapshot_collect import IDENTITY, _Collector, collect_inputs
from snapshot_prepare import counter_contract_from_runtime

WORKSPACE = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT = Path(__file__).resolve().parent / 'selected-ticket-evidence'
SCHEMA = 'paperflow-selected-ticket-evidence/1'
SHA = re.compile('[a-f0-9]{64}')
MAX_JSON = 64 * 1024 * 1024


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf8')


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def _path(value, *, directory=False, exists=True):
    require(isinstance(value, (str, Path)), 'missing independent configured path')
    path = Path(value)
    require(path.is_absolute() and '..' not in path.parts, 'configured path must be absolute')
    if exists:
        _Collector.no_links(path)
        require(path.is_dir() if directory else path.is_file(), 'configured path is unavailable')
    return path


def _json(path):
    path = _path(path)
    info = path.stat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= MAX_JSON,
            'invalid configuration or evidence file')
    with path.open('rb') as stream:
        opened = os.fstat(stream.fileno())
        require((info.st_dev, info.st_ino) == (opened.st_dev, opened.st_ino), 'file changed on open')
        raw = stream.read(MAX_JSON + 1)
        final = os.fstat(stream.fileno())
    require(len(raw) <= MAX_JSON and (opened.st_size, opened.st_mtime_ns, opened.st_ctime_ns)
            == (final.st_size, final.st_mtime_ns, final.st_ctime_ns), 'file changed during read')
    _Collector.no_links(path)
    after = path.stat()
    # Windows CRT fstat and Path.stat expose different ctime precision/semantics.
    # Compare ctime only between fstat observations of the same open handle.
    require((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            == (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns),
            'file changed after read')
    value = json.loads(raw)
    require(isinstance(value, dict), 'expected configuration object')
    return value, dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest())


@dataclass(frozen=True)
class Sources:
    workspace: Path
    settings: dict
    queue: dict
    classification: dict
    pins: dict

    @property
    def control_database(self):
        return Path(self.classification['classification_root']) / 'group-task-summaries/summary-index.sqlite3'


def load_sources(workspace=WORKSPACE):
    """Read actual fixed configuration locations, never paths from materials."""
    workspace = _path(Path(workspace), directory=True)
    settings, sp = _json(workspace / 'prefect-paperflow/config.json')
    queue, qp = _json(workspace / 'candidates/attachment-prefect-live-link-20261031/attachment-queue-config.json')
    classification, cp = _json(workspace / 'candidates/first-step-control-20261029/runtime/workspace-config.json')
    require(set(queue) == {'queue_db', 'receipt_root', 'journal_root'}, 'unexpected queue configuration')
    for value in (settings.get('data_root'), settings.get('library_root'),
                  queue.get('receipt_root'), queue.get('journal_root'), classification.get('classification_root')):
        _path(value, directory=True)
    _path(queue['queue_db'])
    _path(settings.get('catalog_path'))
    _, rp = _json(_path(settings.get('runtime_config')))
    api = str(settings.get('prefect_api_url', '')).rstrip('/')
    parsed = urlparse(api)
    require(parsed.scheme in {'http', 'https'} and parsed.hostname and not parsed.username
            and not parsed.password and not parsed.query and not parsed.fragment
            and api == str(classification.get('prefect_api_url', '')).rstrip('/'),
            'independent Prefect configurations disagree')
    # Backend signature inputs are pinned when present. No credential file is
    # read or persisted. Synthetic loaders need only the minimal runtime config.
    pins = dict(settings=sp, queue=qp, classification=cp, runtime=rp)
    for key in ('model_config', 'template_path', 'source_manifest'):
        if key in settings:
            path = _path(settings[key])
            if path.suffix.lower() == '.json':
                _, pins[key] = _json(path)
            else:
                with path.open('rb') as stream:
                    pins[key] = dict(path=str(path), sha256=hashlib.file_digest(stream, 'sha256').hexdigest())
    return Sources(workspace, settings, queue, classification, pins)


def _fresh_sources(sources):
    require(load_sources(sources.workspace) == sources, 'independent configuration changed during check')


def _snapshot(sources, task_id, selected=None, *, require_attachment=True, resume_job_id=None):
    """One consistent readonly snapshot. No schema initialization or claims."""
    require(type(require_attachment) is bool, 'explicit attachment selection mode required')
    if resume_job_id is not None:
        require(selected is not None and isinstance(resume_job_id, str)
                and str(UUID(resume_job_id)) == resume_job_id,
                'one original stopped stage is required')
    try:
        require(str(UUID(task_id)) == task_id, 'invalid explicit parent task')
    except (TypeError, AttributeError) as exc:
        raise ValueError('invalid explicit parent task') from exc
    database = _path(sources.control_database)
    with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        require(task is not None, 'explicit parent task is unavailable')
        task = dict(task)
        require(task['desired_state'] == 'running' and task['start_pending'] == 0
                and task['task_status'] not in {'deleting', 'delete_failed', 'deleted', 'unknown'}
                and task['prefect_order_model'] == 'v2'
                and type(task['start_generation']) is int and task['start_generation'] > 0,
                'original parent task is not running')
        require(not db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone(),
                'original parent task was deleted')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        args = [task_id]
        sql = 'SELECT * FROM llm_folder_queue WHERE task_id=?'
        if selected is not None:
            sql += ' AND group_id=?'; args.append(selected['group_id'])
        candidates = db.execute(sql + ' ORDER BY rowid', args)
        for queue in candidates:
            queue = dict(queue)
            identity = {key: queue[key] for key in IDENTITY}
            if selected is not None and identity != selected:
                raise ValueError('the originally selected folder identity changed')
            if (queue['state'] != 'waiting_postprocessing' or queue['parent_run_id'] != task['prefect_run_id']
                    or type(queue['generation']) is not int or not 0 < queue['generation'] <= task['start_generation']):
                continue
            admissions = [dict(r) for r in db.execute('SELECT * FROM llm_request_admissions '
                'WHERE task_id=? AND task_run_id=? ORDER BY rowid', (task_id, identity['task_run_id']))]
            kinds = {r['kind'] for r in admissions}
            if (not admissions or any(r['state'] != 'raw_complete' for r in admissions)
                    or 'body' not in kinds or (require_attachment and 'attachment' not in kinds)):
                continue
            if resume_job_id is None:
                occupied = any(table in tables and db.execute('SELECT 1 FROM ' + table
                    + ' WHERE task_id=? AND group_id=? LIMIT 1', (task_id, identity['group_id'])).fetchone()
                    for table in ('postprocessing_claims', 'postprocessing_results', 'paperflow_postprocess_stage_jobs'))
                if occupied:
                    continue
                stage = None
            else:
                require({'postprocessing_claims', 'postprocessing_results', 'paperflow_postprocess_stage_jobs'} <= tables,
                        'original stopped stage history is unavailable')
                for table in ('postprocessing_claims', 'postprocessing_results'):
                    require(not db.execute('SELECT 1 FROM ' + table + ' WHERE task_id=? AND group_id=? LIMIT 1',
                                           (task_id, identity['group_id'])).fetchone(),
                            'stopped stage already has a claim or result')
                stages = [dict(r) for r in db.execute('SELECT * FROM paperflow_postprocess_stage_jobs '
                    'WHERE task_id=? AND group_id=?', (task_id, identity['group_id']))]
                require(len(stages) == 1, 'one original stopped stage is required')
                stage = stages[0]
                require(stage['job_id'] == resume_job_id and stage['state'] == 'stopped_before_submit'
                        and all(stage[key] == identity[key] for key in IDENTITY)
                        and stage['folder_run_id'] == identity['task_run_id']
                        and type(stage['generation']) is int
                        and 1 <= queue['generation'] == stage['source_llm_generation']
                            <= stage['generation'] < task['start_generation']
                        and stage['manifest_sha256'] == queue['manifest_sha256']
                        and stage['owner_pid'] is None and stage['owner_started'] is None
                        and stage['error_type'] is None, 'original stopped stage identity changed')
            require(set(identity) == set(IDENTITY) and all(isinstance(v, str) and v for v in identity.values())
                    and SHA.fullmatch(queue['manifest_sha256']), 'original folder identity is incomplete')
            snapshot = dict(task=task, queue=queue, admissions=admissions, identity=identity)
            if stage is not None:
                snapshot['resume_stage'] = stage
            return snapshot
    raise ValueError('no eligible original raw-complete folder' +
                     (' with an attachment' if require_attachment else ''))


def _minimal_roots(roots):
    paths = sorted(set(roots), key=lambda p: (len(p.parts), str(p)))
    return [p for p in paths if not any(p != other and p.is_relative_to(other) for other in paths)]


def _authorizations(sources, verified, inputs, backend):
    """Only registered SHA references may narrow an independent configured scope.

    Preliminary metadata reads use independent configured scopes, never material
    file_hashes. Those scopes are NOT copied wholesale into the final policy.
    In particular no whole journal, classification or library root is granted.
    """
    identity = inputs['identity']; manifest = verified['manifest']; registered = verified['registered']
    require(all(manifest.get(k) == identity[k] for k in IDENTITY[:5])
            and digest(manifest) == inputs['manifest_sha256'], 'verified original manifest changed')
    require(all(registered.get(k) == identity[k] for k in IDENTITY[:4]), 'registered receipt identity changed')
    require(inputs['body']['receipt'] == manifest['receipt']
            and inputs['body']['material'] == manifest['material']
            and inputs['body']['reading_material'] == manifest['reading_material'], 'input manifest references changed')
    require(Path(registered['receipt_path']) == Path(manifest['receipt']['path'])
            and registered['receipt_sha256'] == manifest['receipt']['sha256'], 'registered receipt pin changed')
    require(Path(backend.settings.path('data_root')) == Path(sources.settings['data_root'])
            and Path(backend.settings.path('catalog_path')) == Path(sources.settings['catalog_path']),
            'backend independent configuration changed')
    data = Path(sources.settings['data_root']); receipt_scope = Path(sources.queue['receipt_root'])
    classification = Path(sources.classification['classification_root'])
    domains = [data, receipt_scope, Path(sources.queue['journal_root']), classification,
               Path(sources.settings['library_root'])]
    roots = [data]; evidence = [dict(category='data_root', authority='settings.data_root', path=str(data))]

    def narrow(path, category, scopes):
        path = _path(path, exists=False)
        require(any(path.is_relative_to(scope) for scope in scopes), 'registered dependency has no independent root')
        _path(path, directory=True)
        require(not any(path == scope for scope in scopes if scope != data),
                'registered dependency would authorize an entire configured scope')
        roots.append(path); evidence.append(dict(category=category, authority='verified_registration_and_config', path=str(path)))

    receipt_path = Path(registered['receipt_path'])
    require(receipt_path.is_relative_to(receipt_scope), 'receipt outside independent receipt root')
    # receipts/ is the schema's fixed registered filename scope, not receipt_root.
    if not receipt_path.is_relative_to(data):
        narrow(receipt_path.parent, 'receipt_directory', [receipt_scope])
    meta = _Collector(domains, defer_binary_hash=True, max_json_file_bytes=MAX_JSON,
                      max_total_json_bytes=256 * 1024 * 1024)
    receipt = meta.read(manifest['receipt'], 'registered.receipt', json_value=True)
    require(all(receipt.get(k) == identity[k] for k in ('group_id','paper_id','receipt_id')),
            'registered receipt content identity changed')
    delivery = _path(registered['delivery_dir'], directory=True)
    require(delivery == Path(receipt['delivery_ref']['path']).parent, 'registered delivery binding changed')
    # Registration independently binds this exact delivery directory; require
    # it to remain under independent receipt/library configuration as well.
    if not delivery.is_relative_to(data):
        narrow(delivery, 'registered_delivery_directory', domains)
    require(Path(inputs['body']['plan']['work']) == Path(manifest['body_work'])
            and Path(manifest['body_work']).is_relative_to(data), 'body work lacks independent data-root binding')
    require(all(Path(item['work_dir']).is_relative_to(data) for item in inputs['attachments']),
            'attachment work lacks independent data-root binding')

    def pinned(ref, role):
        value = meta.read(ref, role, json_value=True)
        if not any(Path(ref['path']).is_relative_to(root) for root in roots):
            narrow(Path(ref['path']).parent, role, domains)
        return value

    handoff = pinned(receipt['handoff_ref'], 'registered_handoff_directory')
    current_count = 0
    for row in [receipt['inventory']['body'], *receipt['inventory']['attachments']]:
        require(row == manifest['body'] if row is receipt['inventory']['body'] else row in manifest['attachments'],
                'registered inventory differs from original manifest')
        require(row['handoff_ref']['sha256'] == receipt['handoff_ref']['sha256'], 'registered handoff SHA differs')
        require(pinned(row['handoff_ref'], 'registered_handoff_directory') == handoff, 'registered handoff content differs')
        for ref in row.get('evidence_records', []):
            record = pinned(ref, 'registered_sidecar_directory')
            from paperflow_prefect.attachment_evidence_handoff import validate_evidence_handoff
            validate_evidence_handoff(record, handoff, verify_files=False)
            source = record['source']
            require(dict(source, group_id=record['group_id']) == row['source_binding']
                    and source['file_id'] == row['upstream_file_id']
                    and source['source_sha256'] == row['sha256'], 'registered sidecar source identity changed')
            items = [record['archive_listing'], *record['text_samples'], *record['images'], record['decision_basis']]
            for item in items:
                provenance = (item or {}).get('provenance')
                if not provenance or not provenance.get('record_ref'):
                    continue
                require(provenance['kind'] == 'current_read' and provenance['entry_ref'] == source['file_id']
                        and provenance.get('record_sha256') in (None, row['handoff_ref']['sha256'])
                        and provenance.get('run_ref') is None, 'unsupported current origin binding')
                path = _path(provenance['record_ref'], exists=False)
                require(path.is_relative_to(classification), 'current origin outside independent classification root')
                require(pinned(dict(path=str(path), sha256=row['handoff_ref']['sha256']),
                               'current_classification_directory') == handoff, 'current origin differs from registered handoff')
                current_count += 1
    return _minimal_roots(roots), evidence, current_count


def _evaluate(sources, snapshot, loader, *, require_attachment=True, resume_job_id=None):
    verified, inputs, backend = loader(dict(snapshot['identity']), snapshot['admissions'])
    require(inputs['identity'] == snapshot['identity']
            and inputs['manifest_sha256'] == snapshot['queue']['manifest_sha256'], 'loader changed original identity')
    require(getattr(backend, 'invoke', None) is None, 'read-only backend must not have a model callback')
    roots, authorities, current_count = _authorizations(sources, verified, inputs, backend)
    result = collect_inputs(inputs, allowed_roots=roots, trusted_catalog_path=sources.settings['catalog_path'],
        defer_binary_hash=True, max_json_file_bytes=MAX_JSON, max_total_json_bytes=256 * 1024 * 1024)
    catalog = result.get('catalog_evidence')
    require(catalog is not None, 'exact configured catalog has not been verified')
    require(backend.runtime == _json(Path(sources.settings['runtime_config']))[0], 'backend runtime changed')
    record = dict(schema=SCHEMA, identity=snapshot['identity'], generation=snapshot['task']['start_generation'],
        source_llm_generation=snapshot['queue']['generation'], manifest_sha256=inputs['manifest_sha256'],
        claim_created=False, server_execution_started=False, requires_fresh_revalidation=True,
        configuration_sources=sources.pins, root_authorities=authorities,
        configuration=dict(allowed_roots=[str(p) for p in roots],
            trusted_files=[dict(path=catalog['path'], sha256=catalog['sha256'])]),
        counter_contract=counter_contract_from_runtime(backend.runtime),
        dependency_graph_sha256=digest(dict(inputs=inputs, files=result['files'])),
        dependency_file_count=len(result['files']),
        dependency_total_bytes=sum(row['bytes'] for row in result['files']),
        dependency_largest_file_bytes=max(row['bytes'] for row in result['files']),
        dependency_json_bytes=sum(row['bytes'] for row in result['files']
            if Path(row['path']).suffix.lower() == '.json'),
        dependency_file_types=dict(sorted(Counter(Path(row['path']).suffix.lower() or '[no_extension]'
            for row in result['files']).items())), current_read_reference_count=current_count,
        evidence_scope=result['evidence_scope'], control_snapshot_sha256=digest(snapshot))
    if resume_job_id is not None:
        record['resume_job_id'] = resume_job_id
    _fresh_sources(sources)
    require(_snapshot(sources, snapshot['identity']['task_id'], snapshot['identity'],
                      require_attachment=require_attachment, resume_job_id=resume_job_id) == snapshot,
            'original task, folder or admissions changed during check')
    return record


def _output_path(output):
    root = Path(EVIDENCE_ROOT)
    output = Path(output)
    require(output.is_absolute() and output.parent == root and output.suffix == '.json'
            and output.name not in {'', '.', '..'}, 'output must be a direct candidate evidence JSON file')
    require(not output.exists() and not output.is_symlink(), 'candidate evidence already exists')
    _Collector.no_links(root if root.exists() else root.parent)
    return output


def _summary(record, **extra):
    return dict(status='checked', dependency_file_count=record['dependency_file_count'],
        dependency_total_bytes=record['dependency_total_bytes'],
        dependency_largest_file_bytes=record['dependency_largest_file_bytes'],
        dependency_json_bytes=record['dependency_json_bytes'],
        dependency_file_types=record['dependency_file_types'], current_read_reference_count=record['current_read_reference_count'],
        evidence_scope=record['evidence_scope'], identity_printed=False, claim_created=False,
        server_execution_started=False, **extra)


def inspect_exact_folder(*, sources, identity, loader=None, resume_job_id=None):
    """Recheck one explicitly supplied folder, including body-only folders.

    This only builds the registered dependency policy in memory. It does not
    select a replacement, write evidence, claim work or start a server job.
    The live loader independently checks the complete expected component set.
    """
    require(isinstance(identity, dict) and set(identity) == set(IDENTITY)
            and all(isinstance(identity[key], str) and identity[key] for key in IDENTITY),
            'one complete original folder identity is required')
    if loader is None:
        with _read_only_boundary(sources):
            return inspect_exact_folder(sources=sources, identity=identity, loader=_live_loader(sources),
                                        resume_job_id=resume_job_id)
    _fresh_sources(sources)
    snapshot = _snapshot(sources, identity['task_id'], identity, require_attachment=False,
                         resume_job_id=resume_job_id)
    return _evaluate(sources, snapshot, loader, require_attachment=False, resume_job_id=resume_job_id)


def select_and_record(*, sources, task_id, output, loader=None):
    """Select at most ONE original folder; success records no paper contents."""
    if loader is None:
        with _read_only_boundary(sources, Path(output)):
            return select_and_record(sources=sources, task_id=task_id, output=output,
                                     loader=_live_loader(sources))
    output = _output_path(output)
    _fresh_sources(sources)
    snapshot = _snapshot(sources, task_id)
    record = _evaluate(sources, snapshot, loader or _live_loader(sources))
    require(not any(output.is_relative_to(Path(root)) for root in record['configuration']['allowed_roots']),
            'candidate output overlaps a source root')
    raw = encode(record) + b'\n'
    # No file is created before all checks, and an existing artifact is never
    # replaced. There is deliberately no claim or new serializing lock here.
    output.parent.mkdir(exist_ok=True)
    _Collector.no_links(output.parent)
    with open(output, 'xb', opener=lambda name, flags: os.open(name, flags, 0o600)) as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    return _summary(record, evidence_sha256=hashlib.sha256(raw).hexdigest())


def verify_record(*, sources, evidence, expected_sha256, loader=None):
    """Check exactly the saved identity; never select a replacement or write."""
    if loader is None:
        with _read_only_boundary(sources):
            return verify_record(sources=sources, evidence=evidence, expected_sha256=expected_sha256,
                                 loader=_live_loader(sources))
    require(isinstance(expected_sha256, str) and SHA.fullmatch(expected_sha256), 'independent evidence SHA required')
    evidence = Path(evidence)
    require(evidence.parent == Path(EVIDENCE_ROOT), 'evidence must be in the candidate evidence directory')
    saved, ref = _json(evidence)
    require(ref['sha256'] == expected_sha256 and saved.get('schema') == SCHEMA, 'selected evidence hash or schema changed')
    _fresh_sources(sources)
    identity = saved['identity']
    snapshot = _snapshot(sources, identity['task_id'], identity)
    current = _evaluate(sources, snapshot, loader or _live_loader(sources))
    require(current == saved, 'saved original identity, configuration or dependency graph changed')
    return _summary(current, same_original_identity=True, evidence_sha256=expected_sha256)


@contextmanager
def _read_only_boundary(sources, output=None):
    """CLI fail-closed guard, not a sandbox for hostile Python extensions.

    The upstream sources remain trusted code. Audit events prevent accidental
    Python writes/initializers and subprocesses; SQLite itself is opened ro.
    Injected test loaders intentionally run outside this live-only guard.
    """
    active = [True]
    allowed_databases = {sources.control_database.as_uri(), Path(sources.queue['queue_db']).as_uri()}
    output = _output_path(output) if output is not None else None
    root = Path(EVIDENCE_ROOT)

    def audit(event, args):
        if not active[0]:
            return
        if event == 'sqlite3.connect':
            name = str(args[0]); parsed = urlparse(name)
            require(name.split('?', 1)[0] in allowed_databases
                    and parse_qs(parsed.query).get('mode') == ['ro'], 'only original read-only databases are allowed')
        elif event == 'open':
            name, mode, flags = args
            writing = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
            if writing:
                require(output is not None and not isinstance(name, int) and Path(name) == output
                        and flags & os.O_CREAT and flags & os.O_EXCL and not flags & os.O_TRUNC,
                        'only a new candidate evidence file may be written')
        elif event == 'os.mkdir':
            require(output is not None and Path(args[0]) == root, 'only candidate evidence directory creation is allowed')
        elif event in {'os.remove', 'os.rmdir', 'os.rename', 'os.link', 'os.symlink', 'os.truncate',
                       'os.chmod', 'os.chown', 'os.utime', 'os.system', 'subprocess.Popen', 'os.exec', 'os.posix_spawn'}:
            raise ValueError('business filesystem mutation and subprocess execution are forbidden')
    sys.addaudithook(audit)
    try:
        yield
    finally:
        active[0] = False


@contextmanager
def _get_only_prefect(api):
    """Reject accidental POST/PATCH/DELETE by the injected Prefect client."""
    import httpx
    original, original_async = httpx.Client.send, httpx.AsyncClient.send
    def checked(client, request, *args, **kwargs):
        require(request.method == 'GET' and str(request.url).startswith(api.rstrip('/') + '/'),
                'only configured Prefect GET requests are allowed')
        return original(client, request, *args, **kwargs)
    async def checked_async(client, request, *args, **kwargs):
        require(request.method == 'GET' and str(request.url).startswith(api.rstrip('/') + '/'),
                'only configured Prefect GET requests are allowed')
        return await original_async(client, request, *args, **kwargs)
    httpx.Client.send = checked
    httpx.AsyncClient.send = checked_async
    try:
        yield
    finally:
        httpx.Client.send = original
        httpx.AsyncClient.send = original_async


def _live_loader(sources):
    """Existing upstream readers only; no LocalQueue constructor or runtime."""
    sys.dont_write_bytecode = True
    for name, value in {'PREFECT_SERVER_ALLOW_EPHEMERAL_MODE':'false', 'PREFECT_API_ENABLE_HTTP2':'false',
                        'PREFECT_LOGGING_TO_API_ENABLED':'false', 'PREFECT_CLIENT_METRICS_ENABLED':'false'}.items():
        os.environ[name] = value
    candidate = sources.workspace / 'candidates/postprocess-prefect-20261031'
    sys.path[:0] = [str(sources.workspace / 'prefect-paperflow'), str(candidate),
                    str(candidate / 'runtime_next')]
    from paperflow_prefect.settings import Settings
    import paperflow_prefect.settings as settings_module
    require(Path(settings_module.__file__).resolve().parents[2] == sources.workspace,
            'loaded upstream package belongs to another workspace')
    from paperflow_prefect.attachment_queue import settings_for_verified_receipt
    from paperflow_prefect.registered_input import verify_registered_receipt
    from paperflow_prefect.unified import UnifiedBackend
    from paperflow_prefect.llm_queue.upstream_flow_binding import UpstreamFlowBinding
    from paperflow_prefect.llm_queue.folder_completion import expected_components, inspect_folder
    from prefect.client.orchestration import SyncPrefectClient
    import stage_runtime
    require(Path(stage_runtime.__file__).resolve(strict=True) ==
            (candidate / 'runtime_next/stage_runtime.py').resolve(strict=True),
            'the active postprocessing retry binding is unavailable')
    namespace = '_paperflow_selected_probe_local_intake'
    if namespace not in sys.modules:
        package = ModuleType(namespace)
        package.__path__ = [str(sources.workspace / 'candidates/local-intake-live-link-20261031')]
        package.__package__ = namespace
        sys.modules[namespace] = package
    receiver = importlib.import_module(namespace + '.local_queue')._current_receiver_session

    def load(identity, rows):
        api = sources.settings['prefect_api_url'].rstrip('/')
        def binding_for(client):
            return stage_runtime.RetryAwareUpstream(UpstreamFlowBinding(
                sources.queue['queue_db'], sources.control_database,
                client, api, 'local-intake', verify_registered_receipt, receiver_session=receiver))
        with _get_only_prefect(api), SyncPrefectClient(api, httpx_settings={'timeout':5, 'trust_env':False}) as client:
            verified = binding_for(client).load(identity, require_running=False)
        # Enabling this in-memory constructor only permits its read-only
        # path/signature resolvers; no backend stage or connection is invoked.
        scoped = settings_for_verified_receipt(Settings({**sources.settings, 'execution_enabled':True}), verified['registered'])
        backend = UnifiedBackend(scoped, invoke=None)
        from handoff_inputs import build_inputs
        from body_adapter import frozen_plan_evidence
        from preextract_scheduler_adapter import request_observation
        manifest_sha = digest(verified['manifest'])
        inputs = build_inputs(identity=identity, verified=verified, admissions=rows, manifest_sha256=manifest_sha,
            expected_components=expected_components, inspect_folder=inspect_folder,
            observe_request=request_observation, frozen_plan=frozen_plan_evidence,
            attachment_work=backend.attachment_work)
        with _get_only_prefect(api), SyncPrefectClient(api, httpx_settings={'timeout':5, 'trust_env':False}) as client:
            checked = binding_for(client).load(identity, require_running=False)
        require(checked.get('source_retry_sha256') == verified.get('source_retry_sha256')
                and checked.get('source_retry_prefect_sha256') == verified.get('source_retry_prefect_sha256')
                and checked.get('manifest') == verified.get('manifest'),
                'the selected retry source changed during policy inspection')
        return verified, inputs, backend
    return load


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, default=WORKSPACE)
    commands = parser.add_subparsers(dest='command', required=True)
    select = commands.add_parser('select'); select.add_argument('--task-id', required=True)
    select.add_argument('--output', type=Path, required=True)
    verify = commands.add_parser('verify'); verify.add_argument('--evidence', type=Path, required=True)
    verify.add_argument('--evidence-sha256', required=True)
    args = parser.parse_args(argv)
    try:
        sources = load_sources(args.workspace)
        if args.command == 'select':
            result = select_and_record(sources=sources, task_id=args.task_id, output=args.output)
        else:
            result = verify_record(sources=sources, evidence=args.evidence, expected_sha256=args.evidence_sha256)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        # Dependency errors can contain paper paths; never print raw exceptions.
        print(json.dumps(dict(status='refused', reason='original identity/configuration/dependency check failed',
                              error_type=type(exc).__name__, identity_printed=False), sort_keys=True))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
