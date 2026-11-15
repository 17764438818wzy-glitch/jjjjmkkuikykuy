"""Explicit single-folder systemd service candidate; importing dispatches nothing.

Installation is deliberately absent. A selected, independently SHA-pinned config,
NEW service state table, pinned native file inventory, staged snapshot and
already reserved RemoteJobs job are required. main(job_id) can be called by the
existing fixed launch_systemd launcher; its default config/hash sidecars must be
installed independently beside this script. No scanning, queue, retry, model call
or RAG upload exists here.

Trust follows the supervisor: fixed business code, same Linux boot/time
namespace, private per-job prefix, and a trusted same-UID administrator. Inventory
hashes verify the explicitly listed native files, not all future OS dependencies
or a sandbox against an administrator changing executables. Currently mapped
executable files are checked
against that inventory at startup. A real Wine/native integration check is still
required before enabling a deployment.

There is NO RPC server, credential handling, production coordinator binding or
pause/resume reconciliation here. STOP.json is a job-bound local stop request,
not proof that the Windows task pause protocol is connected. A prior service
intent never permits automatic restart, including after pause/crash/ACK loss.
Unknown/failed inputs, outputs, logs and prefixes stay retained. A finished job's
pure temporary prefix is collected only after held-receipt revalidation and
confirmed cgroup removal. Explicit free-space and per-job disk thresholds still
need operator sizing; this is not permission to start a 268-folder batch.
"""
from contextlib import closing, contextmanager
import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path, PureWindowsPath
import signal
import shutil
import sqlite3
import stat
import sys
import time
from uuid import uuid4

from remote_jobs import RemoteJobs, _validated, _uuid
from server_admission import start_once, start_with_capacity_result, launch_systemd
from server_release import prepare_environment, _Tree
from server_receipt_inspect import inspect_receipt
from server_supervisor import ServerSupervisor, LinuxScope, process_identity, _boot_id
from snapshot_import import SnapshotImporter
from snapshot_wire import snapshot_sha


# This is the actual inherited PATH of the limited worker and OfflineCounter's
# subprocess, not the interactive shell/preflight PATH. Keep resolution and
# launch bound to the same value; no new lock or dependency scan is involved.
_WORKER_PATH = '/usr/bin:/bin'


class ServiceError(RuntimeError):
    pass


def _require(value, detail):
    if not value:
        raise ServiceError(detail)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _stream_digest(stream):
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
        digest.update(chunk)
    return digest.hexdigest()


def _absolute(value):
    value = Path(value)
    _require(value.is_absolute() and '..' not in value.parts, 'absolute configured server paths required')
    return value


def _read(path, *, max_bytes=64 * 1024 * 1024):
    path = _absolute(path)
    tree = _Tree(path.parent)
    try:
        with tree.regular(path.name) as (stream, before):
            _require(before.st_size <= max_bytes, 'service metadata exceeds its bound')
            raw = stream.read(max_bytes + 1)
            _require(len(raw) <= max_bytes, 'service metadata grew beyond its bound')
            return raw
    finally:
        tree.close()


def _value(path):
    return json.loads(_read(path))


def load_config(path, expected_sha256):
    try:
        raw = _read(path, max_bytes=65536)
        _require(_digest(raw) == expected_sha256, 'independently pinned service configuration changed')
        value = json.loads(raw)
        _config(value)
        return value
    except (OSError, ValueError, TypeError, RuntimeError) as exc:
        if isinstance(exc, ServiceError):
            raise
        raise ServiceError('service configuration cannot be verified') from exc


def _config(value):
    _require(isinstance(value, dict) and value.get('schema') == 'paperflow-job-service/1',
             'explicit single-job service configuration required')
    identity = value['server_identity']
    _require(set(identity) == {'server_id', 'ledger_id'} and isinstance(identity['server_id'], str)
             and identity['server_id'] and _uuid(identity['ledger_id']), 'pinned server/ledger identity required')
    for field in ('jobs_root', 'staging_root', 'base', 'native_manifest'):
        _absolute(value[field])
    jobs, staging = Path(value['jobs_root']), Path(value['staging_root'])
    _require(not jobs.is_relative_to(staging) and not staging.is_relative_to(jobs),
             'service jobs and original staging roots must not overlap')
    _require(value.get('initialize_delegation') is True, 'explicit private-service delegation initialization required')
    for field in ('code_root', 'code_manifest', 'template', 'template_manifest'):
        _absolute(value['release'][field])
    _absolute(value['ledger']['database'])
    _require(type(value['ledger']['capacity']) is int and 1 <= value['ledger']['capacity'] <= 8,
             'server capacity cannot exceed eight')
    for field in ('worker_uid', 'worker_gid'):
        _require(type(value[field]) is int and value[field] > 0, 'non-root worker identity required')
    limits = value['limits']
    for field in ('timeout_seconds', 'poll_seconds', 'cleanup_seconds', 'disk_poll_seconds', 'memory_bytes',
                  'pids', 'file_bytes', 'open_files', 'max_job_bytes', 'min_free_bytes'):
        number = limits[field]
        _require(type(number) in (int, float) and math.isfinite(number) and number > 0,
                 'explicit finite positive resource limits required')
    _require(1 <= limits['timeout_seconds'] <= 7200 and limits['poll_seconds'] <= 5
             and limits['cleanup_seconds'] <= 60, 'bounded job/poll/cleanup timeouts required')
    _require(max(1, limits['poll_seconds']) <= limits['disk_poll_seconds'] <= 60,
             'disk samples must use an explicit lower-frequency interval')
    for field in ('memory_bytes', 'pids', 'file_bytes', 'open_files', 'max_job_bytes', 'min_free_bytes'):
        _require(type(limits[field]) is int, 'integer kernel resource limits required')


def _ledger(config):
    _config(config)
    return RemoteJobs(config['ledger']['database'], capacity=config['ledger']['capacity'],
                      ledger_id=config['server_identity']['ledger_id'])


def initialize_service_state(config):
    """Explicit NEW state installation; never create/recreate the execution ledger."""
    ledger = _ledger(config)
    # Refusing validator cannot accept any receipt; installation executes none.
    ServerSupervisor(ledger, cgroup_parent=_parent_cgroup(config),
        worker_uid=config['worker_uid'], worker_gid=config['worker_gid'],
        receipt_validator=lambda *_: False).initialize()
    with closing(ledger._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('CREATE TABLE service_jobs(job_id TEXT PRIMARY KEY, frozen_json TEXT NOT NULL, '
                   'config_sha256 TEXT NOT NULL, phase TEXT NOT NULL, detail_json TEXT NOT NULL)')


def _mapped_executables():
    _require(sys.platform == 'linux', 'Linux native identity verification required')
    values = set()
    for line in Path('/proc/self/maps').read_text().splitlines():
        fields = line.split(None, 5)
        if len(fields) == 6 and 'x' in fields[1] and fields[5].startswith('/'):
            _require(not fields[5].endswith(' (deleted)'), 'mapped native executable was deleted')
            values.add(str(Path(fields[5]).resolve(strict=True)))
    return values


def verify_native_runtime(config):
    """Verify pinned known bytes and the actual supported counter launch binding.

    These checks do not prove the complete future dynamic dependency closure.
    """
    raw = _read(config['native_manifest'])
    _require(_digest(raw) == config['native_manifest_sha256'], 'native runtime inventory changed')
    manifest = json.loads(raw)
    _require(manifest.get('schema') == 'paperflow-native-runtime/1'
             and manifest.get('verification_scope') == 'pinned-known-files'
             and manifest.get('reviewed_dependency_closure') is False
             and isinstance(manifest.get('files'), list) and manifest['files'],
             'an explicitly scoped, independently pinned native file inventory is required')
    files = {}
    for ref in manifest['files']:
        path = _absolute(ref['path'])
        _require(path.resolve(strict=True) == path and str(path) not in files,
                 'native files must be canonical, independent and unique')
        tree = _Tree(path.parent)
        try:
            with tree.regular(path.name) as (stream, before):
                _require(before.st_size == ref['bytes']
                    and _stream_digest(stream) == ref['sha256'],
                    'native dependency bytes changed')
        finally:
            tree.close()
        files[str(path)] = ref
    roles = manifest['roles']
    required = {'python', 'service_script', 'worker_script', 'wine64', 'counter_config',
                'counter_env', 'counter_runtime_manifest', 'model', 'libllama', 'cl100k'}
    _require(required <= set(roles), 'native runtime roles are incomplete')
    for path in roles.values():
        _require(str(_absolute(path).resolve(strict=True)) in files, 'native role is not inventoried')
    here = Path(__file__).resolve()
    expected = dict(python=Path(sys.executable).resolve(), service_script=here,
        worker_script=here.with_name('server_wine_worker.py'),
        wine64=Path(config['base']) / 'wine-runtime/usr/lib/wine/wine64',
        counter_config=Path(config['base']) / 'tokenizer/server-counter-config.json')
    _require(all(Path(roles[key]).resolve(strict=True) == path.resolve(strict=True)
                 for key, path in expected.items()), 'actual interpreter/worker/Wine/config differs from pinned runtime')
    # A venv interpreter can resolve to the system binary while selecting a
    # different import environment. Preserve its invocation path as well.
    _require(roles['python'] == sys.executable, 'actual Python invocation environment differs from pinned runtime')
    environment = dict(prefix=sys.prefix, base_prefix=sys.base_prefix)
    _require(manifest.get('python_environment') == environment,
             'actual Python prefix differs from the reviewed environment')
    if sys.prefix != sys.base_prefix:
        prefix = _absolute(sys.prefix)
        _require(prefix.resolve(strict=True) == prefix and Path(roles['python']).parent == prefix / 'bin'
                 and str(prefix / 'pyvenv.cfg') in files,
                 'private virtual-environment invocation and pinned pyvenv.cfg are required')
    critical = ['remote_jobs.py', 'server_termination_proof.py', 'server_admission.py', 'server_release.py', 'server_supervisor.py',
                'server_receipt_inspect.py', 'snapshot_import.py', 'snapshot_wire.py',
                'windows_snapshot.py', 'tokenizer_service.py', 'server_path_guard.py',
                'server_service_descriptor.py', 'server_receipt_query.py', 'server_rpc.py']
    _require(all(str(here.with_name(name)) in files for name in critical),
             'service dependency source files are not pinned')
    _require(_mapped_executables() <= set(files), 'currently mapped native loader/library is not pinned')
    counter = _value(roles['counter_config'])
    command = counter.get('counter', {}).get('command')
    _require(isinstance(command, list) and len(command) == 4
        and all(isinstance(part, str) and '\0' not in part for part in command)
        and (command[0] == 'env' or Path(command[0]).is_absolute()),
        'only the reviewed env/LD_LIBRARY_PATH/venv-python/-B counter command is supported')
    actual_env = shutil.which(command[0], path=_WORKER_PATH)
    _require(actual_env is not None
        and Path(actual_env).resolve(strict=True) == Path(roles['counter_env']).resolve(strict=True)
        and command[1] == 'LD_LIBRARY_PATH=' + str(Path(roles['libllama']).resolve(strict=True).parent)
        and command[2:] == [roles['python'], '-B'],
        'counter env resolution, native library directory or venv Python differs from fixed roles')
    runtime_manifest = _absolute(roles['counter_runtime_manifest'])
    _require(str(runtime_manifest.resolve(strict=True)) == str(runtime_manifest)
        and counter['counter'].get('runtime_manifest') == str(runtime_manifest),
        'actual counter runtime manifest differs from the inventoried file')
    for role, policy_key in [('model', 'model_sha256'), ('libllama', 'libllama_sha256'), ('cl100k', 'cl100k_sha256')]:
        ref = files[str(Path(roles[role]).resolve(strict=True))]
        _require(ref['sha256'] == counter['policy'][policy_key], 'counter/native policy identity mismatch')
    return manifest


def verify_job(job_id, config):
    _config(config)
    _require(_uuid(job_id), 'canonical job ID required')
    root = Path(config['jobs_root']) / job_id
    private = _Tree(root, private=True)
    try:
        job = json.loads(private.read('job.json'))
        staging = json.loads(private.read('STAGING.json'))
    finally:
        private.close()
    _validated(job)
    _require(job['job_id'] == job_id, 'service folder and job identity mismatch')
    _require(set(staging) == {'schema', 'job', 'server_identity', 'snapshot_root', 'staging_receipt'}
        and staging['schema'] == 'paperflow-service-staging/1' and staging['job'] == job
        and staging['server_identity'] == config['server_identity'], 'original staging descriptor is not bound to this job/server')
    snapshot = Path(config['staging_root']) / job_id
    _require(staging['snapshot_root'] == str(snapshot), 'original staging path changed; never move or alias it')
    staged = _Tree(snapshot, private=True)
    try:
        manifest = json.loads(staged.read('transport-manifest.json'))
        marker = json.loads(staged.read('INPUT_VERIFIED.json'))
        _require(_json(staging['staging_receipt']) == _json(marker), 'original staging acknowledgement changed')
        _require(snapshot_sha(manifest) == job['snapshot_sha256']
                 and all(manifest[key] == job[key] for key in ('job_id', 'claim_id', 'identity',
                    'generation', 'manifest_sha256', 'code_sha256'))
                 and all(marker.get(key) == value for key, value in job.items()),
                 'staged snapshot is not bound to this original job')
        importer = SnapshotImporter(snapshot, manifest['files'])
        try:
            importer.verify_complete()
        finally:
            importer.close()
    finally:
        staged.close()
    native = verify_native_runtime(config)
    _verify_counter_contract(manifest, native)
    code_files = _code_binding(job, manifest, config)
    return dict(job=job, manifest=manifest, snapshot=str(snapshot), prefix=str(root / 'prefix'),
                native=native, code_files=code_files)


def _code_binding(job, manifest, config):
    raw = _read(config['release']['code_manifest'])
    _require(_digest(raw) == job['code_sha256'], 'reviewed source inventory identity changed')
    rows = json.loads(raw)
    _require(isinstance(rows, list) and rows, 'reviewed source inventory is missing')
    workspace = PureWindowsPath(manifest['business_workspace_windows'])
    core = PureWindowsPath(manifest['paperflow_root_windows'])
    server = workspace / 'candidates/postprocess-server-20261101'
    business = workspace / 'candidates/postprocess-prefect-20261031'
    expected = [server / name for name in ('server_wine_entry.py', 'server_wine_pathfix.py',
                'server_folder_executor.py', 'tokenizer_proxy.py')]
    expected += [business / name for name in ('folder_processor.py', 'body_adapter.py', 'generation_attachment_adapter.py')]
    expected += [core / 'paperflow/embedding_counter.py']
    names = {str(PureWindowsPath(row['source'])).casefold() for row in rows}
    _require(all(str(path).casefold() in names for path in expected),
             'pinned business entry/counter/acceptance adapter dependency is missing')
    return rows


def _verify_installed_code(verified):
    rows = verified.get('code_files')
    _require(isinstance(rows, list) and rows, 'installed reviewed code inventory missing')
    tree = _Tree(verified['snapshot'], private=True)
    try:
        for row in rows:
            with tree.regular(row['relative']) as (stream, info):
                _require(info.st_size == row['bytes'] and _stream_digest(stream) == row['sha256'],
                         'installed business code changed before execution')
    finally:
        tree.close()


def _verify_counter_contract(manifest, native):
    """Never replace the original job's actual budgets with probe defaults."""
    expected = manifest.get('counter_contract')
    fields = {'model_sha256', 'libllama_sha256', 'cl100k_sha256', 'embedding_model_budget', 'cl_limit'}
    _require(isinstance(expected, dict) and set(expected) == fields,
             'snapshot must freeze the original effective per-job counter contract')
    config = _value(native['roles']['counter_config'])
    actual = {key: config['policy'][key] for key in ('model_sha256', 'libllama_sha256', 'cl100k_sha256')}
    actual.update(embedding_model_budget=config['model_budget'],
                  cl_limit=min(config['policy']['pipeline_limit'], config['policy']['adapter_limit']))
    _require(expected == actual and type(expected['embedding_model_budget']) is int
             and expected['embedding_model_budget'] > 0 and type(expected['cl_limit']) is int
             and expected['cl_limit'] > 0 and type(actual['embedding_model_budget']) is int
             and type(actual['cl_limit']) is int, 'server counter identity or original effective budget differs')


def _assert_installed_config(config):
    path = Path(__file__).with_name('server-job-service.json')
    expected = _read(path.with_suffix('.sha256'), max_bytes=128).decode().strip()
    _require(_json(load_config(path, expected)) == _json(config),
             'systemd entry default configuration differs from the preflight configuration')


def preflight_job(job, config):
    """HEAVY staging preflight; caller MUST be outside the task admission gate."""
    _assert_installed_config(config)
    verified = verify_job(job['job_id'], config)
    _require(verified['job'] == job, 'admission job identity mismatch')
    roles, native = verified['native']['roles'], verified['native']['files']
    script = str(Path(roles['service_script']).resolve())
    script_sha = next(ref['sha256'] for ref in native if ref['path'] == script)
    identifier = str(uuid4())
    value = dict(schema='paperflow-service-preflight/1', preflight_id=identifier, job=copy.deepcopy(job),
        server_identity=copy.deepcopy(config['server_identity']), config_sha256=_digest(_json(config).encode('utf8')),
        snapshot_root=verified['snapshot'], snapshot_sha256=job['snapshot_sha256'],
        native_manifest_sha256=config['native_manifest_sha256'],
        entrypoints=dict(python=roles['python'], service_script=script, service_sha256=script_sha),
        boot_id=_boot_id(), created_monotonic=time.monotonic())
    raw = _json(value).encode('utf8')
    _require(len(raw) <= 65536, 'bounded preflight metadata required')
    tree = _Tree(Path(config['jobs_root']) / job['job_id'], private=True)
    try:
        descriptor = os.open('PREFLIGHT-' + identifier + '.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o400, dir_fd=tree.fd)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.fsync(tree.fd)
    finally:
        tree.close()
    return dict(preflight_id=identifier, preflight_sha256=_digest(raw), job=copy.deepcopy(job),
                server_identity=copy.deepcopy(config['server_identity']))


class _AdmissionLedger(RemoteJobs):
    """Same ledger protocol, but bounded lock waits for the tiny start admission."""
    def _connect(self):
        _require(_uuid(self.ledger_id), 'pinned admission ledger identity missing')
        db = sqlite3.connect(self.database.as_uri() + '?mode=rw', uri=True, timeout=.05)
        db.row_factory = sqlite3.Row
        try:
            settings = dict(db.execute('SELECT name,value FROM executor_settings'))
            _require(settings == {'capacity': str(self.capacity), 'ledger_id': self.ledger_id},
                     'admission ledger identity changed')
            _require(not db.execute("SELECT 1 FROM executor_jobs WHERE state NOT IN ('reserved','running','unknown','finished') LIMIT 1").fetchone(),
                     'admission ledger state is uncertain')
            return db
        except BaseException:
            db.close(); raise


def admit_job(job, config, *, preflight_id, preflight_sha256):
    """Small once-only start admission; no input, code-tree or model hashing."""
    started = time.monotonic()
    _validated(job); _config(config)
    _require(_uuid(preflight_id), 'canonical preflight identity required')
    _assert_installed_config(config)
    tree = _Tree(Path(config['jobs_root']) / job['job_id'], private=True)
    try:
        with tree.regular('PREFLIGHT-' + preflight_id + '.json') as (stream, info):
            _require(info.st_size <= 65536, 'preflight metadata exceeds admission bound')
            raw = stream.read(65537)
    finally:
        tree.close()
    _require(len(raw) <= 65536 and _digest(raw) == preflight_sha256, 'exact prior preflight receipt required')
    value = json.loads(raw)
    _require(_json(value).encode('utf8') == raw and value['schema'] == 'paperflow-service-preflight/1'
        and value['preflight_id'] == preflight_id and value['job'] == job
        and value['server_identity'] == config['server_identity']
        and value['config_sha256'] == _digest(_json(config).encode('utf8'))
        and value['snapshot_root'] == str(Path(config['staging_root']) / job['job_id'])
        and value['snapshot_sha256'] == job['snapshot_sha256']
        and value['native_manifest_sha256'] == config['native_manifest_sha256']
        and value['boot_id'] == _boot_id(), 'preflight server/job/configuration/boot binding changed')
    ledger = _AdmissionLedger(config['ledger']['database'], capacity=config['ledger']['capacity'],
                              ledger_id=config['server_identity']['ledger_id'])
    entry = value['entrypoints']
    def launch(actual):
        remaining = 2.4 - (time.monotonic() - started)
        _require(remaining > 0, 'bounded start admission expired before dispatch')
        return launch_systemd(actual, python=entry['python'], service_script=entry['service_script'],
                              service_sha256=entry['service_sha256'], timeout_seconds=min(remaining, 2))
    return start_with_capacity_result(job, ledger=ledger, server_identity=config['server_identity'],
        confirm_prepared=lambda actual: actual == value['job'], launch_service=launch)


def _claim_service(ledger, job, config):
    frozen, config_sha = _validated(job), _digest(_json(config).encode('utf8'))
    with closing(ledger._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        actual = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        _require(actual and actual['frozen_json'] == frozen, 'service requires the exact admitted job')
        existing = db.execute('SELECT * FROM service_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        if existing:
            _require(existing['frozen_json'] == frozen and existing['config_sha256'] == config_sha,
                     'existing service identity/configuration changed')
            return False
        if actual['state'] != 'reserved':
            return False
        db.execute('INSERT INTO service_jobs VALUES(?,?,?,?,?)',
                   (job['job_id'], frozen, config_sha, 'preparing', '{}'))
        return True


def _phase(ledger, job, phase, detail):
    with closing(ledger._connect()) as db, db:
        db.execute('UPDATE service_jobs SET phase=?,detail_json=? WHERE job_id=?',
                   (phase, _json(detail), job['job_id']))


def _mark_unknown(ledger, job):
    if ledger.status(job['job_id'])['state'] in ('reserved', 'running'):
        ledger.mark_unknown(job['job_id'], job['claim_id'])


def _parent_cgroup(config):
    if config.get('cgroup_parent'):
        return _absolute(config['cgroup_parent'])
    rows = [row.split('::', 1)[1] for row in Path('/proc/self/cgroup').read_text().splitlines() if row.startswith('0::')]
    _require(len(rows) == 1, 'current delegated service cgroup is unavailable')
    return Path('/sys/fs/cgroup') / rows[0].lstrip('/')


def _current_cgroup():
    rows = [row.split('::', 1)[1] for row in Path('/proc/self/cgroup').read_text().splitlines() if row.startswith('0::')]
    _require(len(rows) == 1, 'actual service cgroup is unavailable')
    return Path('/sys/fs/cgroup') / rows[0].lstrip('/')


def _cgroup_write(path, value):
    path.write_text(value)


def _delegate_parent(job, config):
    """Initialize ONLY this one-job systemd unit, retaining its original parent.

    A domain cannot enable controllers with an internal process. Move ONLY this
    supervisor PID into its dedicated control child, then delegate memory/pids
    to sibling job scopes. Never derive parent again from the migrated PID.
    """
    _require(config.get('initialize_delegation') is True, 'delegation initialization was not explicitly configured')
    parent = _parent_cgroup(config)
    _require(parent == _current_cgroup()
        and parent.name in ('paperflow-pp-' + job['job_id'] + '.service', 'paperflow-pp-' + job['job_id'] + '.scope'),
        'only the current exact single-job systemd unit may be initialized')
    LinuxScope._check_directory(parent)
    info = parent.stat(); boot = _boot_id()
    _require(info.st_uid == os.getuid(), 'service delegation is not owned by this uid')
    _require(set((parent / 'cgroup.procs').read_text().split()) == {str(os.getpid())},
             'delegated service has another internal process')
    _require(not any(path.is_dir() for path in parent.iterdir()), 'service delegation already contains an unmanaged child')
    _require({'memory', 'pids'} <= set((parent / 'cgroup.controllers').read_text().split()),
             'memory/pids controllers are unavailable in this unit')
    control = parent / 'supervisor-control'
    control.mkdir(exist_ok=False)
    _cgroup_write(control / 'cgroup.procs', str(os.getpid()))
    _require(not (parent / 'cgroup.procs').read_text().split()
             and set((control / 'cgroup.procs').read_text().split()) == {str(os.getpid())},
             'supervisor migration into control subgroup was not confirmed')
    _cgroup_write(parent / 'cgroup.subtree_control', '+memory +pids')
    _require({'memory', 'pids'} <= set((parent / 'cgroup.subtree_control').read_text().split()),
             'service subtree controllers were not enabled')
    current = parent.stat()
    _require((current.st_dev, current.st_ino) == (info.st_dev, info.st_ino) and _boot_id() == boot,
             'original service parent identity changed during initialization')
    return dict(path=str(parent), identity=[info.st_dev, info.st_ino], boot_id=boot)


def _disk_usage(verified, config):
    job_root = Path(config['jobs_root']) / verified['job']['job_id']
    snapshot = Path(verified['snapshot'])
    roots = [job_root] if snapshot.is_relative_to(job_root) else [job_root, snapshot]
    return sum(_job_bytes(root) for root in roots)


def _install_wire_manifest(verified):
    root = Path(verified['snapshot']) / 'drives/f'
    tree = _Tree(root)
    try:
        for name in ('wire-result.json', 'wire-manifest.json'):
            try:
                os.stat(name, dir_fd=tree.fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise ServiceError('reserved wire path already exists; never overwrite or rerun')
        descriptor = os.open('wire-manifest.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o400, dir_fd=tree.fd)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(_json(verified['manifest']).encode('utf8')); stream.flush(); os.fsync(stream.fileno())
        os.fsync(tree.fd)
    finally:
        tree.close()


def _business_receipt(job, verified, receipt):
    """Require the actual pinned entry's full validated result, then inspect bytes."""
    snapshot = Path(verified['snapshot'])
    result = _value(snapshot / 'drives/f/wire-result.json')
    _require(set(result) == {'server_validation', 'receipt', 'receipt_json', 'receipt_sha256'}
             and result['server_validation'] == 'passed' and result['receipt'] == receipt,
             'business entry did not provide its full validated receipt')
    raw = _json(receipt)
    _require(result['receipt_json'] == raw and result['receipt_sha256'] == _digest(raw.encode('utf8'))
             and receipt.get('identity') == job['identity'] and receipt.get('claim_id') == job['claim_id']
             and receipt.get('manifest_sha256') == job['manifest_sha256'], 'business receipt identity/hash changed')
    runtime = _value(snapshot / 'tokenizer-runtime.json')
    _require(runtime == dict(vocab_only=True, context_created=False, inference_requests=0, unified=True),
             'real worker tokenizer runtime is missing or not CPU-tokenization-only')
    inspected = inspect_receipt(job=job, manifest=verified['manifest'], receipt=receipt,
        snapshot_root=snapshot, output_root_windows=verified['manifest']['output_root_windows'])
    _require(inspected['integrity_status'] == 'passed' and inspected['job'] == job
             and inspected['receipt_canonical_sha256'] == result['receipt_sha256'],
             'independent Linux receipt inspection failed')
    return True


def _stop_requested(job, config):
    path = Path(config['jobs_root']) / job['job_id'] / 'STOP.json'
    if not path.exists():
        return False
    value = _value(path)
    _require(value.get('job') == job and isinstance(value.get('reason'), str) and value['reason'],
             'stop evidence is not bound to this job')
    return True


def _job_bytes(root):
    total = 0
    for current, directories, files in os.walk(root, followlinks=False):
        # Prefix drive links are deliberate; never traverse them into originals.
        directories[:] = [name for name in directories if not Path(current, name).is_symlink()]
        for name in files:
            path = Path(current, name)
            if not path.is_symlink():
                total += path.stat().st_size
    return total


def _collect_prefix(verified, expected_identity, config):
    """Delete ONLY the proven-finished job's temporary prefix, never follow links."""
    prefix = _absolute(verified['prefix'])
    expected = _absolute(config['jobs_root']) / verified['job']['job_id'] / 'prefix'
    _require(prefix == expected and prefix.resolve(strict=True) == expected,
             'only the exact canonical job prefix can be collected')
    parent = _Tree(prefix.parent, private=True)
    root_fd = None
    try:
        root_fd = os.open(prefix.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent.fd)
        info = os.fstat(root_fd)
        _require([info.st_dev, info.st_ino] == expected_identity and info.st_uid == os.getuid()
                 and not info.st_mode & 0o077, 'temporary prefix root identity changed')
        def clear(directory):
            for name in os.listdir(directory):
                current = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if stat.S_ISDIR(current.st_mode):
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                    try:
                        opened = os.fstat(child)
                        _require((opened.st_dev, opened.st_ino) == (current.st_dev, current.st_ino)
                                 and opened.st_dev == info.st_dev, 'prefix child changed or crosses a mount')
                        clear(child)
                        visible = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        _require((visible.st_dev, visible.st_ino) == (opened.st_dev, opened.st_ino),
                                 'prefix directory changed during collection')
                    finally:
                        os.close(child)
                    os.rmdir(name, dir_fd=directory)
                else:
                    _require(stat.S_ISREG(current.st_mode) or stat.S_ISLNK(current.st_mode),
                             'unexpected special prefix member')
                    os.unlink(name, dir_fd=directory)
            os.fsync(directory)
        clear(root_fd)
        parent.visible()
        current = os.stat(prefix.name, dir_fd=parent.fd, follow_symlinks=False)
        _require([current.st_dev, current.st_ino] == expected_identity, 'prefix root was replaced')
        os.rmdir(prefix.name, dir_fd=parent.fd); os.fsync(parent.fd)
        return True
    finally:
        if root_fd is not None:
            os.close(root_fd)
        parent.close()


def _cleanup_scope(supervisor, job, config, *, kill):
    """Only the original boot+inode cgroup can be killed/removed; never killpg."""
    with closing(supervisor.ledger._connect()) as db:
        row = db.execute('SELECT scope_json,process_json FROM supervisor_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
    if row is None or not row['scope_json'] or not row['process_json']:
        return dict(terminated=False, scope_removed=False)
    expected = json.loads(row['scope_json'])
    original_process = json.loads(row['process_json'])
    _require(original_process['boot_id'] == expected['boot_id'], 'cleanup process boot is uncertain')
    scope = LinuxScope.reopen(_parent_cgroup(config), job['job_id'], worker_uid=config['worker_uid'], worker_gid=config['worker_gid'])
    def inspect_scope():
        observed = scope.snapshot()
        _require(observed['boot_id'] == expected['boot_id'] and observed['scope_id'] == expected['scope_id'],
                 'cleanup scope identity is uncertain')
        return observed
    observed = inspect_scope()
    if kill and observed['populated']:
        (scope.path / 'cgroup.kill').write_text('1')
    deadline = time.monotonic() + config['limits']['cleanup_seconds']
    while inspect_scope()['populated'] and time.monotonic() < deadline:
        time.sleep(min(.05, config['limits']['poll_seconds']))
    final_scope = inspect_scope()
    if final_scope['populated']:
        return dict(terminated=False, scope_removed=False)
    scope.path.rmdir()
    return dict(terminated=True, scope_removed=True, scope_identity=expected,
                process_identity=original_process, final_scope=final_scope,
                parent=str(_parent_cgroup(config)), observed_monotonic=time.monotonic(), boot_id=_boot_id())


def _release_terminated_slot(ledger, job, original_config, cleanup, accounting_error):
    """A verified dead worker can free only capacity, never its unknown result."""
    if (accounting_error is not None or not isinstance(cleanup, dict)
            or cleanup.get('terminated') is not True or cleanup.get('scope_removed') is not True):
        return False
    try:
        ledger.release_terminated_capacity(job['job_id'], job['claim_id'], original_config)
    except Exception:
        # Missing or inconsistent evidence keeps the conservative reservation.
        return False
    return True


@contextmanager
def _worker_stderr(job, config):
    """Create one private, retained diagnostic file under the exact job directory."""
    _validated(job)
    tree = _Tree(Path(config['jobs_root']) / job['job_id'], private=True)
    fd = None
    try:
        tree.visible()
        fd = os.open('worker.stderr.log', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=tree.fd)
        os.fchmod(fd, 0o600)
        info = os.fstat(fd)
        _require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                 and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1
                 and info.st_size == 0, 'private independent worker diagnostic required')
        tree.named('worker.stderr.log', info)
        os.fsync(fd)
        os.fsync(tree.fd)
        with os.fdopen(fd, 'wb', buffering=0) as stream:
            fd = None
            yield stream
    finally:
        if fd is not None:
            os.close(fd)
        tree.close()


def run_job(job_id, config, *, stopped=None):
    """One already-admitted folder; a duplicate service invocation never launches."""
    ledger, verified, supervisor, job = _ledger(config), None, None, None
    original_config = copy.deepcopy(config)
    owned = False
    try:
        verified = verify_job(job_id, config)
        job = verified['job']; _validated(job)
        actual = ledger.status(job_id)
        _require(actual['frozen_json'] == _validated(job), 'admitted service job identity changed')
        if not _claim_service(ledger, job, config):
            existing = ledger.status(job_id)
            return dict(job=copy.deepcopy(job), state=existing['state'], started=False,
                        receipt_sha256=existing['receipt_sha256'])
        owned = True
        if (stopped and stopped()) or _stop_requested(job, config):
            _mark_unknown(ledger, job)
            _phase(ledger, job, 'unknown', dict(reason='stop_before_prepare'))
            return dict(job=copy.deepcopy(job), state='unknown', started=False)
        _require(shutil.disk_usage(Path(config['jobs_root'])).free >= config['limits']['min_free_bytes'],
                 'configured global free-disk reserve is unavailable')
        template_raw = _read(config['release']['template_manifest'])
        _require(_digest(template_raw) == verified['manifest']['runtime_template_sha256'],
                 'prefix disk estimate is not bound to the original template')
        prefix_bytes = sum(row['bytes'] for row in json.loads(template_raw)['entries'] if row.get('kind') == 'file')
        _require(shutil.disk_usage(Path(config['jobs_root'])).free >= config['limits']['min_free_bytes'] + prefix_bytes,
                 'disk reserve cannot accommodate even this one private prefix')
        prepare_environment(job=job, snapshot=verified['snapshot'], prefix=verified['prefix'], **config['release'])
        _verify_installed_code(verified)
        prefix_info = Path(verified['prefix']).stat()
        prefix_identity = [prefix_info.st_dev, prefix_info.st_ino]
        _install_wire_manifest(verified)
        # Recheck all native bytes after potentially long template/code copying.
        verified['native'] = verify_native_runtime(config)
        if (stopped and stopped()) or _stop_requested(job, config):
            raise ServiceError('job stopped before worker launch')
        delegation = _delegate_parent(job, config)
        # Keep the original service root after this PID moves into control.
        config = copy.deepcopy(config)
        config['cgroup_parent'] = delegation['path']
        config['_delegated_parent_identity'] = delegation
        validator = lambda actual_job, receipt: actual_job == job and _business_receipt(actual_job, verified, receipt)
        supervisor = ServerSupervisor(ledger, cgroup_parent=_parent_cgroup(config),
            worker_uid=config['worker_uid'], worker_gid=config['worker_gid'], receipt_validator=validator)
        roles = verified['native']['roles']
        argv = [roles['python'], '-B', str(Path(__file__).resolve()), '--limited-worker',
                job_id, '--config-json', _json(config)]
        with _worker_stderr(job, config) as diagnostic:
            supervisor.launch(job, argv, timeout_seconds=config['limits']['timeout_seconds'],
                cwd=str(Path(__file__).resolve().parent), stderr=diagnostic,
                env=dict(PATH=_WORKER_PATH, HOME=str(Path(verified['prefix']).parent),
                         PYTHONNOUSERSITE='1', PYTHONUTF8='1', CUDA_VISIBLE_DEVICES=''))
        _phase(ledger, job, 'running', dict(delegated_parent=delegation))
        next_disk_sample = 0.0
        while True:
            if (stopped and stopped()) or _stop_requested(job, config):
                raise ServiceError('job was stopped')
            now = time.monotonic()
            if now >= next_disk_sample:
                next_disk_sample = now + config['limits']['disk_poll_seconds']
                _require(_disk_usage(verified, config) <= config['limits']['max_job_bytes']
                    and all(shutil.disk_usage(path).free >= config['limits']['min_free_bytes']
                            for path in (Path(config['jobs_root']), Path(verified['snapshot']))),
                    'sampled job bytes/free disk exceeded the configured threshold')
            state = supervisor.poll(job)
            if state['state'] == 'unknown':
                raise ServiceError('worker outcome is unknown or timed out')
            if state['server_alive'] is False:
                result = _value(Path(verified['snapshot']) / 'drives/f/wire-result.json')
                state = supervisor.poll(job, receipt=result['receipt'])
                _require(state['state'] == 'finished', 'terminated business result was not accepted')
                cleanup = _cleanup_scope(supervisor, job, config, kill=False)
                cleanup['prefix_removed'] = False
                held = supervisor.held_receipt(job)
                if cleanup.get('terminated') is True and cleanup.get('scope_removed') is True:
                    # A second full receipt inspection follows durable finish
                    # and confirmed scope removal, BEFORE touching temporary data.
                    try:
                        receipt = json.loads(held['receipt_json'])
                        _require(_digest(held['receipt_json'].encode('utf8')) == held['receipt_sha256'],
                                 'held receipt bytes changed before prefix collection')
                        _business_receipt(job, verified, receipt)
                        cleanup['prefix_removed'] = _collect_prefix(verified, prefix_identity, config)
                    except Exception as collection_error:
                        cleanup['prefix_error'] = type(collection_error).__name__
                _phase(ledger, job, 'finished', dict(cleanup=cleanup))
                return dict(job=copy.deepcopy(job), state='finished', started=True,
                            held_receipt=held, cleanup=cleanup,
                            rag_admission='not_granted')
            time.sleep(config['limits']['poll_seconds'])
    except BaseException as exc:
        if job is not None and owned:
            accounting_error = None
            try:
                _mark_unknown(ledger, job)
            except Exception as marking_error:
                # A failed ledger write must never bypass kernel cleanup. The
                # existing running reservation still occupies its slot.
                accounting_error = type(marking_error).__name__
            cleanup = dict(terminated=False, scope_removed=False)
            if supervisor is not None:
                try:
                    cleanup = _cleanup_scope(supervisor, job, config, kill=True)
                except Exception as cleanup_error:
                    cleanup['error'] = type(cleanup_error).__name__
            try:
                _phase(ledger, job, 'unknown', dict(error=type(exc).__name__,
                    accounting_error=accounting_error, cleanup=cleanup))
                _release_terminated_slot(ledger, job, original_config, cleanup, accounting_error)
            except Exception as record_error:
                if hasattr(exc, 'add_note'):
                    exc.add_note('service outcome persistence failed: ' + type(record_error).__name__)
        raise ServiceError('single-job service did not complete with verified evidence') from exc


def _limited_worker(job_id, config):
    """Runs INSIDE the supervisor cgroup; limits precede all business execution."""
    _require(os.environ.get('PATH') == _WORKER_PATH,
             'inherited worker PATH differs from the verified counter environment')
    import resource
    verified = verify_job(job_id, config)
    _verify_installed_code(verified)
    own = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
    parent = _parent_cgroup(config)
    recorded = config.get('_delegated_parent_identity')
    parent_info = parent.stat()
    _require(own == parent / job_id and recorded and recorded['path'] == str(parent)
        and recorded['identity'] == [parent_info.st_dev, parent_info.st_ino]
        and recorded['boot_id'] == _boot_id(), 'limited worker is not in its original delegated job scope')
    for filename, value in [('memory.max', config['limits']['memory_bytes']), ('pids.max', config['limits']['pids'])]:
        path = own / filename
        _require(path.is_file(), 'required cgroup resource controller is not delegated')
        path.write_text(str(value))
        _require(path.read_text().strip() == str(value), 'cgroup resource limit was not applied')
    resource.setrlimit(resource.RLIMIT_FSIZE, (config['limits']['file_bytes'],) * 2)
    resource.setrlimit(resource.RLIMIT_NOFILE, (config['limits']['open_files'],) * 2)
    roles = verified['native']['roles']
    os.execv(roles['python'], [roles['python'], '-B', roles['worker_script'], config['base'],
        verified['snapshot'], verified['prefix'], str(int(config['limits']['timeout_seconds']))])


def _service_status(job_id, result):
    """Only bounded status metadata may leave the private service ledger."""
    _require(_uuid(job_id) and isinstance(result, dict) and isinstance(result.get('job'), dict)
             and result['job'].get('job_id') == job_id
             and result.get('state') in {'reserved', 'running', 'unknown', 'finished'}
             and type(result.get('started')) is bool, 'service result identity or state is unavailable')
    held = result.get('held_receipt')
    receipt_sha256 = result.get('receipt_sha256')
    if held is not None:
        _require(isinstance(held, dict), 'held receipt status is unavailable')
        receipt_sha256 = held.get('receipt_sha256')
    if receipt_sha256 is not None:
        _require(isinstance(receipt_sha256, str) and len(receipt_sha256) == 64
                 and all(char in '0123456789abcdef' for char in receipt_sha256),
                 'held receipt digest is unavailable')
    cleanup = result.get('cleanup') or {}
    _require(isinstance(cleanup, dict), 'service cleanup status is unavailable')
    safe_cleanup = {name: cleanup[name] for name in ('terminated', 'scope_removed', 'prefix_removed')
                    if type(cleanup.get(name)) is bool}
    return dict(job_id=job_id, state=result['state'], started=result['started'],
                receipt_sha256=receipt_sha256, cleanup=safe_cleanup)


def _error_types(exc):
    """Retain bounded diagnostic classes without writing exception text to journal."""
    values, seen = [], set()
    while exc is not None and id(exc) not in seen and len(values) < 8:
        seen.add(id(exc))
        name = type(exc).__name__
        values.append(name if len(name) <= 64 and name.isidentifier() else 'Error')
        exc = exc.__cause__ if exc.__cause__ is not None else (
            None if exc.__suppress_context__ else exc.__context__)
    return values


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run exactly one already-admitted candidate folder')
    parser.add_argument('job_id')
    parser.add_argument('--config', default=str(Path(__file__).with_name('server-job-service.json')))
    parser.add_argument('--config-sha256')
    parser.add_argument('--limited-worker', action='store_true')
    parser.add_argument('--config-json', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.limited_worker:
        _require(sys.version_info >= (3, 11), 'the original release/importer requires Python 3.11 or newer')
        os.umask(0o077)
        _require(args.config_json is not None, 'internal worker requires its frozen configuration')
        return _limited_worker(args.job_id, json.loads(args.config_json))
    try:
        _require(sys.version_info >= (3, 11), 'the original release/importer requires Python 3.11 or newer')
        os.umask(0o077)
        expected = args.config_sha256 or _read(Path(args.config).with_suffix('.sha256'), max_bytes=128).decode().strip()
        config = load_config(args.config, expected)
        stop = [False]
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.__setitem__(0, True))
        result = run_job(args.job_id, config, stopped=lambda: stop[0])
        print(_json(_service_status(args.job_id, result)), flush=True)
        return 0
    except Exception as exc:
        print(_json(dict(job_id=args.job_id if _uuid(args.job_id) else None, error_types=_error_types(exc))),
              file=sys.stderr, flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
