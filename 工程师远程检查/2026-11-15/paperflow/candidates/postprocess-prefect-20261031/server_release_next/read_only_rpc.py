"""Restrict server observation to durable registered receipts; never dispatch."""
from contextlib import closing
import copy
import hashlib
import hmac
import json
import math
from pathlib import Path, PurePosixPath
import re
import shlex
import sqlite3
import time
from uuid import uuid4


FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


def command_for(config, operation):
    return shlex.join([config['server_python'], '-B', config['rpc_script'],
                       config['service_config'], config['service_config_sha256'], operation])


def _one(db, sql, params):
    rows = db.execute(sql, params).fetchall()
    if len(rows) != 1:
        raise ValueError('registered folder is unavailable or ambiguous')
    return dict(rows[0])


SHA = re.compile(r'[0-9a-f]{64}')
VERSION_BOOTSTRAP = ("import hashlib,hmac,pathlib,sys; "
    "p=pathlib.Path(sys.argv[1]); f=p.open('rb'); data=f.read(1048577); f.close(); "
    "(len(data)<=1048576 and hmac.compare_digest(hashlib.sha256(data).hexdigest(),sys.argv[2])) "
    "or sys.exit('version script changed'); "
    "sys.path.insert(0,str(p.parent)); sys.argv=[str(p),*sys.argv[3:]]; "
    "exec(compile(data,str(p),'exec',dont_inherit=True),{'__name__':'__main__','__file__':str(p)})")


def _remote_path(value):
    if (not isinstance(value, str) or not value or '\x00' in value
            or not PurePosixPath(value).is_absolute() or '..' in PurePosixPath(value).parts):
        raise ValueError('an exact absolute server path is required')
    return value


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate version registry key')
        result[key] = value
    return result


def _read_pinned(path, expected, limit=1024 * 1024):
    if not isinstance(expected, str) or not SHA.fullmatch(expected):
        raise ValueError('an independently pinned file hash is required')
    with Path(path).open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
        raise ValueError('the pinned file is unavailable or changed')
    return raw


class PinnedAccessBindings:
    """Keep authentication references separate from the version catalog."""
    def __init__(self, manifest, manifest_sha256, *, server_identity):
        self.manifest = Path(manifest).resolve(strict=True)
        self.manifest_sha256, self.server = manifest_sha256, copy.deepcopy(server_identity)
        self._sources()

    def _sources(self):
        value = json.loads(_read_pinned(self.manifest, self.manifest_sha256).decode('utf8'),
                           object_pairs_hook=_json_object)
        if (not isinstance(value, dict) or set(value) != {'schema', 'server_identity', 'sources'}
                or value['schema'] != 'paperflow-server-access-bindings/1'
                or value['server_identity'] != self.server or not isinstance(value['sources'], list)
                or len(value['sources']) > 32):
            raise ValueError('access bindings are unavailable')
        result = {}
        for row in value['sources']:
            if (not isinstance(row, dict)
                    or set(row) != {'ref', 'config_path', 'config_sha256', 'private_key_sha256'}
                    or not isinstance(row['ref'], str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', row['ref'])
                    or row['ref'] in result
                    or not Path(row['config_path']).is_absolute()
                    or any(not isinstance(row[k], str) or not SHA.fullmatch(row[k])
                           for k in ('config_sha256', 'private_key_sha256'))):
                raise ValueError('an access reference is invalid')
            result[row['ref']] = row
        return result

    def resolve(self, reference):
        source = self._sources().get(reference)
        if source is None:
            raise ValueError('the requested historical access is unavailable')
        config = json.loads(_read_pinned(source['config_path'], source['config_sha256']).decode('utf-8-sig'),
                            object_pairs_hook=_json_object)
        if (not isinstance(config, dict) or config.get('schema') != 'paperflow-remote-live/1'
                or config.get('server_identity') != self.server or not isinstance(config.get('transport_config'), dict)):
            raise ValueError('access configuration server identity differs')
        transport = config['transport_config']
        canonical = json.dumps(transport, sort_keys=True, ensure_ascii=False,
                               separators=(',', ':'), allow_nan=False).encode('utf8')
        if (transport.get('server_identity') != self.server
                or hashlib.sha256(canonical).hexdigest() != config.get('transport_config_sha256')):
            raise ValueError('access transport binding differs')
        connection = transport.get('connection')
        if (not isinstance(connection, dict)
                or set(connection) != {'host', 'port', 'username', 'private_key', 'server_key_sha256'}
                or not isinstance(connection['host'], str) or not connection['host']
                or type(connection['port']) is not int or not 1 <= connection['port'] <= 65535
                or not isinstance(connection['username'], str) or not connection['username']
                or not isinstance(connection['private_key'], str) or not Path(connection['private_key']).is_absolute()
                or not isinstance(connection['server_key_sha256'], str)
                or not SHA.fullmatch(connection['server_key_sha256'])):
            raise ValueError('access connection is invalid')
        _read_pinned(connection['private_key'], source['private_key_sha256'])
        return copy.deepcopy(connection)


class UnreadReviewRequired(ValueError):
    """The exact authenticated report confirms a missing public unread limit."""


class UnreadDisclosureGate:
    """Require a receipt-linked check without returning private report contents."""
    def __init__(self, remote_script, remote_script_sha256):
        self.remote_script = _remote_path(remote_script)
        if not isinstance(remote_script_sha256, str) or not SHA.fullmatch(remote_script_sha256):
            raise ValueError('an independent disclosure script hash is required')
        self.remote_script_sha256 = remote_script_sha256

    def check(self, delegate, connection, selected, request, *, timeout):
        nonce = str(uuid4())
        query = copy.deepcopy(request)
        query['nonce'] = nonce
        command = shlex.join([selected['server_python'], '-B', '-c', VERSION_BOOTSTRAP,
            self.remote_script, self.remote_script_sha256, str(PurePosixPath(selected['rpc_script']).parent),
            selected['service_config'], selected['service_config_sha256']])
        response = delegate(copy.deepcopy(connection), command, query, timeout=timeout, max_bytes=16 * 1024 * 1024)
        expected = {'schema': 'paperflow-unread-disclosure-check/1', 'server_identity': request['server_identity'],
            'job_id': request['job']['job_id'], 'receipt_sha256': request['receipt_sha256'], 'nonce': nonce,
            'status': 'passed', 'reason': 'unread_disclosure_checked'}
        review = dict(expected, status='review_required', reason='unlocated_unread_missing_public_limit')
        if response == review:
            raise UnreadReviewRequired('the public profile lacks a recorded unlocated unread limit')
        if response != expected:
            raise ValueError('receipt-linked unread disclosure evidence is unavailable')
        return True


class HistoricalVersionRouter:
    """Resolve each registered job via authenticated ledger evidence, never guessing."""
    def __init__(self, registry, registry_sha256, *, server_identity, remote_script, remote_script_sha256,
                 access_bindings=None, disclosure_gate=None):
        if (not isinstance(registry_sha256, str) or not SHA.fullmatch(registry_sha256)
                or not isinstance(remote_script_sha256, str) or not SHA.fullmatch(remote_script_sha256)
                or not isinstance(server_identity, dict)):
            raise ValueError('independent registry, server and script hashes are required')
        self.registry = Path(registry).resolve(strict=True)
        self.registry_sha256 = registry_sha256
        self.server = copy.deepcopy(server_identity)
        self.remote_script = _remote_path(remote_script)
        self.remote_script_sha256 = remote_script_sha256
        if access_bindings is not None and (type(access_bindings) is not PinnedAccessBindings
                                             or access_bindings.server != self.server):
            raise ValueError('fixed server access bindings are required')
        self.access_bindings = access_bindings
        if disclosure_gate is not None and type(disclosure_gate) is not UnreadDisclosureGate:
            raise ValueError('a fixed unread disclosure gate is required')
        self.disclosure_gate = disclosure_gate
        self._versions()

    def _versions(self):
        with self.registry.open('rb') as stream:
            raw = stream.read(1024 * 1024 + 1)
        if (len(raw) > 1024 * 1024
                or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), self.registry_sha256)):
            raise ValueError('historical version registry changed')
        value = json.loads(raw.decode('utf8'), object_pairs_hook=_json_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if (not isinstance(value, dict) or set(value) != {'schema', 'server_identity', 'versions'}
                or value['schema'] != 'paperflow-historical-service-versions/1'
                or value['server_identity'] != self.server or not isinstance(value['versions'], list)
                or not 1 <= len(value['versions']) <= 32):
            raise ValueError('historical version registry is invalid')
        versions = {}
        for row in value['versions']:
            if (not isinstance(row, dict)
                    or set(row) != {'canonical_sha256', 'service_config', 'service_config_sha256',
                                    'server_python', 'rpc_script', 'rpc_script_sha256', 'access_ref'}
                    or not isinstance(row['canonical_sha256'], str) or not SHA.fullmatch(row['canonical_sha256'])
                    or not isinstance(row['service_config_sha256'], str)
                    or not SHA.fullmatch(row['service_config_sha256'])
                    or not isinstance(row['rpc_script_sha256'], str) or not SHA.fullmatch(row['rpc_script_sha256'])
                    or not isinstance(row['access_ref'], str)
                    or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', row['access_ref'])
                    or row['canonical_sha256'] in versions):
                raise ValueError('duplicate or invalid historical service configuration')
            _remote_path(row['service_config'])
            _remote_path(row['server_python'])
            _remote_path(row['rpc_script'])
            versions[row['canonical_sha256']] = copy.deepcopy(row)
        return versions

    def inspect(self, delegate, connection, base_config, requests, *, recheck, **kwargs):
        timeout = kwargs.get('timeout')
        if (type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120
                or not callable(recheck)):
            raise ValueError('a bounded version inspection is required')
        deadline = time.monotonic() + timeout
        if self.access_bindings is not None and self.access_bindings.resolve('base') != connection:
            raise ValueError('the base observation access differs from its pinned source')
        result, access_rechecks = [], []
        for request in requests:
            versions = self._versions()
            nonce = str(uuid4())
            query = copy.deepcopy(request)
            query['nonce'] = nonce
            command = shlex.join([base_config['server_python'], '-B', '-c', VERSION_BOOTSTRAP,
                self.remote_script, self.remote_script_sha256, str(PurePosixPath(base_config['rpc_script']).parent),
                base_config['service_config'],
                base_config['service_config_sha256']])
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError('version inspection timed out')
            response = delegate(copy.deepcopy(connection), command, copy.deepcopy(query),
                                timeout=remaining, max_bytes=16 * 1024 * 1024)
            if len(json.dumps(response, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf8')) > 8192:
                raise ValueError('server version response is too large')
            expected = {'schema': 'paperflow-server-job-config/1', 'server_identity': self.server,
                        'job_id': request['job']['job_id'], 'receipt_sha256': request['receipt_sha256'],
                        'nonce': nonce}
            if (not isinstance(response, dict) or set(response) != set(expected) | {'service_config_canonical_sha256'}
                    or any(response.get(k) != v for k, v in expected.items())):
                raise ValueError('server version response does not match the registered receipt')
            selected = versions.get(response['service_config_canonical_sha256'])
            if selected is None or self._versions() != versions:
                raise ValueError('original historical service configuration is unavailable')
            recheck(request)
            chosen = copy.deepcopy(base_config)
            chosen['service_config'], chosen['service_config_sha256'] = (
                selected['service_config'], selected['service_config_sha256'])
            chosen['server_python'], chosen['rpc_script'] = selected['server_python'], selected['rpc_script']
            reference = selected['access_ref']
            if reference == 'base':
                selected_connection = copy.deepcopy(connection)
            elif self.access_bindings is not None:
                selected_connection = self.access_bindings.resolve(reference)
                if any(selected_connection.get(key) != connection.get(key)
                       for key in ('host', 'port', 'server_key_sha256')):
                    raise ValueError('historical access identifies another server connection')
                access_rechecks.append((reference, copy.deepcopy(selected_connection)))
            else:
                raise ValueError('historical access is unavailable')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError('version inspection timed out')
            options = dict(kwargs, timeout=remaining)
            inspect_command = shlex.join([chosen['server_python'], '-B', '-c', VERSION_BOOTSTRAP,
                chosen['rpc_script'], selected['rpc_script_sha256'], chosen['service_config'],
                chosen['service_config_sha256'], 'inspect'])
            response = delegate(selected_connection, inspect_command,
                                {'requests': [copy.deepcopy(request)]}, **options)
            if not isinstance(response, list) or len(response) != 1:
                raise ValueError('historical receipt inspection is incomplete')
            if self.disclosure_gate is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError('version inspection timed out')
                self.disclosure_gate.check(delegate, selected_connection, selected, request, timeout=remaining)
            result.extend(response)
        self._versions()
        if self.access_bindings is not None and self.access_bindings.resolve('base') != connection:
            raise ValueError('the base observation access changed during inspection')
        for reference, used in access_rechecks:
            if self.access_bindings.resolve(reference) != used:
                raise ValueError('historical access changed during inspection')
        for request in requests:
            recheck(request)
        if time.monotonic() >= deadline:
            raise ValueError('version inspection timed out')
        return result


class ExistingInspectVersionRouter(HistoricalVersionRouter):
    """Try fixed existing inspect commands; an exception never identifies a version."""
    def __init__(self, registry, registry_sha256, *, server_identity, access_bindings, disclosure_gate):
        if (not isinstance(registry_sha256, str) or not SHA.fullmatch(registry_sha256)
                or not isinstance(server_identity, dict) or type(access_bindings) is not PinnedAccessBindings
                or access_bindings.server != server_identity or type(disclosure_gate) is not UnreadDisclosureGate):
            raise ValueError('fixed history, access and unread checks are required')
        self.registry = Path(registry).resolve(strict=True)
        self.registry_sha256, self.server = registry_sha256, copy.deepcopy(server_identity)
        self.access_bindings, self.disclosure_gate = access_bindings, disclosure_gate
        self._versions()

    def inspect(self, delegate, connection, base_config, requests, *, recheck, accept, **kwargs):
        timeout = kwargs.get('timeout')
        if (type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120
                or not callable(recheck) or not callable(accept)):
            raise ValueError('a bounded independently fixed observation is required')
        deadline = time.monotonic() + timeout
        if self.access_bindings.resolve('base') != connection:
            raise ValueError('the base observation access differs from its pinned source')
        if time.monotonic() >= deadline:
            raise ValueError('historical inspection timed out')
        versions = self._versions()
        result, used_access, review_error = [], [], None
        for request in requests:
            match = None
            for selected in versions.values():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError('historical inspection timed out')
                try:
                    chosen_connection = self.access_bindings.resolve(selected['access_ref'])
                    if any(chosen_connection.get(key) != connection.get(key)
                           for key in ('host', 'port', 'server_key_sha256')):
                        continue
                    command = command_for(selected, 'inspect')
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ValueError('historical inspection timed out')
                    response = delegate(chosen_connection, command, {'requests': [copy.deepcopy(request)]},
                                        **dict(kwargs, timeout=min(remaining, 10)))
                except Exception:
                    continue
                expected = {'schema': 'paperflow-authenticated-remote-result/1', 'job': request['job'],
                    'server_identity': request['server_identity'], 'receipt_json': request['receipt_json'],
                    'receipt_sha256': request['receipt_sha256'], 'ledger_state': 'finished',
                    'worker_terminated': True, 'business_validation': 'processor_reported_pass',
                    'business_code_sha256': request['job']['code_sha256']}
                if (not isinstance(response, list) or len(response) != 1 or not isinstance(response[0], dict)
                        or set(response[0]) != set(expected) | {'artifact_map'}
                        or any(response[0].get(key) != value for key, value in expected.items())
                        or not isinstance(response[0].get('artifact_map'), dict)):
                    continue
                try:
                    accept(request, response[0])
                except Exception:
                    continue
                match = selected, chosen_connection, response[0]
                break
            if match is None:
                raise ValueError('no complete original-version inspection is available')
            selected, chosen_connection, item = match
            recheck(request)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError('historical inspection timed out')
            try:
                self.disclosure_gate.check(delegate, chosen_connection, selected, request, timeout=remaining)
            except UnreadReviewRequired as exc:
                # A review finding must meet the same final registration,
                # access, version and timeout checks as a passing result.
                review_error = exc
            used_access.append((selected['access_ref'], copy.deepcopy(chosen_connection)))
            if review_error is not None:
                break
            result.append(item)
        if self._versions() != versions or self.access_bindings.resolve('base') != connection:
            raise ValueError('historical bindings changed during inspection')
        for reference, used in used_access:
            if self.access_bindings.resolve(reference) != used:
                raise ValueError('historical access changed during inspection')
        for request in requests:
            recheck(request)
        if time.monotonic() >= deadline:
            raise ValueError('historical inspection timed out')
        if review_error is not None:
            raise review_error
        return result


class GuardedAppDelegate:
    """Translate only pinned read operations to the verified-byte server entry."""
    def __init__(self, delegate, router, bundle, bundle_sha256):
        if not callable(delegate) or type(router) is not ExistingInspectVersionRouter:
            raise ValueError('the fixed historical router and transport are required')
        self.delegate, self.router = delegate, router
        self.bundle, self.bundle_sha256 = Path(bundle).resolve(strict=True), bundle_sha256
        self._bindings()

    def _bindings(self):
        bundle = json.loads(_read_pinned(self.bundle, self.bundle_sha256).decode('utf8'),
                            object_pairs_hook=_json_object)
        expected_keys = {'schema', 'server_identity', 'index_path', 'index_sha256',
                         'entry_path', 'entry_sha256', 'guard_path', 'guard_sha256', 'unread_path', 'unread_sha256'}
        if (not isinstance(bundle, dict) or set(bundle) != expected_keys
                or bundle['schema'] != 'paperflow-server-guard-bundle/1'
                or bundle['server_identity'] != self.router.server
                or not isinstance(bundle['index_path'], str) or not Path(bundle['index_path']).is_absolute()
                or any(not isinstance(bundle[key], str) or not SHA.fullmatch(bundle[key])
                       for key in ('index_sha256', 'entry_sha256', 'guard_sha256', 'unread_sha256'))):
            raise ValueError('the fixed server guard bundle is unavailable')
        for key in ('entry_path', 'guard_path', 'unread_path'):
            _remote_path(bundle[key])
        if (bundle['unread_path'] != self.router.disclosure_gate.remote_script
                or bundle['unread_sha256'] != self.router.disclosure_gate.remote_script_sha256):
            raise ValueError('the guard bundle and unread check differ')
        index = json.loads(_read_pinned(bundle['index_path'], bundle['index_sha256']).decode('utf8'),
                           object_pairs_hook=_json_object)
        versions = self.router._versions()
        if (not isinstance(index, dict) or set(index) != {'schema', 'server_identity', 'versions'}
                or index['schema'] != 'paperflow-guarded-app-versions/1' or index['server_identity'] != self.router.server
                or not isinstance(index['versions'], list) or len(index['versions']) != len(versions)):
            raise ValueError('the fixed app source index is unavailable')
        sources = {}
        for row in index['versions']:
            if (not isinstance(row, dict) or set(row) != {'canonical_sha256', 'app', 'manifest_path', 'manifest_sha256'}
                    or not isinstance(row['canonical_sha256'], str) or row['canonical_sha256'] not in versions
                    or row['canonical_sha256'] in sources or not isinstance(row['manifest_sha256'], str)
                    or not SHA.fullmatch(row['manifest_sha256'])):
                raise ValueError('a fixed app source record is invalid')
            _remote_path(row['app']); _remote_path(row['manifest_path'])
            if row['app'] != str(PurePosixPath(versions[row['canonical_sha256']]['rpc_script']).parent):
                raise ValueError('app source manifest belongs to another historical entry')
            sources[row['canonical_sha256']] = row
        if set(sources) != set(versions):
            raise ValueError('historical app source coverage is incomplete')
        return bundle, versions, sources

    def __call__(self, connection, command, payload, **kwargs):
        timeout = kwargs.get('timeout')
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError('a bounded guarded operation is required')
        deadline = time.monotonic() + timeout
        bundle, versions, sources = self._bindings()
        matches = []
        for canonical, selected in versions.items():
            for mode in ('inspect', 'probe'):
                if command == command_for(selected, mode):
                    matches.append((canonical, selected, mode, selected['rpc_script'], selected['rpc_script_sha256']))
            unread = shlex.join([selected['server_python'], '-B', '-c', VERSION_BOOTSTRAP,
                bundle['unread_path'], bundle['unread_sha256'], str(PurePosixPath(selected['rpc_script']).parent),
                selected['service_config'], selected['service_config_sha256']])
            if command == unread:
                matches.append((canonical, selected, 'unread', bundle['unread_path'], bundle['unread_sha256']))
        if len(matches) != 1:
            raise ValueError('only an exact registered read-only operation is allowed')
        canonical, selected, mode, target, target_sha = matches[0]
        source = sources[canonical]
        guarded = shlex.join([selected['server_python'], '-I', '-B', '-c', VERSION_BOOTSTRAP,
            bundle['entry_path'], bundle['entry_sha256'], bundle['guard_path'], bundle['guard_sha256'],
            source['app'], source['manifest_path'], source['manifest_sha256'], target, target_sha,
            selected['service_config'], selected['service_config_sha256'], mode])
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError('guarded operation timed out')
        result = self.delegate(connection, guarded, payload, **dict(kwargs, timeout=remaining))
        if time.monotonic() >= deadline or self._bindings() != (bundle, versions, sources) or time.monotonic() >= deadline:
            raise ValueError('guarded operation expired or its bindings changed')
        return result


class ReadOnlyRPC:
    def __init__(self, delegate):
        if not callable(delegate):
            raise ValueError('an explicit RPC transport is required')
        self.delegate, self.database, self.inspector, self.config = delegate, None, None, None
        self.version_router = None

    def bind(self, *, database, inspector, transport_config, version_router=None):
        if self.database is not None or not callable(getattr(inspector, '_request', None)):
            raise ValueError('a single original receipt inspector is required')
        if (version_router is not None and (type(version_router) not in (HistoricalVersionRouter, ExistingInspectVersionRouter)
                or version_router.server != inspector.server)):
            raise ValueError('the fixed historical router must bind the original server')
        self.database = Path(database).resolve(strict=True)
        self.inspector, self.config = inspector, copy.deepcopy(transport_config)
        self.version_router = version_router

    def _registered(self, request):
        job = request.get('job') if isinstance(request, dict) else None
        identity = job.get('identity') if isinstance(job, dict) else None
        if not isinstance(identity, dict) or any(not isinstance(identity.get(k), str) or not identity[k] for k in FIELDS):
            raise ValueError('receipt identity is unavailable')
        key = (identity['task_id'], identity['group_id'])
        with closing(sqlite3.connect(self.database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            if db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone():
                raise ValueError('deleted task cannot be observed')
            stage = _one(db, 'SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?', key)
            claim = _one(db, 'SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?', key)
            result = _one(db, 'SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?', key)
            receipt = _one(db, 'SELECT * FROM postprocessing_receipts WHERE claim_id=?', (claim['claim_id'],))
        raw = receipt.get('receipt_json')
        if (any(any(row.get(field) != identity[field] for field in FIELDS) for row in (stage, claim, result))
                or stage.get('state') != 'finished' or claim.get('state') not in {'waiting_rag', 'receipt_held'}
                or result.get('state') != 'passed' or result.get('code') != 'passed'
                or result.get('claim_id') != claim.get('claim_id') or job.get('claim_id') != claim.get('claim_id')
                or any(row.get('manifest_sha256') != claim.get('manifest_sha256') for row in (stage, result))
                or not isinstance(raw, str) or len(raw.encode('utf8')) > 16 * 1024 * 1024
                or hashlib.sha256(raw.encode('utf8')).hexdigest() != receipt.get('receipt_sha256')
                or result.get('stage_receipt_sha256') != receipt.get('receipt_sha256')):
            raise ValueError('registered pass or receipt identity is unavailable')
        expected = self.inspector._request(json.loads(raw), identity, claim['claim_id'])
        if expected != request:
            raise ValueError('receipt does not match the original durable intent')

    def __call__(self, connection, command, payload, **kwargs):
        if self.database is None or connection != self.config.get('connection'):
            raise ValueError('the fixed observation connection is unavailable')
        timeout = kwargs.get('timeout')
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError('a bounded read-only RPC timeout is required')
        deadline = time.monotonic() + timeout
        if command == command_for(self.config, 'inspect'):
            requests = payload.get('requests') if isinstance(payload, dict) else None
            if (not isinstance(payload, dict) or set(payload) != {'requests'}
                    or not isinstance(requests, list) or not 1 <= len(requests) <= 32):
                raise ValueError('a bounded registered-receipt observation is required')
            for request in requests:
                self._registered(request)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError('registered observation timed out')
            if self.version_router is not None:
                options = dict(kwargs, timeout=remaining)
                if type(self.version_router) is ExistingInspectVersionRouter:
                    options['accept'] = self.inspector._accepted
                try:
                    result = self.version_router.inspect(self.delegate, connection, self.config, requests,
                        recheck=self._registered, **options)
                except UnreadReviewRequired:
                    if time.monotonic() >= deadline:
                        raise ValueError('read-only RPC timed out') from None
                    raise
                if time.monotonic() >= deadline:
                    raise ValueError('read-only RPC timed out')
                return result
        elif command == command_for(self.config, 'probe'):
            if (not isinstance(payload, dict) or set(payload) != {'server_identity', 'nonce'}
                    or payload.get('server_identity') != self.inspector.server):
                raise ValueError('server probe identity is unavailable')
            access = self.version_router.access_bindings if self.version_router is not None else None
            if access is not None and access.resolve('base') != connection:
                raise ValueError('the base observation access differs from its pinned source')
        else:
            raise ValueError('only read-only receipt inspection and probe are permitted')
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError('read-only RPC timed out')
        result = self.delegate(connection, command, payload, **dict(kwargs, timeout=remaining))
        if command == command_for(self.config, 'inspect'):
            for request in requests:
                self._registered(request)
        elif access is not None and access.resolve('base') != connection:
            raise ValueError('the base observation access changed during the probe')
        if time.monotonic() >= deadline:
            raise ValueError('read-only RPC timed out')
        return result


def forbid_transfer(*_args, **_kwargs):
    raise ValueError('the observation service cannot transfer or start work')
