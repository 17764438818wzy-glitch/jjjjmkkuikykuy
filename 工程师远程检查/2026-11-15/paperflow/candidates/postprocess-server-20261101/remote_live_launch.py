"""Explicit config check and single-ticket entry for the remote candidate.

Import does not read configuration or start a worker. ``check`` only loads
pinned local configuration, checks identities, and constructs adapters; it
does not contact the server or Prefect. ``run-one`` is a separate explicit
command for ONE supplied task/group, subject to the existing task and folder
gates. ``run-batch`` queries only its explicitly selected task, with an explicit
limit. There is no automatic discovery, journal initialization, retry, or local fallback.

The pinned config and source hashes identify the named files only; they are
not a full runtime dependency inventory or remote attestation. Do not put
passwords, key bytes, model credentials or paper content in this config.
"""
import argparse
import asyncio
from contextlib import closing
from dataclasses import dataclass
import hashlib
import hmac
import importlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from uuid import UUID


_FIELDS = frozenset({'schema', 'runtime_dir', 'runtime_sources', 'prefect_project',
    'settings_sha256', 'tokenizer_runtime_sha256', 'expected_prefect_api_url',
    'journal_database', 'journal_id', 'server_identity', 'transport_config',
    'transport_config_sha256', 'snapshot_options', 'expected_catalog_path',
    'expected_catalog_sha256', 'max_summary_receipts'})
_SOURCE_NAMES = frozenset({'live_runtime.py', 'stage_runtime.py', 'batch_next.py'})
_SNAPSHOT_FIELDS = frozenset({'code_sha256', 'paperflow_root_windows', 'business_workspace_windows',
    'output_root_windows', 'runtime_template_sha256', 'allowed_roots',
    'max_json_file_bytes', 'max_total_json_bytes'})
_LIMIT = 4 * 1024 * 1024


def _sha(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def _path(value):
    try:
        path = Path(value)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError('an absolute configured path is required')
        return path
    except (TypeError, OSError) as exc:
        raise ValueError('an absolute configured path is required') from exc


def _read_pinned(path, expected):
    if not _sha(expected):
        raise ValueError('an independent SHA256 is required')
    try:
        with _path(path).open('rb') as stream:
            data = stream.read(_LIMIT + 1)
    except (OSError, TypeError) as exc:
        raise ValueError('the pinned file is unavailable') from exc
    if len(data) > _LIMIT or not hmac.compare_digest(hashlib.sha256(data).hexdigest(), expected):
        raise ValueError('the pinned file is too large or has changed')
    return data


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON configuration key')
        result[key] = value
    return result


def _bad_constant(_):
    raise ValueError('non-finite JSON configuration value')


def _json(data):
    try:
        value = json.loads(data.decode('utf-8-sig'), object_pairs_hook=_object, parse_constant=_bad_constant)
    except (UnicodeError, TypeError, ValueError) as exc:
        raise ValueError('a valid finite JSON configuration is required') from exc
    if not isinstance(value, dict):
        raise ValueError('configuration must be a JSON object')
    return value


def _config(path, sha256):
    config = _json(_read_pinned(path, sha256))
    if (set(config) != _FIELDS or config['schema'] != 'paperflow-remote-live/1'
            or not isinstance(config['runtime_sources'], dict)
            or set(config['runtime_sources']) != _SOURCE_NAMES
            or not all(_sha(value) for value in config['runtime_sources'].values())
            or not all(_sha(config[key]) for key in ('settings_sha256', 'tokenizer_runtime_sha256',
                'transport_config_sha256', 'expected_catalog_sha256'))
            or not isinstance(config['expected_prefect_api_url'], str)
            or not config['expected_prefect_api_url'].strip().rstrip('/')
            or not isinstance(config['server_identity'], dict)
            or not isinstance(config['transport_config'], dict)
            or not isinstance(config['snapshot_options'], dict)
            or set(config['snapshot_options']) != _SNAPSHOT_FIELDS
            or not _task_id(config['journal_id'])
            or type(config['max_summary_receipts']) is not int
            or not 1 <= config['max_summary_receipts'] <= 10000):
        raise ValueError('the explicit remote configuration is incomplete')
    identity, options, transport_config = (config['server_identity'], config['snapshot_options'],
                                           config['transport_config'])
    if (set(identity) != {'server_id', 'ledger_id'}
            or not isinstance(identity['server_id'], str) or not identity['server_id']
            or identity['server_id'].strip() != identity['server_id'] or not _task_id(identity['ledger_id'])
            or not all(_sha(options[key]) for key in ('code_sha256', 'runtime_template_sha256'))
            or not isinstance(options['allowed_roots'], list) or not options['allowed_roots']
            or any(not isinstance(root, str) for root in options['allowed_roots'])
            or any(type(options[key]) is not int or options[key] <= 0
                   for key in ('max_json_file_bytes', 'max_total_json_bytes'))
            or options['max_total_json_bytes'] < options['max_json_file_bytes']
            or transport_config.get('server_identity') != identity
            or transport_config.get('allowed_roots') != options['allowed_roots']
            or transport_config.get('trusted_files') != [dict(path=config['expected_catalog_path'],
                                                               sha256=config['expected_catalog_sha256'])]):
        raise ValueError('the server, source roots, catalog or budgets differ')
    for root in options['allowed_roots']:
        _path(root)
    for key in ('runtime_dir', 'prefect_project', 'journal_database', 'expected_catalog_path'):
        _path(config[key])
    try:
        private_key = _path(config['transport_config']['connection']['private_key'])
    except (KeyError, TypeError) as exc:
        raise ValueError('an existing SSH private-key path is required') from exc
    if not private_key.is_file() or not _path(config['journal_database']).is_file():
        raise ValueError('the configured SSH key or original journal is unavailable')
    transport = json.dumps(config['transport_config'], ensure_ascii=False, sort_keys=True,
                           separators=(',', ':'), allow_nan=False).encode('utf8')
    if not hmac.compare_digest(hashlib.sha256(transport).hexdigest(), config['transport_config_sha256']):
        raise ValueError('the independently pinned transport configuration changed')
    return config


def _sources(config):
    directory = _path(config['runtime_dir'])
    if directory.name not in ('runtime_next', 'generation_next'):
        raise ValueError('a reviewed original runtime directory is required')
    project = _path(config['prefect_project'])
    if project.resolve() != (directory.parents[2] / 'prefect-paperflow').resolve():
        raise ValueError('the runtime and Prefect project paths do not match')
    for name, expected in config['runtime_sources'].items():
        _read_pinned(directory / name, expected)


def _original_configuration(config):
    project = _path(config['prefect_project'])
    settings = _json(_read_pinned(project / 'config.json', config['settings_sha256']))
    try:
        api = settings['prefect_api_url']
        execution_enabled = settings['execution_enabled']
        catalog = _path(settings['catalog_path']).resolve(strict=True)
        runtime_file = _path(settings['runtime_config']).resolve(strict=True)
        expected_catalog = _path(config['expected_catalog_path']).resolve(strict=True)
    except (KeyError, TypeError, OSError) as exc:
        raise ValueError('original Prefect configuration is incomplete') from exc
    if (type(execution_enabled) is not bool or not isinstance(api, str)
            or api.rstrip('/') != config['expected_prefect_api_url'].rstrip('/')
            or catalog != expected_catalog):
        raise ValueError('original execution setting, Prefect API or catalog identity differs')
    tokenizer = _json(_read_pinned(runtime_file, config['tokenizer_runtime_sha256']))
    catalog_value = _json(_read_pinned(catalog, config['expected_catalog_sha256']))
    return tokenizer, catalog_value, execution_enabled


def _module_file(module, expected):
    try:
        if Path(module.__file__).resolve(strict=True) != Path(expected).resolve(strict=True):
            raise ValueError('an imported runtime module has another origin')
    except (AttributeError, TypeError, OSError) as exc:
        raise ValueError('an imported runtime module origin is unavailable') from exc


def _load_pinned_source(name, path, expected_sha256):
    """Execute exactly the verified source bytes, never an existing pyc cache."""
    if name in sys.modules:
        raise ValueError('a fresh runtime process is required before importing business modules')
    path = _path(path)
    source = _read_pinned(path, expected_sha256)
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError('a pinned runtime source cannot be loaded')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(compile(source, str(path), 'exec', dont_inherit=True), module.__dict__)
    except BaseException:
        if sys.modules.get(name) is module:
            del sys.modules[name]
        raise
    return module


def _import_modules(config):
    directory = _path(config['runtime_dir']).resolve(strict=True)
    candidate = directory.parent
    project = _path(config['prefect_project']).resolve(strict=True)
    local = Path(__file__).resolve().parent
    paths = [directory, candidate / 'results_next', candidate, project, local]
    if any(not path.is_dir() for path in paths):
        raise ValueError('the original runtime import directories are unavailable')
    # A path on an already loaded module does not prove which bytes its code
    # came from. Require a dedicated fresh process before initial construction;
    # the resulting object can be reused for many independently selected jobs.
    expected = {'live_runtime': directory / 'live_runtime.py', 'stage_runtime': directory / 'stage_runtime.py',
        'batch_next': directory / 'batch_next.py', 'folder_state': candidate / 'folder_state.py',
        'postprocessing_results': candidate / 'results_next' / 'postprocessing_results.py',
        'remote_live_runtime': local / 'remote_live_runtime.py',
        'remote_prefect_entry': local / 'remote_prefect_entry.py',
        'remote_runtime_adapter': local / 'remote_runtime_adapter.py',
        'paperflow_prefect.settings': project / 'paperflow_prefect' / 'settings.py'}
    for name in expected:
        if name in sys.modules:
            raise ValueError('a fresh runtime process is required before importing business modules')
    pinned = [str(path) for path in paths]
    keys = {os.path.normcase(os.path.abspath(value)) for value in pinned}
    sys.path[:] = pinned + [value for value in sys.path if not isinstance(value, str)
                            or os.path.normcase(os.path.abspath(value)) not in keys]
    for name in ('stage_runtime', 'live_runtime', 'batch_next'):
        _load_pinned_source(name, expected[name], config['runtime_sources'][name + '.py'])
    live = sys.modules['live_runtime']
    remote = importlib.import_module('remote_live_runtime')
    settings = importlib.import_module('paperflow_prefect.settings')
    for name, path in expected.items():
        if name in sys.modules:
            _module_file(sys.modules[name], path)
    return live, remote, settings


def _task_id(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False


@dataclass
class LoadedRemoteRuntime:
    original_runtime: dict
    remote: object
    live: object
    batch: object
    api: str
    execution_enabled: bool
    _verify: object

    def check_configuration(self):
        """Recheck original pinned inputs before a new explicitly selected job."""
        self._verify()

    async def run_one(self, task_id, group_id):
        """Select one exact existing folder, then invoke the original-gated entry."""
        if not _task_id(task_id) or not isinstance(group_id, str) or not re.fullmatch('[a-f0-9]{32}', group_id):
            raise ValueError('one exact task UUID and folder group are required')
        await asyncio.to_thread(self.check_configuration)
        if self.execution_enabled is not True:
            raise ValueError('the original execution setting is disabled')
        from prefect.settings import (PREFECT_API_URL, PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS,
                                      temporary_settings)
        with temporary_settings({PREFECT_API_URL: self.api,
                                 PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS: False}):
            identity, generation = await asyncio.to_thread(self.live.select_folder,
                self.original_runtime['database'], task_id, group_id)
            if (not isinstance(identity, dict) or identity.get('task_id') != task_id
                    or identity.get('group_id') != group_id or type(generation) is not int or generation < 1):
                raise ValueError('the selected original folder identity changed')
            await asyncio.to_thread(self.check_configuration)
            return await self.remote.entry.run_one(identity, generation)

    def _batch_task_authority(self, task_id):
        """Read only the specified existing task and reuse its original gates."""
        coordinator = self.original_runtime['coordinator']
        coordinator._gates()
        database = _path(self.original_runtime['database']).resolve(strict=True)
        if database != Path(coordinator.database).resolve(strict=True):
            raise ValueError('the original task control database changed')
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            task = db.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
            deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
        if (not task or deleted or task['desired_state'] != 'running' or task['start_pending'] != 0
                or not isinstance(task['task_status'], str) or not task['task_status']
                or task['task_status'] in {'deleting', 'delete_failed', 'unknown'}
                or task['prefect_order_model'] != 'v2' or not _task_id(task['prefect_run_id'])
                or type(task['start_generation']) is not int or task['start_generation'] < 1):
            raise ValueError('the specified task is paused, deleted or not verifiably running')
        coordinator._gates()
        return task['start_generation'], task['prefect_run_id'], task['task_status']

    async def run_batch(self, task_id, max_folders, *, batch_size=8, poll_seconds=.5):
        """Use the original bounded snapshot and dispatch only one selected task."""
        if (not _task_id(task_id) or type(max_folders) is not int or not 1 <= max_folders < 2**63
                or type(batch_size) is not int or not 1 <= batch_size <= 8
                or type(poll_seconds) not in (int, float) or not 0 < poll_seconds < float('inf')):
            raise ValueError('one exact task and explicit positive batch limits are required')
        await asyncio.to_thread(self.check_configuration)
        if self.execution_enabled is not True:
            raise ValueError('the original execution setting is disabled')
        from prefect.settings import (PREFECT_API_URL, PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS,
                                      temporary_settings)
        with temporary_settings({PREFECT_API_URL: self.api,
                                 PREFECT_TASKS_SCHEDULING_DELETE_FAILED_SUBMISSIONS: False}):
            authority = await asyncio.to_thread(self._batch_task_authority, task_id)
            selected = await asyncio.to_thread(self.batch.snapshot_waiting,
                self.original_runtime['database'], task_id, max_folders)
            await asyncio.to_thread(self.check_configuration)
            if authority != await asyncio.to_thread(self._batch_task_authority, task_id):
                raise ValueError('the specified task changed while its snapshot was read')
            if not isinstance(selected, list) or len(selected) > max_folders:
                raise ValueError('the original task snapshot exceeded its selected limit')
            seen, frozen = set(), []
            for pair in selected:
                if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                    raise ValueError('the original task snapshot is malformed')
                identity, generation = pair
                identity = self.batch._identity(identity)
                if (identity['task_id'] != task_id or identity['parent_run_id'] != authority[1]
                        or type(generation) is not int or generation != authority[0]
                        or identity['task_run_id'] in seen):
                    raise ValueError('the original task snapshot changed its identity or generation')
                seen.add(identity['task_run_id'])
                frozen.append((identity, generation))
            completed, attempted = [], 0
            def outcome(*, stopped, detail, last_group, unconfirmed=False):
                return dict(completed=list(completed), stopped=stopped, detail=detail,
                            attempted=None if unconfirmed else attempted,
                            remaining=None if unconfirmed else len(frozen) - attempted,
                            confirmed_attempted=attempted, snapshot_selected=len(frozen), last_group=last_group)

            for start in range(0, len(frozen), batch_size):
                # Stop before creating the next original TaskWorker if the
                # selected task paused or configuration changed meanwhile.
                try:
                    await asyncio.to_thread(self.check_configuration)
                    if authority != await asyncio.to_thread(self._batch_task_authority, task_id):
                        raise ValueError('the selected task generation or state changed')
                except Exception as exc:
                    return outcome(stopped=True, detail=dict(reason='before_batch_check_failed',
                                   error_type=type(exc).__name__), last_group='not_started')
                group = frozen[start:start + batch_size]
                try:
                    result = await self.remote.entry.run_batch(group, batch_size=batch_size, poll_seconds=poll_seconds)
                except Exception as exc:
                    return outcome(stopped=True, detail=dict(reason='batch_outcome_unconfirmed',
                                   error_type=type(exc).__name__), last_group='unconfirmed', unconfirmed=True)
                expected = [self.batch._job_id(identity['task_run_id']) for identity, _ in group]
                if (not isinstance(result, dict) or not isinstance(result.get('completed'), list)
                        or any(not isinstance(job, str) for job in result['completed'])
                        or type(result.get('stopped')) is not bool or type(result.get('attempted')) is not int
                        or not 0 <= result['attempted'] <= len(group)
                        or len(set(result['completed'])) != len(result['completed'])
                        or not set(result['completed']).issubset(set(expected[:result['attempted']]))
                        or len(result['completed']) > result['attempted']
                        or type(result.get('remaining')) is not int
                        or result['remaining'] != len(group) - result['attempted']
                        or (not result['stopped'] and
                            (result['remaining'] or set(result['completed']) != set(expected)))):
                    return outcome(stopped=True, detail=dict(reason='batch_result_inconsistent'),
                                   last_group='unconfirmed', unconfirmed=True)
                completed.extend(result['completed'])
                attempted += result['attempted']
                if result['stopped']:
                    return outcome(stopped=True, detail=result.get('detail'), last_group='stopped')
            return outcome(stopped=False, detail=None, last_group='finished' if frozen else 'none')


def build_from_config(config_path, config_sha256, *, stage_call=None, rpc_call=None):
    """Check fixed local configuration and build the adapters; never start work."""
    config = _config(config_path, config_sha256)
    _sources(config)
    tokenizer, catalog, execution_enabled = _original_configuration(config)
    live, remote_module, settings_module = _import_modules(config)
    batch = importlib.import_module('batch_next')
    if _path(settings_module.DEFAULT_CONFIG).resolve(strict=True) != (
            _path(config['prefect_project']) / 'config.json').resolve(strict=True):
        raise ValueError('the imported original Settings reads another config')
    original = live.build_runtime()
    optional = {}
    if stage_call is not None: optional['stage_call'] = stage_call
    if rpc_call is not None: optional['rpc_call'] = rpc_call
    remote = remote_module.build_remote_live_runtime(original_runtime=original,
        journal_database=config['journal_database'], journal_id=config['journal_id'],
        expected_prefect_api_url=config['expected_prefect_api_url'], server_identity=config['server_identity'],
        transport_config=config['transport_config'], transport_config_sha256=config['transport_config_sha256'],
        snapshot_options=dict(config['snapshot_options'], catalog=catalog),
        expected_catalog_path=config['expected_catalog_path'],
        expected_catalog_sha256=config['expected_catalog_sha256'], tokenizer_runtime=tokenizer,
        max_summary_receipts=config['max_summary_receipts'], **optional)
    def verify_configuration():
        if (_config(config_path, config_sha256) != config
                or _original_configuration(config) != (tokenizer, catalog, execution_enabled)):
            raise ValueError('configuration changed after construction')
        _sources(config)

    # A retained object also rechecks its fixed inputs before each selection.
    verify_configuration()
    return LoadedRemoteRuntime(original, remote, live, batch, config['expected_prefect_api_url'].rstrip('/'),
                               execution_enabled, verify_configuration)


def main(argv=None):
    parser = argparse.ArgumentParser(description='显式核对远程后处理配置，或处理一个指定小工单')
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('check', 'run-one', 'run-batch'):
        p = sub.add_parser(command)
        p.add_argument('--config', required=True)
        p.add_argument('--config-sha256', required=True)
        if command == 'run-one':
            p.add_argument('--task-id', required=True)
            p.add_argument('--group-id', required=True)
        if command == 'run-batch':
            p.add_argument('--task-id', required=True)
            p.add_argument('--max-folders', type=int, required=True)
            p.add_argument('--batch-size', type=int, default=8)
    args = parser.parse_args(argv)
    try:
        runtime = build_from_config(args.config, args.config_sha256)
        if args.command == 'check':
            print(json.dumps(dict(configuration='loaded', execution_enabled=runtime.execution_enabled,
                                  business_work_started=False,
                                  server_connectivity='not_checked'), ensure_ascii=False))
            return 0
        if args.command == 'run-batch':
            result = asyncio.run(runtime.run_batch(args.task_id, args.max_folders, batch_size=args.batch_size))
            summary = dict(confirmed_stages_finished=len(result['completed']),
                           snapshot_selected=result['snapshot_selected'],
                           snapshot_attempted=result['attempted'], snapshot_remaining=result['remaining'],
                           confirmed_attempted=result['confirmed_attempted'], last_group=result['last_group'],
                           stopped=result['stopped'], rag_handoff='not_checked')
            print(json.dumps(summary, ensure_ascii=False))
            return 1 if result['stopped'] else 0
        result = asyncio.run(runtime.run_one(args.task_id, args.group_id))
        summary = {key: result.get(key) for key in ('job_id', 'state', 'observation')}
        summary['rag_handoff'] = 'not_checked'
        print(json.dumps(summary, ensure_ascii=False))
        return 0 if result.get('state') == 'finished' else 1
    except Exception as exc:
        print(json.dumps(dict(status='not_confirmed', error_type=type(exc).__name__), ensure_ascii=False),
              file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
