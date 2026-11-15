"""Inert composition for explicitly selected remote Prefect folder stages.

This candidate neither discovers work nor installs configuration, starts a
worker, invokes a model, transfers a real folder, or uploads to RAG on import.
Callers must inject the reviewed original Prefect components and authenticated
remote transport callbacks. No local business fallback or new lock is supplied.
"""
import asyncio
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

import batch_next
import live_runtime
from remote_runtime_adapter import RemoteRuntimeCoordinator, make_remote_coordinator
from stage_runtime import Coordinator, make_one_worker


_COMPONENT_KEYS = frozenset({'base', 'stage_store', 'journal', 'load_inputs', 'snapshot_options'})
_TRANSPORT_KEYS = frozenset({'transfer_snapshot', 'validate_staging', 'submit_once',
                             'wait_for_result', 'validate_result'})
_ORIGINAL_CALLBACKS = ('task_gate', 'folder_gate', 'slot', 'verify_upstream', 'verify_execution',
                       'postprocess_is_open', 'rag_is_held')


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False


class RemoteCoordinatorFactory:
    """Pin remote identity while rebuilding components for each Prefect TaskRun.

    components_factory returns a base using the ORIGINAL callbacks, remote stage
    store, initialized remote journal, input loader and snapshot options. Reuse
    the same originally constructed base/callbacks within this worker process;
    do not call build_runtime() again to create different gate/slot closures.
    The adapter checks their shared gate, inspector, database and server. The
    supplied callbacks must already implement the authenticated remote contract.
    """
    def __init__(self, *, server_identity, control_database, journal_database, journal_id,
                 original_base, components_factory, transport, admission_wait=None):
        if (not isinstance(server_identity, dict) or set(server_identity) != {'server_id', 'ledger_id'}
                or not isinstance(server_identity['server_id'], str)
                or not 0 < len(server_identity['server_id']) <= 256
                or server_identity['server_id'].strip() != server_identity['server_id']
                or not _uuid(server_identity['ledger_id']) or not _uuid(journal_id)
                or not isinstance(original_base, Coordinator) or isinstance(original_base, RemoteRuntimeCoordinator)
                or not all(callable(getattr(original_base, name, None)) for name in _ORIGINAL_CALLBACKS)
                or not callable(components_factory) or not isinstance(transport, dict)
                or set(transport) != _TRANSPORT_KEYS
                or not all(callable(value) for value in transport.values())
                or (admission_wait is not None and (not isinstance(admission_wait, dict)
                    or set(admission_wait) != {'wait', 'max_attempts'} or not callable(admission_wait['wait'])
                    or (admission_wait['max_attempts'] is not None and
                        (type(admission_wait['max_attempts']) is not int
                         or not 1 <= admission_wait['max_attempts'] <= 1000))))):
            raise ValueError('explicit remote identity, journal and complete transport are required')
        self._database, self._control_identity = self._pin_file(control_database)
        self._journal_database, self._journal_identity = self._pin_file(journal_database)
        if self._database == self._journal_database or self._control_identity == self._journal_identity:
            raise ValueError('the control database and remote journal must be separate files')
        if Path(original_base.database).resolve(strict=True) != self._database:
            raise ValueError('the original Prefect coordinator uses another control database')
        self._original_callbacks = {name: getattr(original_base, name) for name in _ORIGINAL_CALLBACKS}
        self._server_id = server_identity['server_id']
        self._ledger_id = server_identity['ledger_id']
        self._journal_id = journal_id
        self._components = components_factory
        self._transport = dict(transport)
        self._admission_wait = None if admission_wait is None else dict(admission_wait)

    @staticmethod
    def _pin_file(value):
        try:
            path = Path(value)
            if not path.is_absolute() or '..' in path.parts:
                raise ValueError('absolute database path required')
            path = path.resolve(strict=True)
            info = path.stat()
            if not path.is_file():
                raise ValueError('database file required')
            return path, (info.st_dev, info.st_ino)
        except (TypeError, ValueError, OSError) as exc:
            raise ValueError('original database file identity is unavailable') from exc

    def _check_files(self):
        if (self._pin_file(self._database) != (self._database, self._control_identity)
                or self._pin_file(self._journal_database) != (self._journal_database, self._journal_identity)):
            raise ValueError('the control database or remote journal file was replaced')

    def __call__(self):
        self._check_files()
        parts = self._components()
        if not isinstance(parts, dict) or set(parts) != _COMPONENT_KEYS:
            raise ValueError('complete original Prefect and remote components are required')
        base, journal = parts['base'], parts['journal']
        if any(getattr(base, name, None) is not value for name, value in self._original_callbacks.items()):
            raise ValueError('original task/folder gates, Prefect slot or verification callbacks changed')
        try:
            same_database = (Path(base.database).resolve(strict=True) == self._database
                             and Path(journal.control_database).resolve(strict=True) == self._database)
            same_journal = (journal.journal_id == self._journal_id
                            and Path(journal.database).resolve(strict=True) == self._journal_database)
        except (AttributeError, TypeError, OSError) as exc:
            raise ValueError('original control or remote journal identity is unavailable') from exc
        if not same_database or not same_journal:
            raise ValueError('original control or remote journal identity changed')
        coordinator = make_remote_coordinator(**parts,
            server_identity=dict(server_id=self._server_id, ledger_id=self._ledger_id),
            admission_wait=self._admission_wait, **self._transport)
        if not isinstance(coordinator, RemoteRuntimeCoordinator):
            raise ValueError('the worker must execute the remote coordinator')
        self._check_files()
        return coordinator


class RemotePrefectEntry:
    """Reuse the original single, batch and never-submitted recovery paths."""
    def __init__(self, *, factory, client_factory, expected_api_url):
        if (not isinstance(factory, RemoteCoordinatorFactory) or not callable(client_factory)
                or not isinstance(expected_api_url, str) or not expected_api_url.strip()
                or not expected_api_url.rstrip('/')):
            raise ValueError('a server coordinator factory and explicit Prefect identity are required')
        self._factory, self._client_factory = factory, client_factory
        self._api = expected_api_url.rstrip('/')

    @property
    def factory(self):
        return self._factory

    @contextmanager
    def _client(self):
        with self._client_factory() as client:
            if str(getattr(client, 'api_url', '')).rstrip('/') != self._api:
                raise ValueError('the Prefect client identity changed')
            yield client

    def _check_client(self):
        with self._client():
            pass

    def _coordinator(self):
        factory = self.factory
        if not isinstance(factory, RemoteCoordinatorFactory):
            raise ValueError('the server coordinator factory is unavailable')
        return factory, factory()

    async def run_one(self, identity, generation, *, worker_factory=make_one_worker, poll_seconds=.5,
                      retry_job_id=None):
        factory, coordinator = self._coordinator()
        await asyncio.to_thread(self._check_client)
        return await live_runtime.run_selected(coordinator, identity, generation,
            client_factory=self._client, runtime_factory=factory,
            worker_factory=worker_factory, poll_seconds=poll_seconds, retry_job_id=retry_job_id)

    async def run_reserved(self, item, *, worker_factory=make_one_worker, poll_seconds=.5):
        factory, coordinator = self._coordinator()
        await asyncio.to_thread(self._check_client)
        return await live_runtime.run_reserved(coordinator, item,
            client_factory=self._client, runtime_factory=factory,
            worker_factory=worker_factory, poll_seconds=poll_seconds)

    async def run_batch(self, selections, *, worker_factory=batch_next.make_batch_worker,
                        batch_size=8, poll_seconds=.5):
        factory, coordinator = self._coordinator()
        await asyncio.to_thread(self._check_client)
        return await batch_next.run_batch(coordinator, selections,
            client_factory=self._client, runtime_factory=factory, worker_factory=worker_factory,
            batch_size=batch_size, poll_seconds=poll_seconds)

    async def resume_stopped(self, job_id, generation, *, worker_factory=make_one_worker, poll_seconds=.5):
        factory, coordinator = self._coordinator()
        await asyncio.to_thread(self._check_client)
        return await live_runtime.resume_stopped(coordinator, job_id, generation,
            client_factory=self._client, expected_api_url=self._api, runtime_factory=factory,
            worker_factory=worker_factory, poll_seconds=poll_seconds)
