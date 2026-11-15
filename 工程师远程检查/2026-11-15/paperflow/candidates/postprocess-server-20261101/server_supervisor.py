"""Fail-closed Linux supervision for the isolated remote_jobs ledger.

INSTALLATION IS EXPLICIT: initialize() adds supervisor state to the existing,
pinned ledger. Nothing in this module starts a scheduler, SSH, Wine, LLM or RAG.

The caller supplies a dedicated, writable cgroup-v2 parent (for example an
explicit systemd delegated scope), fixed audited argv/environment and an actual
artifact/receipt validator. Each Wine job MUST have a private Wine prefix and
no shared wineserver outside its cgroup. A cgroup includes setsid/double-fork
descendants; a PID, process group, parent exit or exit code cannot prove this.

Trust boundary: the worker runs trusted fixed business scripts, never executes
paper/model text as code, and neither worker nor same-UID administrator migrates
processes out of, injects processes into, or replaces the job cgroup. This is NOT
a sandbox against malicious same-UID cgroup migration. An unavailable/replaced
scope or server reboot keeps the job unknown and its slot occupied. Unknown
jobs require separate reviewed reconciliation; this module never retries them.

Timeouts persist a Linux monotonic deadline with its boot ID. Wall time is for
UI timestamps only. The supervisor schema containing clock_json/held_sha256 is
an explicit installation change: old ledgers need reviewed migration and are
never silently converted, recreated or interpreted with another clock basis.
CLOCK_MONOTONIC does not count system suspend. Supervisors reopening one ledger
must use the same Linux time namespace; boot ID alone cannot prove namespace
clock offsets. A deployment requiring suspend-inclusive timeout needs a separate
reviewed CLOCK_BOOTTIME clock basis, never reinterpret these stored deadlines.
"""
from contextlib import closing
import copy
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

from remote_jobs import JobConflict, _validated, _uuid


class SupervisionUnavailable(RuntimeError):
    pass


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)


def _boot_id():
    if sys.platform != 'linux':
        raise SupervisionUnavailable('Linux process identity is required')
    value = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if not _uuid(value):
        raise SupervisionUnavailable('kernel boot identity unavailable')
    return value


def process_identity(pid):
    """Read the actual Linux identity, guarding the comm field's parentheses."""
    if type(pid) is not int or pid < 1:
        raise ValueError('positive Linux PID required')
    boot = _boot_id()
    text = Path('/proc', str(pid), 'stat').read_text()
    fields = text[text.rfind(')') + 2:].split()
    if not text.startswith(str(pid) + ' (') or len(fields) < 20:
        raise SupervisionUnavailable('invalid kernel process record')
    if _boot_id() != boot:
        raise SupervisionUnavailable('server boot identity changed')
    return dict(boot_id=boot, pid=pid, start_ticks=int(fields[19]))


def same_process(identity):
    """False means this PID is gone/reused, NOT that its children have ended."""
    try:
        return process_identity(identity['pid']) == identity
    except FileNotFoundError:
        return False


class LinuxScope:
    """A job-owned kernel cgroup; never substitutes a process-group probe."""
    def __init__(self, parent, job_id, *, worker_uid, worker_gid):
        self._configure(parent, job_id, worker_uid, worker_gid)
        self.path.mkdir(exist_ok=False)
        self._check_directory(self.path)
        self.process = None
        self.gate = None

    def _configure(self, parent, job_id, uid, gid):
        if sys.platform != 'linux' or not _uuid(job_id):
            raise SupervisionUnavailable('Linux cgroup-v2 job identity required')
        if type(uid) is not int or type(gid) is not int or uid < 1 or gid < 1:
            raise SupervisionUnavailable('explicit non-root worker UID/GID required')
        if os.geteuid() != 0 and (uid != os.geteuid() or gid != os.getegid()):
            raise SupervisionUnavailable('worker credentials are unavailable')
        parent = Path(parent).absolute()
        if parent.resolve(strict=True) != parent:
            raise SupervisionUnavailable('cgroup parent must not contain links')
        self._check_directory(parent)
        if not os.access(parent, os.W_OK | os.X_OK):
            raise SupervisionUnavailable('explicit writable delegated cgroup required')
        self.parent, self.path = parent, parent / job_id
        self.uid, self.gid = uid, gid

    @staticmethod
    def _check_directory(path):
        # statfs validates the actual kernel filesystem, not a lookalike folder.
        info = ctypes.create_string_buffer(256)
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.statfs(os.fsencode(path), info) != 0:
            raise SupervisionUnavailable('cgroup filesystem cannot be inspected')
        if ctypes.c_long.from_buffer(info).value != 0x63677270:
            raise SupervisionUnavailable('process groups cannot prove cgroup-v2 containment')
        if (path.is_symlink() or not (path / 'cgroup.events').is_file()
                or not (path / 'cgroup.procs').is_file()
                or (path / 'cgroup.type').read_text().strip() != 'domain'):
            raise SupervisionUnavailable('owned domain cgroup evidence is unavailable')

    @classmethod
    def reopen(cls, parent, job_id, *, worker_uid, worker_gid):
        instance = cls.__new__(cls)
        instance._configure(parent, job_id, worker_uid, worker_gid)
        instance._check_directory(instance.path)
        instance.process, instance.gate = None, None
        return instance

    def launch(self, argv, *, cwd=None, env=None, stderr=None):
        read_fd, self.gate = os.pipe()
        try:
            self.process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), '--gate', str(read_fd),
                 str(self.uid), str(self.gid), *argv], pass_fds=(read_fd,),
                cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL if stderr is None else stderr,
                start_new_session=True)
            # The launcher cannot exec business argv before the gate is opened.
            (self.path / 'cgroup.procs').write_text(str(self.process.pid))
            if str(self.process.pid) not in (self.path / 'cgroup.procs').read_text().split():
                raise SupervisionUnavailable('gated worker did not enter its cgroup')
            return process_identity(self.process.pid)
        except BaseException:
            if self.gate is not None:
                os.close(self.gate)
                self.gate = None
            raise
        finally:
            os.close(read_fd)

    def release_gate(self):
        try:
            os.write(self.gate, b'1')
        finally:
            os.close(self.gate)
            self.gate = None

    def snapshot(self):
        self._check_directory(self.path)
        boot = _boot_id()
        before = self.path.stat()
        events = dict(line.split() for line in (self.path / 'cgroup.events').read_text().splitlines())
        after = self.path.stat()
        if ((before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
                or events.get('populated') not in ('0', '1') or _boot_id() != boot):
            raise SupervisionUnavailable('cgroup identity or population is uncertain')
        if self.process is not None:
            self.process.poll()  # Reap the direct launcher; never used as exit proof.
        return dict(populated=events['populated'] == '1', boot_id=boot,
                    scope_id=[before.st_dev, before.st_ino])

    def leader_alive(self, identity):
        return same_process(identity)


class ServerSupervisor:
    def __init__(self, ledger, *, cgroup_parent, worker_uid, worker_gid,
                 receipt_validator, clock=time.time, monotonic_clock=time.monotonic,
                 boot_id_provider=_boot_id, business_gate_check=None):
        if not callable(receipt_validator):
            raise ValueError('an actual receipt/artifact validator is required')
        if business_gate_check is not None and not callable(business_gate_check):
            raise ValueError('the optional business gate check must be callable')
        if business_gate_check is not None:
            from task_generation_fence import check_business_gate
            if business_gate_check is not check_business_gate:
                raise ValueError('the fixed task-generation fence check is required')
        self.ledger, self.cgroup_parent = ledger, Path(cgroup_parent).absolute()
        self.worker_uid, self.worker_gid = worker_uid, worker_gid
        self.receipt_validator, self.clock = receipt_validator, clock
        self.monotonic_clock, self.boot_id_provider = monotonic_clock, boot_id_provider
        self.business_gate_check = business_gate_check
        self._requires_business_gate = business_gate_check is not None
        self._scopes = {}

    def initialize(self):
        """Explicit one-time installation; never creates/replaces the ledger."""
        with closing(self.ledger._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE supervisor_jobs ('
                'job_id TEXT PRIMARY KEY, frozen_json TEXT NOT NULL, config_json TEXT NOT NULL, '
                'deadline REAL NOT NULL, process_json TEXT, scope_json TEXT, observation_json TEXT, '
                'receipt_json TEXT, clock_json TEXT NOT NULL, held_sha256 TEXT)')

    def _config(self):
        config = dict(parent=str(self.cgroup_parent), uid=self.worker_uid, gid=self.worker_gid,
                      clock_basis='linux-monotonic-boot/1')
        if self._requires_business_gate:
            config['business_gate_policy'] = 'task-generation-fence/1'
        return _json(config)

    def _clock_sample(self):
        """time.monotonic is shared across processes on the same Linux boot."""
        boot = self.boot_id_provider()
        now = self.monotonic_clock()
        if (not isinstance(boot, str) or not boot or boot != self.boot_id_provider()
                or isinstance(now, bool) or not isinstance(now, (int, float))
                or not math.isfinite(now) or now < 0):
            raise SupervisionUnavailable('Linux boot/monotonic clock is unavailable')
        return boot, now

    def _within_deadline(self, row):
        boot, now = self._clock_sample()
        try:
            started = json.loads(row['clock_json'])
            start = started['started_monotonic']
            deadline = row['deadline']
            if (started['boot_id'] != boot or isinstance(start, bool)
                    or not isinstance(start, (int, float)) or not math.isfinite(start)
                    or not isinstance(deadline, (int, float)) or not math.isfinite(deadline)
                    or start < 0 or deadline <= start or now < start):
                raise ValueError('boot changed or monotonic time regressed')
        except (KeyError, TypeError, ValueError) as exc:
            raise SupervisionUnavailable('persistent boot/monotonic identity is uncertain') from exc
        return now < deadline

    def _row(self, item):
        frozen = _validated(item)
        job = self.ledger.status(item['job_id'])
        if job['frozen_json'] != frozen:
            raise JobConflict('supervisor execution identity changed')
        with closing(self.ledger._connect()) as db:
            columns = {entry[1] for entry in db.execute('PRAGMA table_info(supervisor_jobs)')}
            if not {'clock_json', 'held_sha256'}.issubset(columns):
                raise SupervisionUnavailable('supervisor schema needs explicit reviewed installation/migration')
            row = db.execute('SELECT * FROM supervisor_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
        if row and (row['frozen_json'] != frozen or row['config_json'] != self._config()):
            raise JobConflict('persistent supervisor execution configuration changed')
        return job, dict(row) if row else None

    def _unknown(self, item):
        job = self.ledger.status(item['job_id'])
        if job['state'] in ('reserved', 'running'):
            try:
                self.ledger.mark_unknown(item['job_id'], item['claim_id'])
            except JobConflict:
                if self.ledger.status(item['job_id'])['state'] != 'unknown':
                    raise

    def launch(self, item, argv, *, timeout_seconds, cwd=None, env=None, stderr=None):
        frozen = _validated(item)
        if (not isinstance(argv, (list, tuple)) or not argv
                or any(not isinstance(arg, str) or not arg or '\0' in arg for arg in argv)
                or isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
                or not math.isfinite(timeout_seconds) or timeout_seconds <= 0):
            raise ValueError('fixed argv and positive finite timeout required')
        job, row = self._row(item)
        if job['state'] != 'reserved' or row:
            if row is None and job['state'] != 'finished':
                self._unknown(item)
            elif row and job['state'] in ('reserved', 'running'):
                try:
                    if not self._within_deadline(row):
                        self._unknown(item)
                except (OSError, SupervisionUnavailable):
                    self._unknown(item)
            return dict(started=False, state=self.ledger.status(item['job_id'])['state'])
        # Atomically claim the one permitted launch BEFORE any process creation.
        try:
            boot, started = self._clock_sample()
        except (OSError, SupervisionUnavailable):
            self._unknown(item)
            raise
        deadline = started + timeout_seconds
        runtime = _json(dict(boot_id=boot, started_monotonic=started))
        with closing(self.ledger._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT state FROM executor_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
            if current['state'] != 'reserved':
                return dict(started=False, state=current['state'])
            db.execute('INSERT INTO supervisor_jobs '
                       '(job_id,frozen_json,config_json,deadline,clock_json) VALUES(?,?,?,?,?)',
                       (item['job_id'], frozen, self._config(), deadline, runtime))
            db.execute("UPDATE executor_jobs SET state='running',updated_at_ns=? WHERE job_id=?",
                       (time.time_ns(), item['job_id']))
        scope = None
        try:
            scope = LinuxScope(self.cgroup_parent, item['job_id'],
                               worker_uid=self.worker_uid, worker_gid=self.worker_gid)
            self._scopes[item['job_id']] = scope
            initial = scope.snapshot()
            # Preserve the original scope interface for callers that do not
            # explicitly request a diagnostic stream.
            options = {} if stderr is None else {'stderr': stderr}
            process = scope.launch(argv, cwd=cwd, env=env, **options)
            if process['boot_id'] != initial['boot_id'] or initial['boot_id'] != boot:
                raise SupervisionUnavailable('boot changed before worker launch')
            # Commit the real kernel identities while the gate is CLOSED.
            # An exit after sending permission but before the next transaction
            # commits must not roll these identities back with that transaction.
            with closing(self.ledger._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE supervisor_jobs SET process_json=?,scope_json=? WHERE job_id=?',
                           (_json(process), _json(initial), item['job_id']))
            with closing(self.ledger._connect()) as db, db:
                # This MUST be the main ledger's write lock, not a Python lock
                # or an unrelated sidecar lock. Every RemoteJobs transition,
                # including an external mark_unknown(), uses this same lock.
                # No writer can commit uncertainty between this state check
                # and sending the one-byte business execution authorization.
                db.execute('BEGIN IMMEDIATE')
                current = db.execute('SELECT state FROM executor_jobs WHERE job_id=?',
                                     (item['job_id'],)).fetchone()
                if (not self._within_deadline(dict(clock_json=runtime, deadline=deadline))
                        or current['state'] != 'running'):
                    raise SupervisionUnavailable('launch intent expired or became uncertain before gate release')
                if self._requires_business_gate:
                    try:
                        from task_generation_fence import check_business_gate
                        checker = self.business_gate_check
                        if checker is not check_business_gate or checker(db, item) is not True:
                            raise ValueError('the fixed business gate check did not approve')
                    except Exception as exc:
                        raise SupervisionUnavailable('the business gate was not authorized') from exc
                scope.release_gate()
            return dict(started=True, state='running', process=process)
        except BaseException:
            if scope is not None and getattr(scope, 'gate', None) is not None:
                os.close(scope.gate)
                scope.gate = None
            self._unknown(item)
            raise

    def _scope(self, item):
        if item['job_id'] not in self._scopes:
            self._scopes[item['job_id']] = LinuxScope.reopen(
                self.cgroup_parent, item['job_id'], worker_uid=self.worker_uid, worker_gid=self.worker_gid)
        return self._scopes[item['job_id']]

    def _receipt(self, item, receipt):
        if not isinstance(receipt, dict):
            raise JobConflict('complete validated receipt required, not a digest or exit code')
        encoded = _json(receipt)
        detached = json.loads(encoded)
        if (detached.get('schema') != 'paperflow-postprocessing-receipt/1'
                or detached.get('identity') != item['identity']
                or detached.get('claim_id') != item['claim_id']
                or detached.get('manifest_sha256') != item['manifest_sha256']
                or detached.get('validation_status') != 'passed'):
            raise JobConflict('receipt does not belong to this execution')
        frozen_item = copy.deepcopy(item)
        if (self.receipt_validator(frozen_item, detached) is not True
                or _json(detached) != encoded or frozen_item != item):
            raise JobConflict('actual receipt/artifact verification failed')
        return encoded, hashlib.sha256(encoded.encode('utf8')).hexdigest()

    def _finish_held(self, item, row, encoded, digest, initial):
        """The final proof and release use the SAME main-ledger write lock.

        Do not use RemoteJobs.finish here: its callback precedes acquisition of
        the transaction lock, so a lock wait could invalidate its deadline.
        Business artifact validation has already completed outside this lock.
        """
        error = None
        with closing(self.ledger._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            job = db.execute('SELECT * FROM executor_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
            held = db.execute('SELECT * FROM supervisor_jobs WHERE job_id=?', (item['job_id'],)).fetchone()
            if (not job or not held or job['claim_id'] != item['claim_id']
                    or job['frozen_json'] != _validated(item) or held['frozen_json'] != job['frozen_json']
                    or held['config_json'] != self._config() or held['receipt_json'] != encoded
                    or held['held_sha256'] != digest or any(held[key] != row[key]
                        for key in ('clock_json', 'deadline', 'process_json', 'scope_json'))):
                raise JobConflict('held execution identity or verified receipt changed')
            if job['state'] == 'finished' and job['receipt_sha256'] == digest:
                return  # A competing confirmer already committed the same proof.
            if job['state'] != 'running':
                raise JobConflict('only the same running execution may finish')
            try:
                if not self._within_deadline(held):
                    error = JobConflict('execution expired before final write lock')
                else:
                    latest = self._scope(item).snapshot()
                    if latest['boot_id'] != initial['boot_id'] or latest['scope_id'] != initial['scope_id']:
                        raise SupervisionUnavailable('final cgroup/boot identity changed')
                    if latest['populated'] is not False:
                        raise JobConflict('controlled descendants are still running')
                    # Kernel inspection itself can cross the deadline. Recheck
                    # immediately before UPDATE while still holding this lock.
                    if not self._within_deadline(held):
                        error = JobConflict('execution expired during final kernel inspection')
            except (OSError, SupervisionUnavailable) as exc:
                error = exc
            if error is None:
                db.execute("UPDATE executor_jobs SET state='finished',receipt_sha256=?,updated_at_ns=? "
                           'WHERE job_id=?', (digest, time.time_ns(), item['job_id']))
            else:
                db.execute("UPDATE executor_jobs SET state='unknown',receipt_sha256=NULL,updated_at_ns=? "
                           'WHERE job_id=?', (time.time_ns(), item['job_id']))
        if error is not None:
            # Raise AFTER commit; rollback must not erase durable uncertainty.
            raise error

    def _observation_fresh(self, observation):
        try:
            boot, now = self._clock_sample()
            observed = observation['observed_monotonic']
            expires = observation['valid_until_monotonic']
            return (observation['observation_boot_id'] == boot
                    and type(observed) in (int, float) and type(expires) in (int, float)
                    and math.isfinite(observed) and math.isfinite(expires)
                    and 0 <= observed <= now < expires)
        except (KeyError, OSError, SupervisionUnavailable):
            return False

    def poll(self, item, *, receipt=None, freshness_seconds=5):
        if (isinstance(freshness_seconds, bool) or not isinstance(freshness_seconds, (int, float))
                or not math.isfinite(freshness_seconds) or freshness_seconds <= 0):
            raise ValueError('positive finite freshness interval required')
        job, row = self._row(item)
        now, alive, leader_alive, detail = self.clock(), None, None, 'not_observed'
        try:
            observation_boot, observation_mono = self._clock_sample()
        except (OSError, SupervisionUnavailable):
            observation_boot, observation_mono = None, None
        valid_clock, in_time = True, True
        if row:
            try:
                in_time = self._within_deadline(row)
            except (OSError, SupervisionUnavailable):
                self._unknown(item)
                valid_clock, detail = False, 'server_clock_unknown'
        if not valid_clock:
            pass  # Never probe or release against another/unknown boot.
        elif row is None:
            if job['state'] == 'running':
                self._unknown(item)
            detail = 'not_started' if job['state'] == 'reserved' else 'launch_identity_unconfirmed'
        elif row['scope_json'] is None and row['process_json'] is None:
            # A committed launch intention precedes kernel process creation.
            # A concurrent observer must not turn ordinary preparation into
            # failure. A crash here still cannot retry and keeps the slot;
            # timeout eventually makes its unresolved outcome durable.
            detail = 'launch_preparing'
            if job['state'] == 'running' and not in_time:
                self._unknown(item)
                detail = 'execution_timeout'
        elif not row['scope_json'] or not row['process_json']:
            self._unknown(item)
            detail = 'launch_identity_unconfirmed'
        else:
            try:
                initial = json.loads(row['scope_json'])
                observed = self._scope(item).snapshot()
                if (observed['boot_id'] != initial['boot_id']
                        or observed['scope_id'] != initial['scope_id']):
                    raise SupervisionUnavailable('server boot or cgroup identity changed')
                leader_alive = self._scope(item).leader_alive(json.loads(row['process_json']))
                alive, detail = observed['populated'], 'cgroup_observed'
            except (OSError, ValueError, SupervisionUnavailable):
                self._unknown(item)
                detail = 'server_activity_unknown'
            try:
                in_time = self._within_deadline(row)
            except (OSError, SupervisionUnavailable):
                self._unknown(item)
                in_time, alive, leader_alive, detail = False, None, None, 'server_clock_unknown'
            if job['state'] == 'running' and not in_time:
                self._unknown(item)
                detail = 'execution_timeout'
            if (alive is False and receipt is not None
                    and self.ledger.status(item['job_id'])['state'] == 'running'):
                encoded, digest = self._receipt(item, receipt)
                # Hold the exact verified receipt durably BEFORE slot release.
                # A crash between these writes retains the occupied job.
                with closing(self.ledger._connect()) as db, db:
                    updated = db.execute('UPDATE supervisor_jobs SET receipt_json=?,held_sha256=? WHERE job_id=? '
                        'AND ((receipt_json IS NULL AND held_sha256 IS NULL) '
                        'OR (receipt_json=? AND held_sha256=?))',
                        (encoded, digest, item['job_id'], encoded, digest)).rowcount
                    if updated != 1:
                        raise JobConflict('held verified receipt changed')
                try:
                    self._finish_held(item, row, encoded, digest, initial)
                except (OSError, SupervisionUnavailable):
                    self._unknown(item)
                    alive, detail = None, 'server_activity_unknown'
        result = dict(state=self.ledger.status(item['job_id'])['state'],
                      server_alive=alive, leader_alive=leader_alive,
                      observed_at=now, valid_until=now + freshness_seconds,
                      observation_boot_id=observation_boot, observed_monotonic=observation_mono,
                      valid_until_monotonic=(observation_mono + freshness_seconds
                                             if observation_mono is not None else None),
                      fresh=True, detail=detail)
        if not self._observation_fresh(result):
            result.update(server_alive=None, leader_alive=None, fresh=False)
        if row:
            with closing(self.ledger._connect()) as db, db:
                db.execute('UPDATE supervisor_jobs SET observation_json=? WHERE job_id=?',
                           (_json(result), item['job_id']))
        return result

    def held_receipt(self, item):
        """Return durable receipt bytes only for an independently finished job."""
        job, row = self._row(item)
        if job['state'] != 'finished':
            return None
        if (not row or not row['receipt_json'] or row['held_sha256'] != job['receipt_sha256']
                or hashlib.sha256(row['receipt_json'].encode('utf8')).hexdigest() != job['receipt_sha256']):
            raise JobConflict('finished job has no matching held receipt')
        return dict(receipt_json=row['receipt_json'], receipt_sha256=job['receipt_sha256'])

    def recover_held_receipt(self, item, *, freshness_seconds=5):
        """Recheck ONLY previously held bytes; never launch/rebuild/retry work.

        The injected validator must re-read existing business evidence without
        regenerating outputs. Unknown is terminal for this recovery operation.
        Only ledger state is written, after fresh boot/scope/evidence checks.
        """
        job, row = self._row(item)
        if job['state'] == 'unknown':
            return self.last_observation(item)
        if job['state'] == 'finished':
            self.held_receipt(item)  # Independently check durable byte identity.
            return self.last_observation(item)
        if (job['state'] != 'running' or not row or not row['receipt_json']
                or not row['held_sha256'] or hashlib.sha256(
                    row['receipt_json'].encode('utf8')).hexdigest() != row['held_sha256']):
            raise JobConflict('no matching previously held verified receipt')
        try:
            receipt = json.loads(row['receipt_json'])
            if _json(receipt) != row['receipt_json']:
                raise ValueError('held receipt is not canonical')
        except (TypeError, ValueError) as exc:
            raise JobConflict('held receipt bytes are invalid') from exc
        return self.poll(item, receipt=receipt, freshness_seconds=freshness_seconds)

    def last_observation(self, item):
        job, row = self._row(item)
        clock_known = True
        if row:
            try:
                if not self._within_deadline(row) and job['state'] == 'running':
                    self._unknown(item)
            except (OSError, SupervisionUnavailable):
                self._unknown(item)
                clock_known = False
            job = self.ledger.status(item['job_id'])
        result = json.loads(row['observation_json']) if row and row['observation_json'] else dict(
            state=job['state'], server_alive=None, leader_alive=None, observed_at=None, valid_until=None,
            fresh=False, detail='not_observed')
        result['state'] = job['state']
        if not clock_known or not self._observation_fresh(result):
            result.update(server_alive=None, leader_alive=None, fresh=False)
        return result


def _gated_exec(fd, uid, gid, argv):
    """Internal launcher: EOF on supervisor death can never execute business argv."""
    try:
        if os.read(fd, 1) != b'1':
            return 125
    finally:
        os.close(fd)
    if os.geteuid() == 0:
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)
    if os.geteuid() != uid or os.getegid() != gid:
        return 126
    os.execvpe(argv[0], argv, os.environ)


if __name__ == '__main__':
    if len(sys.argv) < 6 or sys.argv[1] != '--gate' or sys.platform != 'linux':
        raise SystemExit('internal gated Linux worker only')
    raise SystemExit(_gated_exec(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5:]))
