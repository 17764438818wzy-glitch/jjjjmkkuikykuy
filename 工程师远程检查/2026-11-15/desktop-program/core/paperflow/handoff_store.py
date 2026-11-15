"""Opt-in stage-1 ledger. SQL-only transactions; registration never grants admission."""
from __future__ import annotations

import json
import sqlite3
import time


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def protects_remote(kind,owner,code):
    # Shared repair inputs protect every specialty; safety ownership is local to one target.
    return kind in ('repair','repair_target') or (kind=='safety_specialty' and json.loads(owner)[1]==code)


def legacy_write_guard(c,source_id,specialty_code):
    # Source identity survives a new manifest, revision and source-content task.
    if c.execute('SELECT 1 FROM legacy_pending_scopes WHERE source_id=? AND specialty_code=?',
                 (source_id,specialty_code)).fetchone():
        raise ValueError('legacy scope pending reconciliation')


class RegistrationChanged(ValueError):
    """Re-read and verify the registered package outside the write transaction."""


SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS legacy_receipt_archive (
        identity TEXT PRIMARY KEY,sha256 TEXT NOT NULL,data TEXT NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS legacy_runs (
        snapshot_id TEXT NOT NULL,task_id TEXT NOT NULL,original TEXT NOT NULL,
        state TEXT NOT NULL,result TEXT NOT NULL,PRIMARY KEY(snapshot_id,task_id))''',
    '''CREATE TABLE IF NOT EXISTS legacy_targets (
        manifest_sha256 TEXT NOT NULL,specialty_code TEXT NOT NULL,snapshot_id TEXT NOT NULL,
        receipt TEXT NOT NULL,state TEXT NOT NULL,evidence TEXT NOT NULL,
        PRIMARY KEY(manifest_sha256,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS package_stage_dependencies (
        manifest_sha256 TEXT NOT NULL,stage TEXT NOT NULL,dependency TEXT NOT NULL,
        PRIMARY KEY(manifest_sha256,stage))''',
    '''CREATE TABLE IF NOT EXISTS publication_intents (
        thread_id TEXT NOT NULL, input_fingerprint TEXT NOT NULL,
        task_id TEXT NOT NULL, data TEXT NOT NULL,
        manifest_sha256 TEXT, manifest_bytes BLOB,
        lifecycle TEXT NOT NULL DEFAULT 'open'
            CHECK(lifecycle IN ('open','handed_off','deleting')),
        PRIMARY KEY(thread_id,input_fingerprint),
        CHECK((manifest_sha256 IS NULL) = (manifest_bytes IS NULL)))''',
    '''CREATE INDEX IF NOT EXISTS intent_article ON publication_intents(task_id)''',
    '''CREATE TABLE IF NOT EXISTS delivery_packages (
        manifest_sha256 TEXT PRIMARY KEY, task_id TEXT NOT NULL,
        identity_json TEXT NOT NULL, manifest_bytes BLOB NOT NULL,
        revision TEXT NOT NULL, package_dir TEXT NOT NULL,
        targets_json TEXT NOT NULL, mapping_version TEXT NOT NULL,
        result_json TEXT NOT NULL,
        lifecycle TEXT NOT NULL DEFAULT 'retained'
            CHECK(lifecycle IN ('retained','deleting')))''',
    '''CREATE VIEW IF NOT EXISTS legacy_pending_scopes AS
        SELECT json_extract(p.identity_json,'$.source_id') AS source_id,
               l.specialty_code,l.manifest_sha256
        FROM legacy_targets l JOIN delivery_packages p ON p.manifest_sha256=l.manifest_sha256
        WHERE l.state NOT IN ('active','reconciled')
        UNION ALL
        SELECT json_extract(a.data,'$.identity.source_id'),t.value,
               json_extract(a.data,'$.identity.manifest_sha256')
        FROM legacy_receipt_archive a,json_each(a.data,'$.targets') t
        WHERE json_extract(a.data,'$.state')='pending_reconcile' ''',
    '''CREATE TABLE IF NOT EXISTS specialty_tasks (
        manifest_sha256 TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        specialty_code TEXT NOT NULL, mapping_json TEXT NOT NULL,
        mapping_version TEXT NOT NULL, status TEXT NOT NULL
            CHECK(status IN ('pending','mapping_pending','superseded','frozen')),
        mapping_issue TEXT, expectation_outcome TEXT NOT NULL,
        PRIMARY KEY(manifest_sha256,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS target_versions (
        task_id TEXT NOT NULL, specialty_code TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        generation INTEGER NOT NULL CHECK(generation > 0),
        PRIMARY KEY(task_id,specialty_code))''',
    '''CREATE TABLE IF NOT EXISTS package_references (
        package_sha256 TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL,
        resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL,
        PRIMARY KEY(owner_kind,owner_id,resource_kind,resource_id))''',
    '''CREATE TABLE IF NOT EXISTS handoff_results (
        thread_id TEXT NOT NULL, input_fingerprint TEXT NOT NULL,
        manifest_sha256 TEXT NOT NULL REFERENCES delivery_packages(manifest_sha256),
        result_json TEXT NOT NULL,
        PRIMARY KEY(thread_id,input_fingerprint),
        FOREIGN KEY(thread_id,input_fingerprint)
            REFERENCES publication_intents(thread_id,input_fingerprint))''',
    # Freeze/deletion are preconditions only; their control workflows arrive later.
    '''CREATE TABLE IF NOT EXISTS package_freezes (
        manifest_sha256 TEXT NOT NULL, specialty_code TEXT NOT NULL, reason TEXT NOT NULL,
        PRIMARY KEY(manifest_sha256,specialty_code))''',
)


class HandoffLedger:
    def __init__(self, store):
        self.store = store
        def initialize(c):
            for sql in SCHEMA:c.execute(sql)
            # CREATE IF NOT EXISTS alone leaves stage-8 ledgers on the old view.
            view=c.execute("SELECT sql FROM sqlite_master WHERE type='view' AND name='legacy_pending_scopes'").fetchone()[0]
            if 'legacy_receipt_archive' not in view:
                c.execute('DROP VIEW legacy_pending_scopes')
                c.execute(next(sql for sql in SCHEMA if 'CREATE VIEW' in sql))
        self._write(initialize)

    def _write(self, operation, *, still_valid=None, before_commit=None, deadline=None):
        """Retry the entire short SQL transaction, including all condition reads."""
        # Avoid dozens of manager IPC threads racing SQLite's single writer.
        # Queue outside the transaction and recheck admission after acquiring it.
        # Queue and SQLite share the previous retry window, below the IPC limit.
        now = time.monotonic()
        deadline = min(deadline, now + .7) if deadline is not None else now + .7
        if not self.store.write_lock.acquire(timeout=max(0, deadline - now)):
            exc = sqlite3.OperationalError('database writer queue busy')
            exc.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise exc
        try:
            return self._write_transaction(operation, deadline, still_valid=still_valid, before_commit=before_commit)
        finally:
            self.store.write_lock.release()

    def _write_transaction(self, operation, deadline, *, still_valid=None, before_commit=None):
        for attempt in range(5):
            if still_valid is not None and not still_valid():
                raise ValueError('transaction admission invalidated')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                exc = sqlite3.OperationalError('database writer queue busy')
                exc.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise exc
            try:
                with self.store.db(timeout=min(.1, remaining)) as c:
                    c.execute('PRAGMA foreign_keys=ON')
                    c.execute('BEGIN IMMEDIATE')
                    result = operation(c)
                    if before_commit is not None:before_commit()
                    return result
            except sqlite3.OperationalError as exc:
                code = getattr(exc, 'sqlite_errorcode', 0)
                if code & 255 != sqlite3.SQLITE_BUSY or attempt == 4:
                    raise
                time.sleep(min(0.02 * (attempt + 1), max(0, deadline - time.monotonic())))

    @staticmethod
    def _intent(c, thread_id, fingerprint):
        row = c.execute('SELECT * FROM publication_intents WHERE thread_id=? AND input_fingerprint=?',
                        (thread_id, fingerprint)).fetchone()
        if row is None:
            raise ValueError('unknown publication intent')
        return dict(json.loads(row['data']), lifecycle=row['lifecycle'],
                    manifest_sha256=row['manifest_sha256'], manifest_bytes=row['manifest_bytes'])

    def intent(self, thread_id, fingerprint):
        with self.store.db(timeout=0.1) as c:
            return self._intent(c, thread_id, fingerprint)

    @staticmethod
    def _retained(intent):
        if intent['lifecycle'] == 'deleting':
            raise ValueError('publication intent is deleting')

    def prepare(self, data):
        def write(c):
            from .cleanup_paths import check_path_reference
            existing=c.execute('SELECT 1 FROM publication_intents WHERE thread_id=? AND input_fingerprint=?',
                (data['thread_id'],data['input_fingerprint'])).fetchone()
            if not existing and len(data['target_snapshot']['targets'])>3:
                raise ValueError('new handoff must contain at most 3 specialties; return to round2 selection')
            for path in (data['material_dir'],data['package_dir'],*[d['directory'] for d in data.get('stage_dependencies',{}).values() if d.get('directory')]):
                check_path_reference(c,path)
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_runs'").fetchone() and c.execute(
                'SELECT 1 FROM cleanup_runs WHERE thread_id=?',(data['thread_id'],)).fetchone():raise ValueError('publication run closed')
            for row in c.execute('SELECT data FROM publication_intents WHERE task_id=?',
                                 (data['identity']['task_id'],)):
                if json.loads(row['data'])['identity'] != data['identity']:
                    raise ValueError('article identity conflict')
            c.execute('INSERT OR IGNORE INTO publication_intents(thread_id,input_fingerprint,task_id,data) '
                      'VALUES(?,?,?,?)', (data['thread_id'], data['input_fingerprint'],
                                         data['identity']['task_id'], canonical(data)))
            current = self._intent(c, data['thread_id'], data['input_fingerprint'])
            self._retained(current)
            # Revision/time/input location are pinned by the first successful intent.
            for key in ('identity', 'target_snapshot', 'mapping_version', 'expected_targets',
                        'material_files', 'sealed', 'processing_history', 'package_dir'):
                if current[key] != data[key]:
                    raise ValueError('publication intent conflict: ' + key)
            return current
        return self._write(write)

    def bind_manifest(self, thread_id, fingerprint, digest, manifest_bytes):
        """Persist exact bytes before rename. This is NOT a package registration."""
        def write(c):
            intent = self._intent(c, thread_id, fingerprint)
            self._retained(intent)
            if intent['manifest_sha256'] is not None:
                if (intent['manifest_sha256'], intent['manifest_bytes']) != (digest, manifest_bytes):
                    raise ValueError('publication manifest conflict')
            else:
                changed = c.execute('UPDATE publication_intents SET manifest_sha256=?,manifest_bytes=? '
                                    'WHERE thread_id=? AND input_fingerprint=? AND manifest_sha256 IS NULL',
                                    (digest, manifest_bytes, thread_id, fingerprint)).rowcount
                if changed != 1:
                    raise ValueError('publication manifest conflict')
        self._write(write)

    @staticmethod
    def _expectation(c, task_id, code):
        row = c.execute('SELECT manifest_sha256,generation FROM target_versions '
                        'WHERE task_id=? AND specialty_code=?', (task_id, code)).fetchone()
        return dict(row) if row else dict(manifest_sha256=None, generation=0)

    def expectations(self, task_id, codes):
        with self.store.db(timeout=0.1) as c:
            # One read snapshot for the complete requested set.
            c.execute('BEGIN')
            return {code: self._expectation(c, task_id, code) for code in codes}

    def deliveries(self, digest):
        with self.store.db(timeout=0.1) as c:
            return [dict(row, mapping=json.loads(row['mapping_json'])) for row in c.execute(
                'SELECT * FROM specialty_tasks WHERE manifest_sha256=? ORDER BY specialty_code', (digest,))]

    def registration(self, digest):
        with self.store.db(timeout=0.1) as c:
            row = c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',
                            (digest,)).fetchone()
            return dict(row) if row is not None else None

    @staticmethod
    def _reference(c, digest, kind, owner, resource_kind, resource):
        for candidate in {digest,resource} if resource_kind=='package' else {digest}:
            row=c.execute('SELECT lifecycle FROM delivery_packages WHERE manifest_sha256=?',(candidate,)).fetchone()
            if not row or row['lifecycle']=='deleting':raise ValueError('reference package missing or deleting')
            if c.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_items'").fetchone():
                for cleanup in c.execute("SELECT specialty_code FROM cleanup_items WHERE manifest_sha256=? AND kind='remote' "
                    "AND state IN ('deleting','pending_reconcile')",(candidate,)):
                    if protects_remote(kind,owner,cleanup['specialty_code']):
                        from .scheduling_policy import ActionDeferred
                        raise ActionDeferred('remote version deleting')
        if resource_kind!='package':
            from .cleanup_paths import check_path_reference
            check_path_reference(c,resource)
        c.execute('INSERT OR IGNORE INTO package_references VALUES(?,?,?,?,?)',
                  (digest, kind, owner, resource_kind, resource))

    def register(self, thread_id, fingerprint, validated, manifest_bytes, *, verified_registration):
        return self._write(lambda c: self.register_sql(c, thread_id, fingerprint, validated,
                           manifest_bytes, verified_registration=verified_registration))

    def register_sql(self, c, thread_id, fingerprint, validated, manifest_bytes, *, verified_registration, frozen_codes=(), maintenance_migration=None):
        """SQL-only body, reusable inside the controller's multi-scope transaction."""
        digest = validated['manifest_sha256']
        if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='structure_migrations'").fetchone():
            migration=c.execute('SELECT * FROM structure_migrations WHERE migration_id=?',(thread_id,)).fetchone()
            if migration:
                if maintenance_migration!=thread_id or migration['state']!='reserved' or migration['fingerprint']!=fingerprint:
                    raise ValueError('migration requires paused maintenance registration')
                from .structure_migration import StructureMigration
                run=json.loads(migration['data'])
                StructureMigration._paused(c,run['control']);StructureMigration._source(c,run)
                qualification=validated.get('embedding_qualification',{})
                if not qualification.get('admission_verified') or qualification.get('rule_fingerprint')!=run['policy_id']:
                    raise ValueError('migration qualification changed')
        intent = self._intent(c, thread_id, fingerprint)
        self._retained(intent)
        if (intent['manifest_sha256'], intent['manifest_bytes']) != (digest, manifest_bytes):
            raise ValueError('bound manifest conflict')
        existing = c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',
                             (digest,)).fetchone()
        if existing and existing['lifecycle'] == 'deleting':
            raise ValueError('package is deleting')
        # None means the caller checked that no package had been registered.
        # A concurrent first registration also requires outside-transaction verification.
        if (dict(existing) if existing is not None else None) != verified_registration:
            raise RegistrationChanged('registered package changed during verification')
        replay = c.execute('SELECT result_json FROM handoff_results '
                           'WHERE thread_id=? AND input_fingerprint=?', (thread_id, fingerprint)).fetchone()
        from .embedding_admission import qualification_summary
        qualification=validated.get('embedding_qualification') or qualification_summary(digest,None)
        if replay:
            old=json.loads(replay['result_json'])
            result=dict(old,embedding_qualification=qualification)
            if result!=old:
                c.execute('UPDATE handoff_results SET result_json=? WHERE thread_id=? AND input_fingerprint=?',
                          (canonical(result),thread_id,fingerprint))
            return result
        identity = intent['identity']
        if existing:
            if (existing['identity_json'] != canonical(identity)
                    or existing['manifest_bytes'] != manifest_bytes
                    or existing['targets_json'] != canonical(intent['target_snapshot'])
                    or existing['mapping_version'] != intent['mapping_version']):
                raise ValueError('existing package identity or target mapping conflict')
            # A second run handing off identical bytes confirms the original transaction.
            result = dict(json.loads(existing['result_json']),embedding_qualification=qualification)
        else:
            targets = intent['target_snapshot']['targets']
            result = dict(status='registered' if targets else 'awaiting_specialty',
                          manifest_sha256=digest, package_dir=intent['package_dir'],
                          routing_issues=intent['target_snapshot']['issues'], targets=[],embedding_qualification=qualification)
            c.execute('INSERT INTO delivery_packages(manifest_sha256,task_id,identity_json,manifest_bytes,'
                      'revision,package_dir,targets_json,mapping_version,result_json) VALUES(?,?,?,?,?,?,?,?,?)',
                      (digest, identity['task_id'], canonical(identity), manifest_bytes,
                       validated['manifest']['revision'], intent['package_dir'],
                       canonical(intent['target_snapshot']), intent['mapping_version'], canonical(result)))
            for target in targets:
                code = target['code']
                expected = intent['expected_targets'][code]
                current = self._expectation(c, identity['task_id'], code)
                frozen = c.execute('SELECT reason FROM package_freezes WHERE manifest_sha256=? '
                                   "AND specialty_code IN ('*',?)", (digest, code)).fetchone()
                if frozen or code in frozen_codes:
                    status, outcome = 'frozen', 'frozen'
                elif current != expected:
                    status, outcome = 'superseded', 'expectation_conflict'
                else:
                    status = 'mapping_pending' if target['issue'] else 'pending'
                    outcome = 'advanced'
                    if current['generation'] == 0:
                        changed = c.execute('INSERT OR IGNORE INTO target_versions VALUES(?,?,?,1)',
                                            (identity['task_id'], code, digest)).rowcount
                    else:
                        changed = c.execute('UPDATE target_versions SET manifest_sha256=?,generation=generation+1 '
                                            'WHERE task_id=? AND specialty_code=? AND manifest_sha256=? AND generation=?',
                                            (digest, identity['task_id'], code, current['manifest_sha256'],
                                             current['generation'])).rowcount
                    if changed != 1:
                        raise ValueError('expectation CAS conflict')
                c.execute('INSERT INTO specialty_tasks VALUES(?,?,?,?,?,?,?)',
                          (digest, code, canonical(target['mapping']), intent['mapping_version'],
                           status, target['issue'], outcome))
                self._reference(c, digest, 'delivery', canonical([digest, code]), 'package', digest)
                result['targets'].append(dict(code=code, status=status, expectation_outcome=outcome))
            c.execute('UPDATE delivery_packages SET result_json=? WHERE manifest_sha256=?',
                      (canonical(result), digest))
            c.execute('INSERT INTO events(at,task_id,kind,data) VALUES(?,?,?,?)',
                      (time.time(), identity['task_id'], 'package_registered', canonical(result)))
        owner = canonical([thread_id, fingerprint])
        self._reference(c, digest, 'recovery', owner, 'package', digest)
        self._reference(c, digest, 'recovery', owner, 'input_dir', intent['material_dir'])
        self._reference(c, digest, 'recovery', owner, 'published_dir', intent['package_dir'])
        for expected in intent['expected_targets'].values():
            predecessor = expected['manifest_sha256']
            if predecessor:
                deleting = c.execute('SELECT lifecycle FROM delivery_packages WHERE manifest_sha256=?',
                                     (predecessor,)).fetchone()
                if not deleting or deleting['lifecycle'] == 'deleting':
                    raise ValueError('predecessor missing or deleting')
                self._reference(c, digest, 'recovery', owner, 'package', predecessor)
        for stage,dependency in intent.get('stage_dependencies',{}).items():
            c.execute('INSERT OR IGNORE INTO package_stage_dependencies VALUES(?,?,?)',(digest,stage,canonical(dependency)))
            if dependency.get('directory'):
                self._reference(c,digest,'phase_dependency',canonical([digest,stage]),'phase_output',dependency['directory'])
        if intent.get('repair_source'):
            # Reuse this transaction; constructing a ledger here would open a nested writer.
            from .repair_store import RepairLedger
            RepairLedger(self.store,initialize=False).complete_sql(c,intent,result)
        c.execute('INSERT INTO handoff_results VALUES(?,?,?,?)',
                  (thread_id, fingerprint, digest, canonical(result)))
        c.execute("UPDATE publication_intents SET lifecycle='handed_off' WHERE thread_id=? AND input_fingerprint=?",
                  (thread_id, fingerprint))
        return result
