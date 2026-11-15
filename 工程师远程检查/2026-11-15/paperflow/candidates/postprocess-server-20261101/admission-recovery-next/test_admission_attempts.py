"""Crash-boundary tests using synthetic tickets and a disposable host journal."""
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import threading
from uuid import uuid4, uuid5

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'tests'))
from test_remote_coordinator import sample  # original synthetic Prefect/control fixture
from remote_coordinator import NAMESPACE, RemoteSubmissionJournal
from admission_attempts import AttemptConflict, AdmissionAttempts


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


@pytest.fixture
def context(sample, tmp_path):
    sample['run']()
    job_id = str(uuid5(NAMESPACE, sample['identity']['task_run_id']))
    original = sample['journal']
    transfer, intent = original.get_transfer(job_id), original.get(job_id)
    journal = RemoteSubmissionJournal(tmp_path / 'isolated-attempt-journal.sqlite3',
        control_database=sample['database'], admission_gate=sample['gate'], journal_id=original.journal_id)
    journal.initialize()
    raw = encode(transfer)
    journal._record_transfer(transfer, raw, hashlib.sha256(raw.encode('utf8')).hexdigest())
    attempts = AdmissionAttempts(journal)
    attempts.install()
    preflight = dict(preflight_id=str(uuid4()), preflight_sha256='f' * 64)
    return dict(journal=journal, attempts=attempts, intent=intent, preflight=preflight,
                job=intent['job'], server=intent['server_identity'])


def start(ctx):
    raw = encode(ctx['intent'])
    return ctx['attempts'].record_initial(ctx['intent'], raw, hashlib.sha256(raw.encode('utf8')).hexdigest(),
                                          ctx['preflight'])


def busy(ctx, attempt, reason='capacity_full'):
    return dict(job=copy.deepcopy(ctx['job']), server_identity=copy.deepcopy(ctx['server']),
                accepted=False, reason=reason, this_attempt_started=False,
                attempt_number=attempt['number'], attempt_token=attempt['token'])


def test_crash_after_send_before_reply_is_unknown_and_cannot_restart(context):
    attempt = start(context)
    assert attempt['number'] == 1
    assert context['attempts'].status(context['job']['job_id'])['state'] == 'pending'
    with pytest.raises(AttemptConflict):
        context['attempts'].begin_next(context['job']['job_id'])


@pytest.mark.parametrize('reason', ['capacity_full', 'admission_busy'])
def test_durable_authenticated_no_start_can_resume_same_job_once(context, reason):
    first = start(context)
    context['attempts'].record_response(context['job']['job_id'], first['number'], first['token'], busy(context, first, reason))
    restored = AdmissionAttempts(context['journal'])
    assert restored.status(context['job']['job_id'])['state'] == 'no_start'
    second = restored.begin_next(context['job']['job_id'])
    assert second['number'] == 2 and second['preflight'] == context['preflight']
    assert second['job'] == context['job'] and second['server_identity'] == context['server']
    with pytest.raises(AttemptConflict):
        restored.begin_next(context['job']['job_id'])


@pytest.mark.parametrize('mutation', ['server', 'job', 'generation_type', 'reason', 'started', 'extra'])
def test_unmatched_no_start_reply_cannot_authorize_resume(context, mutation):
    attempt = start(context)
    response = busy(context, attempt)
    if mutation == 'server': response['server_identity']['ledger_id'] = str(uuid4())
    if mutation == 'job': response['job']['claim_id'] = str(uuid4())
    if mutation == 'generation_type': response['job']['generation'] = float(response['job']['generation'])
    if mutation == 'reason': response['reason'] = 'network_timeout'
    if mutation == 'started': response['this_attempt_started'] = 0
    if mutation == 'extra': response['unexpected'] = True
    with pytest.raises(AttemptConflict):
        context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], response)
    with pytest.raises(AttemptConflict):
        context['attempts'].begin_next(context['job']['job_id'])


def test_accepted_response_never_authorizes_another_start(context):
    attempt = start(context)
    response = dict(job=context['job'], server_identity=context['server'], accepted=True,
                    attempt_number=attempt['number'], attempt_token=attempt['token'])
    context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], response)
    assert context['attempts'].status(context['job']['job_id'])['state'] == 'accepted'
    with pytest.raises(AttemptConflict):
        context['attempts'].begin_next(context['job']['job_id'])


def test_parallel_resume_can_create_only_one_new_attempt(context):
    attempt = start(context)
    context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], busy(context, attempt))
    def resume(_):
        try: return context['attempts'].begin_next(context['job']['job_id'])['number']
        except AttemptConflict: return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(resume, range(8))).count(2) == 1


def test_legacy_intent_without_attempt_evidence_is_never_resumed(context):
    journal = context['journal']
    intent = context['intent']
    raw = encode(intent)
    journal._record_start(intent, raw, hashlib.sha256(raw.encode('utf8')).hexdigest())
    with pytest.raises(AttemptConflict):
        context['attempts'].begin_next(context['job']['job_id'])
    with pytest.raises(AttemptConflict):
        start(context)


def test_initial_intent_and_attempt_are_not_partially_written(context):
    bad = dict(context['preflight'], preflight_sha256='bad')
    raw = encode(context['intent'])
    with pytest.raises(AttemptConflict):
        context['attempts'].record_initial(context['intent'], raw, hashlib.sha256(raw.encode()).hexdigest(), bad)
    assert context['journal'].get(context['job']['job_id']) is None
    with sqlite3.connect(context['journal'].database) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_admission_attempts').fetchone() == (0,)


def test_changed_durable_busy_reply_never_permits_resume(context):
    attempt = start(context)
    context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], busy(context, attempt))
    with sqlite3.connect(context['journal'].database) as db:
        db.execute("UPDATE remote_admission_attempts SET response_json='{}' WHERE job_id=?",
                   (context['job']['job_id'],))
    with pytest.raises(AttemptConflict):
        context['attempts'].begin_next(context['job']['job_id'])


def test_earlier_pending_reply_cannot_be_replayed_after_new_attempt(context):
    attempt = start(context)
    context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], busy(context, attempt))
    context['attempts'].begin_next(context['job']['job_id'])
    with pytest.raises(AttemptConflict):
        context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], busy(context, attempt))


def test_missing_transfer_intent_blocks_attempt_even_when_start_record_exists(context):
    attempt = start(context)
    context['attempts'].record_response(context['job']['job_id'], attempt['number'], attempt['token'], busy(context, attempt))
    with sqlite3.connect(context['journal'].database) as db:
        db.execute('DELETE FROM remote_transfer_intents WHERE job_id=?', (context['job']['job_id'],))
    with pytest.raises(AttemptConflict):
        context['attempts'].begin_next(context['job']['job_id'])


def test_changed_transfer_identity_blocks_the_initial_attempt(context):
    with sqlite3.connect(context['journal'].database) as db:
        db.execute('UPDATE remote_transfer_intents SET claim_id=? WHERE job_id=?',
                   (str(uuid4()), context['job']['job_id']))
    with pytest.raises(AttemptConflict):
        start(context)
    assert context['journal'].get(context['job']['job_id']) is None


def test_snapshot_json_boolean_cannot_be_replaced_with_equal_number(context):
    intent = copy.deepcopy(context['intent'])
    assert intent['snapshot']['payload']['synthetic'] is True
    intent['snapshot']['payload']['synthetic'] = 1
    intent['staging']['payload']['synthetic'] = 1
    raw = encode(intent)
    with pytest.raises(AttemptConflict):
        context['attempts'].record_initial(intent, raw, hashlib.sha256(raw.encode('utf8')).hexdigest(),
                                           context['preflight'])


def test_old_no_start_reply_cannot_be_relabelled_as_a_new_attempt(context):
    first = start(context)
    previous = busy(context, first)
    context['attempts'].record_response(context['job']['job_id'], first['number'], first['token'], previous)
    second = context['attempts'].begin_next(context['job']['job_id'])
    with pytest.raises(AttemptConflict):
        context['attempts'].record_response(context['job']['job_id'], second['number'], second['token'], previous)


@pytest.mark.parametrize('operation', ['record_response', 'begin_next'])
def test_slow_history_audit_releases_db_before_other_ticket_write(context, monkeypatch, operation):
    import admission_attempts as attempts_module

    first = start(context)
    if operation == 'begin_next':
        context['attempts'].record_response(context['job']['job_id'], first['number'], first['token'], busy(context, first))
    with sqlite3.connect(context['journal'].database) as db:
        assert db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'delete'
        db.execute('CREATE TABLE unrelated_ticket_evidence(id INTEGER PRIMARY KEY)')
    auditing, release = threading.Event(), threading.Event()
    original = attempts_module._history
    def slow_audit(*args):
        auditing.set()
        assert release.wait(timeout=5)
        return original(*args)
    monkeypatch.setattr(attempts_module, '_history', slow_audit)
    def operate():
        if operation == 'begin_next':
            return context['attempts'].begin_next(context['job']['job_id'])
        return context['attempts'].record_response(context['job']['job_id'], first['number'], first['token'], busy(context, first))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(operate)
        assert auditing.wait(timeout=5)
        try:
            with sqlite3.connect(context['journal'].database, timeout=.05) as db:
                db.execute('INSERT INTO unrelated_ticket_evidence VALUES(1)')
        finally:
            release.set()
        # A concurrent commit invalidates the audited snapshot, so this
        # attempt may be re-audited but must not proceed on stale evidence.
        with pytest.raises(AttemptConflict):
            future.result(timeout=5)


def test_history_change_during_outside_transaction_audit_fails_closed(context, monkeypatch):
    import admission_attempts as attempts_module

    first = start(context)
    context['attempts'].record_response(context['job']['job_id'], first['number'], first['token'], busy(context, first))
    auditing, release = threading.Event(), threading.Event()
    original = attempts_module._history
    def slow_audit(*args):
        auditing.set()
        assert release.wait(timeout=5)
        return original(*args)
    monkeypatch.setattr(attempts_module, '_history', slow_audit)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(context['attempts'].begin_next, context['job']['job_id'])
        assert auditing.wait(timeout=5)
        try:
            with sqlite3.connect(context['journal'].database, timeout=.05) as db:
                db.execute("UPDATE remote_admission_attempts SET response_json='{}' WHERE job_id=?",
                           (context['job']['job_id'],))
        finally:
            release.set()
        with pytest.raises(AttemptConflict):
            future.result(timeout=5)


def test_status_does_not_report_stale_no_start_after_attempt_accepted(context, monkeypatch):
    import admission_attempts as attempts_module

    first = start(context)
    context['attempts'].record_response(context['job']['job_id'], first['number'], first['token'], busy(context, first))
    auditing, release = threading.Event(), threading.Event()
    original = attempts_module._history
    first_caller = [None]
    def slow_first_audit(*args):
        if threading.get_ident() == first_caller[0]:
            auditing.set()
            assert release.wait(timeout=5)
        return original(*args)
    monkeypatch.setattr(attempts_module, '_history', slow_first_audit)
    def observe():
        first_caller[0] = threading.get_ident()
        return context['attempts'].status(context['job']['job_id'])
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(observe)
        assert auditing.wait(timeout=5)
        try:
            second = context['attempts'].begin_next(context['job']['job_id'])
            accepted = dict(job=context['job'], server_identity=context['server'], accepted=True,
                            attempt_number=second['number'], attempt_token=second['token'])
            context['attempts'].record_response(context['job']['job_id'], second['number'], second['token'], accepted)
        finally:
            release.set()
        with pytest.raises(AttemptConflict):
            future.result(timeout=5)
    assert context['attempts'].status(context['job']['job_id'])['state'] == 'accepted'
