"""One-time server admission for a preverified, already-claimed folder.

This candidate owns no scheduler or queue. Its caller must supply the actual
job-bound staging verifier and fixed service launcher, neither of which may
execute model requests or upload to RAG.
"""
import copy
import hashlib
from pathlib import Path
import re
import subprocess

from remote_jobs import _validated, JobConflict, CapacityFull, AdmissionBusy, RemoteJobs


class AdmissionError(RuntimeError):
    pass


def start_once(job, *, ledger, server_identity, confirm_prepared, launch_service, _capacity_as_result=False):
    frozen = _validated(job)
    if (not isinstance(ledger, RemoteJobs) or not isinstance(server_identity, dict)
            or set(server_identity) != {'server_id', 'ledger_id'}
            or not isinstance(server_identity['server_id'], str) or not server_identity['server_id']
            or server_identity['ledger_id'] != ledger.ledger_id
            or not callable(confirm_prepared) or not callable(launch_service)):
        raise AdmissionError('pinned server, ledger, staging verifier and launcher are required')
    candidate = copy.deepcopy(job)
    if confirm_prepared(candidate) is not True or _validated(candidate) != frozen:
        raise AdmissionError('job-bound staged inputs or release are not verified')
    try:
        reservation = ledger.reserve(job)
    except (CapacityFull, AdmissionBusy) as exc:
        if not _capacity_as_result:
            raise
        return dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server_identity),
                    accepted=False, reason='capacity_full' if isinstance(exc, CapacityFull) else 'admission_busy',
                    this_attempt_started=False)
    if reservation['may_start'] is not True:
        return dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server_identity),
                    accepted=False, state=reservation['state'])
    try:
        candidate = copy.deepcopy(job)
        if launch_service(candidate) is not True or _validated(candidate) != frozen:
            raise AdmissionError('fixed service launch was not confirmed')
    except BaseException as exc:
        # A failed or lost service acknowledgement is never permission to retry.
        try:
            current = ledger.status(job['job_id'])
            if current['state'] in {'reserved', 'running'}:
                ledger.mark_unknown(job['job_id'], job['claim_id'])
        except Exception as record_error:
            exc.add_note('server unknown state could not be recorded: ' + type(record_error).__name__)
        raise AdmissionError('server service launch outcome is unknown') from exc
    return dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server_identity), accepted=True)


def start_with_capacity_result(job, *, ledger, server_identity, confirm_prepared, launch_service):
    """Report only a definite pre-reservation capacity rejection as busy.

    This candidate is not connected to the live service. Any launch/transport
    ambiguity still raises and must remain unknown; the original job is never
    declared safe to resend merely because an acknowledgement is absent.
    """
    return start_once(job, ledger=ledger, server_identity=server_identity,
                      confirm_prepared=confirm_prepared, launch_service=launch_service,
                      _capacity_as_result=True)


def launch_systemd(job, *, python, service_script, service_sha256, timeout_seconds=3):
    """Ask systemd once to launch the fixed script; no shell or model input."""
    _validated(job)
    python, service_script = Path(python), Path(service_script)
    if (not python.is_absolute() or not service_script.is_absolute()
            or not python.is_file() or not service_script.is_file()
            or not isinstance(service_sha256, str) or not re.fullmatch('[a-f0-9]{64}', service_sha256)
            or isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds <= 5):
        raise AdmissionError('fixed service executable or timeout is invalid')
    with service_script.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != service_sha256:
            raise AdmissionError('fixed service script changed')
    argv = ['/usr/bin/systemd-run', '--user', '--unit=paperflow-pp-' + job['job_id'],
            '--property=Delegate=yes', '--collect', '--no-block', str(python), '-B',
            str(service_script), job['job_id']]
    try:
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, timeout=timeout_seconds, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AdmissionError('systemd service launch could not be confirmed') from exc
    if result.returncode != 0:
        raise AdmissionError('systemd rejected the service launch')
    return True
