"""Create an inert server service with the existing eight-folder ceiling.

Run only inside a new private ``postprocess-service-*`` directory on the existing
server runtime base, with an already frozen and verified business release. The
ledger has the same eight-folder ceiling as the existing Prefect limit; creating
it does not demonstrate eight concurrent jobs. This creates no worker and changes
no existing queue or Prefect state. The first test must select one folder.

The native manifest records pinned known files only; it does not claim a full
future Python, Wine or operating-system dependency closure. The server account
and its administrator remain trusted. Output contains identities and hashes,
never private keys, passwords or paper contents.
"""
import hashlib
import json
import os
from pathlib import Path
import resource  # imported by the limited worker before its verification
import shutil
import sys
from uuid import uuid4

import server_job_service as service
import server_receipt_query  # expose the service's native imports to inventory
import server_rpc
from remote_jobs import RemoteJobs


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf8')


def write_once(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def main():
    if len(sys.argv) != 4 or os.name != 'posix':
        raise SystemExit('new private service directory, server base and frozen release are required')
    root, base, release = (Path(value).resolve(strict=True) for value in sys.argv[1:])
    if (root.parent != base or not root.name.startswith('postprocess-service-')
            or release.parent != base or not release.name.startswith('postprocess-release-')):
        raise ValueError('only a new dedicated service directory and frozen release are allowed')
    if root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
        raise ValueError('the service directory must be private to its server account')
    app = (root / 'app').resolve(strict=True)
    if app != Path(__file__).resolve().parent:
        raise ValueError('the installer must run from this service app directory')
    # Each explicitly named directory is new and private. Never reuse a prior
    # test's ledger, staging, jobs, receipt or unknown outcome.
    for name in ('jobs', 'staging'):
        (root / name).mkdir(mode=0o700)
    counter_path = base / 'tokenizer/server-counter-config.json'
    counter = json.loads(counter_path.read_bytes())
    cache = list((base / 'tokenizer/cache').iterdir())
    if len(cache) != 1 or not cache[0].is_file():
        raise ValueError('the offline tokenizer cache identity is ambiguous')
    roles = dict(python=sys.executable, service_script=str(app / 'server_job_service.py'),
        worker_script=str(app / 'server_wine_worker.py'),
        wine64=str(base / 'wine-runtime/usr/lib/wine/wine64'), counter_config=str(counter_path),
        model=counter['counter']['model_path'], libllama=counter['counter']['library_path'],
        cl100k=str(cache[0]), counter_env=str(Path(shutil.which('env', path='/usr/bin:/bin')).resolve(strict=True)),
        counter_runtime_manifest=str(Path(counter['counter']['runtime_manifest']).resolve(strict=True)))
    paths = {Path(value).resolve(strict=True) for value in roles.values()}
    paths.update(Path(value).resolve(strict=True) for value in service._mapped_executables())
    paths.update(path.resolve(strict=True) for path in app.glob('*.py'))
    paths.add((Path(sys.prefix) / 'pyvenv.cfg').resolve(strict=True))
    files = []
    for path in sorted(paths):
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        files.append(dict(path=str(path), bytes=path.stat().st_size, sha256=digest))
    inventory = dict(schema='paperflow-native-runtime/1', verification_scope='pinned-known-files',
        reviewed_dependency_closure=False, python_environment=dict(prefix=sys.prefix, base_prefix=sys.base_prefix),
        roles=roles, files=files)
    native_bytes = encode(inventory)
    native_path = root / 'native-known-files.json'
    write_once(native_path, native_bytes)
    ledger_id = str(uuid4())
    config = dict(schema='paperflow-job-service/1',
        server_identity=dict(server_id='paperflow-postprocessing-' + uuid4().hex, ledger_id=ledger_id),
        jobs_root=str(root / 'jobs'), staging_root=str(root / 'staging'), base=str(base),
        native_manifest=str(native_path), native_manifest_sha256=hashlib.sha256(native_bytes).hexdigest(),
        initialize_delegation=True, worker_uid=os.getuid(), worker_gid=os.getgid(),
        release=dict(code_root=str(release), code_manifest=str(release / 'source-code-manifest.json'),
            template=str(base / 'wine-test-prefix4'), template_manifest=str(base / 'wine-template-manifest-v1.json')),
        ledger=dict(database=str(root / 'remote-ledger.sqlite'), capacity=8),
        limits=dict(timeout_seconds=300, poll_seconds=.2, cleanup_seconds=20, disk_poll_seconds=5,
            memory_bytes=4 * 1024**3, pids=256, file_bytes=2 * 1024**3, open_files=1024,
            max_job_bytes=5 * 1024**3, min_free_bytes=20 * 1024**3))
    service._config(config)
    config_bytes = encode(config)
    digest = hashlib.sha256(config_bytes).hexdigest()
    write_once(app / 'server-job-service.json', config_bytes)
    write_once(app / 'server-job-service.sha256', digest.encode('ascii'))
    service.verify_native_runtime(config)
    ledger = RemoteJobs(config['ledger']['database'], capacity=8, ledger_id=ledger_id)
    ledger.initialize()
    service.initialize_service_state(config)
    print(json.dumps(dict(config_sha256=digest, server_identity=config['server_identity'],
        native_file_count=len(files), native_scope='pinned-known-files', capacity=8,
        business_work_started=False, production_dependency_closure_proved=False)))


if __name__ == '__main__':
    main()
