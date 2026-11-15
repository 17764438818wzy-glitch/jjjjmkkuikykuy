"""Create an isolated, disposable *synthetic test* service configuration.

This is not an installer or production native dependency audit.  Its inventory
records the files exercised by this one test, and cannot certify the complete
future Python, Wine, or operating-system dependency closure.
"""
import hashlib
import json
import os
from pathlib import Path
import resource  # the limited worker imports this extension before its verification
import shutil
import sys
from uuid import uuid4

import server_job_service as service
import server_receipt_query  # make its native imports visible to the inventory
import server_rpc
from remote_jobs import RemoteJobs


def write_once(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf8')


def main():
    if len(sys.argv) not in (3, 4):
        raise SystemExit('isolated test directory, existing runtime base and optional synthetic release required')
    root, base = map(Path, sys.argv[1:3])
    root, base = root.resolve(strict=True), base.resolve(strict=True)
    if not root.name.startswith('service-integration-') or root.parent != base:
        raise ValueError('only a dedicated synthetic integration directory is accepted')
    app = (root / 'app').resolve(strict=True)
    if app != Path(__file__).resolve().parent:
        raise ValueError('the synthetic test code must be executed from its own app directory')
    release = ((base / 'synthetic-real-entry-v4') if len(sys.argv) == 3 else Path(sys.argv[3])).resolve(strict=True)
    if release.parent != base or not release.name.startswith('synthetic-real-entry-'):
        raise ValueError('only an isolated synthetic business release is accepted')
    counter_path = base / 'tokenizer/server-counter-config.json'
    counter = json.loads(counter_path.read_bytes())
    cache = list((base / 'tokenizer/cache').iterdir())
    if len(cache) != 1 or not cache[0].is_file():
        raise ValueError('synthetic tokenizer cache identity is ambiguous')
    roles = dict(python=sys.executable, service_script=str(app / 'server_job_service.py'),
                 worker_script=str(app / 'server_wine_worker.py'),
                 wine64=str(base / 'wine-runtime/usr/lib/wine/wine64'),
                 counter_config=str(counter_path), model=counter['counter']['model_path'],
                 libllama=counter['counter']['library_path'], cl100k=str(cache[0]),
                 counter_env=str(Path(shutil.which('env', path='/usr/bin:/bin')).resolve(strict=True)),
                 counter_runtime_manifest=str(Path(counter['counter']['runtime_manifest']).resolve(strict=True)))
    paths = {Path(p).resolve(strict=True) for p in roles.values()}
    paths.update(Path(p).resolve(strict=True) for p in service._mapped_executables())
    paths.update(p.resolve(strict=True) for p in app.glob('*.py'))
    paths.add((Path(sys.prefix) / 'pyvenv.cfg').resolve(strict=True))
    refs = []
    for path in sorted(paths):
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        refs.append(dict(path=str(path), bytes=path.stat().st_size, sha256=digest))
    inventory = dict(schema='paperflow-native-runtime/1', reviewed_dependency_closure=False,
                     verification_scope='pinned-known-files',
                     python_environment=dict(prefix=sys.prefix, base_prefix=sys.base_prefix),
                     roles=roles, files=refs)
    native_bytes = encode(inventory)
    native = root / 'synthetic-native-inventory.json'
    write_once(native, native_bytes)
    ledger_id = str(uuid4())
    config = dict(schema='paperflow-job-service/1',
        server_identity=dict(server_id='synthetic-integration-only', ledger_id=ledger_id),
        jobs_root=str(root / 'jobs'), staging_root=str(root / 'staging'), base=str(base),
        native_manifest=str(native), native_manifest_sha256=hashlib.sha256(native_bytes).hexdigest(),
        initialize_delegation=True, worker_uid=os.getuid(), worker_gid=os.getgid(),
        release=dict(code_root=str(release),
                     code_manifest=str(release / 'source-code-manifest.json'),
                     template=str(base / 'wine-test-prefix4'),
                     template_manifest=str(base / 'wine-template-manifest-v1.json')),
        ledger=dict(database=str(root / 'synthetic-ledger.sqlite'), capacity=1),
        limits=dict(timeout_seconds=300, poll_seconds=.2, cleanup_seconds=20,
                    disk_poll_seconds=5, memory_bytes=4 * 1024**3, pids=256,
                    file_bytes=2 * 1024**3, open_files=1024, max_job_bytes=5 * 1024**3,
                    min_free_bytes=20 * 1024**3))
    service._config(config)
    config_bytes = encode(config)
    write_once(app / 'server-job-service.json', config_bytes)
    write_once(app / 'server-job-service.sha256', hashlib.sha256(config_bytes).hexdigest().encode('ascii'))
    service.verify_native_runtime(config)
    ledger = RemoteJobs(config['ledger']['database'], capacity=1, ledger_id=ledger_id)
    ledger.initialize()
    service.initialize_service_state(config)
    print(json.dumps(dict(config_sha256=hashlib.sha256(config_bytes).hexdigest(),
                          server_identity=config['server_identity'], files=len(refs),
                          environment=inventory['python_environment'],
                          production_dependency_closure_proved=False)))


if __name__ == '__main__':
    main()
