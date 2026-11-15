"""One exact fixed server-identity probe; contains no work-order request."""
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import sys
from uuid import uuid4


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parent
SERVER = HERE.parents[1] / 'postprocess-server-20261101'
BOOTSTRAP_SHA = 'a41014d0952d23836e2e20a7b96f113bd8350380d2db0d676cd420f7965505d5'
GUARD_SHA = '9343b0d7367926fd4365a4672ee51617b60d4e77f30e7f2642e481dbb7e8d2be'
LAYOUT_SHA = '76e10389f88c377746112fd63f7959e09be1f36179aab9ebdb57b001a002b26a'
MANIFEST = HERE / 'release-inventory-three-receipt-probe-20261101-05.json'
MANIFEST_SHA = '9dae3da3926ee3e23b9ef8813c7d500e56f79aaca61b9a5be54d5a57210168d9'
VERSION = '204e39ae7be2b52513d97ebbc7bfd8863ad15b015a29ffe8fd4c0ad88ee7e1cf'


def main():
    sys.dont_write_bytecode = True
    sys.pycache_prefix = str(HERE / ('.transport-probe-no-bytecode-' + uuid4().hex))
    source = HERE / 'selected_probe_bootstrap.py'
    raw = source.read_bytes()
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), BOOTSTRAP_SHA):
        raise ValueError('bootstrap changed')
    spec = importlib.util.spec_from_file_location('_synthetic_probe_bootstrap', source)
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(source), 'exec', dont_inherit=True), module.__dict__)
    release = module._bootstrap('release_guard', HERE / 'release_guard.py', GUARD_SHA)
    layout = module._bootstrap('release_layout', HERE / 'release_layout.py', LAYOUT_SHA)
    roots, required = layout.layout()
    guard = release.ReleaseGuard(MANIFEST, MANIFEST_SHA, roots=roots, required_files=required,
                                 required_packages=layout.PACKAGES)
    guard.verify()
    finder = module._SourceOnlyFinder(guard, release)
    for name, existing in list(sys.modules.items()):
        if name in {'release_guard', 'release_layout'}:
            continue
        filename = getattr(existing, '__file__', None)
        if filename and (name in finder.protected or finder._managed(Path(os.path.abspath(filename)))):
            raise ValueError('candidate module preloaded')
    sys.meta_path.insert(0, finder)
    for path in (SERVER, ROOT, HERE):
        sys.path.insert(0, str(path))
    from read_only_rpc import ExistingInspectVersionRouter, GuardedAppDelegate, PinnedAccessBindings, UnreadDisclosureGate, _read_pinned, command_for
    import verified_ssh_transport as transport
    history = HERE / 'history-registry-candidate-bb7d5d9ae45b.json'
    history_sha = '7fc35333f9d59094d388c4d54b30aed3db8b2b2e96b79dd5bf7d7da76a93ccb2'
    access = HERE / 'history-access-candidate-ed0ef92b075f.json'
    access_sha = 'e1f3444842540ab5fb8f48a6c45ec8fc503600f42ef066f680fe8bff1ea31599'
    bundle = HERE / 'guard-bundle-candidate-20261101-v2.json'
    bundle_sha = '9b304d0f21f8650e8534b9de2c1067a778f81f82d473cfc09962e936f511db13'
    registry = json.loads(_read_pinned(history, history_sha))
    bindings = PinnedAccessBindings(access, access_sha, server_identity=registry['server_identity'])
    bundle_value = json.loads(_read_pinned(bundle, bundle_sha))
    gate = UnreadDisclosureGate(bundle_value['unread_path'], bundle_value['unread_sha256'])
    router = ExistingInspectVersionRouter(history, history_sha, server_identity=registry['server_identity'],
                                         access_bindings=bindings, disclosure_gate=gate)
    selected = router._versions()[VERSION]
    original_popen = transport.subprocess.Popen
    child = {}
    names = ('AuthenticationException', 'BadHostKeyException', 'NoValidConnectionsError', 'SSHException',
             'RemoteCallError', 'FileNotFoundError', 'PermissionError', 'TimeoutError', 'OSError', 'EOFError',
             'ValueError', 'TypeError', 'AttributeError', 'RuntimeError', 'ImportError', 'ModuleNotFoundError',
             'UnicodeEncodeError', 'UnicodeDecodeError', 'JSONDecodeError', 'UnsupportedOperation', 'AssertionError')

    def recorded_popen(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        communicate = process.communicate
        def read(*args, **kwargs):
            stdout, stderr = communicate(*args, **kwargs)
            lines = stderr.splitlines() if len(stderr) <= 65536 else []
            child['stderr_size'] = len(stderr)
            child['has_traceback'] = any(line == b'Traceback (most recent call last):' for line in lines)
            child['known_helper_errors'] = [name for name in names
                if ('isolated SSH request failed: ' + name).encode() in lines]
            child['known_traceback_types'] = [name for name in names
                if any(line == name.encode() or line.startswith((name + ':').encode()) for line in lines)]
            return stdout, stderr
        process.communicate = read
        return process
    transport.subprocess.Popen = recorded_popen
    call = transport.build(guard, SERVER / 'ssh_rpc_client.py', SERVER / 'ssh_rpc_process.py')
    guarded = GuardedAppDelegate(call, router, bundle, bundle_sha)
    nonce = str(uuid4())
    try:
        result = guarded(bindings.resolve(selected['access_ref']), command_for(selected, 'probe'),
            {'server_identity': registry['server_identity'], 'nonce': nonce}, timeout=20, max_bytes=16*1024*1024)
        verified = (isinstance(result, dict) and result.get('ledger_identity_verified') is True
                    and result == {'schema':'paperflow-server-probe/1', 'server_identity':registry['server_identity'],
                                   'nonce':nonce, 'ledger_identity_verified':True})
    except Exception:
        verified = False
    guard.verify()
    guarded._bindings()
    print(json.dumps({'schema': 'paperflow-transport-probe/1', 'probe_verified': verified,
                      'child_diagnostics': child, 'work_order_requested': False,
                      'processing_started': False, 'rag_upload': False}))


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        print(json.dumps({'schema': 'paperflow-transport-probe/1', 'probe_verified': False,
                          'phase': 'setup_unavailable', 'work_order_requested': False}))
        raise SystemExit(1) from None
