"""Server release preparation uses only isolated synthetic files."""
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from snapshot_wire import receive_snapshot, snapshot_sha
from test_snapshot_wire import make_manifest
from server_release import prepare_environment, ReleaseError
import server_release


pytestmark = pytest.mark.skipif(os.name != 'posix', reason='Linux server file isolation')


def fixture(root, *, code_relative='drives/f/release/mod.py', case_collision=False):
    release = root / 'release'
    source = release / 'drives/f/release/mod.py'
    source.parent.mkdir(parents=True)
    source.write_bytes(b'synthetic code')
    row = dict(source=r'F:\release\mod.py', relative=code_relative,
               sha256=hashlib.sha256(source.read_bytes()).hexdigest(), bytes=source.stat().st_size)
    code_manifest = root / 'code-manifest.json'
    code_manifest.write_text(json.dumps([row]), encoding='utf8')
    template = root / 'template'
    (template / 'drive_c/Python').mkdir(parents=True)
    (template / 'drive_c/Python/python.exe').write_bytes(b'synthetic interpreter')
    (template / 'drive_c/users/wzy').mkdir(parents=True)
    (template / 'drive_c/users/wzy/Documents').symlink_to(root, target_is_directory=True)
    (template / 'dosdevices').mkdir()
    (template / 'dosdevices/c:').symlink_to('../drive_c', target_is_directory=True)
    (template / 'dosdevices/z:').symlink_to('/', target_is_directory=True)
    (template / 'dosdevices/com1').symlink_to('/dev/null')
    entries = []
    for path in sorted(template.rglob('*')):
        info = path.lstat(); relative = path.relative_to(template).as_posix()
        if stat.S_ISLNK(info.st_mode): entry = dict(path=relative, kind='link', target=os.readlink(path))
        elif stat.S_ISDIR(info.st_mode): entry = dict(path=relative, kind='directory', mode=stat.S_IMODE(info.st_mode))
        else: entry = dict(path=relative, kind='file', mode=stat.S_IMODE(info.st_mode), bytes=info.st_size,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        entries.append(entry)
    template_manifest = root / 'template-manifest.json'
    template_manifest.write_text(json.dumps(dict(schema='paperflow-runtime-template/1',
        root_mode=stat.S_IMODE(template.stat().st_mode), entries=entries), sort_keys=True, separators=(',', ':')), encoding='utf8')
    manifest = make_manifest()
    manifest['catalog'] = {}
    manifest['code_sha256'] = hashlib.sha256(code_manifest.read_bytes()).hexdigest()
    manifest['runtime_template_sha256'] = hashlib.sha256(template_manifest.read_bytes()).hexdigest()
    if case_collision: manifest['files'][0]['path'] = r'F:\Release\data.json'
    snapshot = root / 'staged'
    receive_snapshot(snapshot, manifest, snapshot_sha(manifest), io.BytesIO(b'{}reply'))
    job = {k: manifest[k] for k in ('job_id', 'claim_id', 'identity', 'generation',
                                     'manifest_sha256', 'code_sha256')}
    job['snapshot_sha256'] = snapshot_sha(manifest)
    args = dict(job=job, snapshot=snapshot, code_root=release, code_manifest=code_manifest,
                template=template, prefix=root / 'prefix')
    # Also exercise the old signature during the RED run, so failures expose
    # the actual unsafe behavior rather than only an unexpected keyword error.
    if hasattr(server_release, 'template_manifest_bytes'): args['template_manifest'] = template_manifest
    return args


def test_staged_files_and_code_are_bound_to_a_private_wine_prefix(tmp_path):
    args = fixture(tmp_path)
    prepared = prepare_environment(**args)
    assert prepared['job'] == args['job']
    assert (args['snapshot'] / 'drives/f/release/mod.py').read_bytes() == b'synthetic code'
    assert (args['prefix'] / 'dosdevices/f:').resolve() == args['snapshot'] / 'drives/f'
    assert not (args['prefix'] / 'dosdevices/z:').exists()
    assert not (args['prefix'] / 'dosdevices/com1').exists()
    docs = args['prefix'] / 'drive_c/users/wzy/Documents'
    assert docs.is_dir() and not docs.is_symlink()
    assert (args['template'] / 'dosdevices/z:').is_symlink()
    assert prepared['template_identity_verified'] is True
    assert prepared['native_runtime_identity_verified'] is False


def test_code_hash_mismatch_does_not_create_prefix(tmp_path):
    args = fixture(tmp_path)
    args['code_manifest'].write_bytes(b'changed')
    with pytest.raises(ReleaseError): prepare_environment(**args)
    assert not args['prefix'].exists()


def test_code_manifest_cannot_escape_its_pinned_release(tmp_path):
    args = fixture(tmp_path, code_relative='drives/f/../outside.py')
    with pytest.raises(ReleaseError): prepare_environment(**args)
    assert not args['prefix'].exists()


def test_missing_input_never_creates_an_execution_prefix(tmp_path):
    args = fixture(tmp_path)
    (args['snapshot'] / 'drives/f/paper/reply.txt').unlink()
    with pytest.raises(ReleaseError): prepare_environment(**args)
    assert not args['prefix'].exists()


def test_manifest_is_parsed_from_the_same_bytes_that_were_hashed(tmp_path, monkeypatch):
    args = fixture(tmp_path)
    alternate = args['code_root'] / 'drives/f/release/other.py'; alternate.write_bytes(b'other code')
    original_open = Path.open; opens = []
    def swapped_open(path, *a, **kw):
        if path == args['code_manifest']:
            opens.append(path)
            if len(opens) == 2:
                raw = json.dumps([dict(source=r'F:\release\other.py', relative='drives/f/release/other.py',
                    bytes=10, sha256=hashlib.sha256(b'other code').hexdigest())]).encode()
                with original_open(path, 'wb') as out: out.write(raw)
        return original_open(path, *a, **kw)
    monkeypatch.setattr(Path, 'open', swapped_open)
    try: prepare_environment(**args)
    except ReleaseError: pass
    assert not (args['snapshot'] / 'drives/f/release/other.py').exists()


@pytest.mark.parametrize('change', ['file', 'directory', 'link', 'new_entry'])
def test_template_must_match_its_independently_frozen_inventory(tmp_path, change):
    args = fixture(tmp_path)
    if change == 'file': (args['template'] / 'drive_c/Python/python.exe').write_bytes(b'changed')
    elif change == 'directory': os.chmod(args['template'] / 'drive_c/Python', 0o700)
    elif change == 'link':
        link = args['template'] / 'dosdevices/z:'; link.unlink(); link.symlink_to('/tmp')
    else: (args['template'] / 'unlisted.bin').write_bytes(b'unknown')
    with pytest.raises(ReleaseError): prepare_environment(**args)
    assert not args['prefix'].exists()


def test_runtime_template_hash_must_be_in_original_snapshot(tmp_path):
    args = fixture(tmp_path)
    (tmp_path / 'template-manifest.json').write_bytes(b'{}')
    with pytest.raises(ReleaseError): prepare_environment(**args)
    assert not args['prefix'].exists()


def test_input_and_code_share_one_windows_path_namespace(tmp_path):
    args = fixture(tmp_path, case_collision=True)
    with pytest.raises(ReleaseError): prepare_environment(**args)
    assert not args['prefix'].exists()


def test_target_parent_swap_cannot_write_outside_snapshot(tmp_path, monkeypatch):
    args = fixture(tmp_path)
    target_parent = args['snapshot'] / 'drives/f/release'
    outside = tmp_path / 'outside'; outside.mkdir()
    original_open = os.open; swapped = []
    def guarded_open(path, flags, *a, **kw):
        descriptor = kw.get('dir_fd')
        parent_name = Path(os.readlink('/proc/self/fd/' + str(descriptor))) if descriptor is not None else None
        absolute_target = Path(path) == target_parent / 'mod.py'
        relative_parent = str(path) == 'release' and parent_name == target_parent.parent
        if not swapped and (absolute_target or relative_parent):
            if target_parent.exists(): target_parent.rename(target_parent.with_name('saved'))
            target_parent.symlink_to(outside, target_is_directory=True); swapped.append(True)
        return original_open(path, flags, *a, **kw)
    monkeypatch.setattr(os, 'open', guarded_open)
    try: prepare_environment(**args)
    except ReleaseError: pass
    assert swapped, 'race injection never reached target installation'
    assert not (outside / 'mod.py').exists()


def test_source_parent_swap_never_opens_a_file_outside_code_root(tmp_path, monkeypatch):
    args = fixture(tmp_path)
    source = args['code_root'] / 'drives/f/release/mod.py'
    outside = tmp_path / 'outside'; outside.mkdir(); (outside / 'mod.py').write_bytes(b'private outside')
    real_fd_open, real_path_open = os.open, Path.open
    swapped, escaped, old_opens = [], [], []
    def swap():
        if not swapped:
            source.parent.rename(source.parent.with_name('saved'))
            source.parent.symlink_to(outside, target_is_directory=True); swapped.append(True)
    def fd_open(path, flags, *a, **kw):
        descriptor = kw.get('dir_fd')
        if str(path) == 'mod.py' and descriptor is not None:
            parent = Path(os.readlink('/proc/self/fd/' + str(descriptor)))
            if parent == source.parent: swap()
        fd = real_fd_open(path, flags, *a, **kw)
        if Path(os.readlink('/proc/self/fd/' + str(fd))) == outside / 'mod.py': escaped.append(True)
        return fd
    def path_open(path, *a, **kw):
        if path == source:
            old_opens.append(True)
            if len(old_opens) == 2: swap(); escaped.append(True)
        return real_path_open(path, *a, **kw)
    monkeypatch.setattr(os, 'open', fd_open); monkeypatch.setattr(Path, 'open', path_open)
    try: prepare_environment(**args)
    except ReleaseError: pass
    assert swapped, 'race injection never reached code reading'
    assert not escaped


def test_template_generator_emits_the_exact_trusted_inventory_bytes(tmp_path):
    args = fixture(tmp_path)
    assert hasattr(server_release, 'template_manifest_bytes'), 'missing pinned-template inventory generator'
    assert json.loads(server_release.template_manifest_bytes(args['template'])) == json.loads(args['template_manifest'].read_bytes())
