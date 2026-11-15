"""Cross-machine evidence paths must never alter or alias original evidence."""
from pathlib import Path
import hashlib
import os

import pytest

from windows_snapshot import EvidenceMap, EvidenceError


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def test_separates_original_windows_identity_from_server_path(tmp_path):
    mapper = EvidenceMap(tmp_path / 'snapshot')
    first = mapper.bind(r'F:\论文\材料\a.png', sha(b'original'), 8)
    second = mapper.bind(r'f:\论文\材料\B.json', sha(b'{}'), 2)
    assert first == tmp_path / 'snapshot' / 'drives' / 'f' / '论文' / '材料' / 'a.png'
    assert second.parent == first.parent
    assert mapper.resolve(r'f:\论文\材料\a.png') == first
    assert mapper.resolve(r'F:\论文\材料') == first.parent
    assert mapper.manifest()[0]['source'] == r'F:\论文\材料\a.png'


@pytest.mark.parametrize('path', [
    r'F:\材料\..\secret', r'F:relative\a', r'..\a',
    r'\\?\F:\材料\a', r'F:\材料\a:alternate', r'F:\材料\NUL.txt',
    'F:\\材料\\tail. ', 'F:\\材料\\bad\x00name',
])
def test_rejects_ambiguous_or_escaping_windows_names(tmp_path, path):
    with pytest.raises(EvidenceError):
        EvidenceMap(tmp_path).bind(path, sha(b'x'), 1)


def test_conflicting_case_aliases_cannot_bind_two_versions(tmp_path):
    mapper = EvidenceMap(tmp_path)
    mapper.bind(r'F:\数据\A.txt', sha(b'first'), 5)
    with pytest.raises(EvidenceError, match='conflict'):
        mapper.bind(r'f:\数据\a.TXT', sha(b'second'), 6)


def test_same_bytes_do_not_prove_case_variant_has_same_source_identity(tmp_path):
    mapper = EvidenceMap(tmp_path)
    mapper.bind(r'F:\数据\A.txt', sha(b'x'), 1)
    with pytest.raises(EvidenceError, match='conflict'):
        mapper.bind(r'F:\数据\a.txt', sha(b'x'), 1)


def test_unicode_casefold_collision_keeps_distinct_windows_names(tmp_path):
    mapper = EvidenceMap(tmp_path)
    one = mapper.bind(r'F:\data\Straße.txt', sha(b'x'), 1)
    two = mapper.bind(r'F:\data\strasse.txt', sha(b'x'), 1)
    assert one != two
    assert len(mapper.manifest()) == 2


def test_unknown_and_unregistered_paths_cannot_be_resolved(tmp_path):
    mapper = EvidenceMap(tmp_path)
    mapper.bind(r'F:\数据\A.txt', sha(b'x'), 1)
    with pytest.raises(EvidenceError, match='unregistered'):
        mapper.resolve(r'F:\数据\secret.txt')
    with pytest.raises(EvidenceError):
        mapper.resolve(r'F:\数据\..\secret.txt')


@pytest.mark.skipif(os.name != 'nt', reason='source snapshot capture runs on Windows')
def test_copy_checks_expected_hash_without_modifying_source(tmp_path):
    source = tmp_path / 'original.txt'
    source.write_bytes(b'original')
    mapper = EvidenceMap(tmp_path / 'snapshot')
    name = str(source.resolve())
    target = mapper.bind(name, sha(b'original'), 8)
    mapper.copy_verified(name, source)
    assert source.read_bytes() == target.read_bytes() == b'original'
    assert mapper.verify_all() is True
    target.write_bytes(b'tampered')
    with pytest.raises(EvidenceError, match='changed'):
        mapper.verify_all()
    assert source.read_bytes() == b'original'


@pytest.mark.skipif(os.name != 'nt', reason='source snapshot capture runs on Windows')
def test_copy_changed_input_and_preexisting_target_are_never_accepted(tmp_path):
    source = tmp_path / 'original.txt'
    source.write_bytes(b'changed')
    mapper = EvidenceMap(tmp_path / 'snapshot')
    name = str(source.resolve())
    target = mapper.bind(name, sha(b'expected'), 8)
    with pytest.raises(EvidenceError, match='changed'):
        mapper.copy_verified(name, source)
    assert not target.exists()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b'preexisting')
    source.write_bytes(b'expected')
    with pytest.raises(EvidenceError, match='exists'):
        mapper.copy_verified(name, source)
    assert target.read_bytes() == b'preexisting'


def test_unc_and_drive_are_independent_namespaces(tmp_path):
    mapper = EvidenceMap(tmp_path)
    drive = mapper.bind(r'F:\share\a.txt', sha(b'x'), 1)
    network = mapper.bind(r'\\server\share\a.txt', sha(b'x'), 1)
    assert drive != network
    assert mapper.resolve(r'\\SERVER\SHARE\a.txt') == network


def test_drive_or_share_root_cannot_be_registered_as_file(tmp_path):
    mapper = EvidenceMap(tmp_path)
    for name in ('F:\\', '\\\\server\\share\\'):
        with pytest.raises(EvidenceError):
            mapper.bind(name, sha(b''), 0)


def test_conflicting_parent_file_and_directory_are_rejected(tmp_path):
    mapper = EvidenceMap(tmp_path)
    mapper.bind(r'F:\data\item', sha(b'x'), 1)
    with pytest.raises(EvidenceError, match='conflict'):
        mapper.bind(r'F:\data\item\inside.txt', sha(b'x'), 1)


@pytest.mark.skipif(os.name != 'nt', reason='source snapshot capture runs on Windows')
def test_source_symlink_is_not_followed(tmp_path):
    actual = tmp_path / 'actual'; actual.write_bytes(b'x')
    link = tmp_path / 'link'
    try:
        link.symlink_to(actual)
    except OSError:
        pytest.skip('creating symlinks is unavailable')
    mapper = EvidenceMap(tmp_path / 'snapshot')
    name = str(link.absolute())
    mapper.bind(name, sha(b'x'), 1)
    with pytest.raises(EvidenceError, match='link'):
        mapper.copy_verified(name, link)


@pytest.mark.skipif(os.name != 'nt', reason='source snapshot capture runs on Windows')
def test_same_hash_different_source_file_cannot_replace_original(tmp_path):
    source = tmp_path / 'original'; source.write_bytes(b'x')
    other = tmp_path / 'other'; other.write_bytes(b'x')
    mapper = EvidenceMap(tmp_path / 'snapshot')
    name = str(source.resolve()); mapper.bind(name, sha(b'x'), 1)
    with pytest.raises(EvidenceError, match='source'):
        mapper.copy_verified(name, other)


def test_empty_snapshot_does_not_prove_completeness(tmp_path):
    with pytest.raises(EvidenceError, match='empty'):
        EvidenceMap(tmp_path).verify_all()
