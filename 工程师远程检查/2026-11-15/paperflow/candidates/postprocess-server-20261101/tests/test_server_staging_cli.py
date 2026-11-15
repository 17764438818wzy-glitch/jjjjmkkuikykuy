"""Staging CLI tests operate exclusively on Linux temporary directories."""
import io
import json
import os
import struct

import pytest

from server_staging_cli import receive
from snapshot_wire import snapshot_sha
from test_snapshot_wire import make_manifest


pytestmark = pytest.mark.skipif(os.name != 'posix', reason='Linux directory descriptor admission')


def packet(manifest):
    header = json.dumps({'manifest': manifest, 'snapshot_sha256': snapshot_sha(manifest)}).encode('utf8')
    return struct.pack('!Q', len(header)) + header + b'{}reply'


def test_receive_stages_one_synthetic_packet_and_rejects_duplicate(tmp_path):
    root = tmp_path / 'staging'; root.mkdir(mode=0o700)
    manifest = make_manifest()
    output = io.StringIO()
    result = receive(root, io.BytesIO(packet(manifest)), output)
    assert json.loads(output.getvalue()) == result
    assert (root / manifest['job_id'] / 'INPUT_VERIFIED.json').is_file()
    duplicate = io.StringIO()
    with pytest.raises(ValueError):
        receive(root, io.BytesIO(packet(manifest)), duplicate)
    assert duplicate.getvalue() == ''


@pytest.mark.parametrize('replacement_kind', ['symlink', 'directory'])
def test_staging_root_remains_bound_while_reading_transfer_header(tmp_path, replacement_kind):
    ancestor = tmp_path / 'ancestor'; ancestor.mkdir(mode=0o700)
    root = ancestor / 'staging'; root.mkdir(mode=0o700)
    replacement = tmp_path / 'replacement'; replacement.mkdir(mode=0o700)
    (replacement / 'staging').mkdir(mode=0o700)
    moved = tmp_path / 'moved-ancestor'
    class SwapHeader(io.BytesIO):
        def read(self, length=-1):
            if not moved.exists():
                ancestor.rename(moved)
                if replacement_kind == 'symlink':
                    ancestor.symlink_to(replacement, target_is_directory=True)
                else:
                    replacement.rename(ancestor)
            return super().read(length)
    manifest = make_manifest()
    output = io.StringIO()
    with pytest.raises(ValueError):
        receive(root, SwapHeader(packet(manifest)), output)
    visible_replacement = replacement if replacement_kind == 'symlink' else ancestor
    assert not (visible_replacement / 'staging' / manifest['job_id']).exists()
    assert not (moved / 'staging' / manifest['job_id'] / 'INPUT_VERIFIED.json').exists()
    assert output.getvalue() == ''
