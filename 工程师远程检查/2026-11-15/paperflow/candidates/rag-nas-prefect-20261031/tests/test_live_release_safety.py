"""Reproduce cleanup of synthetic NTFS files only; no NAS or RAG writes."""
import ast
import hashlib
import os
from pathlib import Path
import sys

import pytest


WORKSPACE = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(WORKSPACE / 'prefect-paperflow'))
from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
from paperflow.original_archive.lifecycle import locked_file, verify_stream
from paperflow_prefect._release.file_identity import identity_from_stream
from paperflow_prefect._release.hardlinks import enumerate_links
from paperflow_prefect._release.namespace_guard import hold_directories
from paperflow_prefect._release.streams import require_default_stream_only

SCRIPT = Path(__file__).resolve().parents[1] / 'live_release_selftest.py'


def release_block(case, target, source, *, verify_only=False):
    """Execute the actual self-test delete block against a synthetic file."""
    tree = ast.parse(SCRIPT.read_text(encoding='utf8'), filename=str(SCRIPT))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    blocks = [n for n in ast.walk(main) if isinstance(n, ast.With)]
    def calls(node, name):
        return any(isinstance(item.context_expr, ast.Call)
                   and isinstance(item.context_expr.func, ast.Name)
                   and item.context_expr.func.id == name for item in node.items)
    block = next((n for n in blocks if calls(n, 'hold_directories')), None)
    if block is None:
        block = next(n for n in blocks if calls(n, 'locked_file')
                     and any(kw.arg == 'delete' for item in n.items for kw in item.context_expr.keywords))
    early = next((n for n in main.body if isinstance(n, ast.If)
                  and isinstance(n.test, ast.Name) and n.test.id == 'verify_only'), None)
    body = ([early] if early is not None else []) + [block]
    fn = ast.FunctionDef(name='run_release', args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[],
                         kw_defaults=[], defaults=[]), body=body, decorator_list=[])
    executable = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    with locked_file(target) as (stream, _):
        identity = identity_from_stream(stream)
    def write_new(path, value):
        with path.open('x', encoding='utf8') as out:
            out.write(str(value))
    namespace = {'__builtins__': __builtins__, 'os': os, 'case': case, 'test_file': target,
                 'source': source, 'source_hash': digest, 'size': target.stat().st_size,
                 'test_identity': identity, 'locked_file': locked_file, 'verify_stream': verify_stream,
                 'identity_from_stream': identity_from_stream, 'hold_directories': hold_directories,
                 'require_default_stream_only': require_default_stream_only, 'enumerate_links': enumerate_links,
                 'intent_path': case / 'release-intent.json', 'intent': {'synthetic': True}, 'write_new': write_new,
                 'verify_only': verify_only, 'json': __import__('json'), 'test_ids': {'a', 'b'},
                 'downloaded': digest,
                 'sha': lambda p: hashlib.sha256(p.read_bytes()).hexdigest()}
    exec(compile(executable, str(SCRIPT), 'exec'), namespace)
    namespace['run_release']()


@pytest.fixture
def synthetic_file(tmp_path):
    case = tmp_path / 'case'; (case / 'input').mkdir(parents=True)
    target, source = case / 'input' / 'body.doc', tmp_path / 'source.doc'
    target.write_bytes(b'synthetic test body')
    source.write_bytes(b'synthetic test body')
    return case, target, source


@pytest.mark.skipif(os.name != 'nt', reason='NTFS named streams are a Windows feature')
def test_unarchived_named_stream_blocks_test_copy_deletion(synthetic_file):
    case, target, source = synthetic_file
    named = Path(str(target) + ':unarchived')
    named.write_bytes(b'synthetic stream not in NAS archive')
    with pytest.raises(ValueError, match='named streams'):
        release_block(case, target, source)
    assert target.read_bytes() == b'synthetic test body'
    assert named.read_bytes() == b'synthetic stream not in NAS archive'
    assert not (case / 'release-intent.json').exists()


def test_default_stream_only_test_copy_can_be_released(synthetic_file):
    case, target, source = synthetic_file
    release_block(case, target, source)
    assert not target.exists()
    assert source.read_bytes() == b'synthetic test body'
    assert (case / 'release-intent.json').is_file()


@pytest.mark.skipif(os.name != 'nt', reason='NTFS named streams are a Windows feature')
def test_verify_only_also_rejects_unarchived_named_stream(synthetic_file):
    case, target, source = synthetic_file
    named = Path(str(target) + ':unarchived')
    named.write_bytes(b'synthetic stream')
    with pytest.raises(ValueError, match='named streams'):
        release_block(case, target, source, verify_only=True)
    assert target.is_file() and named.read_bytes() == b'synthetic stream'
    assert not (case / 'release-intent.json').exists()


def test_verify_only_default_stream_preserves_file_and_creates_no_intent(synthetic_file):
    case, target, source = synthetic_file
    release_block(case, target, source, verify_only=True)
    assert target.is_file() and source.is_file()
    assert not (case / 'release-intent.json').exists()
