"""Temporary-only regression checks; never connect to the live service."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT / 'tests'), str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
               str(WORKSPACE / 'prefect-paperflow/paperflow_prefect/clean_overlay')]
from test_attachment_adapter import fixture as receipt_fixture
from paperflow_prefect.stage3_handoff import read_receipt
from paperflow_prefect.records import sha_file

spec = importlib.util.spec_from_file_location('isolated_named_body', ROOT / 'named_next/body_adapter.py')
body = importlib.util.module_from_spec(spec)
spec.loader.exec_module(body)


def reference(p):
    return {'path': str(p), 'sha256': sha_file(p)}


def case(tmp_path):
    ticket, handoff, _, _ = receipt_fixture(tmp_path, delivered=True)
    _, paper = read_receipt(handoff['receipt_ref']['path'])
    row = paper['body']
    work = tmp_path / 'body-work'
    named = work / 'named-input' / 'renamed.docx'
    named.parent.mkdir(parents=True)
    named.write_bytes(Path(row['path']).read_bytes())
    proof_path = work / 'named-input-proof.json'
    proof = {'schema': 'named-delivery-input/1', 'paper_id': ticket['paper_id'], 'file_id': row['file_id'],
             'kind': 'body', 'source_path': row['path'], 'source_sha256': row['sha256'],
             'bytes': row['bytes'], 'display_name': named.name, 'delivery_path': str(named)}
    proof_path.write_text(json.dumps(proof), encoding='utf8')
    material = {'version': 'preextract-material/1.0',
                'source': {'file': str(named), 'source_sha256': row['sha256']},
                'file_hashes': {str(proof_path): sha_file(proof_path), str(named): row['sha256'],
                                str(Path(row['path']).resolve()): row['sha256']}}
    evidence = {'receipt': handoff['receipt_ref'], 'source': reference(Path(row['path'])),
                'plan': {'schema': 'body-frozen-plan-files/1', 'work': str(work)},
                'named_source': {'proof': reference(proof_path), 'work': str(work)}}
    return ticket, evidence, material, copy.deepcopy(material), proof_path, proof


def check(ticket, evidence, original, reading):
    return body._verify_named_source(evidence, original, reading, ticket)


def test_named_copy_with_real_receipt_and_frozen_proof_is_allowed(tmp_path):
    ticket, evidence, original, reading, _, _ = case(tmp_path)
    check(ticket, evidence, original, reading)


@pytest.mark.parametrize('change', ['proof_identity', 'source', 'target', 'work', 'unbound_proof', 'reading'])
def test_same_hash_cannot_authorize_a_forged_mapping(tmp_path, change):
    ticket, evidence, original, reading, path, proof = case(tmp_path)
    if change == 'proof_identity':
        proof['file_id'] = 'other-file'
    elif change == 'source':
        imposter = tmp_path / 'another.docx'; imposter.write_bytes(Path(proof['source_path']).read_bytes())
        proof['source_path'] = str(imposter)
    elif change == 'target':
        imposter = tmp_path / 'another.docx'; imposter.write_bytes(Path(proof['delivery_path']).read_bytes())
        proof['delivery_path'] = str(imposter)
        original['source']['file'] = reading['source']['file'] = str(imposter)
        original['file_hashes'][str(imposter)] = proof['source_sha256']
    elif change == 'work':
        evidence['named_source']['work'] = str(tmp_path / 'other-work')
    elif change == 'unbound_proof':
        original['file_hashes'].pop(str(path))
    elif change == 'reading':
        reading['source']['file'] = proof['source_path']
    if change in {'proof_identity', 'source', 'target'}:
        path.write_text(json.dumps(proof), encoding='utf8')
        evidence['named_source']['proof'] = reference(path)
        original['file_hashes'][str(path)] = sha_file(path)
    with pytest.raises(ValueError):
        check(ticket, evidence, original, reading)


def test_retained_mapping_is_rechecked_after_proof_changes(tmp_path):
    ticket, evidence, original, reading, path, _ = case(tmp_path)
    check(ticket, evidence, original, reading)
    path.write_bytes(path.read_bytes() + b' ')
    with pytest.raises(ValueError):
        check(ticket, evidence, original, reading)

