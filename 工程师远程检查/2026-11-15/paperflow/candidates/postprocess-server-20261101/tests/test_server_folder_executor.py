"""Exercise the existing folder receipt contract using synthetic inputs only."""
import json
from pathlib import Path
import sys
from uuid import uuid4

import pytest

WORK = Path(__file__).resolve().parents[2] / 'postprocess-prefect-20261031'
sys.path[:0] = [str(WORK / 'tests'), str(WORK), str(WORK.parents[1] / 'prefect-paperflow')]
from test_synthetic_mainline_integration import synthetic_folder
from test_handoff_inputs import compact
from test_body_adapter import FixtureCounter, RELEASE
from folder_completion import expected_components, inspect_folder
from preextract_scheduler_adapter import request_observation
from body_adapter import frozen_plan_evidence
from handoff_inputs import build_inputs
from server_folder_executor import process_verified_folder


def make_inputs(tmp_path, parts):
    identity, manifest, paper, admissions, catalog, work = synthetic_folder(tmp_path / 'inputs', body_parts=parts)
    digest = compact(manifest, sort=True)
    inputs = build_inputs(identity=identity, verified={'manifest': manifest, 'paper': paper}, admissions=admissions,
        manifest_sha256=digest, expected_components=expected_components, inspect_folder=inspect_folder,
        observe_request=request_observation, frozen_plan=frozen_plan_evidence,
        attachment_work=lambda p, row: work)
    return inputs, catalog


@pytest.mark.parametrize('parts', [1, 2])
def test_one_server_folder_receipt_covers_body_and_every_attachment(tmp_path, parts, monkeypatch):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    inputs, catalog = make_inputs(tmp_path, parts)
    result = process_verified_folder(load_inputs=lambda: inputs, identity=inputs['identity'],
        generation=1, manifest_sha256=inputs['manifest_sha256'], claim_id=str(uuid4()),
        catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter(), output_root=tmp_path / 'out')
    receipt = result['receipt']
    assert result['server_validation'] == 'passed'
    assert receipt['identity'] == inputs['identity']
    assert receipt['manifest_sha256'] == inputs['manifest_sha256']
    assert len(receipt['attachments']) == len(inputs['attachments']) == 1
    assert Path(receipt['body']['package_path'], 'article.redacted.md').is_file()
    assert Path(receipt['attachments'][0]['md_path']).is_file()
    assert json.loads(result['receipt_json']) == receipt


def test_changed_folder_snapshot_prevents_server_receipt(tmp_path, monkeypatch):
    inputs, catalog = make_inputs(tmp_path, 1)
    calls = 0
    def load():
        nonlocal calls
        calls += 1
        if calls > 1:
            return {**inputs, 'manifest_sha256': '0' * 64}
        return inputs
    with pytest.raises(ValueError, match='changed'):
        process_verified_folder(load_inputs=load, identity=inputs['identity'], generation=1,
            manifest_sha256=inputs['manifest_sha256'], claim_id=str(uuid4()), catalog=catalog,
            paperflow_root=RELEASE / 'core', counter=FixtureCounter(), output_root=tmp_path / 'out')
