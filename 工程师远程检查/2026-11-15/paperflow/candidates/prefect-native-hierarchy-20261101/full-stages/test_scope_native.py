"""A server receipt can follow its original Prefect folder, including old history."""
import importlib.util
from pathlib import Path
import sys
from uuid import UUID, uuid4

import pytest

here = Path(__file__).resolve().parent
root = here.parents[2]
tests = root / 'candidates/postprocess-prefect-20261031/remote_projection_next/tests'
sys.path.insert(0, str(tests))
from test_scope_verifier import modules, previous, sample, scope


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('parent', ['folder', 'legacy', 'other'])
def test_server_scope_observes_the_original_postprocessing_task(scope, parent):
    sample, _, request = scope
    source = load('candidate_scope', here / 'scope_verifier.py')
    sample.projector.runtime = load('candidate_postprocessing_stage', here / 'stage_runtime.py')
    verifier = source.ScopeVerifier(sample.projector, server_identity=request['server_identity'],
                                    check_original_files=lambda: None,
                                    projection_helpers=load('candidate_projections', here / 'projections.py'))
    job = previous.insert_job(sample, 'finished', task_state='COMPLETED')
    task = sample.client.stages[job]
    task.state.id = uuid4()
    task.flow_run_id = {'folder': UUID(sample.item['task_run_id']), 'legacy': None, 'other': uuid4()}[parent]
    assert verifier(request) is (parent != 'other')
