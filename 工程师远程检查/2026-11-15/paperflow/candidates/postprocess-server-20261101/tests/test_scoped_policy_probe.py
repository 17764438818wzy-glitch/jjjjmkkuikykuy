import copy
from types import SimpleNamespace

import pytest


IDENTITY = dict(task_id='t', group_id='g', paper_id='p', receipt_id='r', attempt_id='a',
                parent_run_id='parent', task_run_id='folder')


def test_probe_returns_only_the_same_unclaimed_original_folder():
    from scoped_policy_probe import read_one
    seen = []
    def inspect(*, sources, identity):
        seen.append((sources, copy.deepcopy(identity)))
        return dict(schema='paperflow-selected-ticket-evidence/1', identity=copy.deepcopy(identity), generation=2, claim_created=False,
                    server_execution_started=False)
    source = SimpleNamespace()
    record = read_one(source, IDENTITY, 2, inspect=inspect)
    assert record['identity'] == IDENTITY and seen == [(source, IDENTITY)]


@pytest.mark.parametrize('changed', ['identity', 'generation', 'claimed', 'started', 'mutated_input'])
def test_probe_refuses_a_stale_or_mutated_folder(changed):
    from scoped_policy_probe import read_one
    def inspect(*, sources, identity):
        result = dict(schema='paperflow-selected-ticket-evidence/1', identity=copy.deepcopy(identity), generation=2, claim_created=False,
                      server_execution_started=False)
        if changed == 'identity': result['identity']['group_id'] = 'another'
        elif changed == 'generation': result['generation'] = 3
        elif changed == 'claimed': result['claim_created'] = True
        elif changed == 'started': result['server_execution_started'] = True
        elif changed == 'mutated_input': identity['group_id'] = 'another'
        return result
    with pytest.raises(ValueError):
        read_one(SimpleNamespace(), IDENTITY, 2, inspect=inspect)
