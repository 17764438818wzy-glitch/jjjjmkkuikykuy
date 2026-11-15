from importlib import import_module

import pytest


@pytest.mark.parametrize(
    ('codes', 'expected'),
    [(['z'], ['z']), (['z', 'b'], ['z', 'b']),
     (['z', 'b', 'a'], ['z', 'b', 'a']),
     (['z', 'b', 'a', 'c'], ['z', 'b', 'a'])],
)
def test_formal_targets_follow_model_relevance_not_code(codes, expected):
    routes = import_module('paperflow_v3.delivery.routes')
    profile = {'routing_candidates': [{'code': code} for code in codes]}
    catalog = {'majors': [{'code': code, 'paper_dataset_id': 'formal-' + code}
                          for code in ['a', 'b', 'c', 'z']]}

    selected = routes.select_formal_routes(profile, [], catalog)

    assert [row['code'] for row in selected] == expected
    assert [row['paper_dataset_id'] for row in selected] == ['formal-' + code for code in expected]


@pytest.mark.parametrize('codes', [[], ['missing']])
def test_no_usable_formal_specialty_cannot_start_publication(codes):
    routes = import_module('paperflow_v3.delivery.routes')
    profile = {'routing_candidates': [{'code': code} for code in codes]}
    with pytest.raises(ValueError):
        routes.select_formal_routes(profile, [], {'majors': []})


def test_existing_professional_without_dataset_is_not_a_valid_target():
    routes = import_module('paperflow_v3.delivery.routes')
    with pytest.raises(ValueError):
        routes.select_formal_routes({'routing_candidates': [{'code': 'a'}]}, [],
                                    {'majors': [{'code': 'a'}]})


