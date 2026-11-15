"""Sanitized, offline diagnostics; no real server or work orders."""
import importlib.util
from pathlib import Path

import pytest


HERE = Path(__file__).resolve().parents[1]


def load():
    spec = importlib.util.spec_from_file_location('synthetic_observation_diagnostics', HERE / 'observation_diagnostics.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_diagnostics_record_only_safe_categories_and_keep_exception():
    module = load()
    recorded = []
    def fail(_sensitive):
        raise ValueError('private response and credential material')
    with pytest.raises(ValueError):
        module.wrap('transport', fail, recorded)('private request')
    assert recorded == [{'stage': 'transport', 'error_type': 'ValueError'}]


def test_only_explicitly_allowed_transport_reasons_are_recorded():
    module = load()
    recorded = []
    def fail():
        error = RuntimeError('private details')
        error.observation_reason = 'authentication_failed'
        raise error
    with pytest.raises(RuntimeError):
        module.wrap('transport', fail, recorded)()
    assert recorded == [{'stage': 'transport', 'error_type': 'RuntimeError',
                         'reason': 'authentication_failed'}]


def test_diagnostics_are_bounded_and_stage_is_fixed():
    module = load()
    recorded = []
    def fail():
        raise ValueError('anything')
    wrapped = module.wrap('access', fail, recorded)
    for _ in range(30):
        with pytest.raises(ValueError):
            wrapped()
    assert len(recorded) == 20
    with pytest.raises(ValueError):
        module.wrap('private task value', fail, recorded)


def test_unknown_exception_names_and_stringification_cannot_leak_or_change_failure():
    module = load()
    recorded = []
    secret_exception = type('SyntheticSecret_1234', (Exception,),
                            {'__str__': lambda _self: (_ for _ in ()).throw(KeyboardInterrupt())})
    original = secret_exception()
    def fail():
        raise original
    with pytest.raises(secret_exception) as failure:
        module.wrap('transport', fail, recorded)()
    assert failure.value is original
    assert recorded == [{'stage': 'transport', 'error_type': 'Unknown'}]
    assert module.safe_exception_type(original) == 'Unknown'


def test_subclass_cannot_impersonate_a_whitelisted_reason():
    module = load()
    recorded = []
    class Imposter(str):
        def __hash__(self):
            return hash('authentication_failed')
        def __eq__(self, _other):
            return True
    def fail():
        error = RuntimeError('private')
        error.observation_reason = Imposter('SyntheticSecret_987')
        raise error
    with pytest.raises(RuntimeError):
        module.wrap('transport', fail, recorded)()
    assert recorded == [{'stage': 'transport', 'error_type': 'RuntimeError'}]
