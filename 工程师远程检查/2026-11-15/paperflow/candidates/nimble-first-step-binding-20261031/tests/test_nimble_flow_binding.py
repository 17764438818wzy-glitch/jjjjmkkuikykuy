from pathlib import Path


def test_controlled_flow_binds_service_check_to_first_step(tmp_path, monkeypatch):
    import first_step_flow
    import paperflow_prefect.controlled_first_step as dispatcher
    import paperflow_prefect.first_step_control as control_module
    import paperflow_prefect.first_step_registration as registration
    import paperflow_prefect.nimble_dependency as dependency

    endpoint = 'http://127.0.0.1:11435/v1/systemone'
    calls = []

    class Settings:
        values = {'nimble_endpoint': endpoint}
        def path(self, name):
            return tmp_path / name
        def require_local_classification(self):
            pass

    class Summary:
        def read(self, *args):
            return {'task_id': 'example'}

    class Control:
        summary = Summary()
        def __init__(self, *args, **kwargs):
            pass
        def run_is_requested(self, task_id, generation):
            return True

    monkeypatch.setattr(first_step_flow.Settings, 'load', lambda: Settings())
    monkeypatch.setattr(control_module, 'FirstStepControl', Control)
    monkeypatch.setattr(registration, 'registration_history_roots', lambda *_: ())
    monkeypatch.setattr(dependency, 'ensure_nimble', lambda *args, **kwargs: calls.append((args, kwargs)))

    def dispatch(*args, **kwargs):
        assert kwargs['endpoint'] == endpoint
        kwargs['dependency_check']()

    monkeypatch.setattr(dispatcher, 'run_first_step', dispatch)
    assert first_step_flow.controlled_first_step.fn('example', 1) == {'task_id': 'example'}
    assert len(calls) == 1
    assert calls[0][0][0] == endpoint
    assert Path(calls[0][0][1]).name == 'prefect-first-step-independent'
    assert Path(calls[0][0][2]) == Path(calls[0][0][1]) / 'start-nimble.ps1'
    assert calls[0][1]['is_needed']()
