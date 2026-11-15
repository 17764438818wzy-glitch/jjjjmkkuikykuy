"""Serve only the independent first-step Prefect deployment."""
from prefect import serve
from prefect.types.entrypoint import EntrypointType
from first_step_flow import controlled_first_step, first_step


if __name__ == '__main__':
    deployment = first_step.to_deployment(
        name='第一步-手动运行', parameters={'person_folders': [], 'verified_new_groups': []},
        concurrency_limit=1, tags=['first-step', 'manual-only'],
        entrypoint_type=EntrypointType.MODULE_PATH)
    controlled = controlled_first_step.to_deployment(
        name='手动运行', concurrency_limit=1, tags=['first-step', 'manual-only', 'controlled'],
        entrypoint_type=EntrypointType.MODULE_PATH)
    serve(deployment, controlled, limit=1, name='paperflow-first-step-independent', pause_on_shutdown=True)
