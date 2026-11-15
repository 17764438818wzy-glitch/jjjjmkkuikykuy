"""Serve only the independent first-step Prefect deployment."""
from prefect import serve
from prefect.types.entrypoint import EntrypointType
from first_step_flow import first_step


if __name__ == '__main__':
    deployment = first_step.to_deployment(
        name='第一步-手动运行', parameters={'person_folders': [], 'verified_new_groups': []},
        concurrency_limit=1, tags=['first-step', 'manual-only'],
        entrypoint_type=EntrypointType.MODULE_PATH)
    serve(deployment, limit=1, name='paperflow-first-step-independent', pause_on_shutdown=True)
