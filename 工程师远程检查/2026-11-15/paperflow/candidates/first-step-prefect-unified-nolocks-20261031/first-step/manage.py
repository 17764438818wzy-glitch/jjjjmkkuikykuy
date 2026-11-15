"""Serve the first-step deployment controlled by the task buttons."""
from prefect import serve
from prefect.types.entrypoint import EntrypointType
from first_step_flow import controlled_first_step


if __name__ == '__main__':
    controlled = controlled_first_step.to_deployment(
        name='手动运行', concurrency_limit=1, tags=['first-step', 'manual-only', 'controlled'],
        entrypoint_type=EntrypointType.MODULE_PATH)
    serve(controlled, limit=1, name='paperflow-first-step-controlled', pause_on_shutdown=True)
