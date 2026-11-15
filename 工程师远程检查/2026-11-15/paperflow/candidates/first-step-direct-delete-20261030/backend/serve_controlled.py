"""Serve the explicitly controlled first-step deployment only."""
from prefect import serve
from prefect.types.entrypoint import EntrypointType
from first_step_flow import controlled_first_step


if __name__ == '__main__':
    deployment = controlled_first_step.to_deployment(
        name='手动运行', concurrency_limit=1, tags=['first-step', 'manual-only', 'controlled'],
        entrypoint_type=EntrypointType.MODULE_PATH)
    serve(deployment, limit=1, name='paperflow-first-step-controlled', pause_on_shutdown=True)
