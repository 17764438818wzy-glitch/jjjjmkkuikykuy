"""Serve the explicitly controlled first-step deployment only."""
from prefect import serve
from prefect.types.entrypoint import EntrypointType
from paperflow_prefect.settings import Settings


if __name__ == '__main__':
    v2 = Settings.load().values.get('first_step_order_model') == 'v2'
    if v2:
        from order_v2_flow import controlled_first_step
    else:
        from first_step_flow import controlled_first_step
    tags = ['first-step', 'manual-only', 'controlled']
    if v2:
        tags.append('paperflow-order-v2-live-20261031')
    deployment = controlled_first_step.to_deployment(
        name='手动运行', concurrency_limit=1, tags=tags, entrypoint_type=EntrypointType.MODULE_PATH)
    name = 'paperflow-first-step-order-v2-live' if v2 else 'paperflow-first-step-controlled'
    serve(deployment, limit=1, name=name, pause_on_shutdown=True)
