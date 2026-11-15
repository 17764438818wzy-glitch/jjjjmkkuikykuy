"""Select the configured first-step implementation without changing it."""
from paperflow_prefect.settings import Settings

if Settings.load().values.get('first_step_order_model') == 'v2':
    from order_v2_flow import controlled_first_step, first_step
else:
    from legacy_first_step_flow import controlled_first_step, first_step
