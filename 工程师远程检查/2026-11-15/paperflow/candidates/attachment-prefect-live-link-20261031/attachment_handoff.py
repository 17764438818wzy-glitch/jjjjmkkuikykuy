"""Forward registered attachments to the existing LLM stage without preprocessing."""
import copy

from local_prepare import _observe_registered
from paperflow_prefect.workflow import execute_stage3


async def dispatch_registered_attachments(item, *, receipt_root, call):
    from paperflow_prefect.stage3_handoff import report_unreceived_attachments
    observed = _observe_registered(item, receipt_root)
    paper = copy.deepcopy(observed.get('paper') or {
        'paper_id': observed['paper_id'], 'body': None, 'attachments': []})
    paper['attachment_input_policy'] = 'upstream_only'
    result = await execute_stage3(paper, call, attachments_only=True)
    return report_unreceived_attachments(observed, result)
