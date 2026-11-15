"""Submit a controlled first-step Prefect run only on an explicit start."""
import json
from urllib.parse import quote
from urllib.request import Request, urlopen


class SchedulerNotSubmitted(Exception):
    """The deployment could not be resolved, before any start was submitted."""


class PrefectScheduler:
    def __init__(self, api_url):
        self.api_url = api_url.rstrip('/')

    def start(self, task_id, generation):
        name = quote('论文第一步-可控预处理', safe='') + '/' + quote('手动运行', safe='')
        try:
            with urlopen(self.api_url + '/deployments/name/' + name, timeout=10) as response:
                deployment = json.load(response)
        except Exception as exc:
            raise SchedulerNotSubmitted('未连接到受控部署') from exc
        payload = json.dumps({'parameters': {'task_id': task_id, 'generation': generation},
                              'idempotency_key': f'first-step-{task_id}-{generation}'}).encode('utf-8')
        request = Request(self.api_url + '/deployments/' + deployment['id'] + '/create_flow_run',
                          data=payload, headers={'Content-Type': 'application/json'}, method='POST')
        with urlopen(request, timeout=10) as response:
            run = json.load(response)
        return {'flow_run_id': str(run['id'])}
