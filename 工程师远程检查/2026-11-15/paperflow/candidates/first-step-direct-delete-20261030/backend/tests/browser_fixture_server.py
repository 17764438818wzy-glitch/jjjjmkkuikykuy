"""Isolated, synthetic browser fixture; never connects to real Prefect or Nimble."""
from pathlib import Path
import sys
import tempfile
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import uvicorn

from paperflow_prefect import controlled_nimble
from paperflow_prefect.controlled_first_step import run_first_step
from paperflow_prefect.first_step_control_api import install_control_routes
from paperflow_prefect.group_task_summary import install_group_summary_routes


class Judge:
    def __init__(self, *args, **kwargs):
        pass
    def choose_body(self, records):
        return {'choice': records[0]['file_id']}
    def choose_other(self, record, body):
        return {'choice': 'attachment'}
    def choose_filename_title(self, filename):
        return None


controlled_nimble.ControlledNimbleJudge = Judge
temporary = tempfile.TemporaryDirectory(prefix='first-step-browser-')
root = Path(temporary.name)
source = root / 'original'
group = source / 'student'
group.mkdir(parents=True)
(group / 'paper.txt').write_text('题目：测试论文\n正文测试', encoding='utf-8')
(group / 'data.csv').write_text('item,value\nfixture,1\n', encoding='utf-8')


class Scheduler:
    control = None
    def start(self, task_id, generation):
        thread = threading.Thread(target=run_first_step,
            args=(self.control, task_id, generation, 'browser-fixture'),
            kwargs={'endpoint': 'http://unused'}, daemon=True)
        thread.start()
        return {'flow_run_id': 'browser-fixture'}


scheduler = Scheduler()
app = FastAPI()
control = install_control_routes(app, root / 'output', root / 'data', scheduler=scheduler)
scheduler.control = control
install_group_summary_routes(app, root / 'output', refresh_task=control.refresh_task)


@app.get('/api-config.json')
def config():
    return {'api_base_url': 'http://127.0.0.1:14765', 'open_current': True}


@app.get('/fixture-info')
def info():
    return {'source': str(source), 'root': str(root), 'original_files': sum(1 for p in source.rglob('*') if p.is_file()),
            'artifact_files': sum(1 for p in (root / 'output' / 'tasks').rglob('*') if p.is_file())}


frontend = Path(__file__).resolve().parents[2] / 'frontend'
app.mount('/', StaticFiles(directory=frontend, html=True), name='frontend')


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=14765, log_level='warning')
