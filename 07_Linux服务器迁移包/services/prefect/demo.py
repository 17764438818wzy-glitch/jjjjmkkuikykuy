"""Prefect UI preview only: synthetic inputs, no model/RAG/NAS/file-library access."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import threading
import time

from prefect import flow, get_run_logger, serve, task
from prefect.artifacts import create_markdown_artifact
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.sync import concurrency
from prefect.exceptions import ObjectNotFound
from prefect.runtime import flow_run
from prefect.task_runners import ThreadPoolTaskRunner


NOTICE = "纯模拟演示：没有读取真实论文，没有调用 LLM、RAG 或 NAS；真实论文文件不变。"
OUTPUT = Path('/demo-output')


def save_result(kind: str, data: dict) -> dict:
    OUTPUT.mkdir(exist_ok=True)
    data = dict(data, simulation_only=True, business_external_calls=0, notice=NOTICE)
    path = OUTPUT / f'{kind}-{flow_run.id}.json'
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    return data


@task(name='① 接收整理目录（模拟）', tags=['demo', 'intake'])
def intake(index: int) -> dict:
    time.sleep(0.3)
    return {'paper_id': f'DEMO-{index:03d}', 'title': f'演示论文 {index}', 'attachments': 1}


@task(name='② 本机预处理正文（模拟）', tags=['demo', 'local'])
def prepare_body(paper: dict) -> dict:
    time.sleep(1)
    return dict(paper, body_material='模拟正文材料', source_blocks=12)


@task(name='② 附件简单识别（模拟）', tags=['demo', 'attachment'])
def identify_attachment(paper: dict) -> dict:
    time.sleep(1.5)
    return {'paper_id': paper['paper_id'], 'description': 'Python 程序源码工程（模拟）'}


@task(name='③ MD 正文画像（模拟）', tags=['demo', 'body'])
def profile_body(material: dict) -> dict:
    time.sleep(2)
    return dict(material, profile='仅正文画像（模拟）', md_template_used=False)


@task(name='④ 正文与画像入库核验（模拟）', tags=['demo', 'rag'])
def publish_body(profile: dict) -> dict:
    time.sleep(1)
    return {'paper_id': profile['paper_id'], 'body_verified_simulation': True}


@task(name='④ 附件说明入库核验（模拟）', tags=['demo', 'rag'])
def publish_attachment(attachment: dict) -> dict:
    time.sleep(0.7)
    return {'paper_id': attachment['paper_id'], 'attachment_verified_simulation': True}


@task(name='⑤ NAS 归档核验（模拟）', tags=['demo', 'nas'])
def archive_originals(body_receipt: dict, attachment_receipt: dict) -> dict:
    assert body_receipt['paper_id'] == attachment_receipt['paper_id']
    time.sleep(1)
    return {'paper_id': body_receipt['paper_id'], 'nas_verified_simulation': True, 'local_folder_retained': True}


@task(name='保存完成记录（模拟）', tags=['demo', 'record'])
def complete_paper(archive_receipt: dict) -> dict:
    return dict(archive_receipt, record_saved_simulation=True)


@flow(name='Paperflow Five Steps DEMO', description=NOTICE,
      task_runner=ThreadPoolTaskRunner(max_workers=32), log_prints=True)
def five_steps_demo(paper_count: int = 3) -> dict:
    if not 1 <= paper_count <= 10:
        raise ValueError('此预览每次仅允许 1 到 10 篇模拟论文')
    print(NOTICE)
    final = []
    for index in range(1, paper_count + 1):
        paper = intake.submit(index)
        material = prepare_body.submit(paper)
        attachment = identify_attachment.submit(paper)
        profile = profile_body.submit(material)
        body_receipt = publish_body.submit(profile)
        attachment_receipt = publish_attachment.submit(attachment)
        archived = archive_originals.submit(body_receipt, attachment_receipt)
        final.append(complete_paper.submit(archived))
    rows = [future.result() for future in final]
    result = save_result('five-steps', {'papers': rows, 'paper_count': len(rows)})
    create_markdown_artifact(
        key='paperflow-preview-five-steps',
        markdown=f'# 五步流程演示\n\n{NOTICE}\n\n完成 {len(rows)} 篇模拟论文。'
                 '\n\n正文 MD 画像和附件简单识别分开；两路模拟入库后再归档；本地保留。',
        description='演示结果，不是真实论文处理记录',
    )
    return result


active = {'body': 0, 'attachment': 0}
peak = {'body': 0, 'attachment': 0, 'total': 0}
counter_lock = threading.Lock()


@task(name='模拟 LLM 等待', tags=['demo', 'simulated-llm'],
      task_run_name='{kind}-{index:03d}', persist_result=False)
def simulated_request(kind: str, index: int, delay_seconds: int) -> dict:
    # These limits belong only to the isolated preview Prefect server.
    with concurrency(['demo-total-llm', f'demo-{kind}-llm'], occupy=1, strict=True):
        with counter_lock:
            active[kind] += 1
            peak[kind] = max(peak[kind], active[kind])
            peak['total'] = max(peak['total'], sum(active.values()))
        started = time.time()
        try:
            time.sleep(delay_seconds)
            return {'kind': kind, 'index': index, 'started': started, 'finished': time.time(), 'simulated': True}
        finally:
            with counter_lock:
                active[kind] -= 1


@flow(name='LLM Concurrency DEMO 100 plus 20', description=NOTICE,
      task_runner=ThreadPoolTaskRunner(max_workers=128), log_prints=True)
def concurrency_demo(body_count: int = 100, attachment_count: int = 20, delay_seconds: int = 20) -> dict:
    if not 1 <= body_count <= 100 or not 0 <= attachment_count <= 20 or not 1 <= delay_seconds <= 60:
        raise ValueError('预览限制：正文 1~100，附件 0~20，模拟等待 1~60 秒')
    with counter_lock:
        active.update(body=0, attachment=0)
        peak.update(body=0, attachment=0, total=0)
    print(NOTICE)
    futures = [simulated_request.submit('body', i, delay_seconds) for i in range(1, body_count + 1)]
    futures += [simulated_request.submit('attachment', i, delay_seconds) for i in range(1, attachment_count + 1)]
    rows = [future.result() for future in futures]
    result = save_result('concurrency', {'body_tasks': body_count, 'attachment_tasks': attachment_count,
                                        'observed_simulated_peak': dict(peak), 'requests': rows})
    create_markdown_artifact(
        key='paperflow-preview-concurrency',
        markdown=f'# 并发演示\n\n{NOTICE}\n\n正文模拟任务 {body_count}，附件模拟任务 {attachment_count}。'
                 f'\n\n观察到的模拟同时等待峰值：正文 {peak["body"]}，附件 {peak["attachment"]}，总计 {peak["total"]}。'
                 '\n\n这些数字不是实际 LLM 请求并发，也不是生产吞吐验收。',
        description='只模拟等待，不消耗模型额度',
    )
    get_run_logger().info('Simulation result: %s', {k: v for k, v in result.items() if k != 'requests'})
    return result


def ensure_demo_limits():
    with get_client(sync_client=True) as api:
        for name, limit in [('demo-body-llm', 100), ('demo-attachment-llm', 20), ('demo-total-llm', 120)]:
            try:
                existing = api.read_global_concurrency_limit_by_name(name)
            except ObjectNotFound:
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name, limit=limit, active=True))
            else:
                if existing.limit != limit or not existing.active:
                    raise RuntimeError(f'Preview concurrency setting changed: {name}')


if __name__ == '__main__':
    ensure_demo_limits()
    if len(sys.argv) > 1 and sys.argv[1] == 'serve':
        serve(
            five_steps_demo.to_deployment(name='five-step-demo', tags=['demo', 'no-real-data'], concurrency_limit=1),
            concurrency_demo.to_deployment(name='body100-attachments20', tags=['demo', 'no-model-cost'], concurrency_limit=1),
            limit=2,
        )
    else:
        five_steps_demo()
        concurrency_demo()
