"""Run the universal queue with the verified education adapter."""
from __future__ import annotations

import argparse
import json
import msvcrt
import re
import time
from pathlib import Path

from auto_config import load_key, load_config
from education_stream_ingest import checked_copy, ready_items
from education_stream_status import IMAGE_NAME, PAPER_NAME
from ingest_education import EducationArtifacts, classify_education, education_specs
from ingest_contracts import Outcome
from ingest_gateway import RagflowGateway
from ingest_queue import QueueStore, VersionConflict
from ingest_scheduler import WorkPool
from ingest_workbench import classify_finance
from ingest_execution_diagnostics import IngestionLogReader
from ragflow_client import RagflowClient


ROOT = Path(__file__).resolve().parent


def configured_source_receiver(data_root: Path):
    """Only trusted host settings choose argv; package content never selects code."""
    from ingest_source_metadata import SourceReceiverCLI
    command = load_config(Path(data_root)).get('source_receiver_command')
    return SourceReceiverCLI(command) if command else None


def classify_task(task: dict, remote: dict) -> Outcome:
    policy=task.get('policy_key')
    if policy=='native_naive':
        # Native vector parsing supplies a technical receipt, not a paper quality verdict.
        chunks=remote.get('chunk_count')
        return (Outcome.accepted if remote.get('run')=='DONE' and
                isinstance(chunks,int) and not isinstance(chunks,bool) and chunks>0
                else Outcome.technical_review)
    if policy=='education':
        return classify_education(task,remote)
    if policy=='finance':
        return classify_finance(task,remote)
    return Outcome.accepted if remote.get('run')=='DONE' else Outcome.technical_review


def configured_router(queue, data_root: Path, service_url: str, client_factory,
                      routes_path: Path | None = None):
    from ingest_model_routes import load_routes
    from ingest_dual_router import DeepSeekRouter
    from ingest_remote_activity import RagflowInFlightCounts
    routes=load_routes(routes_path or data_root/'private/education_dual_channel/routes.json',service_url)
    if not routes:
        return None,None
    return DeepSeekRouter(queue,routes),RagflowInFlightCounts(client_factory,queue,routes)


def enqueue_education_batch(queue: QueueStore, root: Path, run_id: str,
                            deployment: dict, service_url: str,
                            pilot_ids: set[str],is_data_root: bool=False) -> dict:
    if not re.fullmatch(r'[A-Za-z0-9_-]+', run_id):
        raise ValueError('Invalid preprocessing batch ID')
    data = Path(root) if is_data_root else Path(root) / 'data'
    legacy=data/'private'/'education_full_ingest'/run_id
    owned=None
    if (legacy/'receipts.json').exists():
        from education_dual_adopt import legacy_locked
        from upload_tracking import service_key
        marker=legacy/'queue_owner.json'
        owned=json.loads(marker.read_text(encoding='utf-8')) if marker.exists() else None
        if (not owned or owned.get('queue')!=str(queue.path.resolve())
                or owned.get('service_key')!=service_key(service_url) or legacy_locked(legacy/'worker.lock')):
            raise ValueError('Batch already belongs to the active legacy education importer')
    batch_dir = data / 'runs' / run_id
    batch = json.loads((batch_dir / 'manifest.json').read_text(encoding='utf-8'))
    if batch.get('run_id') != run_id:
        raise ValueError('Preprocessing manifest batch ID mismatch')
    copies = data / 'private' / 'education_work_pool' / run_id / 'canonical_packages'
    papers = 0
    images = 0
    errors = []
    for item in ready_items(batch, pilot_ids, {}):
        previous_dataset_id = deployment.get('previous_paper_dataset_id')
        if previous_dataset_id and queue.find_identity(
                service_url, previous_dataset_id, item['paper_id'], 'paper_primary'):
            continue
        if owned and queue.find_identity(service_url,deployment['paper_dataset_id'],item['paper_id'],'paper_primary'):
            continue
        try:
            package, _ = checked_copy(item, batch_dir, copies)
            tasks = education_specs(item, deployment, package, service_url, run_id)
            for task in tasks:
                queue.enqueue(task)
                images += int(task.lane == 'image')
            papers += 1
        except (ValueError, OSError, KeyError, VersionConflict) as exc:
            errors.append({'document_id': item['id'], 'kind': type(exc).__name__})
    return {'papers': papers, 'images': images, 'errors': len(errors),
            'error_ids': errors, 'batch_status': batch.get('status')}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue', required=True, type=Path)
    parser.add_argument('--data-root',type=Path,default=ROOT/'data')
    parser.add_argument('--run-id')
    parser.add_argument('--policy', required=True, choices=['education','all'])
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--parse-limit', type=int, default=3)
    parser.add_argument('--routes',type=Path)
    args = parser.parse_args()
    if str(args.queue).startswith('\\\\'):
        raise ValueError('SQLite work queue must be on a local disk')
    data = args.data_root.resolve()
    settings = json.loads((data / 'settings.json').read_text(encoding='utf-8'))
    service_url = settings['ragflow_url']
    api_key = load_key(data, 'ragflow')
    if not api_key:
        raise ValueError('RAGFlow credential is not available for this Windows account')
    client_factory = lambda: RagflowClient(service_url, api_key)
    if args.policy=='education' and not args.run_id:
        raise ValueError('Education intake requires --run-id')
    deployment=None
    pilot_ids=set()
    if args.policy=='education':
        pilot_dir = data / 'private' / 'education_pilot'
        deployment = json.loads((pilot_dir / 'deployment.json').read_text(encoding='utf-8'))
        pilot_ids = {row['stable_paper_id'] for row in json.loads(
            (pilot_dir / 'selection.json').read_text(encoding='utf-8'))}
        datasets = {row['id']: row['name'] for row in client_factory().datasets()}
        if (datasets.get(deployment['paper_dataset_id']) != PAPER_NAME
                or datasets.get(deployment['image_dataset_id']) != IMAGE_NAME
                or deployment['paper_dataset_id'] == '01885874b4e311f1807c3d9794ef0978'):
            raise ValueError('Education dataset destination identity mismatch')
    queue = QueueStore(args.queue)
    lock_path = args.queue.with_suffix(args.queue.suffix + '.worker.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a+b') as lock:
        lock.seek(0)
        lock.write(b'0')
        lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            raise RuntimeError('Universal ingest queue already has a worker') from None
        output_dir = data / 'private' / 'education_work_pool' / (args.run_id or 'queued')
        execution_reader=IngestionLogReader(client_factory,
            cache_dir=data/'private'/'education_work_pool'/'execution_logs')
        education_artifacts=EducationArtifacts(client_factory, output_dir,source_reader=execution_reader)
        from ingest_research_profile import ProfileArtifacts
        profile_artifacts=ProfileArtifacts(client_factory,data)
        router,activity_reader=configured_router(queue,data,service_url,client_factory,args.routes)
        def on_accepted(task, remote):
            if task['policy_key']=='education':
                education_artifacts(task,remote)
            elif task['policy_key'] in ('research_profile','research_material') and task['role']=='paper_primary':
                profile_artifacts(task,remote)
        def on_invalidated(task):
            if task['policy_key']=='education':
                education_artifacts.invalidate(task)
            elif task['policy_key'] in ('research_profile','research_material'):
                profile_artifacts.invalidate(task)
        pool = WorkPool(queue, lambda: RagflowGateway(client_factory,
                        baseline_recorder=queue.record_parse_baseline,before_parse=on_invalidated),
                        classify_task,
                        on_accepted_for=on_accepted,
                        parse_limit=min(4,max(1,args.parse_limit),router.total_limit if router else 4),router=router,
                        external_counts_reader=activity_reader,
                        diagnostics_reader=execution_reader,on_invalidated_for=on_invalidated,
                        on_source_metadata=configured_source_receiver(data))
        try:
            while True:
                try:
                    intake=(enqueue_education_batch(queue, data, args.run_id,
                                                     deployment, service_url, pilot_ids,is_data_root=True)
                            if args.policy=='education' else {'mode':'workbench_queue'})
                    if router and router.routes.get('watch_batches'):
                        pilot_dir=data/'private/education_pilot'
                        dep=json.loads((pilot_dir/'deployment.json').read_text(encoding='utf-8'))
                        pilot={row['stable_paper_id'] for row in json.loads((pilot_dir/'selection.json').read_text(encoding='utf-8'))}
                        intake={'owned_batches':[enqueue_education_batch(queue,data,run,dep,service_url,pilot,is_data_root=True)
                                                 for run in router.routes['watch_batches']]}
                    progress = pool.tick()
                    if router:
                        progress['model_routes']=router.snapshot()
                    print(json.dumps({'intake': intake, 'queue': progress},
                                     ensure_ascii=False), flush=True)
                    if args.once:
                        break
                    time.sleep(5 if progress['parse_waiting'] else 15)
                except Exception as exc:
                    # Only error classes: no server text, credential or paper contents.
                    print(json.dumps({'worker_error':type(exc).__name__}),flush=True)
                    if args.once:
                        raise
                    time.sleep(5)
        finally:
            pool.close()


if __name__ == '__main__':
    main()
