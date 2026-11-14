"""Native ProcessWorker admission for the dedicated single-queue Paperflow pool."""
import asyncio
from datetime import datetime, timezone
import json
import os
from uuid import UUID

import anyio
from prefect.client.schemas.filters import FlowRunFilter, WorkPoolFilter, WorkQueueFilter
from prefect.client.schemas.objects import ConcurrencyLeaseHolder
from prefect.client.schemas.responses import WorkerFlowRunResponse
from prefect.client.schemas.sorting import FlowRunSort
from prefect.states import AwaitingRetry
from prefect.utilities.engine import propose_state
from prefect.workers.process import ProcessWorker

from .admission import Admission
from .stage_queue import (GRANT_ENV, LEASE_SECONDS, RESOURCE_LABEL, TOKEN_LABEL,
                          capacity_busy, validate_grant, acquire_stage_lease, release_stage_lease)


def next_resources(run, limits):
    raw = run.labels.get(RESOURCE_LABEL)
    names = tuple(sorted(json.loads(raw))) if raw else tuple(sorted(limits['preprocessing']))
    if names not in {tuple(sorted(value)) for value in limits.values()}:
        raise ValueError('工单阶段通道不属于现有配置')
    return names


async def read_ready_tickets(client, pool_name, queue_name):
    """Paged native reads avoid starvation behind200 equally scheduled tickets.

    Callers must verify this dedicated pool/queue is unpaused and has no whole-
    flow limit. This does not implement a general multi-queue scheduler.
    """
    rows = []
    while True:
        page = await client.read_flow_runs(
            work_pool_filter=WorkPoolFilter(name={'any_': [pool_name]}),
            flow_run_filter=FlowRunFilter(state={'type': {'any_': ['SCHEDULED']}},
                next_scheduled_start_time={'before_': datetime.now(timezone.utc)}),
            work_queue_filter=WorkQueueFilter(name={'any_': [queue_name]}),
            sort=FlowRunSort.ID_DESC, limit=200, offset=len(rows))
        rows.extend(page)
        if len(page) < 200:
            return rows


def capacity_candidates(entries, child_deployment_id, limits, caps):
    available = {name: row.limit - row.active_slots for name, row in caps.items()}
    result = []
    for entry in entries:
        run = entry.flow_run
        if run.deployment_id != child_deployment_id:
            result.append(entry)
            continue
        names = next_resources(run, limits)
        if all(available[name] > 0 for name in names):
            for name in names:
                available[name] -= 1
            result.append(entry)
    return result


class StageProcessWorker(ProcessWorker):
    """Reuse native Pending arbitration, execution and cancellation handling."""
    @classmethod
    def __dispatch_key__(cls):
        return 'paperflow-stage-process'

    def __init__(self, *, child_deployment_id, parent_deployment_id, stage_limits, **kwargs):
        super().__init__(**kwargs)
        self.child_deployment_id = UUID(str(child_deployment_id))
        self.parent_deployment_id = UUID(str(parent_deployment_id))
        self.stage_limits = stage_limits
        self.phase_grants = {}
        self.active_phases = set()
        self.limit_rows = {}
        self.busy_resources = set()
        self.task_running = {}
        self.submission_gate = asyncio.Lock()
        self.admission_decisions = {}

    async def _get_scheduled_flow_runs(self):
        # Preserve native queue/deployment readiness heartbeats. The native
        # endpoint itself is capped at200; public native pagination supplies
        # the remaining tickets for this verified, single unbounded queue.
        await super()._get_scheduled_flow_runs()
        pool = await self.client.read_work_pool(self._work_pool_name)
        queues = await self.client.read_work_queues(work_pool_name=pool.name)
        if len(queues) != 1 or queues[0].name != 'default':
            raise RuntimeError('阶段执行器仅适用于现有独立 default 队列')
        queue = queues[0]
        if pool.concurrency_limit is not None or queue.concurrency_limit is not None:
            raise RuntimeError('阶段队列不允许整单池或队列上限')
        if pool.is_paused or queue.is_paused:
            return []
        names = sorted({name for value in self.stage_limits.values() for name in value})
        rows = await asyncio.gather(*(self.client.read_global_concurrency_limit_by_name(name) for name in names))
        if any(not row.active or row.slot_decay_per_second for row in rows):
            raise RuntimeError('阶段原生并发额度不可用')
        self.limit_rows = {row.name: row for row in rows}
        self.busy_resources.clear()
        self.task_running.clear()
        runs = await read_ready_tickets(self.client, pool.name, queue.name)
        allowed = {self.child_deployment_id, self.parent_deployment_id}
        return [WorkerFlowRunResponse(work_pool_id=pool.id, work_queue_id=queue.id, flow_run=run)
                for run in runs if run.deployment_id in allowed and run.id not in self.active_phases
                and run.empirical_policy.retry_type != 'in_process']

    async def _submit_scheduled_flow_runs(self, flow_run_response):
        submitted = []
        for entry in flow_run_response:
            run = entry.flow_run
            if run.id in self._submitting_flow_run_ids or run.id in self.active_phases:
                continue
            if run.deployment_id == self.child_deployment_id and next_resources(run, self.stage_limits) in self.busy_resources:
                continue
            if not capacity_candidates([entry], self.child_deployment_id, self.stage_limits, self.limit_rows):
                continue
            decision = self.admission_decisions[run.id] = asyncio.Event()
            try:
                rows = await super()._submit_scheduled_flow_runs([entry])
                if rows:
                    # Only a successful lease consumes this poll's capacity.
                    # OS startup remains parallel in the native task group.
                    await decision.wait()
                    submitted.extend(rows)
            finally:
                self.admission_decisions.pop(run.id, None)
        return submitted

    async def _propose_pending_state(self, flow_run):
        async with self.submission_gate:
            try:
                return await self._admit_pending(flow_run)
            except Exception as exc:
                # An unknown lease ACK is not capacity refusal. Preserve this
                # ticket for review without cancelling other running children.
                self.get_flow_run_logger(flow_run).error('阶段准入待核对：%s', type(exc).__name__)
                return False
            finally:
                if decision := self.admission_decisions.get(flow_run.id):
                    decision.set()

    async def _admit_pending(self, flow_run):
        task_id = flow_run.parameters.get('task_id') if flow_run.deployment_id == self.child_deployment_id else str(flow_run.id)
        if task_id not in self.task_running:
            self.task_running[task_id] = await Admission(task_id).running()
        if not self.task_running[task_id]:
            return False
        if flow_run.deployment_id != self.child_deployment_id:
            return await super()._propose_pending_state(flow_run)
        token = flow_run.labels.get(TOKEN_LABEL)
        if token:
            states = await self.client.read_flow_run_states(flow_run.id)
            source = next((row for row in states if str(row.id) == token), None)
            if source is None or any(row.timestamp > source.timestamp and row.is_final() for row in states):
                return False
        names = next_resources(flow_run, self.stage_limits)
        if (flow_run.id in self.active_phases or names in self.busy_resources
                or any(self.limit_rows[name].active_slots >= self.limit_rows[name].limit for name in names)):
            return False
        # Native Pending gives exactly one worker submission rights. A second
        # worker cannot take this run while the atomic stage lease is acquired.
        if not await super()._propose_pending_state(flow_run):
            return False
        try:
            lease = await acquire_stage_lease(self.client, names, flow_run.id)
        except Exception as exc:
            if not capacity_busy(exc):
                raise
            self.busy_resources.add(names)
            # This worker owns Pending; no infrastructure or request was begun.
            from .stage_queue import return_unstarted_to_queue
            await return_unstarted_to_queue(flow_run, self.client)
            return False
        expected = {name: str(self.limit_rows[name].id) for name in names}
        try:
            validate_grant(expected, lease.limits)
        except Exception:
            await release_stage_lease(self.client, lease.lease_id)
            raise
        self.phase_grants[flow_run.id] = {
            'flow_run_id': str(flow_run.id), 'previous_run_count': flow_run.run_count,
            'lease_id': str(lease.lease_id), 'limit_ids': expected, 'names': list(names),
            'token': flow_run.labels.get(TOKEN_LABEL, '')}
        self.active_phases.add(flow_run.id)
        for name in names:
            self.limit_rows[name].active_slots += 1
        return True

    async def _submit_run(self, flow_run):
        try:
            await super()._submit_run(flow_run)
        finally:
            if decision := self.admission_decisions.get(flow_run.id):
                decision.set()
            # Before run() starts there is no business process. Afterwards the
            # isolated child owns renewal/release; cancellation cannot free its
            # capacity while an original HTTP operation is still draining.
            grant = self.phase_grants.get(flow_run.id)
            if grant is not None and not grant.get('process_started'):
                await self._return_grant(flow_run.id, grant)

    async def _return_grant(self, run_id, grant):
        # Native reports a failed startup before its infrastructure coroutine
        # has finished unwinding. Both callers must await one lease return.
        async def release():
            lease_id = UUID(grant['lease_id'])
            await release_stage_lease(self.client, lease_id)
            if self.phase_grants.get(run_id) is grant:
                self.phase_grants.pop(run_id, None)
                self.active_phases.discard(run_id)
        if '_return_task' not in grant:
            grant['_return_task'] = asyncio.create_task(release())
        await asyncio.shield(grant['_return_task'])

    async def run(self, flow_run, configuration, task_status=anyio.TASK_STATUS_IGNORED):
        grant = self.phase_grants.get(flow_run.id)
        if grant is None:
            return await super().run(flow_run, configuration, task_status)
        configuration = configuration.model_copy(deep=True)
        configuration.env[GRANT_ENV] = json.dumps({key: value for key, value in grant.items() if not key.startswith('_')})
        original_status = task_status
        class ProcessStatus:
            def started(self, pid):
                grant['process_started'] = True
                original_status.started(pid)
        task_status = ProcessStatus()
        try:
            result = await super().run(flow_run, configuration, task_status)
            return result
        finally:
            if not grant.get('process_started'):
                await self._return_grant(flow_run.id, grant)
            # Ownership transferred on process start. Releasing here again
            # could replay a child's lost ACK and decrement the same lease twice.
            # The child returns its lease; native expiry handles an OS loss.
            elif self.phase_grants.get(flow_run.id) is grant:
                self.phase_grants.pop(flow_run.id, None)
                self.active_phases.discard(flow_run.id)


def main():
    import argparse
    from .flows import load_runtime, resource_limits
    parser = argparse.ArgumentParser(description='Paperflow native stage process worker')
    parser.add_argument('--pool', required=True)
    args = parser.parse_args()
    load_runtime()
    worker = StageProcessWorker(work_pool_name=args.pool, create_pool_if_not_found=False,
        child_deployment_id=os.environ['PAPERFLOW_V3_CHILD_DEPLOYMENT_ID'],
        parent_deployment_id=os.environ['PAPERFLOW_V3_PARENT_DEPLOYMENT_ID'], stage_limits=resource_limits())
    asyncio.run(worker.start())


if __name__ == '__main__':
    main()
