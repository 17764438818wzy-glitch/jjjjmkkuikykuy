"""Read saved task views without controlling the underlying Prefect runs."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import httpx
from async_lru import alru_cache
from fastapi import HTTPException
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import AsyncClientContext
from prefect.exceptions import ObjectNotFound

from .admission import Admission
from .stage_admission import pause_execution_progress as pause_progress
from .parent import _children
from .snapshots import load_task_snapshot, read_task_summaries
from .task_state import task_state


def _merge_saved_folders(saved_folders, active_folders):
    active = {row["ticket_id"]: row for row in active_folders}
    fields = {
        "preprocessing": ("preprocessing_result", "preprocessing_detail"),
        "llm": ("llm_result", "llm_detail", "llm_body_running", "llm_attachment_running"),
        "postprocessing": ("postprocessing_result", "postprocessing_detail"),
        "cleanup": ("cleanup_status",),
    }
    folders = []
    for row in saved_folders:
        progress = active.get(row["ticket_id"])
        if not progress:
            folders.append(row)
            continue
        saved_stages = row.get("stage_snapshots", {})
        progress_stages = progress.get("stage_snapshots", {})
        folder = {**row, **progress, "stage_snapshots": {**progress_stages, **saved_stages}}
        for stage, snapshot in saved_stages.items():
            earlier = progress_stages.get(stage)
            changed = earlier is None or any(
                snapshot.get(name) != earlier.get(name)
                for name in ("result_version", "processing_attempt_id")
            )
            if snapshot.get("final") or changed:
                for field in fields.get(stage, ()):
                    if field in row:
                        folder[field] = row[field]
                progress_stage = progress.get("stage", "")
                confirmed_unknown = (progress_stage == "unresolved" and snapshot.get("final")
                                     and earlier is not None and not earlier.get("final"))
                if changed and (progress_stage.startswith(stage + "_") or
                                progress_stage == "waiting_" + stage or
                                progress_stage.startswith("waiting_" + stage + "_") or
                                progress.get("failed_stage") == stage or confirmed_unknown):
                    folder["stage"] = row["stage"]
                    for field in ("failed_stage", "failure_detail", "last_retry", "retryable", "retry_reason"):
                        if field in row:
                            folder[field] = row[field]
        folders.append(folder)
    return folders


def _older_than(view, other):
    if not view.get("as_of") or not other.get("as_of"):
        return False
    timestamp = datetime.fromisoformat(view["as_of"])
    previous_timestamp = datetime.fromisoformat(other["as_of"])
    if timestamp != previous_timestamp:
        return timestamp < previous_timestamp
    folders = {row["ticket_id"]: row for row in view.get("folders", [])}
    for previous in other.get("folders", []):
        stages = folders.get(previous["ticket_id"], {}).get("stage_snapshots", {})
        for stage, known in previous.get("stage_snapshots", {}).items():
            current = stages.get(stage)
            if (current and known.get("final") and known.get("recorded_at") and current.get("recorded_at")
                    and known.get("processing_attempt_id") == current.get("processing_attempt_id")
                    and datetime.fromisoformat(known["recorded_at"]) > datetime.fromisoformat(current["recorded_at"])):
                return True
    return False


def _view_task_state(run, running, snapshot):
    if run.state.is_failed() or run.state.is_crashed():
        return "failed"
    if not run.state.is_completed():
        if run.state.is_paused():
            return ("unresolved" if running else "created" if run.run_count == 0 else
                    snapshot.get("pause_progress", {}).get("state", "paused"))
        if not running:
            return snapshot.get("pause_progress", {}).get("state", "paused")
        if run.state.is_scheduled() or run.state.is_pending():
            return "waiting"
    groups = snapshot["items"]
    missing_evidence = any(row["status"] == "passed" and
                           (row.get("delivery_complete") is None or
                            row.get("registration_status") is None) for row in groups)
    if missing_evidence:
        return "unresolved"
    state = task_state(snapshot.get("classification_complete") is True, groups, snapshot["folders"],
                       "running" if running else "paused", snapshot.get("complete") is True)
    if state == "completed" and snapshot.get("has_pending") is not False:
        return "unresolved"
    if run.state.is_completed():
        return "completed" if state == "completed" else "unresolved"
    return state


@asynccontextmanager
async def _owned_prefect_context():
    token = AsyncClientContext.__var__.set(None)
    try:
        context = AsyncClientContext()
        entered = False
        try:
            async with context:
                entered = True
                yield
        finally:
            # Prefect 3.9 enters its client before checking the API version;
            # if that check fails, its context manager never calls __aexit__.
            if not entered:
                await context.client.__aexit__(None, None, None)
    finally:
        AsyncClientContext.__var__.reset(token)


class QueryService:
    def __init__(self, settings):
        self.deployment_id = UUID(str(settings["deployment_id"]))
        self.recovery_scopes = settings.get("recovery_scopes", False)
        from .retry import read_rag_retry_settings
        self.rag_retry_settings = read_rag_retry_settings(settings.get("project_config"))
        self._generation = {}
        self._list_generation = 0
        self._full_generation = {}
        self._folders = {}
        self._folder_versions = {}
        self._last = {}
        self._last_list = None
        self._last_classifications = {}
        self._history_tasks = {}
        self._deleted = set()
        self._snapshot_writers = {}
        self._query_reads = {}
        self._detail_reads = alru_cache(maxsize=None, ttl=0)(self._read_detail)
        self._list_reads = alru_cache(maxsize=None, ttl=0)(self._read_list)
        self._classification_reads = alru_cache(maxsize=None, ttl=0)(self._read_classifications)
        self._refresh = alru_cache(
            maxsize=None, ttl=10,
            key=lambda task_id, generation, folder_id, folder_generation, children:
                (task_id, generation, folder_id, folder_generation),
        )(self._refresh_snapshot)

    async def _load_snapshot(self, task_id, children, *, repair):
        options = {"repair": repair}
        if self.rag_retry_settings:
            options["rag_retry_settings"] = self.rag_retry_settings
        return await load_task_snapshot(task_id, children, **options)

    async def _refresh_snapshot(self, task_id, generation, folder_id, folder_generation, children):
        async with self._track_query(task_id, writer=True):
            # A refresh can outlive the first HTTP waiter, so it owns its client context.
            try:
                async with _owned_prefect_context():
                    return await self._load_snapshot(UUID(task_id), children, repair=True)
            except (httpx.HTTPError, OSError, TimeoutError):
                return None
            except RuntimeError as error:
                # Prefect wraps a failed first API-version request in RuntimeError.
                if isinstance(error.__cause__, (httpx.HTTPError, OSError, TimeoutError)):
                    return None
                raise

    def _stale_task(self, key, saved=None):
        if key in self._last:
            last = self._last[key]
            if saved is not None and not _older_than(saved, last):
                last = {**last, "folders": _merge_saved_folders(saved["folders"], last["folders"])}
                self._last[key] = last
            return {**last, "query_status": "stale", "has_pending": None}
        return {"task_id": key, "as_of": None, "complete": False,
                "has_pending": None, "query_status": "unavailable"}

    def _missing_history_ids(self, snapshot):
        if "history_targets" in snapshot:
            return set(snapshot["history_targets"])
        folders = {folder["ticket_id"]: folder for folder in snapshot["folders"]}
        return {
            str(row["ticket_id"])
            for row in snapshot.get("missing", [])
            if row.get("ticket_id") and (row["ticket_id"] not in folders or
               row["stage"] not in folders[row["ticket_id"]].get("stage_snapshots", {}))
        }

    def _needs_history(self, snapshot):
        return bool(self._missing_history_ids(snapshot))

    async def list_tasks(self):
        generation = self._list_generation
        result = await self._list_reads(generation)
        # Let async-lru expire a completed zero-TTL entry before the next read.
        await asyncio.sleep(0)
        if generation != self._list_generation:
            return await self.list_tasks()
        if "tasks" in result:
            return {**result, "tasks": [row for row in result["tasks"] if row["task_id"] not in self._deleted]}
        return result

    async def _read_list(self, generation):
        try:
            async with _owned_prefect_context():
                if generation != self._list_generation:
                    return await self.list_tasks()
                return await self._list_tasks(generation)
        except (httpx.HTTPError, OSError, TimeoutError, RuntimeError) as error:
            if isinstance(error, RuntimeError) and not isinstance(
                    error.__cause__, (httpx.HTTPError, OSError, TimeoutError)):
                raise
            if self._last_list is not None:
                return {**self._last_list, "query_status": "stale", "has_pending": None}
            return {"as_of": None, "complete": False, "has_pending": None, "query_status": "unavailable"}

    async def _list_tasks(self, generation):
        try:
            runs = []
            async with get_client() as client:
                while True:
                    page = await client.read_flow_runs(
                        flow_run_filter=FlowRunFilter(deployment_id={"any_": [self.deployment_id]}),
                        limit=200,
                        offset=len(runs),
                    )
                    runs.extend(page)
                    if len(page) < 200:
                        break
            summaries = await read_task_summaries([run.id for run in runs]) if runs else {}
            tasks = [
                {
                    **summaries.get(str(run.id), {}),
                    "task_id": str(run.id),
                    "parent_flow_run_id": str(run.id),
                    "task_name": run.parameters["task_name"],
                    "created_at": run.created.isoformat(),
                }
                for run in runs
            ]
            pending = [row.get("has_pending") for row in tasks]
            has_pending = True if True in pending else False if all(value is False for value in pending) else None
            if generation != self._list_generation:
                return await self.list_tasks()
            self._last_list = {"as_of": datetime.now(timezone.utc).isoformat(), "complete": True,
                               "has_pending": has_pending, "query_status": "updated", "tasks": tasks}
            return self._last_list
        except (httpx.HTTPError, OSError, TimeoutError):
            if self._last_list is not None:
                return {**self._last_list, "query_status": "stale", "has_pending": None}
            return {"as_of": None, "complete": False, "has_pending": None, "query_status": "unavailable"}

    async def task_details(self, task_id):
        if str(task_id) in self._deleted:
            raise HTTPException(404, "Task is being deleted")
        key = str(UUID(str(task_id)))
        generation = self._generation.get(key, 0)
        result = await self._detail_reads(key, generation)
        # Completed views must not conceal a newly persisted terminal result.
        await asyncio.sleep(0)
        if generation != self._generation.get(key, 0):
            return await self.task_details(key)
        return result

    async def _read_detail(self, task_id, generation):
        async with self._track_query(task_id):
            try:
                async with _owned_prefect_context():
                    if generation != self._generation.get(task_id, 0):
                        return await self.task_details(task_id)
                    return await self._task_details(task_id)
            except (httpx.HTTPError, OSError, TimeoutError, RuntimeError) as error:
                if isinstance(error, RuntimeError) and not isinstance(
                        error.__cause__, (httpx.HTTPError, OSError, TimeoutError)):
                    raise
                return self._stale_task(str(UUID(str(task_id))))

    async def _task_details(self, task_id):
        task_id = UUID(str(task_id))
        key = str(task_id)
        generation = self._generation.get(key, 0)
        previous = self._last.get(key)
        try:
            async with get_client() as client:
                try:
                    run = await client.read_flow_run(task_id)
                except ObjectNotFound:
                    raise HTTPException(404, "任务不存在") from None
            if run.deployment_id != self.deployment_id:
                raise HTTPException(404, "任务不存在")
            children = await _children(task_id)
            saved = await self._load_snapshot(task_id, children, repair=False)
            summaries = await read_task_summaries([task_id])
            # Admission and snapshot reads share the request's managed client.
            running = await Admission(task_id).running()
            if not running:
                saved = {**saved, "pause_progress": await pause_progress(task_id, snapshot=saved, verify_legacy=False)}
            # Reuse the existing action rules after api.py has finished loading.
            from .api import _task_actions

            summary = summaries.get(key, {})
            description = {
                **summary,
                "task_id": key, "parent_flow_run_id": key,
                "task_name": run.parameters["task_name"],
                "desired_state": "running" if running else "paused",
                "task_state": _view_task_state(run, running, saved),
                "pause_progress": saved.get("pause_progress"),
                "actions": _task_actions(run, running, children),
                "stop_reason": summary.get("stop_reason"),
            }
            if self.recovery_scopes:
                from .recovery import read_scope, actions
                scope = await read_scope(task_id)
                if scope is not None:
                    description["actions"] = {**actions(scope, children, running), "delete": {"allowed": True, "reason": None}}
                    description["recovery"] = {"mode": "existing_children", "eligible": len(scope["eligible"]),
                                               "blocked": len(scope["blocked"])}
                    if not running:
                        description["stop_reason"] = (f"关机中断后已暂停：仅恢复现有工单；"
                            f"{len(scope['blocked'])} 张原请求待核对，父单剩余计划不派发")
            current = {**description, **saved, "query_status": "updated"}
            if generation != self._generation.get(key, 0):
                return await self.task_details(task_id)
            if key not in self._last and saved.get("as_of") is not None:
                self._last[key] = current

            full_generation = self._full_generation.get(key, 0)
            full_args = (key, full_generation, None, 0, children)
            if self._needs_history(saved):
                targets = self._folders.get(key)
                missing_ids = self._missing_history_ids(saved)
                history_children = ([child for child in children
                                     if child.parameters.get("group_id") in targets or
                                     str(child.id) in missing_ids]
                                    if targets else children)
                tag = ("history",)
                history_key = (key, generation)
                if "history_targets" in saved:
                    history_children = [child for child in children if str(child.id) in missing_ids or
                                        (targets and child.parameters.get("group_id") in targets)]
                    tag = ("history", *sorted(missing_ids | {str(child.id) for child in history_children}))
                    history_key = (key, generation, tag)
                history_args = (key, full_generation, tag, generation, history_children)
                history = self._history_tasks.get(history_key)
                if history is not None and history.done():
                    if history.result() is None and self._refresh.cache_contains(*history_args):
                        return self._stale_task(key, saved)
                if history is None or history.done():
                    self._history_tasks[history_key] = asyncio.create_task(self._refresh(*history_args))
                return {**current, "query_status": "preparing_history"}

            needs_active = ("history_targets" not in saved or any(not child.state.is_final() for child in children))
            if saved.get("has_pending") is not False and needs_active:
                changed_folders = dict(self._folders.get(key, {}))
                reused = self._refresh.cache_contains(*full_args)
                if changed_folders and reused and previous is not None:
                    active = previous
                else:
                    active = await self._refresh(*full_args)
                    if active is None:
                        return self._stale_task(key, saved)
                    if changed_folders == self._folders.get(key, {}):
                        self._folders.pop(key, None)
                        changed_folders = {}
                if generation != self._generation.get(key, 0):
                    return await self.task_details(task_id)
                active_folders = {row["ticket_id"]: row for row in active["folders"]}
                for folder_id, folder_generation in changed_folders.items():
                    scoped_children = [child for child in children
                                       if child.parameters.get("group_id") == folder_id]
                    scoped_args = (key, full_generation, folder_id, folder_generation, scoped_children)
                    reused = self._refresh.cache_contains(*scoped_args) and reused
                    scoped = await self._refresh(*scoped_args)
                    if scoped is None:
                        return self._stale_task(key, saved)
                    for row in scoped["folders"]:
                        if row["folder_id"] == folder_id:
                            active_folders[row["ticket_id"]] = row
                if generation != self._generation.get(key, 0):
                    return await self.task_details(task_id)
                latest = await self._load_snapshot(task_id, children, repair=False)
                if generation != self._generation.get(key, 0):
                    return await self.task_details(task_id)
                folders = _merge_saved_folders(latest["folders"], active_folders.values())
                current = {
                    **description, **latest, "folders": folders,
                    "query_status": "preparing_history" if self._needs_history(latest) else
                    "reused" if reused else "updated",
                }
                current["task_state"] = _view_task_state(run, running, current)
            if current.get("as_of") is not None:
                published = self._last.get(key)
                if published is not None and _older_than(current, published):
                    return {**published, "desired_state": description["desired_state"],
                            "task_state": description["task_state"],
                            "pause_progress": description["pause_progress"],
                            "actions": description["actions"], "query_status": "reused"}
                self._last[key] = current
            return current
        except (httpx.HTTPError, OSError, TimeoutError):
            return self._stale_task(key)
        except RuntimeError as error:
            if isinstance(error.__cause__, (httpx.HTTPError, OSError, TimeoutError)):
                return self._stale_task(key)
            raise

    async def classifications(self, task_id):
        if str(task_id) in self._deleted:
            raise HTTPException(404, "Task is being deleted")
        key = str(UUID(str(task_id)))
        generation = self._generation.get(key, 0)
        result = await self._classification_reads(key, generation)
        await asyncio.sleep(0)
        if generation != self._generation.get(key, 0):
            return await self.classifications(key)
        return result

    async def _read_classifications(self, task_id, generation):
        async with self._track_query(task_id):
            task_id = UUID(str(task_id))
            key = str(task_id)
            if generation != self._generation.get(key, 0):
                return await self.classifications(task_id)
            try:
                async with _owned_prefect_context():
                    async with get_client() as client:
                        try:
                            run = await client.read_flow_run(task_id)
                        except ObjectNotFound:
                            raise HTTPException(404, "任务不存在") from None
                    if run.deployment_id != self.deployment_id:
                        raise HTTPException(404, "任务不存在")
                    saved = await self._load_snapshot(task_id, [], repair=False)
                if generation != self._generation.get(key, 0):
                    return await self.classifications(task_id)
                detail = {**saved, "task_id": key, "query_status": "updated"}
                if saved.get("classification_complete") is not True and (
                        "history_targets" not in saved or key in saved["history_targets"]):
                    # An interrupted parent can legitimately remain incomplete.
                    # Its classification view never needs child metadata or journals.
                    args = (key, self._full_generation.get(key, 0), ("classifications",), generation, [])
                    history_key = (key, generation, "classifications")
                    history = self._history_tasks.get(history_key)
                    if history is not None and history.done() and history.result() is None:
                        detail["query_status"] = "stale"
                    if history is None or (history.done() and not self._refresh.cache_contains(*args)):
                        self._history_tasks[history_key] = asyncio.create_task(self._refresh(*args))
            except (httpx.HTTPError, OSError, TimeoutError, RuntimeError) as error:
                if isinstance(error, RuntimeError) and not isinstance(
                        error.__cause__, (httpx.HTTPError, OSError, TimeoutError)):
                    raise
                previous = self._last_classifications.get(key)
                if previous is not None:
                    return {**previous[1], "query_status": "stale", "has_pending": None}
                return {"task_id": key, "as_of": None, "complete": False,
                        "has_pending": None, "query_status": "unavailable"}
            if generation != self._generation.get(key, 0):
                return await self.classifications(task_id)
            previous = self._last_classifications.get(key)
            if previous is not None and detail["query_status"] in {"stale", "unavailable"}:
                return {**previous[1], "query_status": "stale", "has_pending": None}
            if detail["query_status"] == "unavailable":
                return detail
            items = detail["items"]
            complete = detail.get("classification_complete") is True
            pending = (None if detail["query_status"] == "stale" or
                       detail.get("classification_complete") is None else not complete)
            counts = {
                status: (sum(row["status"] == status for row in items) if complete else
                         sum(row["status"] == status for row in items) or None)
                for status in ("passed", "failed", "unrecognized", "unresolved")
            }
            result = {"task_id": detail["task_id"], "as_of": detail["as_of"],
                      "complete": complete, "has_pending": pending,
                      "query_status": detail["query_status"], "items": items, "counts": counts}
            if result["query_status"] != "stale" and result["as_of"] is not None:
                if previous is not None and previous[0] == generation:
                    last = previous[1]
                    if _older_than(result, last):
                        return {**last, "query_status": "reused"}
                    same_time = datetime.fromisoformat(result["as_of"]) == datetime.fromisoformat(last["as_of"])
                    if same_time and last["complete"] and not result["complete"]:
                        return {**last, "query_status": "stale", "has_pending": None}
                self._last_classifications[key] = (generation, result)
            return result

    async def invalidate(self, task_id, *, folder_id=None):
        key = str(task_id)
        self._list_generation += 1
        old = self._generation.get(key, 0)
        self._generation[key] = old + 1
        full = self._full_generation.get(key, 0)
        if folder_id is None:
            self._full_generation[key] = full + 1
            self._refresh.cache_invalidate(key, full, None, 0, None)
            for folder, version in self._folders.pop(key, {}).items():
                self._refresh.cache_invalidate(key, full, folder, version, None)
        else:
            versions = self._folder_versions.setdefault(key, {})
            previous = versions.get(folder_id, 0)
            versions[folder_id] = previous + 1
            self._folders.setdefault(key, {})[folder_id] = versions[folder_id]
            self._refresh.cache_invalidate(key, full, folder_id, previous, None)


    @asynccontextmanager
    async def _track_query(self, task_id, *, writer=False):
        key = str(task_id)
        if key in self._deleted:
            raise HTTPException(404, "Task is being deleted")
        task = asyncio.current_task()
        tracked = self._snapshot_writers if writer else self._query_reads
        tracked.setdefault(key, set()).add(task)
        try:
            yield
        finally:
            tracked.get(key, set()).discard(task)

    async def forget_task(self, task_id):
        key = str(task_id)
        self._deleted.add(key)
        await self.invalidate(key)
        writers = set(self._snapshot_writers.get(key, set()))
        writers.discard(asyncio.current_task())
        if writers:
            await asyncio.wait_for(asyncio.shield(asyncio.gather(*writers, return_exceptions=True)), timeout=120)
        self._snapshot_writers.pop(key, None)
        pending = set(self._query_reads.pop(key, set()))
        for history_key, task in list(self._history_tasks.items()):
            if history_key[0] == key:
                pending.add(task)
                self._history_tasks.pop(history_key, None)
        pending.discard(asyncio.current_task())
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        self._last.pop(key, None)
        self._last_classifications.pop(key, None)
        self._folder_versions.pop(key, None)
        if self._last_list is not None:
            self._last_list = {**self._last_list,
                "tasks": [row for row in self._last_list["tasks"] if row["task_id"] != key]}

    async def close(self):
        await self._detail_reads.cache_close()
        await self._list_reads.cache_close()
        await self._classification_reads.cache_close()
        for task in self._history_tasks.values():
            task.cancel()
        await asyncio.gather(*self._history_tasks.values(), return_exceptions=True)
        await self._refresh.cache_close()
