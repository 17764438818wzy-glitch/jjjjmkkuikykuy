"""Reuse first-step classification with Prefect-backed request history."""

import base64
import hashlib
import importlib
import importlib.util
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
import re
import sqlite3
import sys
from urllib.error import HTTPError
from urllib.request import urlopen
from uuid import uuid4

import httpx
from filelock import FileLock

from .receipts import (read_folder_receipts_by_key, read_receipts_sync, read_source_receipts_sync,
                       receipt_data, save_receipt_sync)


async def read_classification_records(task_id, folder_id, source_path, *, initial_records=()):
    """Read this folder's original request/dispatch evidence, including legacy keys."""
    task_id = str(task_id)
    source = hashlib.sha256(str(Path(source_path).resolve()).casefold().encode('utf8')).hexdigest()
    dispatch = re.sub(r'[^a-z0-9]', '', (task_id + '-' + folder_id + '-dispatch-').lower())
    patterns = {'paperflow-v3-firststep' + source + '%', 'paperflow-v3-' + dispatch + '%'}
    kinds = {'classification_model', 'child_dispatch'}
    historical = [row for row in initial_records if row.get('task_id') == task_id
                  and row.get('folder_id') == folder_id and row.get('kind') in kinds]
    for row in historical:
        if row['kind'] == 'classification_model':
            prefix = row['event_id'].rsplit('-', 1)[0]
            patterns.add('paperflow-v3-' + re.sub(r'[^a-z0-9]', '', prefix.lower()) + '%')
    selected = await read_folder_receipts_by_key([task_id], patterns=tuple(sorted(patterns)))
    fresh = [receipt_data(row) for row in selected[task_id]]
    records = {(row['event_id'], row.get('recorded_at')): row for row in historical}
    for row in fresh:
        if row.get('task_id') == task_id and row.get('folder_id') == folder_id and row.get('kind') in kinds:
            records[row['event_id'], row.get('recorded_at')] = row
    return sorted(records.values(), key=lambda row: datetime.fromisoformat(row['recorded_at'])
                  if row.get('recorded_at') else datetime.min.replace(tzinfo=timezone.utc), reverse=True)


def first_step_modules(root):
    package_name = "paperflow_v3_first_step"
    package_file = Path(root).resolve() / "paperflow_prefect" / "__init__.py"
    package = sys.modules.get(package_name)
    if package is None:
        spec = importlib.util.spec_from_file_location(
            package_name, package_file, submodule_search_locations=[str(package_file.parent)]
        )
        package = importlib.util.module_from_spec(spec)
        sys.modules[package_name] = package
        spec.loader.exec_module(package)
    elif Path(package.__file__).resolve() != package_file:
        raise ValueError("不能混用不同版本的第一步脚本")
    return {name: importlib.import_module(package_name + "." + module) for name, module in (
        ("control", "first_step_control"), ("nimble", "nimble_group"),
        ("classification", "group_classification"), ("sort", "group_sort_cli"),
        ("summary", "group_task_summary"), ("intake", "intake_handoff"),
        ("receipts", "receipts"), ("records", "records"), ("settings", "settings"),
        ("nimble_probe", "nimble_dependency"), ("resources", "resources"),
        ("registration", "first_step_registration"),
    )}


def make_judge(first_step_root, task_id, folder_id, snapshot_id, admission, endpoint, nimble_lock_path,
               *, source_path=None):
    modules = first_step_modules(first_step_root)
    control = modules["control"]
    source_key = (hashlib.sha256(str(Path(source_path).resolve()).casefold().encode("utf8")).hexdigest()
                  if source_path is not None else None)

    class JournaledNimbleJudge(modules["nimble"].NimbleJudge):
        def _request_id(self, request):
            payload = hashlib.sha256(request.data).hexdigest()
            return hashlib.sha256((snapshot_id + ":" + payload).encode()).hexdigest()

        def _cached_response(self, request):
            # The existing transport lock performs the only fresh cache/history
            # check. An unlocked pre-read would repeat the same source query.
            return None

        def _locked_cached_response(self, request):
            states = {}
            try:
                records = (read_source_receipts_sync("paperflow-v3-firststep" + source_key) if source_key else
                           read_receipts_sync(task_id))
            except httpx.HTTPError as exc:
                raise control.UnknownRequest("分类回执读取失败，当前分类状态待核对；未自动重发") from exc
            for row in records:
                if row["kind"] == "classification_model" and (source_key or row["folder_id"] == folder_id):
                    states.setdefault((row["task_id"], row["data"]["request_id"]), row["data"])
            if any(value["state"] in ("intent", "unknown") for value in states.values()):
                raise control.UnknownRequest("分类模型原请求结果待核对")
            current = states.get((str(task_id), self._request_id(request)))
            if current and current["state"] == "complete":
                return base64.b64decode(current["response"])
            if current and current["state"] == "failed":
                raise RuntimeError("分类模型请求已失败，等待人工处理")
            return None

        def _transmit(self, request, remaining):
            request_id = self._request_id(request)

            def save(state, **extra):
                try:
                    return save_receipt_sync(task_id=task_id, folder_id=folder_id, child_flow_run_id=task_id,
                        event_id="firststep-" + (source_key + "-" if source_key else "") + request_id + "-" + state,
                        kind="classification_model",
                        data={"request_id": request_id, "snapshot_id": snapshot_id, "state": state, **extra})
                except httpx.HTTPError as exc:
                    raise control.UnknownRequest("分类回执保存失败，当前分类状态待核对；未自动重发") from exc

            # This is the existing first-step Nimble resource gate, shared by its users.
            Path(nimble_lock_path).parent.mkdir(parents=True, exist_ok=True)
            with FileLock(str(nimble_lock_path)):
                cached = self._locked_cached_response(request)
                if cached is not None:
                    return cached
                if not admission.running_sync():
                    raise control.PauseRequested()
                save("intent")
                if not admission.running_sync():
                    save("not_sent")
                    raise control.PauseRequested()
                try:
                    with urlopen(request, timeout=remaining) as response:
                        raw = response.read(256 * 1024 + 1)
                except HTTPError as exc:
                    save("failed", http_status=exc.code)
                    raise
                except Exception as exc:
                    save("unknown", error_type=type(exc).__name__)
                    raise control.UnknownRequest("分类模型请求结果待核对") from exc
                save("complete", response=base64.b64encode(raw).decode("ascii"))
                return raw

    return JournaledNimbleJudge(endpoint=endpoint)


def check_legacy_source(first_step_root, classification_root, group, source_digest):
    """Consult the old first-step journal without starting or changing it."""
    control = first_step_modules(first_step_root)["control"]
    database = Path(classification_root) / "group-task-summaries" / "summary-index.sqlite3"
    if not database.is_file():
        return
    current = "local:" + Path(group).resolve().as_posix().casefold()
    try:
        with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
            active = db.execute(
                "SELECT g.group_root, g.source_digest, d.request_scope "
                "FROM control_groups g JOIN model_decisions d ON d.task_id=g.task_id AND d.seq=g.seq "
                "WHERE d.state IN ('inflight','unknown')"
            ).fetchall()
            deleted = db.execute(
                "SELECT group_scope, source_digest, request_scope FROM deleted_unknown_requests"
            ).fetchall()
    except sqlite3.Error as exc:
        raise control.UnknownRequest("历史分类请求记录无法核对") from exc
    for old_path, digest, scope in active:
        old_group = "local:" + Path(old_path).resolve().as_posix().casefold() if old_path else None
        if (control._same_request_scope(current, old_group) or control._same_request_scope(current, scope)
                or (source_digest and source_digest == digest and not old_group and not scope)):
            raise control.UnknownRequest("该资料组存在历史未知分类请求")
    for group_scope, digest, request_scope in deleted:
        if (control._same_request_scope(current, group_scope)
                or control._same_request_scope(current, request_scope)
                or (source_digest and source_digest == digest and not group_scope and not request_scope)):
            raise control.UnknownRequest("该资料组存在已删除任务的未知分类请求")


def _registration_history_roots(history_roots, registration_root):
    """Keep other history while excluding this registration's own output directory."""
    output = Path(registration_root).resolve()
    if output.is_dir() and next(output.rglob("inventory.json"), None) is not None:
        raise ValueError("登记目录中存在历史清单，需要核对")
    roots = []
    for value in history_roots:
        root = Path(value).resolve()
        if root == output or not output.is_relative_to(root) or not root.is_dir():
            roots.append(root)
            continue
        current = root
        for name in output.relative_to(root).parts:
            if not current.exists():
                break
            for sibling in current.iterdir():
                if sibling == current / name:
                    continue
                if ((sibling.is_dir() and not sibling.is_symlink() and not sibling.is_junction())
                        or (sibling.is_file() and sibling.name == "inventory.json")):
                    roots.append(sibling)
            current = current / name
    return tuple(dict.fromkeys(roots))


def classify_and_register(*, first_step_root, task_id, group_path, library_root, work_root,
                          admission, nimble_endpoint, nimble_lock_path, history_roots,
                          legacy_classification_root=None, source_state=None, group_history=None):
    """Classify and register one source group using the established first-step scripts."""
    modules = first_step_modules(first_step_root)
    classification = modules["classification"]
    group = Path(group_path).resolve()
    library = Path(library_root).resolve()
    group_id = classification._group_id(group, library)
    root = Path(work_root).resolve() / group_id
    identity = dict(task_id=task_id, folder_id=group_id, child_flow_run_id=task_id)
    captures = {}
    for record in group_history or ():
        if record['kind'] == 'classification_capture':
            data = record['data']
            captures.setdefault((data['snapshot'], data['version']), data['capture'])

    def save(kind, data):
        return save_receipt_sync(**identity, event_id=uuid4(), kind=kind, data=data)

    def bind_source(digest, actual_group_id):
        if legacy_classification_root is not None:
            check_legacy_source(first_step_root, legacy_classification_root, group, digest)
        previous = (source_state["data"] if source_state is not None else
                    next((row["data"] for row in (group_history if group_history is not None else read_receipts_sync(task_id))
                          if row["folder_id"] == group_id and row["kind"] == "classification_source"), None))
        current = {"digest": digest, "group_id": actual_group_id}
        if previous is not None and previous != current:
            raise modules["control"].UnknownRequest("资料组来源已改变，原分类请求需核对")
        if previous is None:
            save("classification_source", current)
            if source_state is not None:
                source_state["data"] = current

    def save_capture(snapshot, version, capture):
        save("classification_capture", {"snapshot": snapshot, "version": version, "capture": capture})
        captures[snapshot, version] = capture

    def load_capture(snapshot, version):
        if group_history is not None or (snapshot, version) in captures:
            return captures.get((snapshot, version))
        return next((row["data"]["capture"] for row in read_receipts_sync(task_id)
                     if row["folder_id"] == group_id and row["kind"] == "classification_capture"
                     and row["data"]["snapshot"] == snapshot and row["data"]["version"] == version), None)

    if not admission.running_sync():
        raise modules["control"].PauseRequested()
    reader = partial(modules["sort"]._read_for_sort, work_root=root / "conversion", task_id=str(task_id),
                     stop_requested=lambda: not admission.running_sync())
    item = classification.process_one_group(
        group, library, root / "classification",
        lambda snapshot: make_judge(first_step_root, task_id, group_id, snapshot, admission,
                                    nimble_endpoint, nimble_lock_path, source_path=group),
        reader, title_reader=modules["sort"]._title_from_body, task_id=str(task_id),
        durable_journal=True, source_binder=bind_source, capture_saver=save_capture,
        capture_loader=load_capture, source_library=str(library),
    )
    row, status, _ = modules["summary"].make_group_summary_row(item, library)
    save("classification", row)
    if status != "passed" or not admission.running_sync():
        return row
    registration = root / "registration"
    store = modules["receipts"].ReceiptStore(
        registration, history_roots=_registration_history_roots(history_roots, registration))
    receipt = modules["intake"].receive_handoff(
        store=store, delivery_dir=item["delivery_dir"], retain_unused_delivery_history=False,
        strict_history=True,
    )
    body = next((entry for entry in receipt.get("items", []) if entry.get("kind") == "body"), None)
    if not (receipt.get("state") == "已接收" and receipt.get("paper_id")
            and body and body.get("state") == "已接收"):
        row.update(registration_status="failed", registration_reason=receipt.get("error") or "交付资料未完整登记")
        save("classification", row)
        return row
    row.update(task_id=str(task_id), registration_status="registered", paper_id=receipt["paper_id"],
               receipt_id=receipt["receipt_id"], receipt_path=receipt["record_path"],
               receipt_sha256=modules["records"].sha_file(receipt["record_path"]),
               library_root=receipt["library_root"], paper=receipt["inventory"])
    save("registration", row)
    save("classification", {key: value for key, value in row.items() if key != "paper"})
    return row
