"""A paper enters the first queue at registration and leaves when work starts."""

from types import SimpleNamespace

from paperflow_v3.control.api import _folder_identity, _folder_snapshot_complete
from paperflow_v3.control.folder_status import folder_status


def child(group):
    return SimpleNamespace(parameters={"task_id": "task", "group_id": group})


def test_paper_waits_until_the_first_preprocessing_start_receipt():
    waiting = folder_status([], is_active=True)
    started = folder_status([{"kind": "preprocessing_started", "data": {}}], is_running=True, is_active=True)
    assert waiting["stage"] == "waiting_preprocessing"
    assert started["stage"] == "preprocessing_in_progress"
    assert folder_status([], is_active=False)["stage"] == "unresolved"
    assert folder_status([{"kind": "child_error", "data": {}}], is_active=True)["stage"] == "unresolved"


def test_a_complete_child_list_does_not_require_the_batch_to_be_classified():
    records = [{"kind": "classification", "folder_id": "one", "data": {"status": "passed"}}]
    assert _folder_snapshot_complete(records, [child("one"), child("two")], "task")
    assert _folder_snapshot_complete([], [], "task")


def test_unknown_child_dispatch_remains_unconfirmed_until_the_child_is_visible():
    records = [{"kind": "child_dispatch", "folder_id": "one", "data": {"state": "unknown"}}]
    assert not _folder_snapshot_complete(records, [], "task")
    assert _folder_snapshot_complete(records, [child("one")], "task")


def test_delivery_stage_identity_is_bound_to_the_actual_native_child():
    registered = {"task_id": "task", "group_id": "one", "paper_id": "paper", "receipt_id": "receipt"}
    native = SimpleNamespace(id="child", parameters={"task_id": "task", "group_id": "one",
                                                      "registered": registered})
    assert _folder_identity(native) == {**registered, "attempt_id": "child", "parent_run_id": "task",
                                        "task_run_id": "child"}
    native.parameters["group_id"] = "another"
    assert _folder_identity(native) is None
