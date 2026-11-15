from paperflow_v3.control.task_state import task_state


def group(folder_id):
    return {
        "group_id": folder_id,
        "status": "passed",
        "delivery_complete": True,
        "registration_status": "registered",
    }


def folder(folder_id, stage, cleanup_status=None):
    return {"folder_id": folder_id, "stage": stage, "cleanup_status": cleanup_status}


def test_completion_requires_classification_and_every_paper_cleanup():
    groups = [group("a"), group("b")]
    folders = [folder("a", "completed", "complete"), folder("b", "waiting_rag_upload")]
    assert task_state(True, groups, folders, "running", True) == "running"
    folders[1] = folder("b", "completed", "incomplete")
    assert task_state(True, groups, folders, "running", True) == "unresolved"
    folders[1] = folder("b", "completed", "complete")
    assert task_state(False, groups, folders, "running", True) == "running"
    assert task_state(True, groups, folders, "running", True) == "completed"


def test_failure_unrecognized_or_unknown_never_appear_complete():
    groups = [group("a")]
    folders = [folder("a", "completed", "complete")]
    assert task_state(True, groups, folders, "running", False) == "unresolved"
    assert task_state(True, groups, [folder("a", "failed")], "running", True) == "failed"
    groups[0]["status"] = "unrecognized"
    assert task_state(True, groups, folders, "running", True) == "unrecognized"
    assert task_state(True, [], [], "running", True) == "unresolved"


def test_paused_task_waits_to_create_its_registered_child():
    groups = [group("a")]
    assert task_state(True, groups, [], "paused", True) == "paused"
    assert task_state(True, groups, [], "running", True) == "running"


def test_duplicate_or_unrelated_folders_do_not_count_as_completion():
    groups = [group("a"), group("b")]
    duplicate = [folder("a", "completed", "complete"), folder("a", "completed", "complete")]
    unrelated = [folder("a", "completed", "complete"), folder("c", "completed", "complete")]
    assert task_state(True, groups, duplicate, "running", True) == "unresolved"
    assert task_state(True, groups, unrelated, "running", True) == "unresolved"
