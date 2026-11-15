"""Project whole-task state from first-step rows and current folder tickets."""


def task_state(classification_done, groups, folders, desired_state, snapshot_complete):
    if not snapshot_complete:
        return "unresolved"
    if any(row["status"] == "failed" or row.get("registration_status") == "failed" for row in groups):
        return "failed"
    if any(row["stage"] == "failed" for row in folders):
        return "failed"
    if any(row["status"] == "unrecognized" for row in groups):
        return "unrecognized"
    if any(row["stage"] == "unresolved" for row in folders):
        return "unresolved"
    if not classification_done:
        return "paused" if desired_state == "paused" else "running"
    if not groups:
        return "unresolved"

    group_ids = [row["group_id"] for row in groups]
    folder_ids = [row["folder_id"] for row in folders]
    if (any(row["status"] != "passed" or row["delivery_complete"] is not True for row in groups)
            or len(group_ids) != len(set(group_ids)) or len(folder_ids) != len(set(folder_ids))
            or not set(folder_ids).issubset(group_ids)):
        return "unresolved"
    if (len(folders) == len(groups)
            and all(row["registration_status"] == "registered" for row in groups)
            and all(row["stage"] == "completed" for row in folders)):
        return "completed" if all(row.get("cleanup_status") == "complete" for row in folders) else "unresolved"
    return "paused" if desired_state == "paused" else "running"
