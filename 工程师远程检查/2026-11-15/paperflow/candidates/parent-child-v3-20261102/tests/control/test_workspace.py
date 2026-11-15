from pathlib import Path
from uuid import uuid4

import pytest


class Settings:
    def __init__(self, values):
        self.values = values


def test_each_folder_has_its_own_legacy_data_root(tmp_path):
    from paperflow_v3.control.workspace import folder_settings

    original = Settings({"data_root": str(tmp_path), "catalog_path": "shared-catalog"})
    task = uuid4()
    first = folder_settings(original, task, "group-a", {"body": {}, "attachments": []})
    second = folder_settings(original, task, "group-b", {"body": {}, "attachments": []})
    assert Path(first.values["data_root"]) != Path(second.values["data_root"])
    assert Path(first.values["data_root"]).is_relative_to(tmp_path)
    assert first.values["catalog_path"] == "shared-catalog"
    assert original.values["data_root"] == str(tmp_path)


def test_existing_history_outside_the_new_folder_is_not_touched(tmp_path):
    from paperflow_v3.control.workspace import folder_settings

    settings = Settings({"data_root": str(tmp_path / "new")})
    historical = tmp_path / "historical" / "old-model-work"
    paper = {"body": {"history": {"work_dirs": [str(historical)]}}, "attachments": []}
    with pytest.raises(ValueError, match="历史工作目录"):
        folder_settings(settings, uuid4(), "group-a", paper)
