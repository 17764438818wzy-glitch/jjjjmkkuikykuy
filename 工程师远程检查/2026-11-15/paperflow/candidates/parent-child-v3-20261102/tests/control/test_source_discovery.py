"""Selected source folders use the existing first-step grouping rules."""

from pathlib import Path

import pytest

from paperflow_v3.control.sources import discover_sources


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_discover_groups_and_report_ungrouped_files(tmp_path):
    library = tmp_path / "library"
    (library / "one" / "正文").mkdir(parents=True)
    (library / "one" / "正文" / "body.txt").write_text("one")
    (library / "two").mkdir()
    (library / "two" / "body.txt").write_text("two")
    (library / "loose.txt").write_text("unassigned")

    result = discover_sources([str(library)], FIRST_STEP)
    assert {Path(row["group_path"]).name for row in result["groups"]} == {"one", "two"}
    assert all(Path(row["library_root"]) == library for row in result["groups"])
    assert result["ungrouped"] == [str(library / "loose.txt")]


def test_discover_a_single_group_and_reject_overlapping_selections(tmp_path):
    group = tmp_path / "group"
    group.mkdir()
    (group / "body.txt").write_text("synthetic")
    result = discover_sources([str(group)], FIRST_STEP)
    assert result["groups"] == [{"group_path": str(group), "library_root": str(tmp_path)}]
    assert not result["ungrouped"]
    with pytest.raises(ValueError, match="重叠"):
        discover_sources([str(tmp_path), str(group)], FIRST_STEP)
    with pytest.raises(ValueError, match="工作"):
        discover_sources([str(tmp_path)], FIRST_STEP, forbidden_roots=[tmp_path / "work"])
