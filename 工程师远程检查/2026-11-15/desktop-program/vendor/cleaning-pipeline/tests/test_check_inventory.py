from __future__ import annotations

from pathlib import Path

from src.paper_cleaning.check_inventory import build_report, count_files, write_report
from src.paper_cleaning.inventory import build_inventory, write_inventory_csv


def write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_check_inventory_generates_report_with_counts(tmp_path: Path) -> None:
    write_file(tmp_path / "a.txt", b"same")
    write_file(tmp_path / "nested" / "b.pdf", b"same")
    inventory_csv = tmp_path / "inventory.csv"
    report_path = tmp_path / "report.txt"
    records = build_inventory(tmp_path, mode="documents")
    write_inventory_csv(records, inventory_csv)

    report = build_report(tmp_path, inventory_csv)
    write_report(report, report_path)

    assert report_path.read_text(encoding="utf-8") == report
    assert "输入目录是否存在: yes" in report
    assert "原始文件数量: 3" in report
    assert "inventory 记录数量: 2" in report
    assert "二者是否一致: no" in report
    assert "路径不存在数量: 0" in report
    assert "重复 sha256 组数: 1" in report
    assert "重复文件数量: 2" in report


def test_check_inventory_counts_files_with_same_exclude_rules(tmp_path: Path) -> None:
    write_file(tmp_path / "a.txt", b"alpha")
    write_file(tmp_path / "output" / "inventory.csv", b"skip")
    write_file(tmp_path / "logs" / "run.log", b"skip")
    write_file(tmp_path / "skip_me" / "secret.txt", b"skip")
    inventory_csv = tmp_path / "output" / "inventory.csv"
    records = build_inventory(tmp_path, mode="full", exclude=["skip_me"])
    write_inventory_csv(records, inventory_csv)

    report = build_report(tmp_path, inventory_csv, exclude=["skip_me"])

    assert count_files(tmp_path, exclude=["skip_me"]) == 1
    assert "原始文件数量: 1" in report
    assert "inventory 记录数量: 1" in report
    assert "二者是否一致: yes" in report
