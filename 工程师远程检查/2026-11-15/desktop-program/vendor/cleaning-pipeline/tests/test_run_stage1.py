from __future__ import annotations

import csv
import zipfile
from pathlib import Path

from src.paper_cleaning.run_stage1 import run_stage1

from helpers import write_minimal_docx


def test_run_stage1_completes_on_temporary_sample(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    log_path = tmp_path / "logs" / "stage1.log"
    write_minimal_docx(
        input_dir / "乱目录" / "学生A" / "paper.docx",
        "题目：测试论文\n专业：财务管理\n摘要\n第一章\n" + "正文" * 2000 + "\n参考文献",
    )

    paths = run_stage1(input_dir, output_dir, limit=10, log_path=log_path)

    assert paths["archive_report"].exists()
    assert paths["inventory"].exists()
    assert paths["extract_report"].exists()
    assert paths["sanitize_report"].exists()
    assert paths["mapping"].exists()
    assert paths["quality_report"].exists()
    assert paths["subject_report"].exists()
    assert paths["tech_report"].exists()
    assert paths["tech_group_index"].exists()
    assert (output_dir / "sanitized_text" / "经管财会类" / "财务管理" / "FM_000001.txt").exists()
    assert not (output_dir / "sanitized_text" / "乱目录").exists()

    with paths["mapping"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["original_relative_path"] == "乱目录/学生A/paper.docx"
    assert rows[0]["detected_major"] == "财务管理"
    assert rows[0]["detected_subject_group"] == "经管财会类"
    assert rows[0]["detected_major_or_direction"] == "财务管理"
    assert rows[0]["quality_status"] == "pass"

    with paths["tech_report"].open("r", encoding="utf-8-sig", newline="") as handle:
        tech_rows = list(csv.DictReader(handle))
    assert len(tech_rows) == 1


def test_run_stage1_skips_duplicate_files_and_keeps_originals(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    body = "题目：重复论文\n专业：财务管理\n摘要\n第一章\n" + "正文" * 2000 + "\n参考文献"
    first = input_dir / "a" / "same.docx"
    second = input_dir / "b" / "same-copy.docx"
    write_minimal_docx(first, body)
    second.parent.mkdir(parents=True)
    second.write_bytes(first.read_bytes())

    paths = run_stage1(input_dir, output_dir, limit=10)

    sanitized_files = list((output_dir / "sanitized_text").rglob("*.txt"))
    assert len(sanitized_files) == 1
    assert first.exists()
    assert second.exists()

    with paths["dedupe_report"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["duplicate_type"] == "file"
    assert rows[0]["status"] == "duplicate_skipped"


def test_run_stage1_skips_duplicate_text_with_different_files(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_minimal_docx(
        input_dir / "a.docx",
        "题目：A\n专业：财务管理\n摘要\n第一章\n" + "相同正文" * 2000 + "\n参考文献",
    )
    write_minimal_docx(
        input_dir / "b.docx",
        "题目：B\n专业：财务管理\n摘要\n第一章\n" + "相同 正文" * 2000 + "\n参考文献",
    )

    paths = run_stage1(input_dir, output_dir, limit=10)

    sanitized_files = list((output_dir / "sanitized_text").rglob("*.txt"))
    assert len(sanitized_files) == 1
    with paths["dedupe_report"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert any(row["status"] == "duplicate_text_skipped" for row in rows)


def test_run_stage1_batches_and_cleans_temporary_files(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_minimal_docx(
        input_dir / "a.docx",
        "题目：A\n专业：财务管理\n摘要\n第一章\n" + "正文A" * 2000 + "\n参考文献",
    )
    write_minimal_docx(
        input_dir / "b.docx",
        "题目：B\n专业：财务管理\n摘要\n第一章\n" + "正文B" * 2000 + "\n参考文献",
    )

    paths = run_stage1(input_dir, output_dir, limit=10, batch_size=1, keep_intermediate=False)

    sanitized_files = sorted((output_dir / "sanitized_text").rglob("*.txt"))
    assert len(sanitized_files) == 2
    assert paths["sanitize_report"].exists()
    assert paths["mapping"].exists()
    assert paths["quality_report"].exists()
    assert not paths["temp_dir"].exists()
    assert not paths["raw_text"].exists()
    assert not paths["archive_output"].exists()


def test_run_stage1_resume_does_not_duplicate_outputs(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_minimal_docx(
        input_dir / "a.docx",
        "题目：A\n专业：财务管理\n摘要\n第一章\n" + "正文A" * 2000 + "\n参考文献",
    )
    write_minimal_docx(
        input_dir / "b.docx",
        "题目：B\n专业：财务管理\n摘要\n第一章\n" + "正文B" * 2000 + "\n参考文献",
    )

    run_stage1(input_dir, output_dir, limit=10, batch_size=1, keep_intermediate=False)
    paths = run_stage1(input_dir, output_dir, limit=10, batch_size=1, resume=True, keep_intermediate=False)

    sanitized_files = sorted((output_dir / "sanitized_text").rglob("*.txt"))
    assert len(sanitized_files) == 2
    with paths["mapping"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2


def test_run_stage1_resume_continues_after_interrupted_batch(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    write_minimal_docx(
        input_dir / "a.docx",
        "题目：A\n专业：财务管理\n摘要\n第一章\n" + "正文A" * 2000 + "\n参考文献",
    )
    write_minimal_docx(
        input_dir / "b.docx",
        "题目：B\n专业：财务管理\n摘要\n第一章\n" + "正文B" * 2000 + "\n参考文献",
    )

    interrupted_paths = run_stage1(
        input_dir,
        output_dir,
        limit=10,
        batch_size=1,
        keep_intermediate=True,
        max_batches=1,
    )

    assert interrupted_paths["temp_dir"].exists()
    assert len(list((output_dir / "sanitized_text").rglob("*.txt"))) == 1

    paths = run_stage1(input_dir, output_dir, limit=10, batch_size=1, resume=True, keep_intermediate=False)

    assert len(list((output_dir / "sanitized_text").rglob("*.txt"))) == 2
    assert not paths["temp_dir"].exists()
    with paths["mapping"].open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2


def test_run_stage1_processes_archive_inside_batch_and_cleans_temp(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    source_docx = tmp_path / "paper.docx"
    write_minimal_docx(
        source_docx,
        "题目：压缩包论文\n专业：护理学\n摘要\n第一章\n" + "正文A" * 2000 + "\n参考文献",
    )
    archive_path = input_dir / "bundle.zip"
    archive_path.parent.mkdir(parents=True)
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(source_docx, "nested/paper.docx")

    paths = run_stage1(input_dir, output_dir, batch_size=1, keep_intermediate=False)

    sanitized_files = list((output_dir / "sanitized_text").rglob("*.txt"))
    assert len(sanitized_files) == 1
    assert not paths["temp_dir"].exists()
    assert not paths["archive_output"].exists()
    with paths["archive_report"].open("r", encoding="utf-8-sig", newline="") as handle:
        archive_rows = list(csv.DictReader(handle))
    assert len(archive_rows) == 1
    assert archive_rows[0]["status"] == "success"


def test_run_stage1_does_not_extract_future_archive_batches(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    for index in range(2):
        source_docx = tmp_path / f"paper-{index}.docx"
        write_minimal_docx(
            source_docx,
            f"题目：压缩包论文{index}\n专业：护理学\n摘要\n第一章\n" + f"正文{index}" * 2000 + "\n参考文献",
        )
        archive_path = input_dir / f"bundle-{index}.zip"
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.write(source_docx, f"nested/paper-{index}.docx")

    paths = run_stage1(
        input_dir,
        output_dir,
        batch_size=1,
        keep_intermediate=True,
        max_batches=1,
    )

    with paths["archive_report"].open("r", encoding="utf-8-sig", newline="") as handle:
        archive_rows = list(csv.DictReader(handle))
    assert len(archive_rows) == 1
    assert archive_rows[0]["archive_path"].endswith("bundle-0.zip")
