from __future__ import annotations

from pathlib import Path

from src.paper_cleaning.sample_builder import build_sample


def write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_sample_builder_skips_default_and_custom_excludes(tmp_path: Path) -> None:
    sample_dir = tmp_path / "sample_small"
    write_file(tmp_path / "data" / "paper.docx", b"paper")
    write_file(tmp_path / "output" / "inventory.csv", b"skip")
    write_file(tmp_path / "logs" / "run.log", b"skip")
    write_file(tmp_path / "private" / "secret.pdf", b"skip")

    copied = build_sample(
        tmp_path,
        sample_dir,
        limit=20,
        extensions=[".docx", ".pdf", ".csv", ".log"],
        exclude=["private"],
    )

    assert copied == ["data/paper.docx"]
    assert (sample_dir / "data" / "paper.docx").exists()
    assert not (sample_dir / "output" / "inventory.csv").exists()
    assert not (sample_dir / "logs" / "run.log").exists()
    assert not (sample_dir / "private" / "secret.pdf").exists()
