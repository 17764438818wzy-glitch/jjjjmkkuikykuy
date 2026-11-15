from pathlib import Path

import pytest

from engine import Processor, Store, scan_sources
from test_workbench import paper


def test_title_uses_cover_text_instead_of_student_filename():
    from content_rules import detect_title
    text = "本科毕业论文\n轻资产运营模式的财务风险管理研究题目\t——以小米公司为例\n学号：202600123456\n摘要\n正文"
    assert detect_title(text, "会计学_202600123456_张小测.docx") == "轻资产运营模式的财务风险管理研究——以小米公司为例"
    assert detect_title("摘要\n正文", "会计学_202600123456_张小测.docx") == ""


@pytest.mark.parametrize("heading", ["致谢", "致　谢", "第六章 致谢", "## 致谢", "Acknowledgements", "致谢：感谢老师。"])
def test_ack_sections_preserve_following_references_and_appendix(heading):
    from content_rules import remove_acknowledgements
    content = f"论文正文\n{heading}\n感谢张老师和我的家人。\n参考文献\n[1] 财务风险研究\n附录 A\n问卷数据"
    cleaned, removed = remove_acknowledgements(content)
    assert removed == 1
    assert "感谢" not in cleaned and "张老师" not in cleaned
    assert "论文正文" in cleaned and "[1] 财务风险研究" in cleaned and "问卷数据" in cleaned


def test_ack_toc_entry_does_not_delete_body_or_quotes():
    from content_rules import remove_acknowledgements
    content = "目录\n致谢…………20\n参考文献……21\n第一章 绪论\n本研究分析致谢语的语言特点。\n致谢\n感谢老师。"
    cleaned, count = remove_acknowledgements(content)
    assert "第一章 绪论" in cleaned and "本研究分析致谢语的语言特点。" in cleaned
    assert "感谢老师" not in cleaned and "致谢…………20" not in cleaned
    assert count == 1


def test_real_output_title_filenames_collision_and_ack_removal(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    title = "幼儿阅读活动的案例研究"
    (source / "a.txt").write_text(paper(title) + "\n致谢\n感谢张老师。", encoding="utf-8")
    (source / "b.txt").write_text(paper(title) + "\n附录\n另一组观察数据\n致谢\n感谢家人。", encoding="utf-8")
    store = Store(tmp_path / "out")
    manifest = Processor(store).run(scan_sources(source))
    assert manifest["status"] == "completed", manifest["message"]
    assert len(manifest["items"]) == 2
    paths = [Path(i["path"]) for i in manifest["items"]]
    assert len(set(paths)) == 2
    assert all(p.name == 'ingest.md' for p in paths)
    assert all(title in p.read_text(encoding='utf-8') for p in paths)
    assert all("感谢" not in p.read_text(encoding="utf-8") for p in paths)
    assert "感谢张老师" in (source / "a.txt").read_text(encoding="utf-8")
    assert not Processor(store).run(scan_sources(source))["items"]


def test_old_processing_version_can_be_reprocessed(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    (source / "a.txt").write_text(paper(), encoding="utf-8")
    store = Store(tmp_path / "out")
    first = Processor(store).run(scan_sources(source))
    for item in first["items"]:
        item.pop("processing_version", None)
    store.save(first)
    second = Processor(store).run(scan_sources(source))
    assert second["items"], "Old outputs must not prevent applying new cleanup rules"
