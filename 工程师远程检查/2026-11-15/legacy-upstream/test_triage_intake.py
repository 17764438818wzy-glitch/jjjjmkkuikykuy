import pytest
from screening import similarity_report


@pytest.mark.parametrize('name',['garbled.pdf','论文检测报告.pdf'])
def test_bracketed_comparison_report_is_an_attachment(name):
    raw='1\n检测字符数：11697\n检测时间：2023-03-19\n报告编号：fixture\n【原文对照报告-大学生版】\n全文总相似比\n7.44%\n摘要\n论文正文'
    assert '辅助材料' in similarity_report(raw,name)


def test_paper_quoting_a_report_after_abstract_is_not_an_attachment():
    assert not similarity_report('论文标题\n摘要\n分析【原文对照报告-大学生版】\n全文总相似比\n7.44%','原文对照报告.pdf')


def test_education_taxonomy_covers_declared_majors_in_real_failed_papers():
    from engine import load_legacy_taxonomy
    assert {'人文教育','数学教育','汉语国际教育'} <= set(load_legacy_taxonomy()['教育教学类'])
