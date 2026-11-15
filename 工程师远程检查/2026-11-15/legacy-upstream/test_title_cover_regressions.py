import pytest

from content_rules import detect_title, valid_title


@pytest.mark.parametrize('cover', [
    '本科毕业设计（论文）', '本科毕业设计(论文)', '毕业论文（设计）',
    '本 科 毕 业 设 计（论 文）', '2026届本科毕业设计（论文）',
])
def test_generic_cover_is_not_a_research_title(cover):
    assert not valid_title(cover)
    assert detect_title(cover+'\nBCZY公司存货管理研究\n摘要\n正文') == 'BCZY公司存货管理研究'


@pytest.mark.parametrize('title', [
    '本科毕业论文质量管理研究', '企业绩效评价研究（以制造业为例）',
    '2026年企业经营风险分析', 'BCZY公司存货管理研究',
])
def test_real_title_keeps_parentheses_and_years(title):
    assert valid_title(title)
    assert detect_title('本科毕业设计（论文）\n'+title+'\n摘要') == title


def test_english_identity_field_is_never_a_title_fallback():
    assert not valid_title('Author: Sample Student')
    assert not valid_title('Instructor: Sample Teacher')
    assert detect_title('Author: Sample Student\n企业经营绩效评价研究\n摘要') == '企业经营绩效评价研究'
