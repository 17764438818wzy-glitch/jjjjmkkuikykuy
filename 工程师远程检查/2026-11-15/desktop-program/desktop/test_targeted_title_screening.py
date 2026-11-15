"""Regressions from saved failures in the September 28 recovery cohort."""
import pytest

from auto_pipeline import coarse_screen
from content_rules import detect_title, valid_title


TITLE = '受欢迎型幼儿在建构游戏中合作行为研究'
SUBTITLE = '——以中山市Y 幼儿园中班幼儿为例'


def test_vertical_cover_fields_do_not_become_title():
    raw = ('本科毕业设计（论文）\n二\n级\n学\n院教育与音乐学院\n'
           '专\n业学前教育\n年\n级2021 级\n学\n号2151032032\n'
           '学\n生\n姓\n名测试甲\n指\n导\n教\n师测试乙\n'
           '职称或学位硕士\n完\n成\n时\n间2023 年4 月15 日\n'
           + TITLE + '\n' + SUBTITLE + '\n\n独创性声明\n摘要\n正文')
    assert detect_title(raw) == TITLE + SUBTITLE


@pytest.mark.parametrize('value', ['级2021 级', '2021 级', '职称或学位硕士'])
def test_administrative_cover_values_cannot_pass_title_validation(value):
    assert not valid_title(value)


@pytest.mark.parametrize('title', ['年级差异对课堂互动的影响研究', '专业认同对职业发展的影响研究'])
def test_titles_that_start_with_field_words_remain_valid(title):
    assert valid_title(title)
    assert detect_title(title + '\n摘要\n正文') == title


@pytest.mark.parametrize('heading', [
    '知网个人查重服务报告单(全文对照)',
    '知网个人查重服务报告单(全文标明引文)',
    '知网查重服务报告单(全文标明引文)',
    '文本复制检测报告单（全文对照）',
])
def test_cnki_reports_reject_before_unreliable_text_is_sent_to_ai(heading):
    raw = (heading + '\n报告编号:BC2022000001\n检测时间:2022-11-19 14:39:29\n'
           '检测文献:课堂互动的调查研究\n检测范围:学术期刊网络出版总库\n'
           '检测结果\n总文字复制比：\n3.2%\n' + '文献比对内容' * 700)
    result = coarse_screen(raw, False, '课堂互动的调查研究.pdf')
    assert result['decision'] == 'reject'
    assert result['attachment'] is True
    assert '辅助材料' in result['reason']


def test_paperpass_report_rejects_with_report_fields_and_percentage():
    raw = ('PaperPass旗舰版检测报告\n简明打印版\n比对结果(相似度):\n总　体：\n'
           '11%  (总体相似度是指本地库、互联网的综合对比结果)\n'
           '报告编号：631C86FBA01F4H1S8\n检测版本：旗舰版\n论文题目：幼儿习惯的培养\n'
           '论文字数：3208\n提交时间：2022-9-10 20:45:47\n'
           '比对范围：学术期刊、学位论文\n' + '报告内容' * 1000)
    result = coarse_screen(raw, False, '幼儿习惯的培养.pdf')
    assert result['decision'] == 'reject'
    assert result['attachment'] is True


@pytest.mark.parametrize('heading', ['知网个人查重服务报告单(全文对照)', 'PaperPass旗舰版检测报告'])
def test_report_names_or_research_mentions_alone_do_not_reject_papers(heading):
    raw = ('高校查重系统的应用研究\n摘要\n' + heading + '\n报告编号：TEST123\n'
           '检测时间：2022-09-10\n总文字复制比：12%\n总体：12%\n'
           '比对范围：学术期刊\n' + '论文正文' * 1000 + '\n参考文献')
    assert coarse_screen(raw, True, heading + '.pdf')['decision'] != 'reject'
    raw = '题目：高校查重系统的应用研究\n' + heading + '\n摘要\n' + '论文正文' * 1000 + '\n参考文献'
    assert coarse_screen(raw, True, 'paper.pdf')['decision'] != 'reject'
