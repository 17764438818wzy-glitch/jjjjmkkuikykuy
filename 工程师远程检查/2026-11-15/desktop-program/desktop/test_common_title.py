import pytest
from content_rules import detect_title

@pytest.mark.parametrize('lines,expected', [
    ('新收入政策对制造企业的影响\n——以示例企业为例','新收入政策对制造企业的影响——以示例企业为例'),
    ('数字化转型背景下企业营运资金\n管理研究','数字化转型背景下企业营运资金管理研究'),
    ('绿色转型背景下制造行业\nESG评价指标体系研究','绿色转型背景下制造行业ESG评价指标体系研究'),
    ('\n企业内部控制\n有效性研究','企业内部控制有效性研究'),
])
def test_explicit_title_continuation_is_preserved(lines, expected):
    assert detect_title('本科毕业论文\n题目：'+lines+'\n专业：会计学\n学生姓名：测试\n摘要\n正文') == expected


@pytest.mark.parametrize('date_line', ['2021年6月12日', '2023 年 3 月 8 日', '2021-06-12', '03/08/2023'])
def test_pdf_date_line_is_never_used_as_title(date_line):
    text = f'本科毕业论文\n{date_line}\n专业：会计学\n摘要\n正文'
    assert detect_title(text) == ''


def test_split_abstract_heading_after_date_does_not_become_title():
    text = '# 2023 年3 月8 日\n\n摘\n要：经济时代的到来让企业在发展中面临更大挑战，而营运能力作为企业发展关键指标\n正文'
    assert detect_title(text) == ''

@pytest.mark.parametrize('boundary', ['\n\n研究内容简介','\n学 院：管理学院','\n摘 要\n研究内容','\n指导教师：测试'])
def test_title_stops_before_other_cover_fields_and_body(boundary):
    assert detect_title('题目：企业绩效评价研究'+boundary) == '企业绩效评价研究'

def test_english_title_wrap_preserves_word_boundary():
    assert detect_title('题目：Financial Performance\nof Small Enterprises\n专业：会计学') == 'Financial Performance of Small Enterprises'

def test_bilingual_cover_does_not_append_separate_english_title():
    assert detect_title('题目：企业财务绩效研究\nFinancial Performance of Enterprises\n专业：会计学') == '企业财务绩效研究'

def test_spaced_identity_fields_are_not_title_continuations():
    assert detect_title('题目：企业绩效评价研究\n姓 名 张三\n专 业 会计学\n摘要')=='企业绩效评价研究'

@pytest.mark.parametrize('lines',[
    '企业绩效评价研究\n张三','企业绩效评价研究\n欧阳明月',
    '企业内部控制\n有效性\n研究',
    '中小企业财务风险的\n实证\n研究',
    '互联网金融对传统银行盈利\n能力\n的影响',
])
def test_ambiguous_short_cover_line_requires_existing_classification_fallback(lines):
    # Plain text cannot establish whether this is a name or a title fragment.
    # An empty title triggers the existing classifier, not a truncated success.
    assert detect_title('题目：'+lines+'\n专业：会计学')==''

@pytest.mark.parametrize('translation',["Research on Firms in 2026","Research on Firms & Markets","Firms' Financial Performance"])
def test_english_translation_can_contain_numbers_and_punctuation(translation):
    assert detect_title('题目：企业绩效评价研究\n'+translation+'\n专业：会计学')=='企业绩效评价研究'

def test_short_initial_fragment_is_validated_after_joining():
    assert detect_title('题目：\n基于\n财务数据的企业绩效评价研究\n专业：会计学')=='基于财务数据的企业绩效评价研究'


@pytest.mark.parametrize('filename',[
    '张三_小学体育活动现状研究.docx',
    '欧阳明月-小学体育活动现状研究.docx',
    '张三.小学体育活动现状研究.docx',
    '张三（定稿）_小学体育活动现状研究.docx',
    'Alice_Study of Financial Performance.docx',
    '某某客户有限公司_小学体育活动现状研究.docx',
    'AcmeCorp-Study of Financial Performance.docx',
])
def test_filename_author_prefix_cannot_become_published_title(filename):
    assert detect_title('摘要\n正文',filename)==''


def test_explicit_cover_title_is_used_even_when_filename_contains_author():
    assert detect_title('题目：小学体育活动现状研究\n摘要\n正文',
                        '张三_小学体育活动现状研究.docx')=='小学体育活动现状研究'

def test_ambiguous_cover_context_reaches_existing_classifier(tmp_path):
    from auto_pipeline import AutoProcessor
    from engine import Store,scan_sources
    folder=tmp_path/'in';folder.mkdir()
    cover='题目：企业内部控制\n有效性\n研究\n专业：会计学\n'
    (folder/'paper.txt').write_text(cover+'摘要\n'+('企业内部控制和财务管理的研究资料。'*300)+'\n参考文献\n[1]示例文献',encoding='utf-8')
    calls=[]
    class Agent:
        def call(self,task,payload,**kwargs):
            calls.append(task)
            if task=='classify':
                assert '企业内部控制\n有效性\n研究' in payload['cover_title_context']
                return {'decision':'pass','reason':'完整题名','major':'会计学','title':'企业内部控制有效性研究'}
            if task=='privacy_text':return {'decision':'pass','reason':'研究内容','sensitive_strings':[]}
            return {'decision':'pass','reason':'研究论文'}
    processor=AutoProcessor(Store(tmp_path/'out'));processor.agent=Agent()
    item=processor.run(scan_sources(folder))['items'][0]
    assert item['status']=='pass',item['reason']
    assert item['title']=='企业内部控制有效性研究'
    assert calls.count('classify')==1
