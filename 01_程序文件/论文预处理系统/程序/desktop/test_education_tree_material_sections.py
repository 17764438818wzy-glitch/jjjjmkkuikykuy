import copy
import hashlib

from education_direct_tree import bind_tree_chunks, extract_tree


PAPER_ID = 'paper_' + 'a' * 24


def test_questionnaire_items_and_options_stay_out_of_tree():
    ingest = '''# 第1章 研究设计
资料关联（程序生成）：section_1111aaaa
## 1.1 问卷编制
资料关联（程序生成）：section_2222bbbb
# 附录A 调查问卷
资料关联（程序生成）：section_3333cccc
# 1.您所在学校：
资料关联（程序生成）：section_4444dddd
###### A.初级 B.中级 C.高级
资料关联（程序生成）：section_5555eeee
# 2.我希望上体育课
资料关联（程序生成）：section_6666ffff
# 附录B 访谈提纲
资料关联（程序生成）：section_7777aaaa
# 致谢
资料关联（程序生成）：section_8888bbbb
'''
    tree = extract_tree(ingest, PAPER_ID, 'remote-1')
    assert [n['title'] for n in tree['nodes']] == [
        '第1章 研究设计', '1.1 问卷编制', '附录A 调查问卷', '附录B 访谈提纲', '致谢',
    ]
    assert [n['section_id'] for n in tree['nodes']] == [
        'section_1111aaaa', 'section_2222bbbb', 'section_3333cccc',
        'section_7777aaaa', 'section_8888bbbb',
    ]


def test_plain_appendix_and_scale_title_identify_questionnaire_context():
    ingest = '''# 第1章 研究设计
资料关联（程序生成）：section_1111aaaa
# 1.预实验
资料关联（程序生成）：section_2222bbbb
附录A
大学生学习兴趣量表
# 1.很希望上体育课
资料关联（程序生成）：section_3333cccc
# 2.每次学会有一定难度的运动技巧时，我都会认
资料关联（程序生成）：section_4444dddd
真思考
# 第2章 研究结论
资料关联（程序生成）：section_5555eeee
# 1.研究局限
资料关联（程序生成）：section_6666ffff
'''
    tree = extract_tree(ingest, PAPER_ID, 'remote-1')
    assert [n['title'] for n in tree['nodes']] == [
        '第1章 研究设计', '1.预实验', '第2章 研究结论', '1.研究局限',
    ]


def test_questionnaire_numbering_variants_do_not_hide_real_numbered_sections():
    ingest = '''# 附录A 调查问卷
## 1.1 问卷设计说明
# 10您认为制约学生参加课外体育活动的主要因素有 (可多选):
# 11.我能独立完成问卷
###### A.1-2次
# 附录B 补充分析
## 1.研究方法
'''
    tree = extract_tree(ingest, PAPER_ID, 'remote-1')
    assert [n['title'] for n in tree['nodes']] == [
        '附录A 调查问卷', '1.1 问卷设计说明', '附录B 补充分析', '1.研究方法',
    ]


def test_questionnaire_salutations_and_form_fields_are_body_not_chapters():
    ingest = '''# 第1章 研究说明
## 指导老师：
# 附  录
## 附录A
###### 课余体育锻炼情况调查（学生问卷）
###### 亲爱的同学：
###### 您好！
###### 姓名：
###### 指导老师：
## 附录B
###### 课余体育锻炼情况调查（教师问卷）
###### 尊敬的老师：
###### 您好！
# 附录C 研究补充
## 姓名：
'''
    tree = extract_tree(ingest, PAPER_ID, 'remote-1')
    assert [n['title'] for n in tree['nodes']] == [
        '第1章 研究说明', '指导老师：', '附  录', '附录A',
        '课余体育锻炼情况调查（学生问卷）', '附录B',
        '课余体育锻炼情况调查（教师问卷）', '附录C 研究补充', '姓名：',
    ]


def test_appendix_reading_list_is_not_a_directory_but_remains_in_source_chunks():
    ingest = '''# 第1章 研究设计
资料关联（程序生成）：section_1111aaaa
# 附录
资料关联（程序生成）：section_2222bbbb
附件D
部编版一年级古诗词全部
上册：
# 1.语文园地一——日积月累——《咏鹅》骆宾王
资料关联（程序生成）：section_3333cccc
# 2.课文三：《江南》汉乐府“江南可采莲”
资料关联（程序生成）：section_4444dddd
# 附录E 补充分析
资料关联（程序生成）：section_5555eeee
## 1.补充研究
资料关联（程序生成）：section_6666ffff
### 1.1 研究说明
资料关联（程序生成）：section_7777aaaa
'''
    chunks = [
        {'id': 'body-1', 'document_id': 'remote-1', 'available': True,
         'important_keywords': ['role=body', 'section_id=section_1111aaaa'],
         'content': '# 第1章 研究设计'},
        {'id': 'appendix-material', 'document_id': 'remote-1', 'available': True,
         'important_keywords': ['role=body', 'section_id=section_2222bbbb'],
         'content': ingest[ingest.index('# 附录'):ingest.index('# 附录E')]},
        {'id': 'appendix-analysis', 'document_id': 'remote-1', 'available': True,
         'important_keywords': ['role=body', 'section_id=section_5555eeee'],
         'content': ingest[ingest.index('# 附录E'):]},
    ]
    original_chunks = copy.deepcopy(chunks)
    tree = bind_tree_chunks(extract_tree(ingest, PAPER_ID, 'remote-1'), chunks)
    assert [n['title'] for n in tree['nodes']] == [
        '第1章 研究设计', '附录', '附录E 补充分析', '1.补充研究', '1.1 研究说明',
    ]
    assert [n['id'] for n in tree['nodes']] == [
        PAPER_ID + ':section_' + suffix for suffix in
        ('1111aaaa', '2222bbbb', '5555eeee', '6666ffff', '7777aaaa')
    ]
    assert tree['nodes'][1]['source_chunk_ids'] == ['appendix-material']
    assert [n['order'] for n in tree['nodes']] == [0, 1, 2, 3, 4]
    assert tree['nodes'][3]['parent_id'] == PAPER_ID + ':section_5555eeee'
    assert tree['nodes'][4]['parent_id'] == PAPER_ID + ':section_6666ffff'
    assert tree['relations'][1]['source_chunk_ids'] == ['appendix-analysis']
    assert tree['unlinked_headings'] == 0
    assert tree['ingest_sha256'] == hashlib.sha256(ingest.encode()).hexdigest()
    assert chunks == original_chunks
