import copy
import pytest
from auto_evidence import text_privacy,Rejected
from frontmatter_privacy import redact_frontmatter_blocks

def meta(*texts):
    return {'title':'财务研究','blocks':[{'type':'paragraph','text':s,'position':{'page':2}} for s in texts],
            'resources':[],'formulas':[]}

class Agent:
    def call(self,task,payload,**kwargs):
        self.seen=payload['text']
        return {'decision':'pass','reason':'已检查','sensitive_strings':[]}

@pytest.mark.parametrize('field',['Author:Wu Mian','Author：\nLiang\nXuan','Instructor:Ao   hua',
                                  'author: Huang Xiaomi','Supervisor:李小测'])
def test_explicit_native_identity_is_removed_before_model(field):
    m=meta(field,'Abstract: the company has 16526890259 yuan.','Wu Mian 提出的理论应保留。')
    a=Agent();text_privacy(m,a,lambda *args:None)
    assert m['blocks'][0]['text']=='[已脱敏]'
    assert field not in a.seen and m['privacy_check']['explicit_identity_fields']==1
    assert 'Wu Mian 提出的理论应保留。' in a.seen
    assert '16526890259 yuan' in a.seen

def test_adjacent_pdf_label_and_name_mask_only_those_blocks():
    m=meta('Author:','Wu Mian','Wu Mian')
    m['blocks'][0]['position'].update(bbox=[100,100,160,112])
    m['blocks'][1]['position'].update(bbox=[100,116,160,128])
    assert redact_frontmatter_blocks(m)==2
    assert [b['text'] for b in m['blocks']]==['[已脱敏]','[已脱敏]','Wu Mian']

@pytest.mark.parametrize('heading',['参考文献','References','Bibliography'])
def test_reference_author_fields_are_retained(heading):
    m=meta(heading,'Author: Adam Smith')
    before=copy.deepcopy(m);assert redact_frontmatter_blocks(m)==0;assert m==before

def test_research_prose_and_body_fields_are_retained():
    m=meta('The Author: Smith developed this method.','Author: Adam Smith')
    m['blocks'][1]['section']=['文献综述']
    before=copy.deepcopy(m);assert redact_frontmatter_blocks(m)==0;assert m==before

def test_signature_misstyled_as_heading_and_its_section_copies_are_masked():
    m=meta('关键词：企业经营','Instructor:Gao Hao','Abstract: company research')
    m['blocks'][0]['heading_level']=1
    for b in m['blocks'][1:]:b.update(heading_level=1,section=['Instructor:Gao Hao'])
    m['resources']=[{'id':'img','section':['Instructor:Gao Hao']}]
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][1]['text']=='[已脱敏]'
    assert m['blocks'][2]['text']=='Abstract: company research'
    assert m['blocks'][2]['section']==['[已脱敏]']
    assert m['resources'][0]['section']==['[已脱敏]']

def test_title_and_multiple_signatures_sharing_one_block_keep_title():
    m=meta('Financial analysis of T Company\nauthor: Huang Xiaomi\nInstructor:Gao Hao')
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][0]['text']=='Financial analysis of T Company\n[已脱敏]\n[已脱敏]'

def test_ambiguous_cross_paragraph_span_still_rejects():
    m=meta('作者','张小测')
    class Ambiguous:
        def call(self,*args,**kwargs):return {'decision':'pass','reason':'身份','sensitive_strings':['作者\n张小测']}
    with pytest.raises(Rejected,match='跨段'):text_privacy(m,Ambiguous(),lambda *a:None)

def test_separated_pdf_label_cannot_mask_unrelated_name():
    m=meta('Author:','Wu Mian')
    m['blocks'][0]['position'].update(bbox=[100,100,160,112])
    m['blocks'][1]['position'].update(bbox=[100,216,160,228])
    assert redact_frontmatter_blocks(m)==0


@pytest.mark.parametrize('field',[
    'Author: Li Ri Chuan   Tutor: Dong Feng',
    'Author：Li Xu  Tutor：Lecturer Jiang Hong',
    'Author：Hu Xiaomei  Tutor：Dongfeng',
    '    Author: Xu An Jing Instructor: Dong Feng',
    'Author: Zhou Yinan  Tutor: Li Cong',
])
def test_explicit_multiple_signatures_in_one_line_are_masked(field):
    m=meta(field,'Li Xu and Dong Feng are cited in the research.')
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][0]['text']=='[已脱敏]'
    assert m['blocks'][1]['text']=='Li Xu and Dong Feng are cited in the research.'


def test_contents_reference_entry_does_not_end_frontmatter_scan():
    m=meta('目 录','一、引言\t1','参考文献\t25','论文题名','Author: Sample Name')
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][-1]['text']=='[已脱敏]'
    assert m['blocks'][2]['text']=='参考文献\t25'


def test_long_contents_and_blank_blocks_do_not_consume_frontmatter_budget():
    m=meta('目录',*['章节\t1']*70,*['']*20,'论文题名','Author: Sample Name')
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][-1]['text']=='[已脱敏]'


@pytest.mark.parametrize('heading',['一、引言','一、 绪论','第一章 绪论','1. Introduction'])
def test_numbered_body_heading_stops_signature_masking(heading):
    m=meta('目录','参考文献\t25','论文题名','Author: Sample Name',heading,'Author: Adam Smith')
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][3]['text']=='[已脱敏]'
    assert m['blocks'][-1]['text']=='Author: Adam Smith'


@pytest.mark.parametrize('field',[
    'Author: Adam Smith Tutor: sample teaches accounting at the university.',
    'Author: Adam Smith Instructor:',
    'The Author: Adam Smith Tutor: John Doe',
])
def test_multi_role_prose_or_incomplete_fields_are_not_masked(field):
    m=meta(field)
    before=copy.deepcopy(m)
    assert redact_frontmatter_blocks(m)==0
    assert m==before


def test_same_line_signature_section_copies_change_without_deleting_research_names():
    field='Author: Xu An Jing Instructor: Dong Feng'
    m=meta(field,'Introduction','Dong Feng is the author of a cited work.')
    m['blocks'][0]['section']=[field]
    m['resources']=[{'id':'chart','section':[field], 'description':'Dong Feng'}]
    assert redact_frontmatter_blocks(m)==1
    assert m['blocks'][0]['section']==['[已脱敏]']
    assert m['resources'][0]['section']==['[已脱敏]']
    assert m['resources'][0]['description']=='Dong Feng'


def test_appendix_questionnaire_submitter_identity_is_masked_before_ai_without_losing_research_text():
    m=meta(*[f'正文段落{i}' for i in range(70)],
           '附录A 学生问卷',
           '各位同学好！我是样例大学2020级教育专业一班的张小测，为了了解课程参与情况，请回答以下问题。',
           '样例大学教育专业学生：张小测 说明：本问卷不记名。',
           '你认为样例大学的体育课程是否有帮助？')
    a=Agent();text_privacy(m,a,lambda *args:None)
    assert m['blocks'][71]['text']=='各位同学好！[已脱敏]，为了了解课程参与情况，请回答以下问题。'
    assert m['blocks'][72]['text']=='[已脱敏] 说明：本问卷不记名。'
    assert m['blocks'][73]['text']=='你认为样例大学的体育课程是否有帮助？'
    assert '张小测' not in a.seen
    assert '你认为样例大学的体育课程是否有帮助？' in a.seen


def test_research_interview_quote_outside_appendix_is_not_locally_erased():
    quote='受访者说：我是样例大学2020级教育专业一班的张小测，参加了课程。'
    m=meta('第一章 研究结果',quote)
    assert redact_frontmatter_blocks(m)==0
    assert m['blocks'][1]['text']==quote
