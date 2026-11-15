"""Source identity fixtures; never infer degrees from body or file names."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import zipfile

import pytest
from docx import Document


def api():
    assert importlib.util.find_spec('source_education'), 'missing deterministic source education extraction'
    import source_education
    return source_education


def check(value):
    path = Path(__file__).parent/'docs/contracts/source-education-v1/check_examples.py'
    spec = importlib.util.spec_from_file_location('frozen_check', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.check(api().source_education_fields(value))


@pytest.mark.parametrize('cover,values', [
    ('本科毕业论文', ('bachelor','unknown','unknown')),
    ('成人本科毕业论文', ('bachelor','adult_education','correspondence')),
    ('函授本科毕业论文', ('bachelor','correspondence','correspondence')),
    ('成人教育', ('unknown','adult_education','correspondence')),
    ('毕业论文', ('unknown','unknown','unknown')),
    ('非全日制硕士研究生学位论文', ('master','part_time','unknown')),
    ('高等学历继续教育本科毕业论文', ('bachelor','unknown','unknown')),
])
def test_cover_three_independent_values(tmp_path, cover, values):
    source = tmp_path/'博士函授.txt'
    source.write_text(cover+'\n指导教师：硕士 测试导师\n摘要\n本文调查本科和函授学生。', encoding='utf8')
    result = api().extract_source_education(source)
    assert tuple(result[k]['value'] for k in ('education_level','study_mode','business_classification')) == values
    assert all('导师' not in row['quote'] and '学生' not in row['quote'] for row in result['evidence'])
    check(result)


def test_conflicting_identity_fields_keep_all_candidates(tmp_path):
    source = tmp_path/'paper.md'
    source.write_text('本科毕业论文\n学历：硕士\n学习形式：函授\n教育形式：成人教育\n摘要', encoding='utf8')
    value = api().extract_source_education(source)
    assert value['education_level']['status'] == 'conflict'
    assert {c['value'] for c in value['education_level']['candidates']} == {'bachelor','master'}
    assert value['study_mode']['status'] == 'conflict'
    assert value['business_classification']['status'] == 'conflict'
    check(value)


def test_docx_locators_count_empty_table_and_unselected_branch(tmp_path):
    source = tmp_path/'paper.docx'
    ns='http://schemas.openxmlformats.org/wordprocessingml/2006/main'
    xml=f'''<w:document xmlns:w="{ns}" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"><w:body>
    <w:p/><mc:AlternateContent><mc:Choice Requires="wps"><w:p><w:r><w:t>本科毕业论文</w:t></w:r></w:p></mc:Choice><mc:Fallback><w:p><w:r><w:t>博士毕业论文</w:t></w:r></w:p></mc:Fallback></mc:AlternateContent>
    <w:tbl><w:tr><w:tc><w:p><w:r><w:t>学习形式：</w:t></w:r></w:p></w:tc><w:tc><w:p/><w:p><w:r><w:t>函授</w:t></w:r></w:p></w:tc></w:tr></w:tbl>
    <w:p><w:r><w:t>摘要</w:t></w:r></w:p><w:p><w:r><w:t>博士研究对象</w:t></w:r></w:p></w:body></w:document>'''
    with zipfile.ZipFile(source,'w') as archive: archive.writestr('word/document.xml',xml)
    value = api().extract_source_education(source)
    assert value['education_level']['value'] == 'bachelor'
    assert value['study_mode']['value'] == 'correspondence'
    assert [row['locator']['paragraph_index'] for row in value['evidence']] == [2,6]
    check(value)


def test_minimal_quotes_exclude_personal_fields_and_advisor_degree(tmp_path):
    source=tmp_path/'paper.docx';doc=Document()
    doc.add_paragraph('姓名：合成姓名 学号：123456 本科毕业论文 指导教师：合成导师 硕士')
    doc.add_paragraph('研究对象：函授本科学生');doc.add_paragraph('摘要');doc.save(source)
    value=api().extract_source_education(source)
    assert value['education_level']['value']=='bachelor'
    assert value['study_mode']['value']=='unknown'
    assert [row['quote'] for row in value['evidence']]==['本科毕业论文']
    check(value)


def test_pdf_original_page_and_body_boundary(tmp_path):
    import fitz
    source=tmp_path/'paper.pdf';doc=fitz.open();doc.new_page()
    page=doc.new_page();page.insert_text((40,50),'成人本科毕业论文',fontname='china-s')
    page.insert_text((40,90),'摘要',fontname='china-s')
    page.insert_text((40,120),'正文谈论函授',fontname='china-s');doc.save(source);doc.close()
    value=api().extract_source_education(source)
    assert value['study_mode']['value']=='adult_education'
    assert value['evidence'][0]['locator']=={'kind':'pdf_page','page':2}
    check(value)


@pytest.mark.parametrize('relation',['converted','normalized'])
def test_derived_files_preserve_both_hashes_and_record(tmp_path,relation):
    original=tmp_path/'input.doc';original.write_bytes(b'synthetic original binary')
    source=tmp_path/'converted.docx';doc=Document();doc.add_paragraph('本科毕业论文');doc.save(source)
    value=api().extract_source_education(source,original_source=original,source_relation=relation,
                                       derivation_method='fixture conversion',derivation_record_id='private-record-1')
    assert value['original_source_sha256']==hashlib.sha256(original.read_bytes()).hexdigest()
    assert value['evidence_source_sha256']==hashlib.sha256(source.read_bytes()).hexdigest()
    assert value['derivation']['record_id']=='private-record-1'
    check(value)
    with pytest.raises(ValueError):api().extract_source_education(source,original_source=original)
    with pytest.raises(ValueError):api().extract_source_education(source,original_source=original,source_relation=relation)


def test_revision_uses_canonical_bytes_and_rejects_unsafe_numbers(tmp_path):
    source=tmp_path/'paper.txt';source.write_text('\n\n学历：本科\n摘要',encoding='utf8')
    value=api().extract_source_education(source)
    assert value['evidence'][0]['locator']=={'kind':'text_lines','start_line':3,'end_line':3}
    expected=hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    assert api().source_education_revision(dict(reversed(list(value.items()))))==expected
    for number in (1.0,9007199254740993):
        bad=copy.deepcopy(value);bad['evidence'][0]['locator']['start_line']=number
        with pytest.raises((ValueError,TypeError)):api().source_education_revision(bad)


@pytest.mark.parametrize('use_table',[False,True])
def test_advisor_label_and_degree_in_separate_rows_do_not_conflict(tmp_path,use_table):
    source=tmp_path/'paper.docx';doc=Document();doc.add_paragraph('本科毕业论文')
    if use_table:
        table=doc.add_table(rows=2,cols=2)
        table.cell(0,0).text='指导教师';table.cell(0,1).text='合成导师'
        table.cell(1,0).text='学历：';table.cell(1,1).text='硕士'
    else:
        doc.add_paragraph('指导教师');doc.add_paragraph('合成导师');doc.add_paragraph('学历：硕士')
    doc.add_paragraph('摘要');doc.save(source)
    result=api().extract_source_education(source)
    assert result['education_level']['value']=='bachelor'
    assert result['education_level']['status']=='source_explicit'
    check(result)


def test_spaced_cover_title_and_no_colon_table_identity(tmp_path):
    source=tmp_path/'paper.docx';doc=Document()
    doc.add_paragraph('本 科 毕 业 论 文')
    table=doc.add_table(rows=1,cols=2);table.cell(0,0).text='学 习 形 式';table.cell(0,1).text='函 授'
    doc.add_paragraph('摘要');doc.save(source)
    result=api().extract_source_education(source)
    assert result['education_level']['value']=='bachelor'
    assert result['study_mode']['value']=='correspondence'
    assert result['education_level']['raw_labels']==['本 科']
    assert result['study_mode']['raw_labels']==['函 授']
    assert result['evidence'][0]['quote']=='本 科 毕 业 论 文'
    check(result)


@pytest.mark.parametrize('line', [
    '本文讨论函授本科毕业论文写作方法。',
    '这是一篇关于函授本科毕业论文的研究综述。',
    '指 导 教 师 学历：硕士',
    '调查对象：函授本科学生',
])
def test_unmarked_body_and_spaced_advisor_are_not_identity(tmp_path,line):
    source=tmp_path/'paper.txt';source.write_text(line+'\n参考文献',encoding='utf8')
    result=api().extract_source_education(source)
    assert result['education_level']['value']=='unknown'
    assert result['study_mode']['value']=='unknown'
    assert result['evidence']==[]


def test_long_formal_designation_keeps_schema_bounded_exact_excerpts(tmp_path):
    source=tmp_path/'paper.txt';source.write_text('成人本科'*80+'毕业论文\n摘要',encoding='utf8')
    result=api().extract_source_education(source)
    assert result['education_level']['value']=='bachelor'
    assert result['study_mode']['value']=='adult_education'
    assert all(len(row['quote'])<=240 for row in result['evidence'])
    check(result)


def test_formal_bachelor_declaration_and_yearly_correspondence_banner(tmp_path):
    source=tmp_path/'paper.docx';doc=Document()
    doc.add_paragraph('高等学历继续教育（专升本）毕业论文')
    doc.add_paragraph('  2037年函授     ')
    doc.add_paragraph('本科生毕业论文独创性声明')
    doc.add_paragraph('摘要');doc.add_paragraph('非全日制硕士研究生调查对象');doc.save(source)
    result=api().extract_source_education(source)
    assert result['education_level']['value']=='bachelor'
    assert result['study_mode']['value']=='correspondence'
    assert result['business_classification']['value']=='correspondence'
    assert result['education_level']['raw_labels']==['本科']
    assert result['study_mode']['raw_labels']==['函授']
    assert [row['locator']['paragraph_index'] for row in result['evidence']]==[2,3]
    check(result)


def review_source(tmp_path,format,text):
    source=tmp_path/('review.'+format)
    if format=='txt':source.write_text(text,encoding='utf8')
    else:
        doc=Document()
        for line in text.splitlines():doc.add_paragraph(line)
        doc.save(source)
    return api().extract_source_education(source)


@pytest.mark.parametrize('format',['txt','docx'])
@pytest.mark.parametrize('text,degree,mode',[
    ('毕业论文\n1 引言\n案例材料\n学历：本科\n学习形式：函授','unknown','unknown'),
    ('毕业论文\n一 引言\n案例材料\n学历：本科\n学习形式：函授','unknown','unknown'),
    ('本科毕业论文\n导师代表作\n博士学位论文\n摘要','bachelor','unknown'),
    ('毕业论文\n参与者基本信息\n学历：本科\n学习形式：函授','unknown','unknown'),
    ('毕业论文\n父亲学历：博士\n摘要','unknown','unknown'),
    ('毕业论文\n指导教师：合成导师\n学位：博士\n作者：合成作者\n学历：本科\n学习形式：函授\n摘要','bachelor','correspondence'),
    ('高等学历继续教育（专升本）毕业论文\n2037年函授\n指导教师：合成导师\n本科生毕业论文独创性声明\n摘要','bachelor','correspondence'),
])
def test_review_role_and_body_scope(tmp_path,format,text,degree,mode):
    value=review_source(tmp_path,format,text)
    assert value['education_level']['value']==degree
    assert value['study_mode']['value']==mode
    assert value['education_level']['status']!='conflict'
    check(value)


@pytest.mark.parametrize('format',['txt','docx'])
@pytest.mark.parametrize('text,field',[
    ('本科毕业论文\n'*32+'硕士学位论文\n摘要','education_level'),
    ('\n'.join('学历：本'+' '*i+'科' for i in range(1,17))+'\n学历：硕士\n摘要','education_level'),
    ('\n'.join('学习形式：'+v for v in ['函授','成人教育','全日制','非全日制','业余','脱产','自考','开放教育','网络教育'])+'\n摘要','study_mode'),
])
def test_review_bounded_conflict_representatives(tmp_path,format,text,field):
    value=review_source(tmp_path,format,text)
    assert value[field]['status']=='conflict'
    assert value[field]['value']=='unknown'
    assert len(value[field]['candidates'])>=2
    check(value)
    reread=api().extract_source_education(tmp_path/('review.'+format))
    assert api().source_education_revision(reread)==api().source_education_revision(value)


@pytest.mark.parametrize('format',['txt','docx'])
def test_review_unknown_mode_quote_stops_at_unpunctuated_name(tmp_path,format):
    value=review_source(tmp_path,format,'毕业论文\n学习形式：走读姓名合成作者\n摘要')
    assert value['study_mode']['value']=='other'
    assert value['study_mode']['raw_labels']==['走读']
    assert [row['quote'] for row in value['evidence']]==['走读']
    check(value)


@pytest.mark.parametrize('format',['txt','docx'])
@pytest.mark.parametrize('heading,degree,mode',[
    ('作者信息','bachelor','correspondence'),
    ('本人信息','bachelor','correspondence'),
    ('作者姓名','bachelor','correspondence'),
    ('导师代表作','unknown','unknown'),
    ('导师作者信息','unknown','unknown'),
    ('导师本人信息','unknown','unknown'),
    ('研究参与者信息','unknown','unknown'),
    ('作者信息统计','unknown','unknown'),
])
def test_author_information_heading_restores_only_author_scope(tmp_path,format,heading,degree,mode):
    value=review_source(tmp_path,format,'毕业论文\n指导教师：合成导师\n学位：博士\n'+heading+'\n学历：本科\n学习形式：函授\n摘要')
    assert value['education_level']['value']==degree
    assert value['study_mode']['value']==mode
    assert value['education_level']['status']!='conflict'
    assert all(candidate['value']!='doctor' for candidate in value['education_level']['candidates'])
    check(value)
