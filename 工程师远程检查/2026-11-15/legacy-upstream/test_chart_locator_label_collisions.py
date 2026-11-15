"""A category fragment must not borrow another chart's original PDF clip."""
import copy
import zipfile
from xml.etree import ElementTree as ET

import fitz
import pytest
from native_chart_region import C, W, R, find_native_chart_region
from test_native_chart_region import docx, add_clip, add_source_chart, target


def test_chart_with_prefixed_categories_cannot_supply_another_chart_clip(tmp_path):
    source = docx(tmp_path, categories=('Alpha','Beta','Gamma'))
    with zipfile.ZipFile(source) as z:
        files={n:z.read(n) for n in z.namelist()}
    root=ET.fromstring(files['word/document.xml']); body=root.find(W+'body')
    other=copy.deepcopy(body[0]); other.find('.//'+C+'chart').set(R+'id','rId8'); body.append(other)
    files['word/document.xml']=ET.tostring(root)
    rels=ET.fromstring(files['word/_rels/document.xml.rels']); rel=copy.deepcopy(rels[0])
    rel.set('Id','rId8'); rel.set('Target','charts/chart2.xml'); rels.append(rel)
    files['word/_rels/document.xml.rels']=ET.tostring(rels)
    chart=ET.fromstring(files['word/charts/chart1.xml'])
    for value,prefix in zip(chart.findall('.//'+C+'cat//'+C+'v'),('A.','B.','C.')):
        value.text=prefix+value.text
    files['word/charts/chart2.xml']=ET.tostring(chart)
    with zipfile.ZipFile(source,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    with fitz.open() as pdf:
        # Prefixes are separate native words, as in the real Chinese chart9.
        add_clip(pdf, labels=('A. Alpha','B. Beta','C. Gamma'))
        assert find_native_chart_region(source,target(),pdf) is None


def test_custom_label_punctuation_can_locate_its_exact_original_chart(tmp_path):
    source=docx(tmp_path)
    with zipfile.ZipFile(source) as z:files={n:z.read(n) for n in z.namelist()}
    chart=ET.fromstring(files['word/charts/chart1.xml']); labels=ET.SubElement(chart.find('.//'+C+'ser'),C+'dLbls')
    a='{http://schemas.openxmlformats.org/drawingml/2006/main}'
    for i,text in enumerate(('Alpha; 0.6033','Beta; 0.1033','Gamma; 0.2934')):
        label=ET.SubElement(labels,C+'dLbl');ET.SubElement(label,C+'idx',val=str(i))
        rich=ET.SubElement(ET.SubElement(label,C+'tx'),C+'rich')
        ET.SubElement(ET.SubElement(ET.SubElement(rich,a+'p'),a+'r'),a+'t').text=text
    files['word/charts/chart1.xml']=ET.tostring(chart)
    with zipfile.ZipFile(source,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    with fitz.open() as pdf:
        add_clip(pdf,labels=('Alpha; 0.6033','Beta; 0.1033','Gamma; 0.2934'))
        found=find_native_chart_region(source,target(),pdf)
        assert found is not None
        assert found['provenance']['bbox_pt']==[100,110,400,250]


def test_declared_data_label_separator_does_not_hide_the_original_chart(tmp_path):
    source=docx(tmp_path)
    with zipfile.ZipFile(source) as z:files={n:z.read(n) for n in z.namelist()}
    chart=ET.fromstring(files['word/charts/chart1.xml'])
    labels=ET.SubElement(chart.find('.//'+C+'ser'),C+'dLbls')
    ET.SubElement(labels,C+'separator').text='; '
    files['word/charts/chart1.xml']=ET.tostring(chart)
    with zipfile.ZipFile(source,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    with fitz.open() as pdf:
        add_clip(pdf,labels=('Alpha; 0.6033','Beta; 0.1033','Gamma; 0.2934'))
        found=find_native_chart_region(source,target(),pdf)
        assert found is not None
        assert found['provenance']['bbox_pt']==[100,110,400,250]


def test_punctuation_not_declared_by_the_chart_does_not_relax_label_matching(tmp_path):
    source=docx(tmp_path)
    with fitz.open() as pdf:
        add_clip(pdf,labels=('Alpha; 0.6033','Beta; 0.1033','Gamma; 0.2934'))
        assert find_native_chart_region(source,target(),pdf) is None


@pytest.mark.parametrize('wrong_value',[False,True])
def test_default_semicolon_label_requires_exact_source_category_and_raw_value(tmp_path, wrong_value):
    source=docx(tmp_path)
    with zipfile.ZipFile(source) as z:files={n:z.read(n) for n in z.namelist()}
    chart=ET.fromstring(files['word/charts/chart1.xml']);series=chart.find('.//'+C+'ser')
    values=ET.SubElement(ET.SubElement(series,C+'val'),C+'numLit')
    ET.SubElement(values,C+'ptCount',val='3');ET.SubElement(values,C+'formatCode').text='0.00%'
    labels=ET.SubElement(series,C+'dLbls')
    for i,value in enumerate(('0.6033','0.1033','0.2934')):
        ET.SubElement(ET.SubElement(values,C+'pt',idx=str(i)),C+'v').text=value
        label=ET.SubElement(labels,C+'dLbl');ET.SubElement(label,C+'idx',val=str(i))
        ET.SubElement(label,C+'numFmt',formatCode='General')
        ET.SubElement(label,C+'showCatName',val='1');ET.SubElement(label,C+'showVal',val='1')
    files['word/charts/chart1.xml']=ET.tostring(chart)
    with zipfile.ZipFile(source,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    with fitz.open() as pdf:
        add_clip(pdf,labels=('Alpha; 0.9000' if wrong_value else 'Alpha; 0.6033',
                            'Beta; 0.1033','Gamma; 0.2934'))
        found=find_native_chart_region(source,target(),pdf)
        assert (found is not None) is (not wrong_value)


@pytest.mark.parametrize('texts',[('Shared','Shared','Shared'),('Shared','Shared','Other')])
def test_repeated_custom_labels_never_borrow_a_clip_with_only_one_occurrence(tmp_path,texts):
    source=docx(tmp_path)
    with zipfile.ZipFile(source) as z:files={n:z.read(n) for n in z.namelist()}
    chart=ET.fromstring(files['word/charts/chart1.xml'])
    labels=ET.SubElement(chart.find('.//'+C+'ser'),C+'dLbls')
    a='{http://schemas.openxmlformats.org/drawingml/2006/main}'
    for i,text in enumerate(texts):
        label=ET.SubElement(labels,C+'dLbl');ET.SubElement(label,C+'idx',val=str(i))
        rich=ET.SubElement(ET.SubElement(label,C+'tx'),C+'rich')
        ET.SubElement(ET.SubElement(ET.SubElement(rich,a+'p'),a+'r'),a+'t').text=text
    files['word/charts/chart1.xml']=ET.tostring(chart)
    with zipfile.ZipFile(source,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    with fitz.open() as pdf:
        add_clip(pdf,labels=texts)
        add_clip(pdf,labels=tuple(dict.fromkeys(texts)))
        assert find_native_chart_region(source,target(),pdf) is None


def test_plain_categories_cannot_borrow_another_owners_custom_label_fragments(tmp_path):
    source=docx(tmp_path);add_source_chart(source)
    with zipfile.ZipFile(source) as z:files={n:z.read(n) for n in z.namelist()}
    chart=ET.fromstring(files['word/charts/chart2.xml'])
    labels=ET.SubElement(chart.find('.//'+C+'ser'),C+'dLbls')
    a='{http://schemas.openxmlformats.org/drawingml/2006/main}'
    for i,text in enumerate(('Alpha 90','Beta 80','Gamma 70')):
        label=ET.SubElement(labels,C+'dLbl');ET.SubElement(label,C+'idx',val=str(i))
        rich=ET.SubElement(ET.SubElement(label,C+'tx'),C+'rich')
        ET.SubElement(ET.SubElement(ET.SubElement(rich,a+'p'),a+'r'),a+'t').text=text
    files['word/charts/chart2.xml']=ET.tostring(chart)
    with zipfile.ZipFile(source,'w') as z:
        for n,b in files.items():z.writestr(n,b)
    with fitz.open() as pdf:
        add_clip(pdf,labels=('Alpha 90','Beta 80','Gamma 70'))
        assert find_native_chart_region(source,target(),pdf) is None
