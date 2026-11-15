"""Reproducible synthetic acceptance and read-only real-source sampling."""
import json
from pathlib import Path
from docx import Document
from docx.oxml import OxmlElement
from PIL import Image,ImageDraw,ImageFont
from evidence import build_package,sha
from test_evidence import make_document


def main():
    root=Path(__file__).parent/'acceptance-evidence';root.mkdir(exist_ok=True)
    source=make_document(root/'accounting.docx')
    build_package(source,root/'ACC_000001',{'id':'UP_acceptance','legacy_id':'ACC_000001','title':'企业经营数据研究','major':'会计学'},render_screenshots=True,run_ocr=True)
    font=ImageFont.truetype(r'C:\Windows\Fonts\arial.ttf',22)
    trend=Image.new('RGB',(700,440),'white');draw=ImageDraw.Draw(trend)
    draw.line([(75,60),(75,350),(620,350)],fill='black',width=3)
    draw.line([(170,230),(500,120)],fill='blue',width=4)
    for xy,label in [((140,360),'2022'),((470,360),'2023'),((160,195),'100'),((490,85),'120'),((95,20),'Revenue (10k CNY)')]: draw.text(xy,label,font=font,fill='black')
    trend.save(root/'trend.png')
    raster=Image.new('RGB',(700,230),'white');draw=ImageDraw.Draw(raster)
    for y in [10,75,140,210]:draw.line((10,y,690,y),fill='black',width=2)
    for x in [10,220,450,690]:draw.line((x,10,x,210),fill='black',width=2)
    for ri,row in enumerate([['Year','Revenue','Profit'],['2022','100.00','(10)'],['2023','120','-2.50%']]):
        for ci,text in enumerate(row):draw.text((25+ci*220,25+ri*65),text,font=font,fill='black')
    raster.save(root/'table_screenshot.png')
    doc=Document();doc.add_paragraph('论文题目：跨页会计数据检验');doc.add_heading('摘要',1)
    doc.add_paragraph('保留跨页表格结构和图片证据。');doc.add_heading('第一章 数据',1)
    doc.add_paragraph('表1 逐期数据，单位：万元')
    table=doc.add_table(rows=78,cols=3);table.style='Table Grid'
    table.cell(0,0).merge(table.cell(1,0)).text='年份'
    table.cell(0,1).merge(table.cell(0,2)).text='经营数据'
    table.cell(1,1).text='营业收入';table.cell(1,2).text='净利润'
    for ri in range(2,78):
        values=[str(2000+ri),str(ri)+'.00','-2.50%' if ri%2 else '—']
        for cell,text in zip(table.rows[ri].cells,values):cell.text=text
    for row in table.rows[:2]:row._tr.get_or_add_trPr().append(OxmlElement('w:tblHeader'))
    doc.add_paragraph('数据来源：合成测试数据。')
    doc.add_paragraph('图1 收入趋势');doc.add_picture(str(root/'trend.png'))
    doc.add_paragraph('图2 截图型表格');doc.add_picture(str(root/'table_screenshot.png'))
    doc.add_heading('参考文献',1);doc.add_paragraph('[1] 测试来源。')
    src=root/'crosspage.docx';doc.save(src)
    m=build_package(src,root/'ACC_CROSSPAGE',{'id':'UP_crosspage','title':'跨页会计数据检验'},render_screenshots=True,run_ocr=True)
    t=next(b for b in m['blocks'] if b['type']=='table')
    assert t['rows']==78 and len(t['source_page_candidates'])>1
    ingest=(root/'ACC_CROSSPAGE'/'ingest.md').read_text(encoding='utf-8')
    assert '年份=2077' in ingest and '营业收入=77.00' in ingest and '净利润=-2.50%' in ingest
    summary={'synthetic':{'crosspage_rows':t['rows'],'source_pages':t['source_page_candidates'],
                         'images':sum(b['type']=='image' for b in m['blocks']),'resources':len(m['resources']),
                         'ocr_states':[r.get('ocr',{}).get('status') for r in m['resources']]},'real':[]}
    selected=root/'real-sources.json'
    if selected.exists():
        for i,row in enumerate(json.loads(selected.read_text(encoding='utf-8'))):
            source=Path(row['path']);before=sha(source)
            m=build_package(source,root/f'REAL_{i+1:02d}',{'id':f'UP_real_{i+1}','title':'会计学原文核对样本'},render_screenshots=True,run_ocr=i==0)
            assert sha(source)==before
            summary['real'].append({'sample':i+1,'source_unchanged':True,'tables':sum(b['type']=='table' for b in m['blocks']),
                                  'images':sum(b['type']=='image' for b in m['blocks']),'resources':len(m['resources']),
                                  'review_status':m['review_status'],'issues':list(dict.fromkeys(x['code'] for x in m['issues']))})
    (root/'sample-results.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=True))


if __name__=='__main__': main()
