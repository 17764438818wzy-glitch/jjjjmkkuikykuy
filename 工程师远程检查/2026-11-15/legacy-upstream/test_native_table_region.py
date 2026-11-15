import copy
import fitz
import pytest
from native_table_region import find_native_table_region


def grid_pdf(tmp_path, *, duplicate=False):
    path=tmp_path/'table.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page(width=600,height=800)
        for offset in [0,300] if duplicate else [0]:
            for y in [100,150,200,250]:page.draw_line((100,y+offset),(400,y+offset))
            for x in [100,200,300,400]:
                page.draw_line((x,(100 if x in [100,400] else 150)+offset),(x,250+offset))
            page.insert_text((110,125+offset),'Research table')
            for ri,row in enumerate([['A','10','12.5%'],['B','20','25.0%']],1):
                for ci,value in enumerate(row):page.insert_text((110+100*ci,125+50*ri+offset),value)
        pdf.save(path)
    return path


def native_table():
    cells=[{'row':0,'column':0,'rowspan':1,'colspan':3,'text':'Research table'}]
    cells.extend({'row':ri,'column':ci,'rowspan':1,'colspan':1,'text':value}
        for ri,row in enumerate([['A','10','[[IMAGE:image_1]][[IMAGE:image_2]]12.5%'],['B','20','25.0%']],1)
        for ci,value in enumerate(row))
    return {'rows':3,'columns':3,'cells':cells}


def test_unique_pdf_native_grid_proves_full_table_with_merged_title_and_original_decimal(tmp_path):
    target=native_table();before=copy.deepcopy(target)
    with fitz.open(grid_pdf(tmp_path)) as pdf:
        proof=find_native_table_region(target,pdf,{1})
    assert proof['page']==1
    assert proof['bbox']==pytest.approx([98.5/600,98.5/800,401.5/600,251.5/800])
    assert proof['provenance']['native_cell_count']==7
    assert target==before


@pytest.mark.parametrize('case',['duplicate','wrong_decimal','wrong_merge','continuation'])
def test_unproven_pdf_mapping_cannot_authorize_member_removal(tmp_path,case):
    target=native_table()
    if case=='wrong_decimal':target['cells'][-1]['text']='250%'
    if case=='wrong_merge':target['cells'][0]['colspan']=1
    if case=='continuation':target['cells'][0]['continuation_text']=[{'row':1,'text':'More'}]
    with fitz.open(grid_pdf(tmp_path,duplicate=case=='duplicate')) as pdf:
        assert find_native_table_region(target,pdf,{1}) is None
