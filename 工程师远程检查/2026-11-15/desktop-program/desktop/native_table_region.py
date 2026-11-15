"""Prove a complete native table boundary through existing PDF table grids."""
import hashlib
import json
import re
import unicodedata


def _cell_text(value):
    value=re.sub(r'\[\[IMAGE:[^]]+\]\]','',value or '')
    return re.sub(r'\s+','',unicodedata.normalize('NFKC',value))


def find_native_table_region(target,pdf,candidate_pages):
    """Require one complete grid with every native cell and merge unchanged.

    This is only geometry evidence. It does not accept privacy/OCR and never
    copies PDF spellings into native text. Split/ambiguous tables fail closed.
    """
    cells=target.get('cells',[]);rows=target.get('rows');columns=target.get('columns')
    if not cells or type(rows) is not int or type(columns) is not int:return None
    if any(cell.get('continuation_text') for cell in cells):return None
    if sum(bool(_cell_text(cell.get('text'))) for cell in cells)<3:return None
    found=[]
    try:
        for page_number in sorted(candidate_pages):
            page=pdf[page_number-1]
            for table in page.find_tables().tables:
                if (table.row_count,table.col_count)!=(rows,columns):continue
                rendered=table.extract();rectangles=[row.cells for row in table.rows]
                valid=True;covered=set()
                for cell in cells:
                    row,column=cell['row'],cell['column'];rs,cs=cell['rowspan'],cell['colspan']
                    if rectangles[row][column] is None or _cell_text(rendered[row][column])!=_cell_text(cell['text']):
                        valid=False;break
                    for r in range(row,row+rs):
                        for c in range(column,column+cs):
                            if (r,c) in covered:valid=False
                            covered.add((r,c))
                            if (r,c)!=(row,column) and (rectangles[r][c] is not None or _cell_text(rendered[r][c])):
                                valid=False
                if not valid or covered!={(r,c) for r in range(rows) for c in range(columns)}:continue
                # Include the full original border stroke, without clipping.
                x0,y0,x1,y1=table.bbox;rect=page.rect
                bbox=[max(0,(x0-1.5)/rect.width),max(0,(y0-1.5)/rect.height),
                      min(1,(x1+1.5)/rect.width),min(1,(y1+1.5)/rect.height)]
                found.append({'page':page_number,'bbox':bbox,'provenance':{
                    'method':'native_table_unique_pdf_grid','native_rows':rows,'native_columns':columns,
                    'native_cell_count':len(cells),'native_cell_text_sha256':hashlib.sha256(json.dumps(
                        [[c['row'],c['column'],c['rowspan'],c['colspan'],_cell_text(c['text'])] for c in cells],
                        ensure_ascii=False,separators=(',',':')).encode()).hexdigest(),
                    'pdf_table_bbox_points':list(table.bbox),'border_padding_points':1.5}})
    except (AttributeError,IndexError,KeyError,TypeError,ValueError):return None
    return found[0] if len(found)==1 else None
