"""Lossless table data and a clearly labelled structural preview; no model I/O."""
import copy
import csv
import html
import io
import textwrap


def table_data(block, object_id):
    cells=copy.deepcopy(block['cells']); grid=copy.deepcopy(block['grid'])
    width=block['columns']; headers=list(block.get('header_rows',[]))
    if type(width) is not int or width<1 or not grid:
        raise ValueError('知识包表格没有有效网格')
    rows=[]
    for row in grid:
        if len(row)!=width: raise ValueError('知识包表格列数不一致')
        values=[]
        for index in row:
            if index is None:values.append('')
            elif type(index) is int and 0<=index<len(cells) and isinstance(cells[index].get('text'),str):
                values.append(cells[index]['text'])
            else:raise ValueError('知识包表格单元格引用无效')
        rows.append(values)
    if any(type(r) is not int or not 0<=r<len(rows) for r in headers):
        raise ValueError('知识包表头位置无效')
    fields=[]
    for col in range(width):
        labels=[];seen=set()
        for row in headers:
            cell=grid[row][col]
            if cell is not None and cell not in seen:
                labels.append(rows[row][col]);seen.add(cell)
        fields.append({'key':f'c{col}','column_index':col,'header_path':labels,
                       'label':' / '.join(x for x in labels if x),'value_type':'original_text'})
    data_indices=[r for r in range(len(rows)) if r not in headers]
    dataset={'id':object_id+'_data','layout':'table_rows','fields':fields,
             'records':[{f'c{c}':rows[r][c] for c in range(width)} for r in data_indices],
             'source_row_indices':data_indices,'empty_value':'','numeric_conversion':'not_performed',
             'header_status':'verified' if block.get('header_verified') or block.get('automatic_verified') else 'inferred',
             'provenance':block.get('source','unknown'),'externally_verified':False}
    if block.get('native_chart'):
        dataset.update(layout='series_points',series=copy.deepcopy(block['native_chart'].get('series',[])))
    return {'cells':cells,'grid':grid,'rows':rows,'header_rows':headers,'fields':fields,
            'units':block.get('units',''),'notes':copy.deepcopy(block.get('notes',[])),
            'native_structure':block.get('native_xml'), 'dataset':dataset}


def csv_text(rows):
    output=io.StringIO(newline='');writer=csv.writer(output)
    writer.writerows(rows)
    return output.getvalue()


def table_preview(table):
    """SVG is a structural reconstruction, never advertised as an original crop."""
    rows=table['rows']; cols=len(rows[0]); col_width=190; margin=20; top=62
    heights=[36]*len(rows);wrapped={}
    for i,c in enumerate(table['cells']):
        width=max(1,c.get('colspan',1)); lines=[]
        for paragraph in c['text'].split('\n'):
            lines.extend(textwrap.wrap(paragraph,width=max(8,width*12),replace_whitespace=False,drop_whitespace=False) or [''])
        wrapped[i]=lines
        start=c['row'];span=max(1,c.get('rowspan',1))
        if 0<=start<len(rows):
            needed=(len(lines)*22+16)/span
            for row in range(start,min(start+span,len(rows))):heights[row]=max(heights[row],int(needed+1))
    y=[top]
    for height in heights:y.append(y[-1]+height)
    body=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{cols*col_width+margin*2}" height="{y[-1]+margin}">',
          '<rect width="100%" height="100%" fill="white"/>',
          '<g font-family="Microsoft YaHei,SimSun,sans-serif" font-size="15" fill="#20242a">',
          '<text x="20" y="25">表格结构预览（根据单元格重绘，非原文截图）</text>']
    for i,c in enumerate(table['cells']):
        row,col=c['row'],c['column']
        if not (0<=row<len(rows) and 0<=col<cols):raise ValueError('知识包单元格位置无效')
        end=min(row+max(1,c.get('rowspan',1)),len(rows))
        w=min(max(1,c.get('colspan',1)),cols-col)*col_width; x=margin+col*col_width
        fill='#edf1f5' if row in table['header_rows'] else '#ffffff'
        body.append(f'<rect x="{x}" y="{y[row]}" width="{w}" height="{y[end]-y[row]}" fill="{fill}" stroke="#586474"/>')
        for n,line in enumerate(wrapped[i]):
            body.append(f'<text x="{x+8}" y="{y[row]+24+n*22}">{html.escape(line)}</text>')
    body.append('</g></svg>')
    return '\n'.join(body)
