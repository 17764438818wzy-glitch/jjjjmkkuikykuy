"""Bounded optional observations from the existing visual call, never inferred values."""
import copy
from agent_client import AgentError


def visual_fields(result):
    output={}
    def fail():raise AgentError('AI 图表结构字段无效，未生成猜测的数据')
    def text(value):
        if value is not None and (not isinstance(value,str) or len(value)>2000):fail()
        return value
    def strings(value,limit=100):
        if not isinstance(value,list) or len(value)>limit:fail()
        for v in value:
            if not isinstance(v,str):fail()
            text(v)
        return list(value)
    spec=result.get('chart_spec')
    if spec is not None:
        allowed={'chart_type','x_axis','y_axes','series','legend','data_labels'}
        if not isinstance(spec,dict) or set(spec)-allowed:fail()
        chart_type=spec.get('chart_type','unknown')
        if chart_type not in {'line','bar','scatter','pie','doughnut','area','radar','flowchart','combination','other','unknown'}:fail()
        clean={'source':'ai_visual_observation','chart_type':chart_type}
        x=spec.get('x_axis')
        if x is not None:
            if not isinstance(x,dict) or set(x)-{'label','unit'}:fail()
            x={k:text(x.get(k)) for k in ['label','unit']}
        clean['x_axis']=x
        axes=spec.get('y_axes',[])
        if not isinstance(axes,list) or len(axes)>4:fail()
        clean['y_axes']=[]
        for axis in axes:
            if not isinstance(axis,dict) or set(axis)-{'label','unit','side'} or axis.get('side') not in {None,'left','right','unknown'}:fail()
            clean['y_axes'].append({k:text(axis.get(k)) for k in ['label','unit','side']})
        series=spec.get('series',[])
        if not isinstance(series,list) or len(series)>100:fail()
        clean['series']=[]
        for row in series:
            if not isinstance(row,dict) or set(row)-{'name','axis_side','chart_type'} or row.get('axis_side') not in {None,'left','right','unknown'}:fail()
            kind=row.get('chart_type')
            if kind not in {None,'line','bar','scatter','pie','doughnut','area','radar','other','unknown'}:fail()
            clean['series'].append({**{k:text(row.get(k)) for k in ['name','axis_side']},'chart_type':kind})
        clean['legend']=strings(spec.get('legend',[]))
        labels=spec.get('data_labels')
        if labels is not None and type(labels) is not bool:fail()
        clean['data_labels']=labels;output['chart_spec']=clean
    table=result.get('structured_table')
    if table is not None:
        if not isinstance(table,dict) or set(table)-{'rows','header_rows','units','notes'}:fail()
        rows=table.get('rows')
        if not isinstance(rows,list) or not rows or len(rows)>1000 or not all(isinstance(r,list) and r for r in rows):fail()
        width=len(rows[0])
        if width>100 or sum(len(r) for r in rows)>20000 or any(len(r)!=width for r in rows):fail()
        for row in rows:strings(row)
        headers=table.get('header_rows',[])
        if not isinstance(headers,list) or any(type(v) is not int or not 0<=v<len(rows) for v in headers) or len(set(headers))!=len(headers):fail()
        output['structured_table']={'source':'ai_visual_transcription','rows':copy.deepcopy(rows),'header_rows':headers,
            'units':text(table.get('units')) or '', 'notes':strings(table.get('notes',[]))}
    return output
