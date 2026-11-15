"""Read chart caches without rounding, inferring blanks, or executing workbook links."""
import hashlib
from xml.etree import ElementTree as ET

C='{http://schemas.openxmlformats.org/drawingml/2006/chart}'
A='{http://schemas.openxmlformats.org/drawingml/2006/main}'


class ChartUnavailable(ValueError):pass


def _value(parent,name):
    node=parent.find(C+name) if parent is not None else None
    return node.get('val') if node is not None else None


def _boolean(value):
    if value in {'1','true','on'}:return True
    if value in {'0','false','off'}:return False
    return None


def _number_format(parent,redact):
    node=parent.find(C+'numFmt') if parent is not None else None
    if node is None:return None
    code=node.get('formatCode')
    return {'format_code':redact(code) if code is not None else None,
        'source_linked':_boolean(node.get('sourceLinked'))}


def _data_labels(node,scope,redact,series_index=None):
    if node is None:return None
    separator=node.findtext(C+'separator')
    return {'scope':scope,'series_index':series_index,'position':_value(node,'dLblPos'),
        'number_format':_number_format(node,redact),'separator':redact(separator) if separator is not None else None,
        'show_legend_key':_boolean(_value(node,'showLegendKey')),
        'show_value':_boolean(_value(node,'showVal')),
        'show_category_name':_boolean(_value(node,'showCatName')),
        'show_series_name':_boolean(_value(node,'showSerName')),
        'show_percent':_boolean(_value(node,'showPercent')),
        'show_bubble_size':_boolean(_value(node,'showBubbleSize')),
        'show_leader_lines':_boolean(_value(node,'showLeaderLines'))}


def _chart_spec(root,plot,redact):
    plots=[];all_labels=[];cache_series_index=0
    supported={'barChart','lineChart','areaChart','pieChart','doughnutChart','radarChart'}
    for plot_index,chart in enumerate(n for n in plot if n.tag.rsplit('}',1)[-1] in supported):
        chart_type=chart.tag.rsplit('}',1)[-1]
        series_map=[];labels=[]
        for ser in chart.findall(C+'ser'):
            source_index=_value(ser,'idx');source_order=_value(ser,'order')
            series_map.append({'cache_series_index':cache_series_index,'source_index':source_index,
                'source_order':source_order})
            item=_data_labels(ser.find(C+'dLbls'),'series',redact,source_index)
            if item is not None:labels.append(item)
            cache_series_index+=1
        item=_data_labels(chart.find(C+'dLbls'),'plot',redact)
        if item is not None:labels.append(item)
        plots.append({'plot_index':plot_index,'type':chart_type,'grouping':_value(chart,'grouping'),
            'bar_direction':_value(chart,'barDir'),
            'axis_ids':[node.get('val') for node in chart.findall(C+'axId') if node.get('val') is not None],
            'series_index_map':series_map,'data_labels':labels})
        all_labels.extend(dict(label,plot_index=plot_index) for label in labels)
    axes=[]
    for axis in plot:
        axis_type=axis.tag.rsplit('}',1)[-1]
        if not axis_type.endswith('Ax'):continue
        title_node=axis.find(C+'title')
        title=''.join(n.text or '' for n in title_node.iter(A+'t')) if title_node is not None else None
        units=axis.find(C+'dispUnits');display_units=None
        if units is not None:
            label=units.find(C+'dispUnitsLbl')
            label_text=''.join(n.text or '' for n in label.iter(A+'t')) if label is not None else None
            display_units={'built_in_unit':_value(units,'builtInUnit'),'custom_unit':_value(units,'custUnit'),
                'label':redact(label_text) if label_text is not None else None}
        scaling=axis.find(C+'scaling');scale=None
        if scaling is not None:
            scale={'orientation':_value(scaling,'orientation'),'log_base':_value(scaling,'logBase'),
                'minimum':_value(scaling,'min'),'maximum':_value(scaling,'max')}
        axes.append({'id':_value(axis,'axId'),'type':axis_type,'position':_value(axis,'axPos'),
            'title':redact(title) if title is not None else None,'number_format':_number_format(axis,redact),
            'display_units':display_units,'scaling':scale})
    legend_node=root.find('.//'+C+'chart/'+C+'legend')
    legend={'present':legend_node is not None,
        'position':_value(legend_node,'legendPos') if legend_node is not None else None,
        'overlay':_boolean(_value(legend_node,'overlay')) if legend_node is not None else None}
    return {'source':'native_ooxml','chart_types':[item['type'] for item in plots],
        'plots':plots,'axes':axes,'legend':legend,'data_labels':all_labels}


def points(container, numeric=False):
    if container is None:raise ChartUnavailable('图表缺少类别或数值')
    cache=next((n for n in container.iter() if n.tag in {C+'numCache',C+'strCache',C+'numLit',C+'strLit'}),None)
    if cache is None:raise ChartUnavailable('图表缓存未提供可靠数据')
    count=cache.find(C+'ptCount')
    if count is None:raise ChartUnavailable('图表缓存缺少点数')
    total=int(count.get('val','-1'))
    if not 0<total<=20000:raise ChartUnavailable('图表缓存点数无效')
    found={}
    for point in cache.findall(C+'pt'):
        index=int(point.get('idx','-1'));value=point.find(C+'v')
        if not 0<=index<total or index in found or value is None:raise ChartUnavailable('图表缓存索引或值缺失')
        text=value.text or ''
        if numeric and text:
            from decimal import Decimal,InvalidOperation
            try:valid=Decimal(text).is_finite()
            except InvalidOperation:valid=False
            if not valid:raise ChartUnavailable('图表含错误或非数值缓存')
        found[index]=text
    if set(found)!=set(range(total)):raise ChartUnavailable('图表缓存缺项，不能把缺失点自动改成空值或零')
    return [found[i] for i in range(total)],cache.findtext(C+'formatCode','')


def chart_table(archive,part,position,redact):
    blob=archive.read(part);root=ET.fromstring(blob)
    plot=root.find('.//'+C+'plotArea')
    if plot is None:raise ChartUnavailable('图表没有绘图区')
    if len(plot.findall(C+'valAx'))>1:
        raise ChartUnavailable('多数值轴图表需核对各系列的坐标轴与单位，交局部视觉恢复')
    if any(n.tag in {C+'errBars',C+'trendline',C+'dLbl',C+'extLst'} for n in root.iter()):
        raise ChartUnavailable('图表含误差线、趋势线、自定义标签或扩展数据，需局部视觉恢复')
    # Title uses rendered rich text only; never include field formulas or style numbers.
    title_node=root.find('.//'+C+'chart/'+C+'title')
    title=''.join(n.text or '' for n in title_node.iter(A+'t')) if title_node is not None else ''
    supported={'barChart','lineChart','areaChart','pieChart','doughnutChart','radarChart'}
    rows=[['数据系列','类别（原文顺序）','原始数值','原始数值格式','原始类别格式']]
    series_records=[]
    for chart in plot:
        series=chart.findall(C+'ser')
        if not series:continue
        if chart.tag.rsplit('}',1)[-1] not in supported:raise ChartUnavailable('此类原生图表需要局部视觉恢复')
        for ser in series:
            category=ser.find(C+'cat')
            if category is not None and category.find(C+'multiLvlStrRef') is not None:
                raise ChartUnavailable('多级类别暂交局部恢复，不能打平后假定对应关系完整')
            cats,cat_fmt=points(category);values,fmt=points(ser.find(C+'val'),numeric=True)
            if len(cats)!=len(values):raise ChartUnavailable('图表类别与数值点数不一致')
            tx=ser.find(C+'tx');name=''
            if tx is not None:
                literal=tx.find(C+'v')
                if literal is not None:name=literal.text or ''
                else:
                    names,_=points(tx)
                    if len(names)!=1:raise ChartUnavailable('图表系列名称不明确')
                    name=names[0]
            if not name:name=f'原图第{len(series_records)+1}个系列（未命名）'
            rows.extend([[redact(name),redact(cat),value,redact(fmt),redact(cat_fmt)] for cat,value in zip(cats,values)])
            series_records.append({'name':redact(name),'categories':[redact(c) for c in cats],'values':values,
                'format_code':redact(fmt),'category_format':redact(cat_fmt)})
    if not series_records:raise ChartUnavailable('图表未提供可读取的系列')
    axes=[]
    for axis in plot:
        if not axis.tag.endswith('Ax'):continue
        axis_title=''.join(n.text or '' for n in axis.findall('.//'+C+'title//'+A+'t'))
        units=axis.find(C+'dispUnits');unit_text=''
        if units is not None:
            unit_text='；'.join(n.tag.rsplit('}',1)[-1]+'='+n.get('val','') for n in units if n.tag in {C+'builtInUnit',C+'custUnit'})
        if axis_title or unit_text:axes.append(redact('；'.join(t for t in [axis_title,unit_text] if t)))
    cells=[{'row':r,'column':c,'rowspan':1,'colspan':1,'text':text} for r,row in enumerate(rows) for c,text in enumerate(row)]
    chart_spec=_chart_spec(root,plot,redact)
    return {'type':'table','position':dict(position),'source':'native_chart_cache','rows':len(rows),'columns':5,
        'cells':cells,'grid':[[r*5+c for c in range(5)] for r in range(len(rows))],'header_rows':[0],
        'header_verified':True,'complex':False,'caption':redact(title) or '原图表数据',
        'units':'；'.join(axes),'notes':['保留图表缓存原始数值及格式；百分比和显示单位未自动换算。'],
        'native_chart':{'part':part,'sha256':hashlib.sha256(blob).hexdigest(),'title':redact(title),'series':series_records,
            'axes':axes,'chart_spec':chart_spec},
        'context_before':[],'context_after':[]}
