import io
import json
import zipfile

from native_chart import chart_table


def _archive(chart_xml):
    data=io.BytesIO()
    with zipfile.ZipFile(data,'w') as archive:
        archive.writestr('word/charts/chart1.xml',chart_xml)
    data.seek(0)
    return zipfile.ZipFile(data)


def test_chart_spec_preserves_native_line_recipe_without_inference_or_links():
    chart='''<c:chartSpace xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart"
        xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">
      <c:chart><c:title><c:tx><c:rich><a:p><a:r><a:t>内部趋势</a:t></a:r></a:p></c:rich></c:tx></c:title>
        <c:plotArea>
          <c:lineChart><c:grouping val="standard"/>
            <c:ser><c:idx val="4"/><c:order val="2"/><c:tx><c:v>内部利润率</c:v></c:tx>
              <c:cat><c:strLit><c:ptCount val="2"/><c:pt idx="0"><c:v>甲</c:v></c:pt><c:pt idx="1"><c:v>乙</c:v></c:pt></c:strLit></c:cat>
              <c:val><c:numRef><c:f>https://private.example/workbook.xlsx!A1:A2</c:f><c:numCache>
                <c:formatCode>0.00%</c:formatCode><c:ptCount val="2"/>
                <c:pt idx="0"><c:v>0.0100</c:v></c:pt><c:pt idx="1"><c:v>0.0250</c:v></c:pt>
              </c:numCache></c:numRef></c:val>
            </c:ser>
            <c:dLbls><c:showLegendKey val="0"/><c:showVal val="1"/><c:showCatName val="0"/>
              <c:numFmt formatCode="0.0% &quot;内部&quot;" sourceLinked="0"/><c:separator>内部 / </c:separator></c:dLbls>
            <c:axId val="10"/><c:axId val="20"/>
          </c:lineChart>
          <c:catAx><c:axId val="10"/><c:scaling><c:orientation val="minMax"/></c:scaling>
            <c:axPos val="b"/><c:title><c:tx><c:rich><a:p><a:r><a:t>内部年度</a:t></a:r></a:p></c:rich></c:tx></c:title>
            <c:crossAx val="20"/></c:catAx>
          <c:valAx><c:axId val="20"/><c:scaling><c:logBase val="10"/><c:min val="0.001"/><c:max val="1.000"/></c:scaling>
            <c:axPos val="l"/><c:title><c:tx><c:rich><a:p><a:r><a:t>内部百分比</a:t></a:r></a:p></c:rich></c:tx></c:title>
            <c:numFmt formatCode="0.00% &quot;内部&quot;" sourceLinked="0"/>
            <c:dispUnits><c:builtInUnit val="millions"/></c:dispUnits><c:crossAx val="10"/></c:valAx>
        </c:plotArea>
        <c:legend><c:legendPos val="r"/><c:overlay val="0"/></c:legend>
      </c:chart>
    </c:chartSpace>'''
    redact=lambda text:text.replace('内部','[已脱敏]')
    with _archive(chart) as archive:
        table=chart_table(archive,'word/charts/chart1.xml',{'body_index':7},redact)

    spec=table['native_chart']['chart_spec']
    assert spec=={
        'source':'native_ooxml',
        'chart_types':['lineChart'],
        'plots':[{
            'plot_index':0,'type':'lineChart','grouping':'standard','bar_direction':None,
            'axis_ids':['10','20'],
            'series_index_map':[{'cache_series_index':0,'source_index':'4','source_order':'2'}],
            'data_labels':[{
                'scope':'plot','series_index':None,'position':None,'number_format':{
                    'format_code':'0.0% "[已脱敏]"','source_linked':False},'separator':'[已脱敏] / ',
                'show_legend_key':False,'show_value':True,'show_category_name':False,
                'show_series_name':None,'show_percent':None,'show_bubble_size':None,
                'show_leader_lines':None,
            }],
        }],
        'axes':[
            {'id':'10','type':'catAx','position':'b','title':'[已脱敏]年度',
             'number_format':None,'display_units':None,
             'scaling':{'orientation':'minMax','log_base':None,'minimum':None,'maximum':None}},
            {'id':'20','type':'valAx','position':'l','title':'[已脱敏]百分比',
             'number_format':{'format_code':'0.00% "[已脱敏]"','source_linked':False},
             'display_units':{'built_in_unit':'millions','custom_unit':None,'label':None},
             'scaling':{'orientation':None,'log_base':'10','minimum':'0.001','maximum':'1.000'}},
        ],
        'legend':{'present':True,'position':'r','overlay':False},
        'data_labels':[{
            'scope':'plot','plot_index':0,'series_index':None,'position':None,
            'number_format':{'format_code':'0.0% "[已脱敏]"','source_linked':False},'separator':'[已脱敏] / ',
            'show_legend_key':False,'show_value':True,'show_category_name':False,
            'show_series_name':None,'show_percent':None,'show_bubble_size':None,
            'show_leader_lines':None,
        }],
    }
    encoded=json.dumps(spec,ensure_ascii=False)
    assert 'private.example' not in encoded and 'workbook.xlsx' not in encoded
    assert table['native_chart']['series'][0]['values']==['0.0100','0.0250']


def test_chart_spec_marks_absent_recipe_properties_unknown():
    chart='''<c:chartSpace xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart">
      <c:chart><c:plotArea><c:pieChart><c:ser><c:tx><c:v>系列</c:v></c:tx>
        <c:cat><c:strLit><c:ptCount val="1"/><c:pt idx="0"><c:v>类别</c:v></c:pt></c:strLit></c:cat>
        <c:val><c:numLit><c:ptCount val="1"/><c:pt idx="0"><c:v>1</c:v></c:pt></c:numLit></c:val>
      </c:ser></c:pieChart></c:plotArea></c:chart></c:chartSpace>'''
    with _archive(chart) as archive:
        spec=chart_table(archive,'word/charts/chart1.xml',{},lambda text:text)['native_chart']['chart_spec']

    assert spec['plots']==[{'plot_index':0,'type':'pieChart','grouping':None,'bar_direction':None,
        'axis_ids':[],'series_index_map':[{'cache_series_index':0,'source_index':None,'source_order':None}],
        'data_labels':[]}]
    assert spec['axes']==[]
    assert spec['legend']=={'present':False,'position':None,'overlay':None}
    assert spec['data_labels']==[]
