import pytest
from evidence import redactor
from auto_evidence import text_privacy

@pytest.mark.parametrize('text',[
    '所有者权益总额从14603100740元增长到15372423363元。',
    '总资产为16526890259 元。','净资产为人民币16526890259元。',
    'Total assets: 16526890259 yuan.', 'Revenue USD 16526890259.',
    '金额￥16526890259','16526890259',
])
def test_financial_amounts_are_not_phone_numbers(text):
    assert redactor()(text)==text

def test_same_digits_are_preserved_only_in_monetary_context():
    text='资产16526890259元；联系电话：16526890259。'
    result=redactor()(text)
    assert '资产16526890259元' in result
    assert result.count('16526890259')==1

def test_unlabelled_phone_is_still_redacted():
    assert '16526890259' not in redactor()('请致电16526890259联系。')

def test_context_survives_text_privacy_before_model_call():
    text='所有者权益总额从14603100740元增长到15372423363元。'
    meta={'title':'财务分析','blocks':[{'type':'paragraph','text':text}],'resources':[],'formulas':[]}
    class Agent:
        def call(self,task,payload,**kw):
            assert text in payload['text'] and '[PHONE]' not in payload['text']
            return {'decision':'pass','reason':'只有财务数据','sensitive_strings':[]}
    text_privacy(meta,Agent(),lambda *args:None)
    assert meta['blocks'][0]['text']==text
