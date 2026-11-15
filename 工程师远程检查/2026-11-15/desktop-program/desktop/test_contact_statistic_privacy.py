"""A chart's platform percentage is data, while actual contact accounts stay private."""
import copy

import pytest

from auto_evidence import text_privacy
from evidence import redactor


@pytest.mark.parametrize('text',[
    '微信 50%', '微信：50％', '微信 50 %', 'WeChat: 62.5 %',
    'QQ 36.5%', 'QQ：14.5\t％', '微信 +2.5%', 'WeChat -2.5 %',
    'H.微信 50%\nI.电子邮件 7%\nJ.班级QQ群、微信群 36.5%',
])
def test_platform_chart_percentages_keep_original_characters(text):
    assert redactor()(text)==text


@pytest.mark.parametrize('text,expected',[
    ('微信：study_lab2026','微信：[CONTACT]'),
    ('WeChat: Example_2026','WeChat: [CONTACT]'),
    ('QQ 223456789','QQ [CONTACT]'),
    ('QQ: 223456789','QQ: [CONTACT]'),
    ('手机：13800138000','手机：[PHONE]'),
    ('电话 13800138000','电话 [PHONE]'),
    ('微信 50_user','微信 [CONTACT]'),
    ('微信 50','微信 [CONTACT]'),
    ('微信 50 %private','微信 [CONTACT] %private'),
])
def test_actual_or_ambiguous_contact_values_remain_redacted(text,expected):
    assert redactor()(text)==expected


def test_percentage_exception_does_not_exempt_an_account_elsewhere():
    source='H.微信 50 %；联系人微信：study_lab2026；QQ 223456789；电话 13800138000'
    assert redactor()(source)=='H.微信 50 %；联系人微信：[CONTACT]；QQ [CONTACT]；电话 [PHONE]'


def test_chart_ocr_percentage_no_longer_triggers_pixel_text_privacy_mismatch():
    chart='H.微信 50%\nI.电子邮件 7%\nJ.班级QQ群、微信群 36.5%'
    item={'title':'家园共育方式的喜好程度','formulas':[],
          'blocks':[{'type':'paragraph','text':'各类家园共育方式的调查比例。'}],
          'resources':[{'id':'image_0007','ocr':{'status':'recognized','text':chart},
                        'description':{'text':'家长喜好方式的条形统计图'}}]}
    expected=copy.deepcopy(item['resources'])
    class Agent:
        def call(self,task,payload,images=()):
            assert task=='privacy_text'
            assert '[CONTACT]' not in payload['text']
            return {'decision':'pass','reason':'图中为研究比例，没有实际私人联系方式',
                    'sensitive_strings':[]}
    text_privacy(item,Agent(),lambda *args:None)
    assert item['resources']==expected
    assert item['text_privacy_complete'] is True
