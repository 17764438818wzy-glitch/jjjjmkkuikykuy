"""Blank field labels and explicit statistics are not populated identities."""
import copy

import pytest

from auto_evidence import Rejected,text_privacy
from evidence import redactor


SAFE_FORMS=[
    '学校    年级    班级    姓名',
    '观察班级：  授课教师：  授课内容：  观察时间：  年  月  日  课堂性质：',
    '班级 数量/节\n(1)班 122\n(2)班 77\n(3)班 149',
    '编号 | 性别 | 任教班级 | 学历 | 教龄 | 专业',
    '班级\t数量（人）\n大一班\t32\n大二班\t31',
    '电话、微信、QQ 92.31%、95.74%',
    '电话、微信、QQ 87.91%、89.36%',
    'QQ：+12.50％、-2.50%、3.00％',
]


@pytest.mark.parametrize('source',SAFE_FORMS)
def test_empty_labels_and_explicit_statistics_preserve_every_original_character(source):
    assert redactor()(source).encode('utf-8')==source.encode('utf-8')


@pytest.mark.parametrize('source,token',[
    ('班级：高二三班','[CLASS]'),
    ('专业班级 2021级软件工程2班','[CLASS]'),
    ('班级：大一班 姓名：张小测','[CLASS]'),
    ('班级 姓名：张小测','[CLASS]'),
    ('观察班级：授课教师：张小测 授课内容：计算','[CLASS]'),
    ('班级 | 学历 | 教龄 | 专业 | 高二三班','[CLASS]'),
    ('班级 数量/节 班级值','[CLASS]'),
    ('班级 数量（人）32','[CLASS]'),
    ('QQ 223456789','[CONTACT]'),
    ('QQ 92.31%、study_account','[CONTACT]'),
    ('QQ 92.31%、223456789','[CONTACT]'),
    ('QQ 92.31、95.74%','[CONTACT]'),
    ('QQ 92.31%、95.74','[CONTACT]'),
    ('QQ 92.31%、95.74%_account','[CONTACT]'),
    ('微信：study_account','[CONTACT]'),
    ('电话 13800138000','[PHONE]'),
])
def test_populated_or_ambiguous_identity_values_still_change(source,token):
    result=redactor()(source)
    assert result!=source and token in result


def test_percentage_whitelist_does_not_exempt_real_contacts_in_the_same_text():
    source='QQ 92.31%、95.74%；联系人QQ 223456789；电话 13800138000；班级：高二三班'
    assert redactor()(source)=='QQ 92.31%、95.74%；联系人QQ [CONTACT]；电话 [PHONE]；班级：[CLASS]'


@pytest.mark.parametrize('source',SAFE_FORMS)
def test_safe_image_observations_survive_the_unchanged_image_text_gate(source):
    meta={'title':'研究图表','blocks':[],'formulas':[],
          'resources':[{'id':'image','position':{'page':1},'ocr':{'text':source,'verified':True},
                        'description':{'text':'原图可见的表头、空白字段或统计数值'}}]}
    expected=copy.deepcopy(meta['resources'])
    class Agent:
        def call(self,task,payload,images=()):
            assert task=='privacy_text' and not images
            return {'decision':'pass','reason':'No populated identity','sensitive_strings':[]}
    text_privacy(meta,Agent(),lambda *args:None)
    assert meta['resources']==expected and meta['text_privacy_complete'] is True


def test_real_identity_in_image_ocr_still_requires_pixel_proof():
    meta={'title':'研究图表','blocks':[],'formulas':[],
          'resources':[{'id':'image','position':{'page':1},'ocr':{'text':'QQ 223456789','verified':True}}]}
    class Agent:
        def call(self,task,payload,images=()):
            return {'decision':'pass','reason':'Local gate must still check image changes','sensitive_strings':[]}
    with pytest.raises(Rejected,match='图片中的额外敏感内容'):
        text_privacy(meta,Agent(),lambda *args:None)
