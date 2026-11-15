"""Ambiguous placeholders must reach the model with actual source anchors."""
import copy
import pytest

from evidence_tasks import resolve_scoped
from test_pdf_table_recovery import table_target


class Captured(BaseException):pass
class Capture:
    def call(self,task,payload,images=()):
        self.payload=payload
        raise Captured()


def test_repair_request_retains_source_identity_and_adjacent_original_text(tmp_path):
    source,package,meta=table_target(tmp_path)
    target=meta['blocks'][0]
    target['position']['object_ref']='original_object_7'
    before={'id':'caption_original','type':'paragraph','text':'表 3 原始统计（原值 -2.50%）',
            'position':{'page':1,'bbox':[60,80,530,95]}}
    meta['blocks'].insert(0,before)
    untouched=copy.deepcopy(meta)
    capture=Capture()
    with pytest.raises(Captured):
        resolve_scoped(meta,package,source,tmp_path/'work',80,capture,lambda *args:None)
    known=next(x for x in capture.payload['known_blocks'] if x['id']=='table_region')
    assert known.get('position')=={'page':1,'bbox':[60,100,530,220],
                                  'vector_region':True,'object_ref':'original_object_7'}
    assert known.get('caption')=='Table 3 Research records'
    anchors=known.get('source_context')
    assert anchors is not None
    assert anchors['before'][-1]['id']=='caption_original'
    assert anchors['before'][-1]['text']=='表 3 原始统计（原值 -2.50%）'
    assert anchors['after'][0]['id']=='native_rows'
    assert 'North (1,250.00) -2.50%' in anchors['after'][0]['text']
    assert meta['blocks']==untouched['blocks']
    known['position']['object_ref']='changed'
    assert target['position']['object_ref']=='original_object_7'
