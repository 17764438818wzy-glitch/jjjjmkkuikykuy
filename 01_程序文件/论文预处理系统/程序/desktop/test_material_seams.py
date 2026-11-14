import json
import pytest
from generic_profile_publish import invalidation_patch


def test_material_source_chunks_do_not_depend_on_api_order():
    from generic_profile_publish import source_chunk_binding
    source='甲'*1000+'乙'*1000+'丙'
    assert source_chunk_binding(source,[{'id':'c3','content':'丙'},{'id':'c1','content':'甲'*1000},{'id':'c2','content':'乙'*1000}])==['c1','c2','c3']
    with pytest.raises(ValueError):source_chunk_binding(source,[{'id':'fake','content':'甲'*1000}])
    with pytest.raises(ValueError):source_chunk_binding(source,[{'id':'c1','content':'甲'*1000},{'id':'c1','content':'乙'*1000},{'id':'c3','content':'丙'}])


def test_invalidation_disables_material_and_dependency_metadata():
    old={'research_profile_revision':'rev','research_profile_binding':json.dumps({'task_id':1,'execution_pipeline_id':'p'}),
         'research_material':'active','research_learning_uses':'active','other':'preserved'}
    changed=invalidation_patch(old,'rev',1,'p')
    assert changed['research_material']=='' and changed['research_learning_uses']==''
    assert changed['other']=='preserved'


def test_insufficient_use_cannot_be_promoted_by_rehashing_admission():
    from generic_profile_publish import validate_material_packet
    import hashlib
    source='图例清单';version=hashlib.sha256(source.encode()).hexdigest()
    payload={'learning_use':'空间布局','author_spans':[{'id':'s1','text':source}],'source_version':version}
    packet={'source_text':source,'source_text_sha256':version,'profile':{'source_version':version},'learning_use':'空间布局',
        'routes':[{'verification':'verified'}],'admission':{'status':'limited'},'use_review':{'status':'insufficient','reason':'未读取原图',
        'reviewer':'saved','source_version':version,'span_ids':[],'input_hash':hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),
        'read_scope':[{'id':'s1','start':0,'end':len(source),'quote':source}]}}
    with pytest.raises(ValueError,match='material_admission_changed'):validate_material_packet(packet)
    packet['admission']['status']='pending';assert validate_material_packet(packet)=='pending'
