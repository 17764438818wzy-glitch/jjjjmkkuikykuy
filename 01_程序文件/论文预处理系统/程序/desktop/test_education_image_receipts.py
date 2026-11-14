from education_image_receipts import image_receipt_key, eligible_for_auxiliary_index


def test_image_receipt_key_is_unique_per_paper_and_path():
    first = image_receipt_key('paper_' + 'a' * 24, 'ragflow_assets.md')
    second = image_receipt_key('paper_' + 'b' * 24, 'ragflow_assets.md')
    assert first != second
    assert image_receipt_key('paper_' + 'a' * 24, 'assets/visual_p1.png') != first


def test_auxiliary_index_requires_admitted_primary_with_matching_identity():
    paper_id = 'paper_' + 'a' * 24
    doc = {'run': 'DONE', 'meta_fields': {'education_admission': '入库',
                                         'education_paper_id': paper_id}}
    assert eligible_for_auxiliary_index(doc, paper_id)
    assert not eligible_for_auxiliary_index({**doc, 'run': 'FAIL'}, paper_id)
    assert not eligible_for_auxiliary_index({**doc, 'run': 'RUNNING'}, paper_id)
    assert not eligible_for_auxiliary_index({**doc, 'meta_fields': {
        'education_admission': '暂缓', 'education_paper_id': paper_id}}, paper_id)
    assert not eligible_for_auxiliary_index(doc, 'paper_' + 'b' * 24)
