import pytest


def test_legacy_dataset_map_is_upgraded_to_paper_image_pair():
    from auto_config import normalize_dataset_map

    assert normalize_dataset_map({'会计学': 'md-kb'}) == {
        '会计学': {'paper': 'md-kb', 'image': ''}
    }


def test_dataset_pair_requires_md_library_but_allows_missing_image_library():
    from auto_config import validate_dataset_pair

    assert validate_dataset_pair('会计学', {'paper': 'md-kb', 'image': ''}) == {
        'paper': 'md-kb', 'image': '', 'image_warning': True
    }
    with pytest.raises(ValueError, match='会计学.*md'):
        validate_dataset_pair('会计学', {'paper': '', 'image': 'image-kb'})


def test_manifest_role_has_priority_over_extension():
    from ragflow_client import artifact_route

    assert artifact_route('ragflow_images.md', 'image_projection') == 'image'
    assert artifact_route('ingest.md', 'paper_primary') == 'paper'
    assert artifact_route('ragflow_assets.md', None) == 'paper'
    assert artifact_route('assets/figure.png', None) == 'image'


def test_unknown_artifact_type_is_rejected():
    from ragflow_client import artifact_route

    with pytest.raises(ValueError, match='未知文件类型'):
        artifact_route('assets/figure.svg', None)
