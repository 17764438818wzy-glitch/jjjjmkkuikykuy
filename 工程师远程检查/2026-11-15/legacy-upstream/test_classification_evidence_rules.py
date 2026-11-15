from classification_rules import resolve_identity, source_context
import json
import pytest


def test_filename_topic_and_university_name_are_not_exact_major_directories():
    resolved=resolve_identity('专业：法学\n摘要\n正文',
                              r'C:\sources\法学与经济学院\人工智能应用研究.docx','人工智能应用研究')
    assert resolved['major']=='法学' and not resolved['conflict']
    assert resolved['classification']['source']=='front_matter'
    assert not any(row['kind']=='source_major_directory' for row in resolved['classification']['evidence'])


def test_major_mentions_in_body_do_not_conflict_with_source_major():
    resolved=resolve_identity('摘要\n研究正文\n专业：计算机科学与技术',
                              'sources/法学/paper.docx','人工智能应用研究')
    assert resolved['major']=='法学' and not resolved['conflict']
    assert resolved['classification']['source']=='source_directory'


def test_inconsistent_cohort_and_major_directory_requires_review():
    resolved=resolve_identity('摘要\n正文','sources/法学/2019思想政治教育2班/paper.docx','规范比较研究')
    assert resolved['conflict']
    assert resolved['classification']['confidence']=='conflict'


def test_archive_member_retains_original_parent_professional_directory():
    record={'input_source_path':r'C:\sources\法学\论文集.zip','archive_member_path':'2019法学2班/paper.txt'}
    path=source_context(record,r'C:\data\private\archive\paper.txt')
    resolved=resolve_identity('摘要\n正文',path,'人工智能应用研究')
    assert resolved['major']=='法学'
    assert resolved['classification']['source']=='source_directory'


def test_old_archive_checkpoint_recovers_member_path_with_existing_identity_helper():
    record={'path':r'C:\private\archive\paper.txt','input_source_path':r'C:\sources\法学\论文集.zip'}
    resolved=resolve_identity('摘要\n正文',source_context(record,record['path']),'人工智能应用研究')
    assert resolved['major']=='法学'
    assert resolved['classification']['source']=='source_directory'


@pytest.mark.parametrize('native_field',[True,False])
def test_normalized_major_does_not_publish_unverified_parenthetical_identity(native_field):
    text='专业：法学（张小测）\n摘要\n正文' if native_field else '摘要\n正文'
    path='sources/paper.docx' if native_field else 'sources/法学（张小测）/paper.docx'
    resolved=resolve_identity(text,path,'制度规范比较研究')
    assert resolved['major']=='法学'
    assert '张小测' not in json.dumps(resolved['classification'],ensure_ascii=False)


def test_repeated_character_typo_needs_exact_directory_support():
    resolved=resolve_identity('专业：思想政治教教育\n摘要\n正文',
                              'sources/思想政治教育/paper.docx','课堂教学设计研究')
    assert resolved['major']=='思想政治教育' and not resolved['conflict']
    evidence=resolved['classification']['evidence']
    assert any(row.get('quote')=='思想政治教教育' and row.get('normalization') for row in evidence)


def test_disagreeing_declared_cover_majors_are_not_resolved_by_title():
    resolved=resolve_identity('专业：法学\n专业：社会工作\n摘要\n正文','paper.docx','人工智能应用研究')
    assert resolved['conflict']
    assert resolved['classification']['source']=='source_conflict'
