from knowledge_outline import heading_info


def test_questionnaire_items_with_answer_blank_are_not_chapter_headings():
    for text in ('1.您的性别 （  ）', '4.您是否了解学生的身体健康状况 (  )',
                 '8.您认为学校领导对体育课场地器材配置重视度情况 (  )'):
        assert heading_info({'type': 'paragraph', 'text': text}) == (0, None)
        assert heading_info({'type': 'paragraph', 'text': text, 'heading_level': 1}) == (0, None)
        assert heading_info({'type': 'paragraph', 'text': '# ' + text}) == (0, None)
    assert heading_info({'type': 'paragraph', 'text': '2.1研究对象'}) == (2, (2, 1))
