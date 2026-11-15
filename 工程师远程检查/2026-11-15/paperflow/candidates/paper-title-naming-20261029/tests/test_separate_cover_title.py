import copy
from pathlib import Path
import tempfile
import unittest

from paperflow_prefect.group_classification import _version, classify_group, verify_handoff


TITLE = '基于Java的宠物领养系统开发'


class Judge:
    def choose_body(self, rows):
        return {'choice': next(row['file_id'] for row in rows if row['relative'] == '正文.docx')}

    def choose_other(self, row, body_id):
        return {'choice': 'trash'}


def cover_evidence(path, sid='201913137111', person='刘婉婷'):
    if '封面' not in Path(path).name:
        return {'format': 'docx', 'text': '摘要\n这篇正文没有单独列出标题。'}
    return {'format': 'docx', 'text': (f'题目：{TITLE}\n学院：计算机学院\n'
                                       f'学号：{sid}\n学生姓名：{person}\n')}


def title_reader(row, evidence):
    if '封面' not in row['relative']:
        return ('', '正文 Word 开头文本', '', False)
    return (TITLE, '正文 Word 开头文本', TITLE, True)


class SeparateCoverTitleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.library = Path(self.temp.name)/'library'
        self.group = self.library/'刘婉婷_201913137111'
        self.group.mkdir(parents=True)
        (self.group/'正文.docx').write_bytes(b'body')
        (self.group/'毕业论文封面.docx').write_bytes(b'cover')

    def test_unique_same_person_cover_can_confirm_body_title(self):
        handoff = classify_group(self.group,self.library,Judge(),cover_evidence,title_reader=title_reader)
        self.assertEqual(handoff['paper_title'], TITLE)
        self.assertEqual(handoff['title_source']['reference'], '同组独立封面')
        self.assertEqual(handoff['title_source']['file_id'], handoff['body']['file_id'])
        self.assertTrue(verify_handoff(handoff,evidence_reader=cover_evidence))
        wrong = copy.deepcopy(handoff)
        wrong['title_source']['cover_sha256'] = '0'*64
        wrong['result_version'] = _version(wrong)
        with self.assertRaises(ValueError):
            verify_handoff(wrong,evidence_reader=cover_evidence)

    def test_cover_for_another_student_is_not_used(self):
        def wrong_cover(path):
            return cover_evidence(path,sid='201913137112',person='李四')
        handoff = classify_group(self.group,self.library,Judge(),wrong_cover,title_reader=title_reader)
        self.assertIsNone(handoff['paper_title'])

    def test_multiple_cover_files_do_not_get_picked_arbitrarily(self):
        (self.group/'另一个封面.docx').write_bytes(b'another cover')
        handoff = classify_group(self.group,self.library,Judge(),cover_evidence,title_reader=title_reader)
        self.assertIsNone(handoff['paper_title'])


if __name__ == '__main__':
    unittest.main()
