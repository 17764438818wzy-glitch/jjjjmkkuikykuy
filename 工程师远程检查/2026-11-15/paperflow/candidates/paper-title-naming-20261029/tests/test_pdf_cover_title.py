import unittest

from paperflow_prefect.group_classification import _title_proof
from paperflow_prefect.group_sort_cli import _title_from_body


class PdfCoverTitleTests(unittest.TestCase):
    def test_title_wrapped_across_two_cover_lines_is_kept_complete(self):
        text = ('吴门画派纪实类画作中的\n空间展示艺术研究\n\n'
                '姓名\n学号\n指导老师\n环艺1901郭梦真\n张晓熙\n201919150021\n')
        evidence = {'format': 'pdf', 'pages': [{'page': 1, 'text': text}]}
        title, reference, excerpt, confirmed = _title_from_body({'file_id': 'body'}, evidence)
        self.assertEqual(title, '吴门画派纪实类画作中的空间展示艺术研究')
        self.assertEqual(reference, '正文 PDF 第1页文字')
        self.assertEqual(excerpt, '吴门画派纪实类画作中的\n空间展示艺术研究')
        self.assertFalse(confirmed)
        self.assertTrue(_title_proof(evidence, title, excerpt, labelled=False, reference=reference))

    def test_cover_title_is_not_fabricated_without_an_identity_field_boundary(self):
        from paperflow_prefect.group_sort_cli import _pdf_cover_title_lines
        evidence = {'format': 'pdf', 'pages': [{'page': 1,
                    'text': '第一段可能是正文\n第二段也可能是正文\n没有封面字段'}]}
        self.assertIsNone(_pdf_cover_title_lines(evidence))

    def test_unrelated_cover_fields_are_not_joined_to_title(self):
        from paperflow_prefect.group_sort_cli import _pdf_cover_title_lines
        evidence = {'format': 'pdf', 'pages': [{'page': 1,
                    'text': '本科毕业设计\n基于Python的\n图书馆管理系统研究\n姓名：张三\n学号：123456'}]}
        self.assertEqual(_pdf_cover_title_lines(evidence),
                         ('基于Python的图书馆管理系统研究', '基于Python的\n图书馆管理系统研究'))


if __name__ == '__main__':
    unittest.main()
