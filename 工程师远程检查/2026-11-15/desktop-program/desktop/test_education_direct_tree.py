import unittest

from education_direct_tree import bind_tree_chunks, extract_tree


PAPER_ID = 'paper_' + 'a' * 24


class DirectTreeTest(unittest.TestCase):
    def test_uses_only_original_headings_and_stops_before_assets(self):
        ingest = '''# 论文题目

# 第1章 绪论
资料关联（程序生成）：section_1111aaaa
正文。
## 1.1 研究背景
资料关联（程序生成）：section_2222bbbb
正文。
# 结论
资料关联（程序生成）：section_3333cccc
正文。
# 图表/表格写作参考结构
## 表格 table_12345678
'''
        tree = extract_tree(ingest, PAPER_ID, 'remote-1')
        self.assertEqual([n['title'] for n in tree['nodes']],
                         ['论文题目', '第1章 绪论', '1.1 研究背景', '结论'])
        self.assertEqual([n['level'] for n in tree['nodes']], [1, 1, 2, 1])
        self.assertEqual(tree['nodes'][2]['section_id'], 'section_2222bbbb')
        self.assertEqual(tree['nodes'][2]['parent_id'], tree['nodes'][1]['id'])
        self.assertIsNone(tree['nodes'][3]['parent_id'])
        self.assertEqual(len(tree['relations']), 1)
        self.assertEqual(tree['source_doc_id'], 'remote-1')

    def test_links_merged_parent_heading_to_real_body_chunk(self):
        ingest = '''# 第1章 绪论
资料关联（程序生成）：section_1111aaaa
## 1.1 研究背景
资料关联（程序生成）：section_2222bbbb
内容。
'''
        chunks = [
            {'id': 'chunk-1', 'available': True,
             'important_keywords': ['role=body', 'section_id=section_2222bbbb'],
             'content': '# 第1章 绪论\n## 1.1 研究背景\n内容。'},
            {'id': 'asset-1', 'available': True,
             'important_keywords': ['role=assets', 'section_id=section_1111aaaa'],
             'content': '# 第1章 绪论'},
        ]
        tree = bind_tree_chunks(extract_tree(ingest, PAPER_ID, 'remote-1'), chunks)
        self.assertEqual(tree['nodes'][0]['source_chunk_ids'], ['chunk-1'])
        self.assertEqual(tree['nodes'][1]['source_chunk_ids'], ['chunk-1'])
        self.assertEqual(tree['unlinked_headings'], 0)

    def test_parent_child_relation_cites_child_heading_chunk(self):
        ingest = '''# 第1章 绪论
资料关联（程序生成）：section_1111aaaa
## 1.1 研究背景
资料关联（程序生成）：section_2222bbbb
正文。'''
        chunks = [{'id': 'chunk-1', 'available': True,
                   'important_keywords': ['role=body', 'section_id=section_1111aaaa'],
                   'content': '# 第1章 绪论'},
                  {'id': 'chunk-2', 'available': True,
                   'important_keywords': ['role=body', 'section_id=section_2222bbbb'],
                   'content': '## 1.1 研究背景\n正文。'}]
        tree = bind_tree_chunks(extract_tree(ingest, PAPER_ID, 'remote-1'), chunks)
        self.assertEqual(tree['relations'][0]['source_chunk_ids'], ['chunk-2'])
        self.assertEqual(tree['relations'][0]['source_doc_id'], 'remote-1')

    def test_missing_heading_source_is_flagged_without_inventing_evidence(self):
        ingest = '# 第1章 绪论\n资料关联（程序生成）：section_1111aaaa\n正文。'
        tree = bind_tree_chunks(extract_tree(ingest, PAPER_ID, 'remote-1'), [])
        self.assertEqual(tree['nodes'][0]['source_chunk_ids'], [])
        self.assertEqual(tree['unlinked_headings'], 1)
        self.assertIn('章节标题未能定位正文切片', tree['issues'])

    def test_questionnaire_items_marked_as_markdown_headings_are_not_chapters(self):
        ingest = '''# 附录 问卷
资料关联（程序生成）：section_1111aaaa
# 10.您所在的学校属于哪一类（单选题）
资料关联（程序生成）：section_2222bbbb
# 11.您使用哪些方法（多选题）
资料关联（程序生成）：section_3333cccc
'''
        tree = extract_tree(ingest, PAPER_ID, 'remote-1')
        self.assertEqual([n['title'] for n in tree['nodes']], ['附录 问卷'])


if __name__ == '__main__':
    unittest.main()
