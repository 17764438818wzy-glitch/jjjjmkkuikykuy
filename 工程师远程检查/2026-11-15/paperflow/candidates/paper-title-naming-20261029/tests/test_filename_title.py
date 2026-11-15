import tempfile
import unittest
import copy
from pathlib import Path
from unittest.mock import patch

from paperflow_prefect.filename_title import filename_title_candidates
from paperflow_prefect.group_classification import _version, classify_group, verify_handoff
from paperflow_prefect.nimble_group import NimbleJudge


class FilenameTitleTests(unittest.TestCase):
    def test_candidates_remove_student_identity_but_preserve_title(self):
        name = '001_1908034124_姚聪聪_体育经济与管理_短视频营销下保定学院大学生体育快消品消费现状调查研究.doc'
        candidates = filename_title_candidates(name)
        self.assertIn('短视频营销下保定学院大学生体育快消品消费现状调查研究', candidates)
        self.assertNotIn('姚聪聪', candidates)
        self.assertTrue(all('1908034124' not in candidate for candidate in candidates))

    def test_id_and_name_without_title_have_no_candidates(self):
        self.assertEqual(filename_title_candidates('主修_计算机学院_201913137139_丁立锴.docx'), [])
        self.assertEqual(filename_title_candidates('190907123456-李小明-计算机1903.doc'), [])
        self.assertEqual(filename_title_candidates('8 徐义威 毕业论文 正文.docx'), [])
        self.assertEqual(filename_title_candidates('001_1908034124_姚聪聪.doc'), [])
        self.assertEqual(filename_title_candidates('070_莫位凯本科毕业论文(1).docx'), [])

    def test_candidates_preserve_separators_inside_actual_title(self):
        name = '008_1903014201-包胜男-历史学-大概念视域下的高中历史教学设计——以《诸侯纷争》为例.docx'
        self.assertIn('大概念视域下的高中历史教学设计——以《诸侯纷争》为例', filename_title_candidates(name))
        underscored = '001_1908034124_姚聪聪_基于Python_Web的图书馆管理系统设计.docx'
        self.assertIn('基于Python_Web的图书馆管理系统设计', filename_title_candidates(underscored))

    def test_candidates_also_cover_number_plus_title_or_name(self):
        self.assertIn('小学数学课堂中不同课型的问题设计研究',
                      filename_title_candidates('053_小学数学课堂中不同课型的问题设计研究.doc'))
        self.assertIn('海口市小学防溺水教育课程开展现状与研究分析',
                      filename_title_candidates('056_张臻 海口市小学防溺水教育课程开展现状与研究分析.docx'))

    def test_model_can_select_only_offered_filename_substring(self):
        name = '001_1908034124_姚聪聪_体育经济与管理_短视频营销现状研究.doc'
        judge = NimbleJudge()
        with patch.object(judge, '_decide', return_value={
            'choice': 'title_02', 'model': judge.model,
            'confidence': 0.9, 'probabilities': {'title_01': 0.04, 'title_02': 0.9, 'none': 0.06}
        }) as call:
            decision = judge.choose_filename_title(name)
        self.assertEqual(decision['title'], '短视频营销现状研究')
        self.assertEqual(decision['choice'], 'title_02')
        self.assertIn('none', call.call_args.args[2])

    def test_model_none_or_unrecognized_choice_never_fabricates_title(self):
        name = '001_1908034124_姚聪聪_体育经济与管理_短视频营销现状研究.doc'
        judge = NimbleJudge()
        with patch.object(judge, '_decide', return_value={'choice': 'none', 'model': judge.model}):
            self.assertIsNone(judge.choose_filename_title(name)['title'])
        with patch.object(judge, '_decide', return_value={'choice': 'title_99', 'model': judge.model}):
            with self.assertRaises(ValueError):
                judge.choose_filename_title(name)

    def test_no_title_candidates_does_not_call_the_model(self):
        judge = NimbleJudge()
        with patch.object(judge, '_decide') as call:
            self.assertIsNone(judge.choose_filename_title('主修_201913137139_丁立锴.docx'))
        call.assert_not_called()

    def test_filename_only_result_stays_candidate_and_is_bound_to_original(self):
        with tempfile.TemporaryDirectory() as temp:
            library = Path(temp) / 'library'
            group = library / 'one'
            group.mkdir(parents=True)
            filename = '001_1908034124_姚聪聪_体育经济与管理_短视频营销现状研究.docx'
            (group / filename).write_bytes(b'original')
            judge = NimbleJudge()
            def body(records):
                return {'choice': records[0]['file_id']}
            judge.choose_body = body
            response = {'choice': 'title_02', 'model': judge.model,
                        'confidence': 0.9, 'probabilities': {'title_01': 0.04, 'title_02': 0.9, 'none': 0.06}}
            evidence = lambda _: {'format': 'docx', 'text': '摘要和正文，没有明确题目'}
            title_reader = lambda *_: ('', '正文 Word 开头文本', '', False)
            with patch.object(judge, '_decide', return_value=response):
                handoff = classify_group(group, library, judge, evidence, title_reader=title_reader)
            self.assertIsNone(handoff['paper_title'])
            self.assertEqual(handoff['paper_title_candidate'], '短视频营销现状研究')
            self.assertEqual(handoff['title_candidate_source']['file_id'], handoff['body']['file_id'])
            self.assertTrue(verify_handoff(handoff, evidence_reader=evidence))
            tampered = copy.deepcopy(handoff)
            tampered['title_candidate_source']['decision']['choice'] = 'title_01'
            tampered['result_version'] = _version(tampered)
            with self.assertRaises(ValueError):
                verify_handoff(tampered, evidence_reader=evidence)

    def test_filename_cannot_override_a_confirmed_body_title(self):
        with tempfile.TemporaryDirectory() as temp:
            library = Path(temp) / 'library'
            group = library / 'one'
            group.mkdir(parents=True)
            (group / '001_1908034124_姚聪聪_错误文件标题.docx').write_bytes(b'original')
            judge = NimbleJudge()
            judge.choose_body = lambda records: {'choice': records[0]['file_id']}
            evidence = lambda _: {'format': 'docx', 'text': '题目：正确的论文题目\n学院：测试学院'}
            title_reader = lambda *_: ('正确的论文题目', '正文 Word 开头文本', '正确的论文题目', True)
            response = {'choice': 'title_01', 'model': judge.model,
                        'confidence': 0.9, 'probabilities': {'title_01': 0.9, 'none': 0.1}}
            with patch.object(judge, '_decide', return_value=response) as call:
                handoff = classify_group(group, library, judge, evidence, title_reader=title_reader)
            self.assertEqual(handoff['paper_title'], '正确的论文题目')
            call.assert_not_called()
            self.assertNotIn('filename_title_observation', handoff)
            self.assertTrue(verify_handoff(handoff, evidence_reader=evidence))

    def test_filename_only_model_wait_is_bounded_and_original_timeout_restored(self):
        name = '001_1908034124_姚聪聪_体育经济与管理_短视频营销现状研究.doc'
        judge = NimbleJudge(timeout=120)
        observed = []
        def slow(*args):
            observed.append(judge.timeout)
            raise TimeoutError('local model unavailable')
        with patch.object(judge, '_decide', side_effect=slow):
            with self.assertRaises(TimeoutError): judge.choose_filename_title(name)
        self.assertEqual(observed, [5])
        self.assertEqual(judge.timeout, 120)


if __name__ == '__main__':
    unittest.main()
