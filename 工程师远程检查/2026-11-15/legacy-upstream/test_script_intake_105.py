"""Observed report classification and archive completion regressions."""
import pytest
from auto_pipeline import coarse_screen
from script_task import task_snapshot, task_rows
from test_parallel_result_regressions import processor, record, archive


REPORT = ('检测文献：企业偿债能力分析 (免费版)\n文献作者：测试\n'
          '报告时间：2023-04-17 22:28:43\n报告编号：YY202304172228414929\n'
          '检测范围：中国期刊库 中国图书库\n总文字复制比：13.1%\n'
          '去除引用文献复制比：12.1%\n重复字数：1,483\n总字数：11,316（不含参考文献）')


def test_paperyy_report_is_an_attachment_even_with_unreadable_archive_filename():
    result = coarse_screen(REPORT, True, 'garbled-name.pdf')
    assert result['decision'] == 'reject'
    assert result.get('attachment') is True
    assert '辅助材料' in result['reason']
    assert '不足' not in result['reason']


@pytest.mark.parametrize('raw', [
    '学术检测技术研究\n摘要\n' + REPORT,
    '学术检测技术研究\n报告时间：2023-04-17\n总文字复制比：13.1%',
    REPORT.replace('报告编号：YY202304172228414929\n', ''),
])
def test_report_fields_in_research_or_incomplete_headers_do_not_exclude_paper(raw):
    assert not coarse_screen(raw, True, '论文.docx').get('attachment')


def test_archive_with_only_duplicate_children_has_no_pending_source_row(tmp_path):
    loose = tmp_path / 'original.txt'
    loose.write_text('same content', encoding='utf-8')
    zipped = archive(tmp_path / 'archive.zip', {'nested.txt': 'same content'})
    other = tmp_path / 'other.txt'
    other.write_text('other short content', encoding='utf-8')
    proc = processor(tmp_path)
    from engine import digest
    proc._known_sources = lambda: {digest(loose)}
    manifest = proc.run([record(zipped), record(other)])
    rows = task_rows(task_snapshot(proc.store, manifest))
    assert not any(r['script_status'] == '待处理' for r in rows)
    assert sum(r['script_status'] == '重复跳过' for r in rows) == 1
    assert sum(r['script_status'] == '不通过' for r in rows) == 1
    assert len(rows) == 2


def test_pipeline_recovers_only_private_docx_copy_and_checkpoint_keeps_original_identity(tmp_path):
    import struct
    import zipfile
    from auto_pipeline import AutoProcessor, VERSION
    from engine import Store, digest
    from ai_resume import validate_prepared
    from test_stage_resume import document, CONFIG, KEY, Transport
    source = tmp_path / 'paper.docx'
    document(source)
    data = bytearray(source.read_bytes())
    with zipfile.ZipFile(source) as z:
        central = z.start_dir
        for member in z.infolist():
            if member.filename.startswith('word/media/'):
                struct.pack_into('<L', data, member.header_offset + 14, 0)
                struct.pack_into('<L', data, central + 16, 0)
            name, extra, comment = struct.unpack_from('<3H', data, central + 28)
            central += 46 + name + extra + comment
    source.write_bytes(data)
    original = source.read_bytes()
    store = Store(tmp_path / 'data')
    proc = AutoProcessor(store, CONFIG, KEY)
    proc.agent.opener = Transport()
    result = proc.run([record(source)])
    item = result['items'][0]
    assert item['status'] == 'pass', item['reason']
    assert source.read_bytes() == original
    assert item['source_hash'] == digest(source)
    assert len(item['source_recovery']['members']) == 2
    folder, saved = validate_prepared({'path': str(source),
        'prepared_checkpoint': item['prepared_checkpoint']}, store.root, VERSION)
    with zipfile.ZipFile(folder / saved['source_name']) as verified:
        assert verified.testzip() is None


def test_successful_archive_retry_replaces_parent_error_with_member_results(tmp_path):
    from engine import Store
    store = Store(tmp_path / 'data')
    parent = str(tmp_path / 'papers.rar')
    store.save({'run_id': 'old', 'status': 'completed', 'inputs': [{'path': parent}],
        'items': [{'id': 'archive', 'source_path': parent, 'status': 'reject',
                   'reason_kind': 'technical', 'reason': 'archive unsupported'}], 'events': []})
    retry = {'run_id': 'new', 'retry_of': 'old', 'status': 'completed',
        'inputs': [{'path': parent}], 'events': [],
        'items': [{'id': 'child', 'source_path': str(tmp_path / 'unpacked' / 'paper.txt'),
                   'input_source_path': parent, 'status': 'reject',
                   'script_result': {'status': 'rejected', 'reason': 'too short'}}]}
    rows = task_rows(task_snapshot(store, retry))
    assert len(rows) == 1
    assert rows[0]['name'] == 'paper.txt'
    assert rows[0]['script_reason'] == 'too short'


def test_pipeline_rar_member_reaches_screening_without_archive_error(tmp_path):
    from test_archive_input_105 import archive as rar_archive
    source = rar_archive(tmp_path / 'source.rar', payload=b'short paper')
    before = source.read_bytes()
    result = processor(tmp_path).run([record(source)])
    assert len(result['items']) == 1
    assert result['items'][0]['reason_kind'] == 'screening'
    assert result['items'][0]['source_path'].endswith('paper.txt')
    assert len(task_rows(task_snapshot(processor(tmp_path).store, result))) == 1
    assert source.read_bytes() == before
