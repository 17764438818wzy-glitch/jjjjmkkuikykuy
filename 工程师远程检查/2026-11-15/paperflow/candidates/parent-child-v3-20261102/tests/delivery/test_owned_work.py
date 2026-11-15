from pathlib import Path

import pytest


def test_per_paper_work_files_include_its_own_closed_database(tmp_path):
    from paperflow_v3.delivery.owned_work import owned_work_files

    group = tmp_path / 'classification' / 'group'
    processing = tmp_path / 'processing' / 'group'
    source = tmp_path / 'source'
    nas = tmp_path / 'nas'
    for directory in (group, processing, source, nas):
        directory.mkdir(parents=True)
    for file in (group / 'receipt.json', group / 'receipts.sqlite3', processing / 'model.json',
                 processing / 'state.db-wal', source / 'trash.txt', nas / 'paper.docx'):
        file.write_text('fixture')
    result = owned_work_files((group, processing), source, nas)
    assert set(map(Path, result)) == {group / 'receipt.json', group / 'receipts.sqlite3',
                                     processing / 'model.json', processing / 'state.db-wal'}
    assert (source / 'trash.txt').is_file() and (nas / 'paper.docx').is_file()


def test_source_or_nas_overlapping_a_work_root_is_rejected(tmp_path):
    from paperflow_v3.delivery.owned_work import owned_work_files

    work = tmp_path / 'work'
    source = work / 'source'
    source.mkdir(parents=True)
    with pytest.raises(ValueError, match='overlap'):
        owned_work_files((work,), source, tmp_path / 'nas')
