"""Stable keys for auxiliary education image-knowledge uploads."""

import re
from pathlib import PurePosixPath


def image_receipt_key(paper_id: str, relative_path: str) -> str:
    if not re.fullmatch(r'paper_[0-9a-f]{24}', paper_id):
        raise ValueError('稳定论文编号无效')
    path = PurePosixPath(relative_path.replace('\\', '/'))
    if path.is_absolute() or '..' in path.parts or not path.parts:
        raise ValueError('图片库相对路径无效')
    return paper_id + ':' + path.as_posix()


def eligible_for_auxiliary_index(primary_doc: dict, paper_id: str) -> bool:
    meta = primary_doc.get('meta_fields') or {}
    return (primary_doc.get('run') == 'DONE'
            and meta.get('education_admission') == '入库'
            and meta.get('education_paper_id') == paper_id)
