"""Prepare isolated, identity-checked knowledge packages for education trials."""

import json
import shutil
from pathlib import Path

from evidence import export_package
from knowledge_package import knowledge_files


def prepare_pilot_copy(source: Path, destination: Path) -> dict:
    source=Path(source)
    destination=Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    knowledge_files(source)
    metadata=json.loads((source/'metadata.json').read_text(encoding='utf-8'))
    if not (metadata.get('preprocessing_complete') and metadata.get('text_privacy_complete')
            and metadata.get('automatic_approval')):
        raise ValueError('试跑仅接受已完成且通过预处理的知识包')
    shutil.copytree(source,destination)
    copied=json.loads((destination/'metadata.json').read_text(encoding='utf-8'))
    export_package(destination,copied,canonical_paper_id=True)
    knowledge_files(destination,require_stable_paper_id=True)
    return json.loads((destination/'manifest.json').read_text(encoding='utf-8'))
