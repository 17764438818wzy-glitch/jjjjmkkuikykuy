"""Word alignment padding must not stall the exporter or the desktop's GIL."""
import subprocess
import sys
from pathlib import Path

import pytest


def test_padded_paragraphs_export_without_hanging_or_changing_source(tmp_path):
    # A real Word heading had 3,716 trailing spaces. The old suffix search
    # spends minutes backtracking while holding the GIL, freezing Tk too.
    script = r'''
import sys
from pathlib import Path
from knowledge_outline import heading_info
from test_knowledge_general import export_text, paragraph

padding = ' ' * 3716
blocks = [paragraph('1 Study'), paragraph('Body text' + padding),
          paragraph(padding + 'Other body text'),
          paragraph('Final body text' + padding + '\t')]
for block in blocks[1:]:
    assert heading_info(block) == (0, None)
folder = Path(sys.argv[1])
manifest, units = export_text(folder, blocks)
assert [u['title'] for u in units] == ['1 Study']
text = (folder / 'ingest.md').read_text(encoding='utf-8')
assert all(b['text'].strip() in text for b in blocks)
'''
    try:
        result = subprocess.run(
            [sys.executable, '-B', '-X', 'utf8', '-c', script, str(tmp_path)],
            cwd=Path(__file__).resolve().parent, capture_output=True, text=True,
            encoding='utf-8', timeout=5,
        )
    except subprocess.TimeoutExpired:
        pytest.fail('Padded Word paragraphs froze heading detection/export for over 5 seconds')
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(('text', 'expected'), [
    ('1 Study  123', (0, None)),
    ('1 Study\t-123.45%   ', (0, None)),
    ('1 Study  +123.45%\t', (0, None)),
    ('1 Study', (1, (1,))),
    ('1.2 Study   ', (2, (1, 2))),
])
def test_numeric_rows_and_real_headings_keep_their_meaning(text, expected):
    from knowledge_outline import heading_info
    assert heading_info({'type': 'paragraph', 'text': text}) == expected
