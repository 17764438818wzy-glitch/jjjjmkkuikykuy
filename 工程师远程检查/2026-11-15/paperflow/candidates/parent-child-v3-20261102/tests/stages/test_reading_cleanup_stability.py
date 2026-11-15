"""Exercise real document cleanup under a bounded, separate CPU process."""
from pathlib import Path
import json
import random
import re
import subprocess
import sys

import pytest
import reading_cleanup


@pytest.mark.parametrize('gap', [' ' * 30000, '\t ' * 15000, '.' * 30000 + '1' * 30000],
                         ids=['spaces', 'tabs_and_spaces', 'leaders_and_digits'])
def test_cleanup_does_not_hang_on_non_page_suffix(gap):
    blocks = [
        dict(id='chapter', type='paragraph', text='第一章 绪论', heading_level=1),
        dict(id='body', type='paragraph', text='研究正文。' + gap + '这是正文。'),
    ]
    script = '''
import json,sys
sys.path.insert(0,sys.argv[1])
from reading_cleanup import clean_body_blocks
blocks=json.load(sys.stdin)
kept,removed,decisions=clean_body_blocks(blocks,continue_on_uncertainty=True)
assert kept==blocks
assert not removed
print('kept_original_blocks')
'''
    try:
        result = subprocess.run(
            [sys.executable, '-c', script, str(Path(reading_cleanup.__file__).parent)],
            input=json.dumps(blocks), text=True, capture_output=True, timeout=4, check=True,
        )
    except subprocess.TimeoutExpired:
        pytest.fail('document cleanup exceeded 4 seconds on a non-page suffix')
    assert result.stdout.strip() == 'kept_original_blocks'


def test_page_suffix_keeps_match_and_removal_semantics():
    # Fixed old behavior is the oracle for directory labels and page removal.
    old = re.compile(r'(?:\.{2,}|[·…]{2,}|\t|\s{2,})\s*(?:\d+|[ivxlc]+)\s*$', re.I)
    new = re.compile(reading_cleanup.TOC_PAGE_SUFFIX, re.I)
    examples = ['第一章 绪论\t1', '摘要...... III', '正文  12\n', '标题 …·… １２',
                '普通正文。', '标题  İı', '标题\t\n１２\n\t', '页号  ²', 'a .  1']
    rng = random.Random(20261111)
    chars = '目录abcIVXLCİı١１２0129.·… \t\n\r\u2003\u00a0'
    examples.extend(''.join(rng.choices(chars, k=rng.randrange(65))) for _ in range(10000))
    for value in examples:
        before, after = old.search(value), new.search(value)
        assert (before.span() if before else None) == (after.span() if after else None)
        assert old.sub('', value) == new.sub('', value)


def test_strong_page_suffix_keeps_recognition_semantics():
    old = re.compile(r'(?:\.{2,}|[·…]{2,}|\t)\s*(?:\d+|[ivxlc]+)\s*$', re.I)
    new = re.compile(reading_cleanup.TOC_STRONG_PAGE_SUFFIX, re.I)
    rng = random.Random(20261112)
    chars = '目录abcIVXLCİı١１２0129.·… \t\n\r\u2003\u00a0'
    examples = ['附录  12', '附录  \t12', '附录\t\t XII', '附录......１２', '附录 ·… ١']
    examples.extend(''.join(rng.choices(chars, k=rng.randrange(65))) for _ in range(10000))
    for value in examples:
        assert bool(old.search(value)) == bool(new.search(value))


@pytest.mark.parametrize('pattern_name', ['TOC_PAGE_SUFFIX', 'TOC_STRONG_PAGE_SUFFIX', 'TOC_LEADER_PAGE_SUFFIX'])
def test_page_suffix_has_bounded_cost_on_large_whitespace_runs(pattern_name):
    script = '''
import re,sys
sys.path.insert(0,sys.argv[1])
import reading_cleanup
pattern=getattr(reading_cleanup,sys.argv[2])
assert not re.search(pattern,'text'+'\\t '*500000+'1'*500000+'X',re.I)
'''
    subprocess.run([sys.executable, '-c', script, str(Path(reading_cleanup.__file__).parent), pattern_name],
                   capture_output=True, text=True, timeout=4, check=True)
