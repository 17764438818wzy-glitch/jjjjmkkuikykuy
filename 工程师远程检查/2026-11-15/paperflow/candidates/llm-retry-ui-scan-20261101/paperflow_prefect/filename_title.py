"""Build exact title candidates from a body filename without inventing text."""
from pathlib import Path
import re


_IDENTITY_AND_TAIL = re.compile(
    r'(?:^|[_\s-])\d{6,}[_\s-]+[\u4e00-\u9fff·]{2,5}[_\s-]+(?P<tail>.+)$')
_IDENTITY_ALONE = re.compile(r'(?:^|[_\s-])\d{6,}[_\s-]+[\u4e00-\u9fff·]{2,5}[_\s-]*$')
_SERIAL_AND_TAIL = re.compile(r'^\d{3}[_\s-]+(?P<tail>.+)$')
_GENERIC = re.compile(
    r'(?:正文|毕业论文|论文正文|毕业设计|设计说明书|'
    r'计算机(?:科学与技术)?\d{0,6}|'
    r'论文|论文终稿|最终版|终稿|定稿|'
    r'[\u4e00-\u9fff·]{2,5}(?:本科)?毕业(?:论文|设计)(?:\(\d+\))?)', re.I)


def filename_title_candidates(filename):
    """Return at most twelve exact filename suffixes following a student identity.

    Recognize a separated student ID and name, or a three-digit list number.
    Candidates are never synthesized or truncated and are not confirmed titles.
    """
    if not isinstance(filename, str) or Path(filename).name != filename:
        return []
    stem = Path(filename).stem
    match = _IDENTITY_AND_TAIL.search(stem)
    if not match:
        if _IDENTITY_ALONE.search(stem):
            return []
        match = _SERIAL_AND_TAIL.match(stem)
        if not match:
            return []
    tail = match.group('tail').strip(' _-')
    starts = [0] + [m.end() for m in re.finditer(r'[_\s-]+', tail)]
    candidates = []
    for start in starts:
        candidate = tail[start:].strip(' _-')
        if (len(candidate) < 6 or len(candidate) > 160 or re.match(r'^\d{6,}(?:[_\s-]|$)', candidate)
                or _GENERIC.fullmatch(candidate) or candidate in candidates):
            continue
        candidates.append(candidate)
    return candidates if len(candidates) <= 12 else []
