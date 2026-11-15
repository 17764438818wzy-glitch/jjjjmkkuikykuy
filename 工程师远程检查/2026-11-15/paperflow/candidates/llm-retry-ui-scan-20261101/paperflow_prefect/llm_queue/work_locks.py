"""Directory ownership shared by raw requests and response reconciliation."""
from contextlib import ExitStack, contextmanager
from pathlib import Path

from filelock import FileLock


@contextmanager
def work_locks(work, kind):
    """Use the same legacy-entry lock names and ordering in every caller."""
    if kind not in {'body', 'attachment'}:
        raise ValueError('LLM 工作目录类型无效')
    work = Path(work)
    with ExitStack() as stack:
        if kind == 'attachment':
            stack.enter_context(FileLock(str(work / 'attachment-identification.lock'), timeout=0))
        stack.enter_context(FileLock(str(work / 'profile-recovery.lock'), timeout=0))
        stack.enter_context(FileLock(str(work / 'operation.lock'), timeout=0))
        yield
