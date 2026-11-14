"""Native transparent allocation compression for a NEW model stream log only.

The caller keeps the ordinary raw writer, bytes, flushes and filename. Existing
evidence is never truncated. Optional allocation failure must not lose a reply.
"""
import os
from pathlib import Path


def compress_new_stream(stream):
    if os.name != 'nt':
        return False
    import msvcrt
    import struct
    import win32file
    import winioctlcon
    import pywintypes
    stat = os.fstat(stream.fileno())
    if stream.tell() != 0 or stat.st_size != 0 or stat.st_nlink != 1:
        raise ValueError('only a new empty private stream may be compressed')
    # Borrow the existing CRT handle; never close or reopen the writer handle.
    try:
        win32file.DeviceIoControl(msvcrt.get_osfhandle(stream.fileno()),
            winioctlcon.FSCTL_SET_COMPRESSION, struct.pack('<H', 2), 0)
    except pywintypes.error as exc:
        raise OSError('optional NTFS stream compression unavailable') from exc
    return True


def open_stream_log(path):
    # FSCTL_SET_COMPRESSION requires a read/write handle, not write-only.
    stream = Path(path).open('x+b')
    try:
        compress_new_stream(stream)
    except (OSError, ImportError):
        # Compression is optional. A sent request must still retain every byte.
        pass
    except BaseException:
        stream.close()
        raise
    return stream


def write_new_generated_json(path, value):
    """Use the original atomic JSON layout on a NEW whitelisted temp file.

    Return False for any existing target/temp or unrelated document. No prior
    file is opened here; the caller then retains its original writer semantics.
    """
    import json
    path = Path(path)
    allowed = ('material.json', 'validation.json', 'private/index.json',
               'private/candidate.json', 'private/candidate-raw.json',
               'model/request.json', 'model/request-planned.json')
    eligible = any(path.name == Path(name).name
                   and path.as_posix().endswith('/preextract/'+name)
                   for name in allowed)
    temp = path.with_suffix(path.suffix+'.tmp')
    if not eligible or path.exists() or temp.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        stream = temp.open('x+', encoding='utf-8')
    except FileExistsError:
        return False
    with stream:
        try:
            compress_new_stream(stream)
        except (OSError, ImportError):
            pass
        stream.write(json.dumps(value, ensure_ascii=False, indent=2))
    from atomic_json import replace_generated
    replace_generated(temp,path)
    return True
