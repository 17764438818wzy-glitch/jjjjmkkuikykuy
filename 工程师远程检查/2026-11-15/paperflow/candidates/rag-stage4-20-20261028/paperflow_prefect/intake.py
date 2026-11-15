"""Receive confirmed per-person folders without repeating classification."""
import os
from pathlib import Path
from .records import identity, sha_file

BODY_FORMATS = {'.docx', '.doc', '.pdf', '.txt', '.md', '.rtf', '.wps', '.odt'}


class IntakeConflict(ValueError): pass


def files_below(directory):
    directory = Path(directory)
    if not directory.exists(): return []
    if directory.is_symlink() or directory.is_junction():
        raise IntakeConflict('来源目录本身是链接，不能自动接管：' + str(directory))
    result = []
    for current, dirs, files in os.walk(directory, followlinks=False):
        for name in [*dirs, *files]:
            path = Path(current) / name
            if path.is_symlink() or path.is_junction():
                raise IntakeConflict('来源目录含链接，不能自动接管：' + str(path))
        for name in files:
            path = Path(current) / name
            if not path.resolve().is_relative_to(directory.resolve()):
                raise IntakeConflict('来源路径越界')
            result.append(path)
    return sorted(result, key=lambda p: str(p).casefold())


def inspect_person(person, library_root):
    person, library = Path(person).resolve(), Path(library_root).resolve()
    if person == library or not person.is_relative_to(library):
        raise IntakeConflict('个人目录不在资料库内')
    bodies = files_below(person / '正文')
    if not bodies: raise IntakeConflict('正文目录没有稿件')
    if any(p.suffix.lower() not in BODY_FORMATS for p in bodies):
        raise IntakeConflict('正文目录含待展开或不支持的材料，请保留并核对分流结果')
    originals = []
    for kind, paths in [('body', bodies), ('attachment', files_below(person / '附件'))]:
        for path in paths:
            before = path.stat(); sha = sha_file(path); after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise IntakeConflict('文件仍在变化，尚不能接收')
            relative = path.relative_to(person).as_posix()
            originals.append({'path': str(path), 'relative': relative, 'bytes': after.st_size,
                              'sha256': sha, 'kind': kind,
                              'file_id': identity([relative, sha])[:24]})
    body_copies = [r for r in originals if r['kind'] == 'body']
    if len({r['sha256'] for r in body_copies}) != 1:
        raise IntakeConflict('有多个内容不同的正文，不能自动任选一份')
    paper_id = 'PF_' + identity([str(library).casefold(), person.relative_to(library).as_posix().casefold()])[:24]
    revision = identity([(r['relative'], r['sha256'], r['bytes']) for r in originals])[:24]
    excluded = [p.relative_to(person).as_posix() for p in files_below(person / '垃圾材料')]
    return {'schema': 'prefect-paper/1', 'paper_id': paper_id, 'revision': revision,
            'person_dir': str(person), 'library_root': str(library),
            'body': body_copies[0], 'body_copies': body_copies,
            'attachments': [r for r in originals if r['kind'] == 'attachment'],
            'archive_files': originals, 'excluded': excluded}


def verify_inventory(paper):
    person = Path(paper['person_dir']).resolve()
    current = {str(p.resolve()) for name in ('正文', '附件') for p in files_below(person / name)}
    if current != {row['path'] for row in paper['archive_files']}:
        raise ValueError('个人目录文件清单已改变')
    for row in paper['archive_files']:
        path = Path(row['path'])
        if not path.resolve().is_relative_to(person) or path.stat().st_size != row['bytes'] or sha_file(path) != row['sha256']:
            raise ValueError('原稿或附件与登记内容不符')
    return True


def discover(library_root):
    root = Path(library_root).resolve()
    if not root.is_dir(): raise FileNotFoundError('整理后的资料库不存在')
    result = []
    for current, dirs, _ in os.walk(root, followlinks=False):
        dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()
                   and not (Path(current) / name).is_junction()]
        if '正文' in dirs:
            result.append(str(Path(current).resolve()))
            dirs[:] = []
        else:
            dirs[:] = [name for name in dirs if name not in {'附件', '垃圾材料'}]
    return sorted(result)
