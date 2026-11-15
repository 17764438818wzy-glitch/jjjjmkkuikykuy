"""Bind immutable Windows evidence names to a separate server snapshot.

The original names and bytes remain part of the evidence.  This module only
resolves explicitly registered names; it does not rewrite JSON or calculate a
new paper, receipt, request, or task identity.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path, PureWindowsPath
import re
import stat
import uuid


class EvidenceError(ValueError):
    pass


_SHA = re.compile(r'^[0-9a-f]{64}$')
_RESERVED = re.compile(r'^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?$', re.I)


def _windows_name(name):
    if not isinstance(name, str) or not name or '\0' in name:
        raise EvidenceError('invalid Windows name')
    if name.startswith(('\\\\?\\', '\\\\.\\')):
        raise EvidenceError('unsupported Windows device name')
    path = PureWindowsPath(name)
    if not path.is_absolute() or not path.drive:
        raise EvidenceError('Windows name must be absolute')
    components = path.parts[1:]
    for component in components:
        if (component in ('.', '..') or component.endswith((' ', '.'))
                or any(ord(c) < 32 or c in '<>:"|?*' for c in component)
                or _RESERVED.fullmatch(component)):
            raise EvidenceError('invalid Windows path component')
    drive = path.drive
    if re.fullmatch(r'[A-Za-z]:', drive):
        root = ('drives', drive[0].lower())
    elif drive.startswith('\\\\'):
        fields = drive[2:].split('\\')
        if len(fields) != 2 or any(not s or s in ('.', '..') for s in fields):
            raise EvidenceError('invalid UNC root')
        root = ('unc', *fields)
    else:
        raise EvidenceError('unsupported Windows root')
    return path, root, components


def _no_links(path):
    path = Path(path).absolute()
    for item in (path, *path.parents):
        try:
            info = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise EvidenceError('link or reparse point in evidence path')


class EvidenceMap:
    """One per immutable job snapshot, never a process-global path hook."""

    def __init__(self, snapshot_root):
        self.root = Path(snapshot_root).absolute()
        _no_links(self.root)
        self._names = {}
        self._entries = {}
        self._spellings = {}

    @staticmethod
    def _key(path):
        # PureWindowsPath uses lower(), not Unicode casefold(), for Windows
        # name comparisons.  Casefold merges distinct names such as ß and ss.
        return str(path).lower()

    def bind(self, source, digest, size):
        path, root_parts, components = _windows_name(source)
        if not components:
            raise EvidenceError('root is not an evidence file')
        if not isinstance(digest, str) or not _SHA.fullmatch(digest):
            raise EvidenceError('invalid evidence digest')
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise EvidenceError('invalid evidence size')
        key = self._key(path)
        existing = self._entries.get(key)
        if existing:
            previous = PureWindowsPath(existing['source'])
            if (previous.parts[1:] != path.parts[1:]
                    or (existing['sha256'], existing['bytes']) != (digest, size)):
                raise EvidenceError('conflicting case-insensitive evidence binding')
            return self._names[key]
        if key in self._names:
            raise EvidenceError('file conflicts with bound directory')

        anchor = PureWindowsPath(path.anchor)
        anchor_key = self._key(anchor)
        physical = self._names.setdefault(anchor_key, self.root.joinpath(*root_parts))
        self._spellings.setdefault(anchor_key, ())
        for index, component in enumerate(components):
            ancestor = anchor.joinpath(*components[:index + 1])
            part_key = self._key(ancestor)
            if part_key in self._entries:
                raise EvidenceError('directory conflicts with bound file')
            spelling = ancestor.parts[1:]
            if part_key in self._spellings and self._spellings[part_key] != spelling:
                raise EvidenceError('conflicting case-insensitive path spelling')
            self._spellings[part_key] = spelling
            physical = self._names.setdefault(part_key, physical / component)
        self._entries[key] = {'source': source, 'sha256': digest, 'bytes': size,
                              'relative': physical.relative_to(self.root).as_posix()}
        return physical

    def resolve(self, source):
        path, _, _ = _windows_name(source)
        try:
            key = self._key(path)
            if self._spellings[key] != path.parts[1:]:
                raise KeyError(key)
            return self._names[key]
        except KeyError:
            raise EvidenceError('unregistered evidence path') from None

    def manifest(self):
        return [dict(row) for row in self._entries.values()]

    def copy_verified(self, source_name, source_file):
        path, _, _ = _windows_name(source_name)
        if os.name != 'nt':
            raise EvidenceError('source copying requires the original Windows host')
        key = self._key(path)
        if key not in self._entries:
            raise EvidenceError('unregistered evidence file')
        entry = self._entries[key]
        target = self._names[key]
        source_file = Path(source_file)
        actual_name = PureWindowsPath(str(source_file.absolute()))
        if (actual_name.drive.lower() != path.drive.lower()
                or actual_name.parts[1:] != path.parts[1:]):
            raise EvidenceError('source file does not match its registered name')
        _no_links(source_file)
        before = source_file.stat()
        if before.st_nlink != 1:
            raise EvidenceError('hard-linked source file is not accepted')
        _no_links(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        _no_links(target)
        if target.exists():
            raise EvidenceError('snapshot target already exists')
        temporary = target.parent / ('.transfer-' + uuid.uuid4().hex)
        digest = hashlib.sha256()
        size = 0
        try:
            with source_file.open('rb') as src, temporary.open('xb') as dst:
                opened = os.fstat(src.fileno())
                if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                    raise EvidenceError('source file changed while opening')
                while True:
                    data = src.read(1024 * 1024)
                    if not data:
                        break
                    digest.update(data)
                    size += len(data)
                    dst.write(data)
                dst.flush()
                os.fsync(dst.fileno())
                after = os.fstat(src.fileno())
                if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
                        opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns):
                    raise EvidenceError('source file changed during transfer')
            if size != entry['bytes'] or digest.hexdigest() != entry['sha256']:
                raise EvidenceError('evidence changed during transfer')
            try:
                os.link(temporary, target)
            except FileExistsError as exc:
                raise EvidenceError('snapshot target already exists') from exc
        finally:
            if temporary.exists():
                temporary.unlink()
        return target

    def verify_all(self):
        if not self._entries:
            raise EvidenceError('empty snapshot has no evidence')
        for key, entry in self._entries.items():
            target = self._names[key]
            _no_links(target)
            digest, size = hashlib.sha256(), 0
            try:
                with target.open('rb') as stream:
                    while True:
                        data = stream.read(1024 * 1024)
                        if not data:
                            break
                        digest.update(data)
                        size += len(data)
            except OSError as exc:
                raise EvidenceError('snapshot evidence missing') from exc
            if size != entry['bytes'] or digest.hexdigest() != entry['sha256']:
                raise EvidenceError('snapshot evidence changed')
        return True
