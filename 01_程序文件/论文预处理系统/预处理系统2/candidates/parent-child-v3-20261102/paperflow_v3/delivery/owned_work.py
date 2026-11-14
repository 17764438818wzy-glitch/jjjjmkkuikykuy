"""List files only inside the two independently owned paper work roots."""

import os
from pathlib import Path


def owned_work_files(roots, source, nas):
    protected = [Path(source).resolve(), Path(nas).resolve()]
    owned = [Path(root).resolve() for root in roots]
    for root in owned:
        if any(root == path or root.is_relative_to(path) or path.is_relative_to(root) for path in protected):
            raise ValueError('paper work root overlaps source or archive')
        if not root.is_dir():
            raise ValueError('paper work root is unavailable')
    if len(owned) != len(set(owned)) or any(
            left.is_relative_to(right) or right.is_relative_to(left)
            for index, left in enumerate(owned) for right in owned[index + 1:]):
        raise ValueError('paper work roots overlap')

    files = []
    def walk_error(error):
        raise error

    for root in owned:
        for directory, children, names in os.walk(root, followlinks=False, onerror=walk_error):
            for name in children:
                path = Path(directory) / name
                if path.is_symlink() or path.is_junction():
                    raise ValueError('paper work root contains a linked directory')
            for name in names:
                path = Path(directory) / name
                if path.is_symlink() or not path.resolve().is_relative_to(root):
                    raise ValueError('paper work file is outside its owner')
                files.append(str(path))
    return sorted(files, key=os.path.normcase)
