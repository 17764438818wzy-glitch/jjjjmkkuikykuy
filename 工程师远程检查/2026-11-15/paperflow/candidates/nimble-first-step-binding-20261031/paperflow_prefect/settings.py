from dataclasses import dataclass
import os
from pathlib import Path
from .records import read_json

PROJECT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT / 'config.json'


class ExecutionDisabled(RuntimeError): pass


@dataclass(frozen=True)
class Settings:
    values: dict

    @classmethod
    def load(cls, path=None):
        chosen = path if path is not None else os.environ.get('PAPERFLOW_CONFIG', DEFAULT_CONFIG)
        return cls(read_json(chosen))

    def path(self, key): return Path(self.values[key]).resolve()
    def __getitem__(self, key): return self.values[key]

    def require_local_classification(self):
        if not self.path('source_manifest').is_file():
            raise FileNotFoundError('本地分类依赖清单不存在')

    def require_execution(self):
        if self.values.get('execution_enabled') is not True:
            raise ExecutionDisabled('真实论文执行尚未启用；当前只允许接线检查。')
        root, data = self.path('library_root'), self.path('data_root')
        if root == data or root.is_relative_to(data) or data.is_relative_to(root):
            raise ValueError('系统记录区必须与论文资料库分开')


def first_step_shared_data(settings):
    """Use the independent workspace's shared gates for its CLI and API."""
    data = settings.path('data_root')
    candidates = (PROJECT / 'runtime' / 'workspace-config.json',
                  data.parent / 'workspace-config.json',
                  data.parent.parent / 'workspace-config.json')
    for path in candidates:
        if not path.is_file():
            continue
        values = read_json(path)
        if any(Path(values[key]).resolve() != settings.path(key)
               for key in ('program_root', 'release_root')):
            continue
        workspace_data = Path(values['data_root']).resolve()
        if data == workspace_data or data == (path.parent / 'data').resolve():
            return workspace_data
    return data
