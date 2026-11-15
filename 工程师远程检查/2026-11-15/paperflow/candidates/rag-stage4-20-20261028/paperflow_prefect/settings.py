from dataclasses import dataclass
from pathlib import Path
from .records import read_json

PROJECT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT / 'config.json'


class ExecutionDisabled(RuntimeError): pass


@dataclass(frozen=True)
class Settings:
    values: dict

    @classmethod
    def load(cls, path=DEFAULT_CONFIG):
        return cls(read_json(path))

    def path(self, key): return Path(self.values[key]).resolve()
    def __getitem__(self, key): return self.values[key]

    def require_execution(self):
        if self.values.get('execution_enabled') is not True:
            raise ExecutionDisabled('真实论文执行尚未启用；当前只允许接线检查。')
        root, data = self.path('library_root'), self.path('data_root')
        if root == data or root.is_relative_to(data) or data.is_relative_to(root):
            raise ValueError('系统记录区必须与论文资料库分开')
