"""Offline text token estimate for the configured DeepSeek route."""

from functools import lru_cache
import hashlib
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent / 'body_segment_vendor'
ROUTE = 'workbuddy-intl-cli-deepseek-free'
TOKENIZER_SHA256 = 'c90dfa01249db1be4245780a052ede752e1361c612ac6d08e2bdada7d599476b'
ENGINE_SHA256 = 'cef9accc734aed0f2095ba7c073a48f90cb2f6dfd50e02b22f9f7d75727aadd9'


@lru_cache(maxsize=1)
def _official_counter():
    data = ROOT / 'deepseek-v41-tokenizer.json'
    if hashlib.sha256(data.read_bytes()).hexdigest() != TOKENIZER_SHA256:
        raise ValueError('本地分词器文件与固定版本不一致')
    vendor_path = ROOT / 'python'
    engine = vendor_path / 'tokenizers' / 'tokenizers.pyd'
    if hashlib.sha256(engine.read_bytes()).hexdigest() != ENGINE_SHA256:
        raise ValueError('本地分词器引擎与固定版本不一致')
    loaded = sys.modules.get('tokenizers')
    if loaded is not None and not Path(loaded.__file__).resolve().is_relative_to(vendor_path.resolve()):
        raise ImportError('进程已加载不同的分词器，使用未确认字节估算')
    vendor = str(vendor_path)
    sys.path.insert(0, vendor)
    try:
        from tokenizers import Tokenizer
    finally:
        sys.path.remove(vendor)
    tokenizer = Tokenizer.from_file(str(data))
    tokenizer.no_truncation()
    tokenizer.no_padding()

    def count(text):
        return len(tokenizer.encode(text, add_special_tokens=False).ids)

    return count


def with_body_token_counter(config):
    """Return a copy; use bytes as an unconfirmed fallback if unavailable."""
    settings = dict(config)
    if settings.get('model') != ROUTE or callable(settings.get('text_token_counter')):
        return settings
    try:
        settings['text_token_counter'] = _official_counter()
    except (ImportError, OSError, ValueError, RuntimeError):
        settings['text_counter_source'] = 'utf8-byte-upper-bound-unconfirmed'
    else:
        settings['text_counter_source'] = 'deepseek-v4.1-official-tokenizer-estimate'
    return settings
