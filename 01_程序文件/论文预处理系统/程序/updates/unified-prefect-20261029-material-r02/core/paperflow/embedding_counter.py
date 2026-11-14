"""Offline mature-tokenizer adapter: local cl100k + deployed vocab-only libllama.

Each worker owns its adapter. A persistent local subprocess loads vocabulary once;
there is no context/backend/inference, service, SSH, database, or retry fallback.
Artifacts are SHA checked before loading. Missing/drifted dependencies fail closed.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time

from .embedding_recovery import CounterProcessExited, CounterProtocolError


CL100K_CACHE = '9b5ad71b2ce5302211f9c61530b329a4922fc6a4'


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def linux_path(path):
    """Only absolute ordinary drive paths can cross the explicit WSL adapter."""
    path = Path(path).resolve()
    if os.name != 'nt':
        return str(path)
    if not path.drive or path.drive.startswith('\\\\'):
        raise ValueError('WSL adapter needs a local drive artifact')
    return '/mnt/' + path.drive[0].lower() + '/' + path.as_posix()[3:]


class OfflineCounter:
    def __init__(self, *, model_path, library_path, cache_dir, command,
                 model_sha256, libllama_sha256, cl100k_sha256, timeout=30,
                 unified=False, python_path=None, runtime_manifest=None):
        started = time.perf_counter()
        self.process = None
        self.owner = threading.get_ident()
        if not 0 < timeout <= 120:
            raise ValueError('bounded local counter timeout required')
        self.timeout = timeout
        self.read_buffer = b''
        self.unified = unified
        self.model_path, self.library_path = Path(model_path), Path(library_path)
        cache_dir = Path(cache_dir).resolve()
        expected = dict(model_sha256=model_sha256, libllama_sha256=libllama_sha256,
                        cl100k_sha256=cl100k_sha256)
        actual = dict(model_sha256=file_sha(self.model_path), libllama_sha256=file_sha(self.library_path),
                      cl100k_sha256=file_sha(cache_dir / CL100K_CACHE))
        if actual != expected:
            raise ValueError('offline tokenizer artifact drift')
        self.identity = actual
        extra = []
        if unified:
            if python_path is None or runtime_manifest is None:
                raise ValueError('explicit offline Linux tokenizer runtime required')
            runtime_root = Path(python_path).resolve()
            manifest = json.loads(Path(runtime_manifest).read_text(encoding='utf-8'))
            if manifest.get('tiktoken') != '0.12.0' or not manifest.get('files'):
                raise ValueError('unverified tokenizer runtime manifest')
            for row in manifest['files']:
                path = (runtime_root / row['path']).resolve()
                if not path.is_relative_to(runtime_root) or file_sha(path) != row['sha256']:
                    raise ValueError('offline tokenizer runtime drift')
            extra = [linux_path(cache_dir), linux_path(runtime_root)]
        else:
            # Kept for standalone offline audit tools; managed workers use unified.
            import tiktoken
            if tiktoken.__version__ != '0.12.0':
                raise ValueError('unverified tiktoken runtime version')
            previous = os.environ.get('TIKTOKEN_CACHE_DIR')
            os.environ['TIKTOKEN_CACHE_DIR'] = str(cache_dir)
            try:
                self.encoder = tiktoken.get_encoding('cl100k_base')
            finally:
                if previous is None:
                    os.environ.pop('TIKTOKEN_CACHE_DIR', None)
                else:
                    os.environ['TIKTOKEN_CACHE_DIR'] = previous
        script = Path(__file__).with_name('embedding_counter_worker.py')
        self.process = subprocess.Popen([*command, linux_path(script), linux_path(self.model_path),
            linux_path(self.library_path), *extra], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, bufsize=0,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            os.set_blocking(self.process.stdin.fileno(), False)
            os.set_blocking(self.process.stdout.fileno(), False)
            ready = self.exchange(None)
            if not isinstance(ready, dict) or ready.get('ready') is not True or ready.get('unified', False) is not unified:
                raise CounterProtocolError('offline vocabulary failed to load')
            self.runtime = dict(ready, cold_seconds=time.perf_counter() - started,
                                tiktoken_version='0.12.0', transport='local-stdio')
        except BaseException:
            self.close()
            raise

    def exchange(self, payload):
        """Bounded nonblocking local pipe IO, without a helper thread or lock."""
        outgoing = memoryview(payload or b'')
        deadline = time.monotonic() + self.timeout
        while True:
            if time.monotonic() >= deadline:
                self.close()
                raise TimeoutError('offline counter deadline exceeded')
            if self.process is None or self.process.poll() is not None:
                raise CounterProcessExited('offline counter exited')
            if outgoing:
                try:
                    n = os.write(self.process.stdin.fileno(), outgoing[:65536])
                    outgoing = outgoing[n:]
                except BlockingIOError:
                    pass
            try:
                incoming = os.read(self.process.stdout.fileno(), 65536)
                if incoming:
                    self.read_buffer += incoming
            except BlockingIOError:
                pass
            if not outgoing and b'\n' in self.read_buffer:
                line, self.read_buffer = self.read_buffer.split(b'\n', 1)
                try:
                    return json.loads(line)
                except (ValueError, UnicodeError):
                    raise CounterProtocolError('offline counter invalid JSON reply') from None
            time.sleep(.005)

    def count_many(self, texts):
        if threading.get_ident() != self.owner:
            raise ValueError('offline counter belongs to its worker thread')
        if self.process is None or self.process.poll() is not None:
            raise CounterProcessExited('offline counter unavailable')
        texts = list(texts)
        hashes = [hashlib.sha256(t.encode()).hexdigest() for t in texts]
        result = self.exchange((json.dumps(dict(texts=texts), ensure_ascii=False) + '\n').encode())
        if (not isinstance(result, dict) or result.get('hashes') != hashes or
                not isinstance(result.get('counts'), list) or len(result['counts']) != len(texts)):
            raise CounterProtocolError('offline counter response binding mismatch')
        if self.unified:
            native = result.get('cl100k_counts')
            if not isinstance(native, list) or len(native) != len(texts):
                raise CounterProtocolError('unified counter response binding mismatch')
            return [dict(model_tokens=count, cl100k_tokens=cl)
                    for count, cl in zip(result['counts'], native)]
        return [dict(model_tokens=count, cl100k_tokens=len(self.encoder.encode(text, disallowed_special=())))
                for text, count in zip(texts, result['counts'])]

    def close(self):
        process, self.process = self.process, None
        if process is not None:
            if process.stdin:
                process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.stdout:
                process.stdout.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
