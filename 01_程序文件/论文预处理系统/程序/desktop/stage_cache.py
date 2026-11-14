"""Private response checkpoints; never an approval or an exported evidence file."""
import hashlib
import json
import os
import re
import sqlite3
import tempfile
import threading
from contextlib import closing
from pathlib import Path


FORMAT = 'validated-model-response/1'
MAX_BYTES = 8*1024*1024
_DIAGNOSTIC_LOCK = threading.RLock()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


class StageCache:
    def __init__(self, store_root):
        self.store_root = Path(store_root).resolve()
        self.root = self.store_root/'private'/'stage_cache'

    def key(self, source_hash, version, endpoint, request_bytes):
        # Do not erase IDs, paths or source positions from the request. Native
        # block IDs are stable on retry; run/item IDs are not sent to the model.
        if not isinstance(source_hash, str) or not re.fullmatch(r'[0-9a-f]{64}', source_hash):
            return None
        if not isinstance(version, str) or not version:
            return None
        return hashlib.sha256(encoded([FORMAT, source_hash, version, endpoint,
                                       hashlib.sha256(request_bytes).hexdigest()])).hexdigest()

    @staticmethod
    def _field_hashes(value, prefix='', output=None):
        """Return field names and digests only; never retain request values."""
        output = {} if output is None else output
        if isinstance(value, dict):
            for name, child in value.items():
                if name == 'content' and isinstance(child, str) and prefix.endswith(']'):
                    try: decoded = json.loads(child)
                    except (TypeError, ValueError): decoded = None
                    if isinstance(decoded, dict):
                        StageCache._field_hashes(decoded, prefix, output); continue
                path = f'{prefix}.{name}' if prefix else str(name)
                StageCache._field_hashes(child, path, output)
            return output
        if isinstance(value, list):
            for index, child in enumerate(value):
                StageCache._field_hashes(child, f'{prefix}[{index}]', output)
            return output
        output[prefix] = hashlib.sha256(encoded(value)).hexdigest()
        return output

    def request_diagnostic(self, source_hash, version, endpoint, request_bytes, key=None):
        """Build a redacted cache identity suitable for local diagnostics."""
        try: request = json.loads(request_bytes)
        except (TypeError, ValueError, UnicodeError): request = None
        return {'source_hash': source_hash, 'processing_version': version,
                'endpoint': endpoint, 'cache_key_prefix': (key or '')[:12],
                'request_fields': self._field_hashes(request) if request is not None else {}}

    def _path(self, key):
        if not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{64}', key):
            raise ValueError('Invalid checkpoint key')
        path = self.root/(key+'.json')
        if not self.root.resolve().is_relative_to(self.store_root) or not path.resolve().is_relative_to(self.root.resolve()):
            raise ValueError('Checkpoint path escaped private storage')
        return path

    def load(self, key):
        try:
            path = self._path(key)
            with path.open('rb') as stream:
                raw = stream.read(MAX_BYTES+1)
            if len(raw) > MAX_BYTES: return None
            record = json.loads(raw)
            if not isinstance(record, dict) or record.get('format') != FORMAT or record.get('cache_key') != key:
                return None
            result = record.get('result')
            if not isinstance(result, dict) or result.get('decision') != 'pass': return None
            if hashlib.sha256(encoded(result)).hexdigest() != record.get('result_sha256'): return None
            self._index_diagnostic(key,record.get('cache_diagnostic'))
            return result
        except (OSError, ValueError, TypeError, RecursionError):
            return None

    def save(self, key, result, diagnostic=None):
        temp = None
        try:
            if not isinstance(result, dict) or result.get('decision') != 'pass': return False
            record = {'format': FORMAT, 'cache_key': key, 'result': result,
                      'result_sha256': hashlib.sha256(encoded(result)).hexdigest()}
            if isinstance(diagnostic, dict):
                record['cache_diagnostic'] = {k: diagnostic[k] for k in (
                    'source_hash', 'processing_version', 'endpoint', 'request_fields') if k in diagnostic}
            raw = encoded(record)
            if len(raw) > MAX_BYTES: return False
            path = self._path(key)
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            with tempfile.NamedTemporaryFile(dir=self.root, prefix='.pending-', delete=False) as stream:
                temp = Path(stream.name)
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
            os.replace(temp, path)
            self._index_diagnostic(key,record.get('cache_diagnostic'))
            return True
        except (OSError, ValueError, TypeError, RecursionError):
            # A checkpoint failure must never turn evidence into approval or
            # expose a response in an error. A later retry can call the model.
            return False
        finally:
            if temp is not None:
                try: temp.unlink(missing_ok=True)
                except OSError: pass

    def discard(self, key):
        try: self._path(key).unlink(missing_ok=True)
        except (OSError, ValueError): pass
        self._diagnostic_index(lambda connection:connection.execute('DELETE FROM diagnostics WHERE cache_key=?',(key,)))

    @staticmethod
    def _diagnostic_scope(diagnostic):
        if not isinstance(diagnostic,dict):return None
        values=[diagnostic.get(name) for name in ('source_hash','processing_version','endpoint')]
        if not all(isinstance(value,str) and value for value in values):return None
        return hashlib.sha256(encoded(values)).hexdigest()

    def _diagnostic_index(self,operation,create=False):
        path=self.root/'diagnostics.sqlite3'
        with _DIAGNOSTIC_LOCK:
            if not create and not path.exists():return None
            try:
                self.root.mkdir(parents=True,exist_ok=True,mode=0o700)
                for attempt in range(2):
                    try:
                        with closing(sqlite3.connect(path,timeout=5)) as connection:
                            with connection:
                                connection.execute('CREATE TABLE IF NOT EXISTS diagnostics (cache_key TEXT PRIMARY KEY, scope TEXT NOT NULL, fields TEXT NOT NULL)')
                                connection.execute('CREATE INDEX IF NOT EXISTS diagnostic_scope ON diagnostics(scope)')
                                return operation(connection)
                    except sqlite3.DatabaseError as exc:
                        if attempt or isinstance(exc,sqlite3.OperationalError):raise
                        # Derived metadata can rebuild lazily on exact-key
                        # loads/saves; never scan response bodies on a miss.
                        path.unlink(missing_ok=True)
            except (OSError,sqlite3.Error,ValueError,TypeError):return None

    def _index_diagnostic(self,key,diagnostic):
        scope=self._diagnostic_scope(diagnostic)
        if scope is None:return
        fields=diagnostic.get('request_fields')
        if not isinstance(fields,dict):return
        # Persist only field names and digests even if a caller supplies a
        # malformed diagnostic. Model answers and request values never enter
        # this optional index.
        safe={name:value for name,value in fields.items() if isinstance(name,str)
              and isinstance(value,str) and re.fullmatch(r'[0-9a-f]{64}',value)}
        self._diagnostic_index(lambda connection:connection.execute(
            'INSERT INTO diagnostics VALUES (?,?,?) ON CONFLICT(cache_key) DO UPDATE SET scope=excluded.scope,fields=excluded.fields',
            (key,scope,encoded(safe).decode('utf-8'))),create=True)

    def diagnose_miss(self, key, diagnostic):
        report = {'status': 'miss', 'reason': 'cache_key_not_found',
                  'source_hash': diagnostic.get('source_hash'), 'cache_key_prefix': (key or '')[:12]}
        current_fields = diagnostic.get('request_fields') or {}
        scope=self._diagnostic_scope(diagnostic)
        def compare(connection):
            found=False
            for saved_key,raw in connection.execute('SELECT cache_key,fields FROM diagnostics WHERE scope=? ORDER BY rowid',(scope,)):
                if not self._path(saved_key).is_file():continue
                previous=json.loads(raw);found=True
                names=sorted(set(previous)|set(current_fields))
                changed=next((name for name in names if previous.get(name)!=current_fields.get(name)),None)
                if changed:
                    report['reason']='request_field_changed';report['first_difference']=changed;break
            return found
        found=self._diagnostic_index(compare) if scope is not None else False
        if found:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                with (self.root/'misses.jsonl').open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps(report, ensure_ascii=False, sort_keys=True) + '\n')
            except OSError: pass
        return report

    @staticmethod
    def hit_diagnostic(diagnostic, key):
        return {'status': 'hit', 'source_hash': diagnostic.get('source_hash'),
                'cache_key_prefix': (key or '')[:12]}
