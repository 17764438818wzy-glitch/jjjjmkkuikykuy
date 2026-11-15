"""Single-admin LAN sessions; credentials and session tokens are never logged."""
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path


def create_admin(path, username, password):
    path = Path(path)
    if path.exists(): raise ValueError('管理员已存在，不会覆盖现有密码')
    if len(password) < 12: raise ValueError('管理员密码至少 12 个字符')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    salt = secrets.token_bytes(32)
    record = {'username': username, 'salt': salt.hex(), 'iterations': 600000,
              'hash': hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 600000).hex()}
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream: json.dump(record, stream)


class Sessions:
    def __init__(self, path, admin_path):
        self.path, self.admin_path = Path(path), Path(admin_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, csrf TEXT NOT NULL, expires REAL NOT NULL);
                               CREATE TABLE IF NOT EXISTS attempts(peer TEXT NOT NULL, time REAL NOT NULL);''')

    @staticmethod
    def digest(token): return hashlib.sha256(token.encode()).hexdigest()

    def login(self, username, password, peer):
        if not isinstance(username, str) or not isinstance(password, str) or len(password) > 1024:
            raise ValueError('用户名或密码不正确')
        with sqlite3.connect(self.path, timeout=10) as db:
            db.execute('DELETE FROM attempts WHERE time<?', (time.time() - 300,))
            count = db.execute('SELECT COUNT(*) FROM attempts WHERE peer=?', (peer,)).fetchone()[0]
            if count >= 10: raise ValueError('登录尝试过于频繁，请五分钟后重试')
            db.execute('INSERT INTO attempts VALUES(?,?)', (peer, time.time()))
        try: admin = json.loads(self.admin_path.read_text(encoding='utf-8'))
        except (OSError, ValueError): raise ValueError('管理员尚未初始化') from None
        result = hashlib.pbkdf2_hmac('sha256', password.encode(), bytes.fromhex(admin['salt']), admin['iterations']).hex()
        if not hmac.compare_digest(username, admin['username']) or not hmac.compare_digest(result, admin['hash']):
            raise ValueError('用户名或密码不正确')
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        with sqlite3.connect(self.path, timeout=10) as db:
            db.execute('DELETE FROM attempts WHERE peer=?', (peer,))
            db.execute('DELETE FROM sessions WHERE expires<?', (time.time(),))
            db.execute('INSERT INTO sessions VALUES(?,?,?)', (self.digest(token), csrf, time.time() + 43200))
        return token, csrf

    def get(self, token):
        if not token or len(token) > 100: return None
        with sqlite3.connect(self.path, timeout=10) as db:
            row = db.execute('SELECT csrf,expires FROM sessions WHERE token=?', (self.digest(token),)).fetchone()
        return {'csrf': row[0]} if row and row[1] > time.time() else None

    def logout(self, token):
        with sqlite3.connect(self.path, timeout=10) as db:
            db.execute('DELETE FROM sessions WHERE token=?', (self.digest(token or ''),))
