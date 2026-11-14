import hashlib
import json
from uuid import NAMESPACE_URL, uuid5


def stable_id(*parts):
    return uuid5(NAMESPACE_URL,json.dumps(parts,ensure_ascii=False,separators=(',',':'))).hex


def fingerprint(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,
        separators=(',',':'),allow_nan=False).encode()).hexdigest()
