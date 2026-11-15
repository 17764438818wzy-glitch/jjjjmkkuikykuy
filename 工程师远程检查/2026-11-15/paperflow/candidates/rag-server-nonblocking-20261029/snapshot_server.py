"""Read a bounded snapshot of running server source without changing the server."""
from __future__ import annotations

import base64
import getpass
import hashlib
import json
from pathlib import Path
import shlex
import sys
from datetime import datetime, timezone

import paramiko


FILES = [
    "api/apps/restful_apis/chunk_api.py",
    "api/apps/__init__.py",
    "api/ragflow_server.py",
    "api/db/services/document_service.py",
    "api/db/services/knowledgebase_service.py",
    "api/db/services/tenant_llm_service.py",
    "api/db/joint_services/tenant_model_service.py",
    "api/db/db_models.py",
    "common/misc_utils.py",
    "common/llm_request_context.py",
    "rag/utils/es_conn.py",
]


def main():
    secret = getpass.getpass("SSH authentication: ")
    if not secret:
        raise SystemExit("missing credential on stdin")
    root = Path(__file__).resolve().parent
    client = paramiko.SSHClient()
    client.load_host_keys(str(Path.home() / ".ssh" / "known_hosts"))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect("192.168.1.120", username="wzy", password=secret,
                   look_for_keys=False, allow_agent=False, timeout=8, auth_timeout=8)
    source = """import base64,hashlib,importlib.metadata,json,os,platform\nfrom pathlib import Path\nfiles=%r\nresult={}\nfor name in files:\n p=Path('/ragflow')/name; b=p.read_bytes(); result[name]={'sha256':hashlib.sha256(b).hexdigest(),'size':len(b),'data':base64.b64encode(b).decode('ascii')}\nresult['__versions__']={'quart':importlib.metadata.version('quart'),'python':platform.python_version()}\nprint(json.dumps(result))\n""" % FILES
    cmd = "sudo -S -p '' docker exec ragflow-server-ragflow-1 python3 -c " + shlex.quote(source)
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    stdin.write(secret + "\n")
    stdin.flush()
    stdin.channel.shutdown_write()
    raw = stdout.read(2_000_001)
    error = stderr.read(10_000)
    status = stdout.channel.recv_exit_status()
    client.close()
    if status or len(raw) > 2_000_000:
        raise RuntimeError(f"read-only source snapshot failed: exit={status}; stderr_bytes={len(error)}")
    data = json.loads(raw)
    versions = data.pop("__versions__")
    entries = []
    for name in FILES:
        item = data[name]
        blob = base64.b64decode(item["data"], validate=True)
        digest = hashlib.sha256(blob).hexdigest()
        if len(blob) != item["size"] or digest != item["sha256"]:
            raise RuntimeError(f"snapshot integrity mismatch: {name}")
        destination = root / "baseline" / Path(name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(blob)
        entries.append({"path": name, "size": len(blob), "sha256": digest})
    record = {"snapshot_at_utc": datetime.now(timezone.utc).isoformat(),
              "source": "read-only running container file snapshot",
              "container": "ragflow-server-ragflow-1", "versions": versions,
              "files": entries}
    (root / "baseline" / "snapshot.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"file_count": len(entries), "versions": versions,
                      "snapshot": str(root / "baseline" / "snapshot.json")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
