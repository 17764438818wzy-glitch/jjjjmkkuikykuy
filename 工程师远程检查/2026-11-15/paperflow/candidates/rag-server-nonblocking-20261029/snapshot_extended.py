"""Read only selected source files; never write to the remote host/container."""
import base64
from datetime import datetime, timezone
import getpass
import hashlib
import json
from pathlib import Path
import shlex

import paramiko


FILES = [
    "api/apps/restful_apis/chunk_api.py",
    "api/apps/restful_apis/document_api.py",
    "api/apps/services/document_api_service.py",
    "api/db/services/document_service.py",
    "api/db/services/knowledgebase_service.py",
    "api/db/db_models.py",
    "rag/svr/task_executor.py",
    "rag/llm/embedding_model.py",
    "rag/utils/es_conn.py",
]


def main():
    secret = getpass.getpass("SSH authentication: ")
    if not secret:
        raise SystemExit("missing credential")
    destination = Path(__file__).resolve().parent / "baseline-extended"
    client = paramiko.SSHClient()
    client.load_host_keys(str(Path.home() / ".ssh" / "known_hosts"))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect("192.168.1.120", username="wzy", password=secret,
                   look_for_keys=False, allow_agent=False, timeout=8, auth_timeout=8)
    source = """import base64,hashlib,importlib.metadata,json,platform\nfrom pathlib import Path\nfiles=%r\nout={}\nfor name in files:\n b=(Path('/ragflow')/name).read_bytes(); out[name]={'sha256':hashlib.sha256(b).hexdigest(),'data':base64.b64encode(b).decode('ascii')}\nout['__versions__']={'quart':importlib.metadata.version('quart'),'python':platform.python_version()}\nprint(json.dumps(out))\n""" % FILES
    cmd = "sudo -S -p '' docker exec ragflow-server-ragflow-1 python3 -c " + shlex.quote(source)
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    stdin.write(secret + "\n")
    stdin.flush()
    stdin.channel.shutdown_write()
    raw, err = stdout.read(3_000_001), stderr.read(10_000)
    status = stdout.channel.recv_exit_status()
    client.close()
    if status or len(raw) > 3_000_000:
        raise RuntimeError(f"read-only snapshot failed: exit={status}; stderr_bytes={len(err)}")
    result = json.loads(raw)
    versions = result.pop("__versions__")
    entries = []
    for name in FILES:
        item = result[name]
        blob = base64.b64decode(item["data"], validate=True)
        digest = hashlib.sha256(blob).hexdigest()
        if digest != item["sha256"]:
            raise RuntimeError(f"snapshot checksum mismatch: {name}")
        file = destination / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(blob)
        entries.append({"path": name, "sha256": digest, "bytes": len(blob)})
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "snapshot.json").write_text(json.dumps({"snapshot_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "read-only server container source; executor container identity not established",
        "versions": versions, "files": entries}, ensure_ascii=False, indent=2), encoding="utf8")
    print(f"Read-only snapshot: {len(entries)} files; Python {versions['python']}; Quart {versions['quart']}")


if __name__ == "__main__":
    main()
