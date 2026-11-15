# Remote Harness Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for inline execution. Independent review is read-only under reuse-first-dev.

**Goal:** Run Harness on the LAN server while preserving the existing PC workflow and operator controls.

**Architecture:** Existing producer calls a start/run/close-compatible remote adapter. A restricted SSH gateway runs the existing SDK in an isolated container based on the existing server image. Manager exit qualification includes remote exit proof.

**Tech Stack:** Existing Python SDK 0.1.5rc1, official DSH CLI 0.2.0-rc.2, OpenSSH, Docker, Python stdlib, existing Windows Job/IPC/SQLite/FastAPI.

**Spec:** `E:/rag mid/deployments/workbench-8089-20261018/REMOTE_HARNESS.md`.

## Constraints

- No article task or RAG writes during deployment; no bulk resume; no concurrency increase.
- Other services remain on the PC. Old server Harness and data remain intact.
- Each model turn needs a current Manager LLM permit; no remote fallback to unapproved local execution.
- A local SSH exit does not prove remote exit. Unknown remote state denies replay/resume.
- Remote heartbeat expiry plus hard shutdown is at most 8 seconds; existing Manager Job deadline remains 10 seconds.

## Review focus

Unicode filenames, spaces and nested paths; archive traversal/symlinks and changed inputs; same-session diagnostic updates; delayed create after cancellation; manager crash during remote creation.

## Tasks

1. Files and transport (`remote_files.py`, `remote_transport.py`, `tests/test_remote_harness.py`): failing path/hash/archive tests, implement bounded streaming and safe staging, run focused tests.
2. Server runner/gateway (`remote_runner.py`, `remote_gateway.py`, server deployment scripts): failing lease/identity/tombstone tests, implement fixed image/owner management, verify in a disposable owned container. SDK and file tools stay native.
3. Producer/Manager seams (`remote_harness.py`, `remote_control.py`, config/control/IPC/managed/workbench): failing current-permit and remote-exit tests, implement optional adapter and exit guard, run existing control/producer/cache tests.
4. Workbench and deployment: display location/health, merge the other window's final fixes, run scoped regressions and real LAN UI verification; promote and start paused. Save SHA/exit receipts and report pending concurrency and real-article acceptance separately.

This workspace has no Git checkout. Deployment records exact file hashes and rollback snapshots instead of inventing Git history.

## Execution outcome

All four tasks completed. Related regressions: 150 passed, 1 Windows symlink skip with native Linux equivalent passed. Real SDK/file/session, real-model synthetic canary, pause, bridge kill and Manager crash recovery receipts are in the deployment evidence directory. CLI 0.2.0-rc.2 / SDK 0.1.5rc1 verified in the actual image. Canonical application and runtime configuration promoted with rollback snapshots; 8089 restarted paused, admission closed, active units 0, no new RAG actions. No concurrency or real-article ingestion acceptance performed.
