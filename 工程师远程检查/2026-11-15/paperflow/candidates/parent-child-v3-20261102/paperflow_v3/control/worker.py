"""Start the Prefect executor only after invalidating prior task starts."""

import asyncio
import os
import subprocess
import sys

import psutil

from .admission import begin_control_session
from .flows import load_runtime


async def prepare_executor():
    load_runtime()
    return await begin_control_session()


def stop_process(process):
    """Stop only the process tree started by the candidate launcher."""
    if process.poll() is not None:
        return
    try:
        children = psutil.Process(process.pid).children(recursive=True)
    except psutil.NoSuchProcess:
        children = []
    for child in reversed(children):
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    process.terminate()
    process.wait(timeout=10)


def wait_for_launcher(process, launcher_pid):
    """Do not leave a polling worker behind when its owning API exits."""
    try:
        launcher = psutil.Process(launcher_pid)
    except psutil.NoSuchProcess:
        stop_process(process)
        return 1
    while True:
        try:
            return process.wait(timeout=.5)
        except subprocess.TimeoutExpired:
            try:
                alive = launcher.is_running() and launcher.status() != psutil.STATUS_ZOMBIE
            except psutil.NoSuchProcess:
                alive = False
            if not alive:
                stop_process(process)
                return 1


def main():
    asyncio.run(prepare_executor())
    owner = os.environ.get("PAPERFLOW_V3_LAUNCHER_PID")
    process = subprocess.Popen([sys.executable, "-m", "paperflow_v3.control.stage_worker", *sys.argv[1:]])
    return wait_for_launcher(process, int(owner)) if owner else process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
