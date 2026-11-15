"""Authorized one-shot resume: reuse native launcher and frozen existing inputs."""
import sys
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import start_remaining_once as native

native.OUT=native.ROOT/'_verification/purpose-review-20261022/start'

if __name__=='__main__':
    native.main()
