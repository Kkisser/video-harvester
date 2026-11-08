"""Compatibility entry point: exercise coordinator AND agents, not target alone."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.system_check import run

if __name__ == "__main__":
    run("load", workers=10)
