#!/usr/bin/env python3
"""
SportsPulse: Root-Level Pipeline Launcher
Delegates execution to SportsPulse/run_pipeline.py
"""

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PROJECT_DIR = ROOT / "SportsPulse"

if not PROJECT_DIR.is_dir():
    print(f"Error: SportsPulse directory not found at {PROJECT_DIR}")
    sys.exit(1)

runner_script = PROJECT_DIR / "run_pipeline.py"
cmd = [sys.executable, str(runner_script)] + sys.argv[1:]

result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
sys.exit(result.returncode)
