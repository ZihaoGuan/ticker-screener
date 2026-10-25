#!/usr/bin/env python3
"""Schedulable entry point for one-session Martin Bar Retest book continuation."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.replay_martin_bar_retest_range import continue_main

if __name__ == "__main__":
    raise SystemExit(continue_main())
