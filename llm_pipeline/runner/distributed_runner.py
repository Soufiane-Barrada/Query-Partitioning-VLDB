from __future__ import annotations

import sys
from pathlib import Path

# Ensure we import the shared distributed_executor from CHOP/distributed_runner
THIS_DIR = Path(__file__).resolve().parent
ROOT = THIS_DIR.parent.parent
DIST_RUNNER_DIR = ROOT / "distributed_runner"
if str(DIST_RUNNER_DIR) not in sys.path:
    sys.path.insert(0, str(DIST_RUNNER_DIR))

from distributed_executor.distributed_runner import DistributedRunner

__all__ = ["DistributedRunner"]
