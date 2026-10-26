"""E2E driver: boots the real ACNL node, then runs the Node.js client
(experiments/e2e_node.mjs) against it.

The driver is Python; the client is JavaScript. That asymmetry is the point:
a Python client talking to a Python server proves nothing about portability.
Every check prints PASS/FAIL; the exit code is non-zero if any check fails.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from acnl_core.directory import InMemoryDirectory  # noqa: E402
from acnl_engine.config import NodeConfig  # noqa: E402
from acnl_engine.node import Node  # noqa: E402

PORT = 19710


def main() -> int:
    cfg = NodeConfig(host="127.0.0.1", port=PORT, name="node1", zone="lab")
    node = Node(cfg, InMemoryDirectory())
    node.start()
    try:
        env = dict(os.environ, ACNL_PORT=str(PORT))
        proc = subprocess.run(
            ["node", str(ROOT / "experiments" / "e2e_node.mjs")],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
            cwd=str(ROOT),
        )
        print(proc.stdout, end="")
        if proc.stderr.strip():
            print("stderr:", proc.stderr, file=sys.stderr)
        return proc.returncode
    finally:
        node.stop()


if __name__ == "__main__":
    raise SystemExit(main())
