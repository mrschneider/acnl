from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class NodeConfig:
    host: str
    port: int
    zone: str = "lab"
    name: str = "node1"
    urn: str = None  # type: ignore[assignment]
    # the directory node itself is an advertised service
    self_urn: str = None  # type: ignore[assignment]
    directory_ttl: float = 60.0
    broadcast_interval: float = 10.0
    sweep_interval: float = 5.0
    max_peers: int = 32
    queue_depth: int = 64
    max_frame: int = 1_048_576
    fragment_size: int = 256
    peer_ttl: float = 60.0  # seconds a peer is presumed alive

    def __post_init__(self):
        if self.urn is None:
            self.urn = f"acnl:{self.zone}/{self.name}/broker"
        self.self_urn = self.urn
