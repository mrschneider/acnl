from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from acnl_core.security import SecurityPolicy


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
    # security: None = pre-security behavior (no auth, no encryption, no ACL)
    security: Optional["SecurityPolicy"] = None
    # persistence: when set and no explicit directory is passed to Node,
    # the node's directory is a file-backed ShelveDirectory at this path
    data_path: Optional[str] = None

    def __post_init__(self):
        if self.urn is None:
            self.urn = f"acnl:{self.zone}/{self.name}/broker"
        self.self_urn = self.urn


def resolve_directory(config: "NodeConfig", directory=None):
    """Pick the directory the node runs on and whether stop() owns it.
    Shared by the threaded and the asyncio node so persistence semantics
    cannot drift between the two."""
    if directory is not None:
        return directory, False
    if config.data_path:
        from acnl_core.directory import ShelveDirectory

        return ShelveDirectory(config.data_path), True
    from acnl_core.directory import InMemoryDirectory

    return InMemoryDirectory(), False
