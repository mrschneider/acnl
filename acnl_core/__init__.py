"""ACNL core: the contract, IO-free.

Message layer, addressing, query language, directory contract, sequencing.
No sockets in the library; a transport only moves bytes.
"""

from .addresses import (
    PROTOCOL_MAJOR,
    PROTOCOL_VERSION,
    AddressError,
    Reference,
    parse_urn,
)
from .attributes import (
    And,
    Contains,
    Cmp,
    Not,
    Or,
    QueryError,
    _eval,
    parse_query,
)
from .codec import MAX_FRAME, HEADER, CodecError, FrameDecoder, encode_frame
from .directory import (
    DuplicateName,
    Entry,
    InMemoryDirectory,
    ShelveDirectory,
)
from .errors import (
    MESSAGE_CODES,
    SYSTEM_CODES,
    USER_CODES,
    ErrorBody,
    duplicate_name,
    hops_exhausted,
    loop_detected,
    malformed,
    queue_full,
    target_unreachable,
    unavailable,
    unknown_service,
    unknown_verb,
)
from .messages import (
    MAX_TTL,
    OPERATIONS,
    WIRE_KINDS,
    Envelope,
    MessageError,
    WireMessage,
    make_envelope,
    new_id,
)
from .sequencing import (
    ConnectionTracker,
    ConnectionState,
    ReceiveWindow,
    SendWindow,
    SequenceError,
)

__version__ = "0.2.0"

__all__ = [
    "PROTOCOL_MAJOR",
    "PROTOCOL_VERSION",
    "AddressError",
    "Reference",
    "parse_urn",
    "And",
    "Contains",
    "Cmp",
    "Not",
    "Or",
    "QueryError",
    "_eval",
    "parse_query",
    "MAX_FRAME",
    "HEADER",
    "CodecError",
    "FrameDecoder",
    "encode_frame",
    "DuplicateName",
    "Entry",
    "InMemoryDirectory",
    "ShelveDirectory",
    "MESSAGE_CODES",
    "SYSTEM_CODES",
    "USER_CODES",
    "ErrorBody",
    "duplicate_name",
    "hops_exhausted",
    "loop_detected",
    "malformed",
    "queue_full",
    "target_unreachable",
    "unavailable",
    "unknown_service",
    "unknown_verb",
    "MAX_TTL",
    "OPERATIONS",
    "WIRE_KINDS",
    "Envelope",
    "MessageError",
    "WireMessage",
    "make_envelope",
    "new_id",
    "ConnectionTracker",
    "ConnectionState",
    "ReceiveWindow",
    "SendWindow",
    "SequenceError",
]
