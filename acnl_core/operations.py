"""The closed operation vocabulary. Six primitives, no invented verbs."""

from __future__ import annotations

OPERATIONS = frozenset(
    {
        "BIND",          # claim a hierarchical name -> explicit rebind
        "UNBIND",        # release a name
        "ADVERTISE",     # publish a service's attributes (name, version, caps)
        "SOLVE",         # directory query over advertised attributes
        "SEND",          # deliver a payload to a bound agent (or alias)
        "LIFECHECK",     # liveness probe; replies keep the age timer fresh
    }
)

# Operations a directory must refuse when it is not authoritative for the name.
DIRECTORY_OPS = frozenset({"BIND", "UNBIND", "ADVERTISE", "SOLVE", "LIFECHECK"})
# Operations an agent must refuse when it is not a mailbox.
AGENT_OPS = frozenset({"SEND", "LIFECHECK"})


def is_known_operation(op: str) -> bool:
    return op in OPERATIONS
