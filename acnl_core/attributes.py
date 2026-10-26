"""The attribute query language.

Grammar (NOT > AND > OR precedence, parentheses allowed):

    query  := or_expr
    or_expr := and_expr (OR and_expr)*
    and_expr := not_expr (AND not_expr)*
    not_expr := NOT not_expr | primary
    primary := '(' or_expr ')' | comparison
    comparison := ATTR OP VALUE | ATTR CONTAINS VALUE
    OP     := = != > < >= <=
    VALUE  := quoted string | number | bare token

Special attributes (runtime, framework, name, version, urn) are resolved from
the reference before falling back to stored attributes. A ``>`` on text is
refused at parse time instead of coercing — a text comparison is a type error,
not a silent ``"5" > "12"``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

ATTR_RE = re.compile(r"^[a-z0-9_.-]{1,64}$")
BARE_RE = re.compile(r"^[A-Za-z0-9._-]+$")
TEXT_OPS = ("<", ">", "<=", ">=")


class QueryError(ValueError):
    pass


# --- AST ---------------------------------------------------------------

@dataclass
class Cmp:
    attr: str
    op: str
    value: Any


@dataclass
class Contains:
    attr: str
    value: Any


@dataclass
class And:
    parts: List[Any]


@dataclass
class Or:
    parts: List[Any]


@dataclass
class Not:
    part: Any


# --- tokenizer ---------------------------------------------------------

_TOKENS = ("(", ")", "AND", "OR", "NOT", "CONTAINS", "=", "!=", ">=", "<=", ">", "<")


def _tokenize(text: str) -> List[str]:
    tokens: List[str] = []
    i = 0
    while i < len(text):
        c = text[i]
        if c.isspace():
            i += 1
            continue
        if c in "()":
            tokens.append(c)
            i += 1
            continue
        if c == '"':
            j = i + 1
            buf = []
            while j < len(text) and text[j] != '"':
                if text[j] == "\\" and j + 1 < len(text):
                    buf.append(text[j + 1])
                    j += 2
                else:
                    buf.append(text[j])
                    j += 1
            if j >= len(text):
                raise QueryError(f"unterminated quoted string at {i}")
            tokens.append('"S:' + "".join(buf) + '"')
            i = j + 1
            continue
        two = text[i : i + 2]
        if two in ("!=", ">=", "<="):
            tokens.append(two)
            i += 2
            continue
        if c in ("=", ">", "<"):
            tokens.append(c)
            i += 1
            continue
        m = re.match(r"^[A-Za-z0-9._-]+", text[i:])
        if not m:
            raise QueryError(f"unexpected character {c!r} at {i}")
        tokens.append(m.group(0))
        i += len(m.group(0))
    return tokens


# --- parser ------------------------------------------------------------

class _Parser:
    def __init__(self, tokens: List[str]):
        self.toks = tokens
        self.pos = 0

    def peek(self) -> Optional[str]:
        return self.toks[self.pos] if self.pos < len(self.toks) else None

    def next(self) -> str:
        t = self.toks[self.pos]
        self.pos += 1
        return t

    def parse(self):
        node = self._or()
        if self.pos != len(self.toks):
            raise QueryError(f"unexpected trailing token {self.toks[self.pos]!r}")
        return node

    def _or(self):
        parts = [self._and()]
        while self.peek() == "OR":
            self.next()
            parts.append(self._and())
        return parts[0] if len(parts) == 1 else Or(parts)

    def _and(self):
        parts = [self._not()]
        while self.peek() == "AND":
            self.next()
            parts.append(self._not())
        return parts[0] if len(parts) == 1 else And(parts)

    def _not(self):
        if self.peek() == "NOT":
            self.next()
            return Not(self._not())
        return self._primary()

    def _primary(self):
        t = self.next()
        if t == "(":
            node = self._or()
            if self.next() != ")":
                raise QueryError("expected ')'")
            return node
        if not ATTR_RE.match(t):
            raise QueryError(f"expected an attribute name, got {t!r}")
        op = self.peek()
        if op == "CONTAINS":
            self.next()
            return Contains(t, self._value())
        if op in ("=", "!=", ">", "<", ">=", "<="):
            self.next()
            if op in TEXT_OPS:
                return self._text_cmp(t, op)
            return Cmp(t, op, self._value())
        raise QueryError(f"expected a comparison operator after {t!r}, got {op!r}")

    def _text_cmp(self, attr: str, op: str):
        val = self._value()
        if isinstance(val, str):
            raise QueryError(
                f"refusing {op!r} on text: attribute {attr!r} is a string "
                f"(quoted or bare token) and {op!r} is a numeric or scalar comparison"
            )
        return Cmp(attr, op, val)

    def _value(self) -> Any:
        t = self.next()
        if t.startswith('"S:'):
            return t[3:-1]
        if t in ("AND", "OR", "NOT", "CONTAINS", "(", ")"):
            raise QueryError(f"expected a value, got keyword {t!r}")
        if re.fullmatch(r"-?\d+", t):
            return int(t)
        if re.fullmatch(r"-?\d+\.\d+", t):
            return float(t)
        if t == "true":
            return True
        if t == "false":
            return False
        if t == "null":
            return None
        return t


def parse_query(text: str):
    """Parse a query string into an AST. Raises QueryError."""
    tokens = _tokenize(text)
    if not tokens:
        raise QueryError("empty query")
    return _Parser(tokens).parse()


# --- evaluation --------------------------------------------------------

def _resolve_attr(node_value: Any, attr: str) -> Any:
    """Resolve special attributes from the reference before falling back to
    stored attributes."""
    if attr == "urn":
        return node_value.get("urn")
    if attr == "name":
        urn = node_value.get("urn")
        return urn.split("/")[-1] if urn else None
    if attr == "version":
        return node_value.get("protocol_version")
    if attr == "zone":
        urn = node_value.get("urn")
        return urn.split("/")[0].split(":", 1)[-1] if urn else None
    if attr in node_value:
        return node_value.get(attr)
    return node_value.get("attributes", {}).get(attr)


def _eval(node: Any, value: Any) -> bool:
    if isinstance(node, And):
        return all(_eval(p, value) for p in node.parts)
    if isinstance(node, Or):
        return any(_eval(p, value) for p in node.parts)
    if isinstance(node, Not):
        return not _eval(node.part, value)
    if isinstance(node, Contains):
        hay = _resolve_attr(value, node.attr)
        if isinstance(hay, list):
            return node.value in hay
        if isinstance(hay, str):
            return str(node.value) in hay
        return False
    if isinstance(node, Cmp):
        got = _resolve_attr(value, node.attr)
        if node.op == "=":
            return got == node.value
        if node.op == "!=":
            return got != node.value
        if got is None or node.value is None:
            return False
        if node.op == "<":
            return got < node.value
        if node.op == ">":
            return got > node.value
        if node.op == "<=":
            return got <= node.value
        if node.op == ">=":
            return got >= node.value
    raise QueryError(f"unknown AST node {node!r}")
