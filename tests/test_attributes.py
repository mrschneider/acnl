import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from acnl_core.attributes import Contains, Cmp, parse_query, _eval, QueryError


def _ref(urn="acnl:lab/node1/worker", caps=None, attrs=None, version="1.0"):
    return {
        "urn": urn,
        "protocol_version": version,
        "capabilities": caps or [],
        "attributes": attrs or {},
    }


def test_simple_equality():
    assert _eval(parse_query("name = worker"), _ref()) is True
    assert _eval(parse_query("name = boss"), _ref()) is False


def test_special_attributes_resolve_from_reference():
    assert _eval(parse_query('urn = "acnl:lab/node1/worker"'), _ref()) is True
    assert _eval(parse_query('version = "1.0"'), _ref()) is True
    assert _eval(parse_query("zone = lab"), _ref()) is True


def test_not_binds_tighter_than_and():
    # NOT a AND b  ==  (NOT a) AND b
    assert _eval(parse_query("NOT name = worker AND name = worker"), _ref()) is False


def test_and_binds_tighter_than_or():
    # a OR b AND c == a OR (b AND c)
    assert _eval(parse_query("name = worker OR name = boss AND zone = other"), _ref()) is True


def test_parentheses():
    assert _eval(parse_query("(name = worker OR name = boss) AND zone = lab"), _ref()) is True
    # without the parens: name = worker OR (name = boss AND zone = other) is
    # True for the worker, because OR short-circuits the second group
    assert _eval(parse_query("name = worker OR (name = boss AND zone = other)"), _ref()) is True
    # the parens force the group: (name = worker OR name = boss) AND zone = other
    # is False for the worker, because zone = lab
    assert _eval(parse_query("(name = worker OR name = boss) AND zone = other"), _ref()) is False


def test_contains_substring_on_text():
    assert _eval(parse_query('urn CONTAINS "node1"'), _ref()) is True
    assert _eval(parse_query('urn CONTAINS "node9"'), _ref()) is False


def test_contains_membership_on_capability_list():
    assert _eval(parse_query('capabilities CONTAINS "math"'), _ref(caps=["math", "nlp"])) is True
    assert _eval(parse_query('capabilities CONTAINS "vision"'), _ref(caps=["math", "nlp"])) is False


def test_gt_on_text_refused_at_parse_time():
    with pytest.raises(QueryError) as e:
        parse_query('name > "worker"')
    assert "refusing" in str(e.value)


def test_gt_on_number_ok():
    assert _eval(parse_query("load > 50"), _ref(attrs={"load": 80})) is True
    assert _eval(parse_query("load > 90"), _ref(attrs={"load": 80})) is False


def test_ne_and_le():
    assert _eval(parse_query("name != worker"), _ref()) is False
    assert _eval(parse_query("load <= 80"), _ref(attrs={"load": 80})) is True
    assert _eval(parse_query("load < 80"), _ref(attrs={"load": 80})) is False


def test_missing_attribute_is_false_for_comparison():
    assert _eval(parse_query("load = 1"), _ref()) is False


def test_quoted_string_with_spaces():
    assert _eval(parse_query('city = "new york"'), _ref(attrs={"city": "new york"})) is True


def test_not_precedence_with_parens():
    assert _eval(parse_query("NOT (name = worker OR name = boss)"), _ref()) is False


def test_trailing_token_refused():
    with pytest.raises(QueryError):
        parse_query("name = worker extra")


def test_empty_query_refused():
    with pytest.raises(QueryError):
        parse_query("")


def test_unterminated_quote_refused():
    with pytest.raises(QueryError):
        parse_query('name = "worker')
