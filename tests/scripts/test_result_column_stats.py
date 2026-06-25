import json
from decimal import Decimal

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_execution import _column_stats, _emit_results_json
from trilogy.scripts.display_models import ResultSet


def test_column_stats_counts_nulls_distinct_and_range():
    rows = [
        ("a", 10, None),
        ("a", None, None),
        ("b", 30, None),
    ]
    stats = {s["column"]: s for s in _column_stats(["k", "v", "empty"], rows)}
    assert stats["k"] == {
        "column": "k",
        "non_null": 3,
        "nulls": 0,
        "distinct": 2,
        "min": "a",
        "max": "b",
    }
    assert stats["v"]["non_null"] == 2 and stats["v"]["nulls"] == 1
    assert (
        stats["v"]["distinct"] == 2
        and stats["v"]["min"] == 10
        and stats["v"]["max"] == 30
    )
    # all-null column: counts present, no range emitted
    assert stats["empty"]["non_null"] == 0 and stats["empty"]["nulls"] == 3
    assert "min" not in stats["empty"]


def test_column_stats_skips_range_on_mixed_types():
    stats = _column_stats(["mixed"], [("x",), (1,)])
    assert stats[0]["non_null"] == 2
    assert "min" not in stats[0]  # str vs int unorderable → range skipped, no crash


def _emit_capture(results, cap, capsys, query_limit=None):
    _core.set_output_format("json")
    try:
        _emit_results_json(results, cap, query_limit)
    finally:
        _core.set_output_format("rich")
    return json.loads(capsys.readouterr().out)


def test_emit_includes_column_stats_only_when_truncated(capsys):
    cols = ["page", "sales"]
    rows = [(f"p{i}", Decimal("100") if i == 0 else None) for i in range(10)]
    payload = _emit_capture(ResultSet(rows=rows, columns=cols), cap=4, capsys=capsys)
    assert payload["truncated"] and payload["omitted"] == 6
    by_col = {s["column"]: s for s in payload["column_stats"]}
    # sparse measure surfaced even though the shown head/tail is mostly null
    assert by_col["sales"]["non_null"] == 1 and by_col["sales"]["nulls"] == 9
    assert by_col["page"]["distinct"] == 10


def test_emit_omits_column_stats_when_full_set_shown(capsys):
    rows = [("p0", 1), ("p1", 2)]
    payload = _emit_capture(
        ResultSet(rows=rows, columns=["page", "sales"]), cap=50, capsys=capsys
    )
    assert "truncated" not in payload
    assert "column_stats" not in payload


def test_emit_flags_limit_bounded_result_and_scopes_stats(capsys):
    # q14 shape: result hit its own LIMIT, sorted so stats describe a prefix.
    rows = [("CATALOG", i) for i in range(100)]
    payload = _emit_capture(
        ResultSet(rows=rows, columns=["channel", "n"]),
        cap=10,
        capsys=capsys,
        query_limit=100,
    )
    assert payload["limit_bounded"] == 100
    assert "LIMIT" in payload["column_stats_note"]
    ch = next(s for s in payload["column_stats"] if s["column"] == "channel")
    assert ch["distinct"] == 1  # the misleading-looking stat, now scoped by the note


def test_emit_no_limit_flag_when_result_under_limit(capsys):
    rows = [("CATALOG", i) for i in range(30)]
    payload = _emit_capture(
        ResultSet(rows=rows, columns=["channel", "n"]),
        cap=10,
        capsys=capsys,
        query_limit=100,
    )
    assert "limit_bounded" not in payload  # 30 < 100, not a bounded prefix
    assert "column_stats_note" not in payload
