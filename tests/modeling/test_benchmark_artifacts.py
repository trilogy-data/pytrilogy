import tomllib

from pytest import raises

from tests.modeling._benchmark_artifacts import (
    REBASELINE_ENV,
    SIZE_REGRESSION_FLOOR,
    check_query_size,
    cte_count,
    size_budget,
    write_query_log,
)


def _write(root, label: str, gen_length: int, query: str = "select 1") -> None:
    write_query_log(
        root, label, query, gen_length=gen_length, preql_size=1, comp_size=1
    )


def _with_ctes(n: int) -> str:
    ctes = ",\n".join(f"c{i} as (\nselect {i}\n)" for i in range(n))
    return f"WITH\n{ctes}\nselect 1"


def _logged(root, label: str) -> dict:
    return tomllib.loads((root / f"zquery{label}.log").read_text(encoding="utf-8"))


def test_size_budget_leaves_room_for_name_churn():
    assert size_budget(6627) == 6627 + 132
    assert size_budget(100) == 100 + SIZE_REGRESSION_FLOOR


def test_first_write_has_no_baseline(tmp_path):
    _write(tmp_path, "01", 5000)
    assert _logged(tmp_path, "01")["gen_length"] == 5000


def test_growth_within_budget_rebaselines(tmp_path):
    _write(tmp_path, "01", 5000)
    _write(tmp_path, "01", 5100)
    assert _logged(tmp_path, "01")["gen_length"] == 5100


def test_shrinking_rebaselines(tmp_path):
    _write(tmp_path, "01", 5000)
    _write(tmp_path, "01", 4000)
    assert _logged(tmp_path, "01")["gen_length"] == 4000


def test_growth_over_budget_fails_and_keeps_the_baseline(tmp_path):
    _write(tmp_path, "17", 6627)
    with raises(AssertionError, match="6627 -> 7107"):
        _write(tmp_path, "17", 7107)
    # Rewriting would leave the next run passing against the inflated size.
    assert _logged(tmp_path, "17")["gen_length"] == 6627


def test_rebaseline_env_accepts_the_growth(tmp_path, monkeypatch):
    _write(tmp_path, "17", 6627)
    monkeypatch.setenv(REBASELINE_ENV, "1")
    _write(tmp_path, "17", 7107)
    assert _logged(tmp_path, "17")["gen_length"] == 7107


def test_check_query_size_ignores_an_unreadable_baseline(tmp_path):
    (tmp_path / "zquery01.log").write_text("not toml {", encoding="utf-8")
    check_query_size(tmp_path, "01", "select 1", 99999)


def test_cte_count_ignores_inner_text():
    assert cte_count(_with_ctes(3)) == 3
    assert cte_count("select a as (b) from t") == 0


def test_an_added_cte_fails_inside_the_length_budget(tmp_path):
    _write(tmp_path, "64", 5000, _with_ctes(10))
    with raises(AssertionError, match="10 -> 11 CTEs"):
        _write(tmp_path, "64", 5000, _with_ctes(11))
    assert _logged(tmp_path, "64")["generated_sql"] == _with_ctes(10)


def test_a_dropped_cte_rebaselines(tmp_path):
    _write(tmp_path, "64", 5000, _with_ctes(11))
    _write(tmp_path, "64", 5000, _with_ctes(10))
    assert _logged(tmp_path, "64")["generated_sql"] == _with_ctes(10)
