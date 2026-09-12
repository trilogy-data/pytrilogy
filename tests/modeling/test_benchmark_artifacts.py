import tomllib
from pytest import raises

from tests.modeling._benchmark_artifacts import (
    REBASELINE_ENV,
    SIZE_REGRESSION_FLOOR,
    check_query_size,
    size_budget,
    write_query_log,
)


def _write(root, label: str, gen_length: int) -> None:
    write_query_log(
        root, label, "select 1", gen_length=gen_length, preql_size=1, comp_size=1
    )


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
    check_query_size(tmp_path, "01", 99999)
