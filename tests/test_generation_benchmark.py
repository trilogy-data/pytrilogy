import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / ".scripts"))
import generation_benchmark as benchmark


def _run(**seconds: float) -> dict[str, dict]:
    return {
        label: {"seconds": value, "sql": "same"} for label, value in seconds.items()
    }


def test_a_query_is_slower_past_both_the_ratio_and_the_floor():
    assert benchmark.slower(0.2, 0.6)
    # doubled, but by less than a shared runner's own jitter
    assert not benchmark.slower(0.02, 0.06)
    # well past the floor, inside the ratio
    assert not benchmark.slower(1.0, 1.5)


def test_compare_passes_a_run_that_held(capsys):
    assert benchmark.compare(_run(a=0.5, b=0.1), _run(a=0.52, b=0.09)) == 0
    assert "2 queries" in capsys.readouterr().out


def test_compare_names_the_query_that_grew(capsys):
    assert benchmark.compare(_run(a=0.2, b=0.1), _run(a=6.6, b=0.1)) == 1
    assert "SLOWER a: 0.200s -> 6.600s" in capsys.readouterr().out


def test_compare_fails_a_run_slower_across_the_board(capsys):
    base = _run(**{f"q{i}": 0.1 for i in range(10)})
    head = _run(**{f"q{i}": 0.15 for i in range(10)})
    assert benchmark.compare(base, head) == 1
    assert "SLOWER total" in capsys.readouterr().out


def test_compare_reads_only_shared_queries(capsys):
    assert benchmark.compare(_run(a=0.1, gone=9.0), _run(a=0.1, new=9.0)) == 0
    assert benchmark.compare(_run(a=0.1), _run(b=0.1)) == 2


def test_compare_counts_changed_sql(capsys):
    head = _run(a=0.1)
    head["a"]["sql"] = "other"
    assert benchmark.compare(_run(a=0.1), head) == 0
    assert "1 with different SQL" in capsys.readouterr().out


def test_merged_runs_keep_each_querys_faster_time():
    merged = benchmark.merged(_run(a=0.5, b=0.1), _run(a=0.3, b=0.2, c=1.0))
    assert {k: v["seconds"] for k, v in merged.items()} == {
        "a": 0.3,
        "b": 0.1,
        "c": 1.0,
    }
