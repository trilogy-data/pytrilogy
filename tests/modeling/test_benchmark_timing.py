from tests.modeling._benchmark_timing import benchmark_query, repeat_count_for_env


def test_benchmark_query_repeats_each_short_stage() -> None:
    calls = {"generate": 0, "candidate": 0, "reference": 0}

    def generate() -> str:
        calls["generate"] += 1
        return "select 1"

    def candidate(query: str) -> list[int]:
        assert query == "select 1"
        calls["candidate"] += 1
        return [1]

    def reference() -> list[int]:
        calls["reference"] += 1
        return [1]

    result = benchmark_query(
        generate=generate,
        execute_candidate=candidate,
        execute_reference=reference,
        repeat_time_cutoff=1.0,
        repeat_count=3,
    )

    assert calls == {"generate": 4, "candidate": 4, "reference": 4}
    assert result.query == "select 1"
    assert result.candidate_result == result.reference_result == [1]


def test_repeat_count_is_zero_on_ci(monkeypatch) -> None:
    """CI throws the timings away, so the repeat runs are pure cost there."""
    monkeypatch.setenv("CI", "true")
    assert repeat_count_for_env(3) == 0
    monkeypatch.delenv("CI")
    assert repeat_count_for_env(3) == 3
