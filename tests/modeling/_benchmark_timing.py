from collections.abc import Callable
from dataclasses import dataclass
from os import environ
from time import perf_counter
from typing import Generic, TypeVar

ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class BenchmarkResult(Generic[ResultT]):
    parse_time: float
    query: str
    candidate_time: float
    candidate_result: ResultT
    reference_time: float
    reference_result: ResultT


def repeat_count_for_env(default: int) -> int:
    """Zero on CI, `default` elsewhere.

    The repeats only sharpen the numbers the charts are drawn from, and every
    benchmark conftest runs `analyze()` off CI only -- so on CI they are extra
    generations and executions per query, thrown away. Decided here rather than
    inside `benchmark_query`, which must honour the count it is handed.
    """
    return 0 if environ.get("CI") else default


def time_call(function: Callable[[], ResultT]) -> tuple[float, ResultT]:
    start = perf_counter()
    value = function()
    return perf_counter() - start, value


def benchmark_query(
    generate: Callable[[], str],
    execute_candidate: Callable[[str], ResultT],
    execute_reference: Callable[[], ResultT],
    repeat_time_cutoff: float,
    repeat_count: int,
) -> BenchmarkResult[ResultT]:
    parse_time, query = time_call(generate)
    candidate_time, candidate_result = time_call(lambda: execute_candidate(query))
    reference_time, reference_result = time_call(execute_reference)

    if min(parse_time, candidate_time, reference_time) < repeat_time_cutoff:
        for _ in range(repeat_count):
            if parse_time < repeat_time_cutoff:
                parse_time = min(parse_time, time_call(generate)[0])
            if candidate_time < repeat_time_cutoff:
                candidate_time = min(
                    candidate_time, time_call(lambda: execute_candidate(query))[0]
                )
            if reference_time < repeat_time_cutoff:
                reference_time = min(reference_time, time_call(execute_reference)[0])

    return BenchmarkResult(
        parse_time=parse_time,
        query=query,
        candidate_time=candidate_time,
        candidate_result=candidate_result,
        reference_time=reference_time,
        reference_result=reference_result,
    )
