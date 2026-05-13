from pathlib import Path

import pandas as pd


def _numeric_frame(frame: pd.DataFrame) -> pd.DataFrame:
    typed = frame.copy()
    for column in [
        "preql_size",
        "comp_size",
        "exec_time",
        "comp_time",
    ]:
        typed[column] = pd.to_numeric(typed[column], errors="coerce")
    return typed.dropna(subset=["preql_size", "comp_size", "exec_time", "comp_time"])


def _stats(series: pd.Series) -> tuple[float, float, float]:
    clean = series.dropna()
    return (
        float(clean.mean()),
        float(clean.quantile(0.5)),
        float(clean.quantile(0.9)),
    )


def _fmt_chars(value: float) -> str:
    return f"{value:+,.0f}"


def _fmt_seconds(value: float) -> str:
    return f"{value:+,.3f}s"


def _fmt_percent(value: float) -> str:
    return f"{value:+.1f}%"


def _fmt_ratio(value: float) -> str:
    return f"{value:.2f}x"


def _row(label: str, values: tuple[float, float, float], formatter) -> str:
    avg, p50, p90 = values
    return f"| {label} | {formatter(avg)} | {formatter(p50)} | {formatter(p90)} |"


def _section(title: str, frame: pd.DataFrame) -> list[str]:
    data = _numeric_frame(frame)
    if data.empty:
        return [f"## {title}", "", "No timed query results found.", ""]

    comp_size = data["comp_size"].replace({0: pd.NA})
    comp_time = data["comp_time"].replace({0: pd.NA})
    length_delta = data["preql_size"] - data["comp_size"]
    length_pct_delta = (length_delta / comp_size) * 100
    exec_delta = data["exec_time"] - data["comp_time"]
    exec_pct_delta = (exec_delta / comp_time) * 100
    exec_ratio = data["exec_time"] / comp_time

    length_wins = int((length_delta < 0).sum())
    exec_wins = int((exec_delta < 0).sum())
    total = len(data)

    preql_total = int(data["preql_size"].sum())
    reference_total = int(data["comp_size"].sum())
    trilogy_total = float(data["exec_time"].sum())
    reference_exec_total = float(data["comp_time"].sum())

    return [
        f"## {title}",
        "",
        f"Queries: {total}",
        "",
        f"PreQL is shorter than the reference SQL for {length_wins}/{total} queries. Total PreQL length is {preql_total:,} chars vs {reference_total:,} reference SQL chars.",
        "",
        "| Length metric | Avg | P50 | P90 |",
        "| --- | ---: | ---: | ---: |",
        _row("PreQL - Reference SQL chars", _stats(length_delta), _fmt_chars),
        _row("PreQL vs Reference SQL", _stats(length_pct_delta), _fmt_percent),
        "",
        f"Trilogy execution is faster than the reference SQL for {exec_wins}/{total} queries. Total Trilogy execution time is {trilogy_total:.3f}s vs {reference_exec_total:.3f}s reference SQL time.",
        "",
        "| Performance metric | Avg | P50 | P90 |",
        "| --- | ---: | ---: | ---: |",
        _row("Trilogy - Reference SQL seconds", _stats(exec_delta), _fmt_seconds),
        _row("Trilogy vs Reference SQL", _stats(exec_pct_delta), _fmt_percent),
        _row("Trilogy / Reference SQL", _stats(exec_ratio), _fmt_ratio),
        "",
    ]


def build_summary(
    main_frame: pd.DataFrame, alt_frame: pd.DataFrame, fingerprint: str
) -> str:
    lines = [
        "# TPC-DS Result Summary",
        "",
        f"Timing fingerprint: `{fingerprint}`",
        "",
        "Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.",
        "",
    ]
    lines.extend(_section("Suggested Queries", main_frame))
    lines.extend(_section("Alternative Queries", alt_frame))
    return "\n".join(lines).rstrip() + "\n"


def write_summary(
    main_frame: pd.DataFrame,
    alt_frame: pd.DataFrame,
    out_path: Path,
    fingerprint: str,
) -> None:
    out_path.write_text(
        build_summary(main_frame, alt_frame, fingerprint),
        encoding="utf-8",
    )


if __name__ == "__main__":
    from tests.modeling.tpc_ds_duckdb.analyze_test_results import analyze

    analyze()
