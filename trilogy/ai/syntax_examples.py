"""Generic, copy-pasteable Trilogy syntax examples for less-common patterns.

Only the one-line headers are embedded in the always-loaded syntax reference
(cheap); each FULL example is fetched on demand via
``trilogy agent-info syntax example <name>`` so the prompt stays small.

Examples use small, well-known generic datasets — the **iris** flower
measurements and a textbook **university enrollments** schema (students,
courses, enrollments). They teach the SYNTAX of a pattern, not any particular
schema — adapt the concept names to whatever model ``trilogy explore`` shows you.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SyntaxExample:
    name: str
    title: str
    summary: str  # one-liner shown in the header list
    body: str


_EXAMPLES: list[SyntaxExample] = [
    SyntaxExample(
        name="filtered-aggregate",
        title="Filtered aggregate — aggregate only the rows matching a condition",
        summary=(
            "`sum(x ? cond)` / `count(x ? cond)` aggregate just the matching rows; "
            "add `by <grain>` to pin the aggregate's grain"
        ),
        body="""\
# `?` is a per-aggregate filter (a WHERE that applies to ONE aggregate), so
# several differently-filtered measures can share one select. Add `by <grain>`
# to pin an aggregate to a grain other than the select's.
import iris as iris;

select
    iris.species,
    avg(iris.petal_length) as avg_petal,
    avg(iris.petal_length ? iris.petal_width > 1.5) as avg_petal_wide_only,
    count(iris.petal_length ? iris.sepal_length > 6.0) as long_sepal_count,
order by iris.species asc;
""",
    ),
    SyntaxExample(
        name="pivot-columns",
        title="Pivot dimension values into columns",
        summary=(
            "turn distinct values of a dimension into columns with a reusable "
            "`def` macro + filtered aggregate"
        ),
        body="""\
# A `def` macro expands inline at each `@call`. Pair it with a filtered
# aggregate to project one column per category value (a pivot). With no row
# dimension in the select this yields a single summary row.
import iris as iris;

def species_avg(s) -> avg(iris.petal_length ? iris.species = s);

select
    @species_avg('setosa') as setosa_avg_petal,
    @species_avg('versicolor') as versicolor_avg_petal,
    @species_avg('virginica') as virginica_avg_petal,
;
""",
    ),
    SyntaxExample(
        name="case-when",
        title="Conditional values with case (relabel codes / bucket a measure)",
        summary=(
            "`case when <cond> then <val> ... else <val> end` to relabel codes or "
            "bucket a measure into a derived value, then group by it"
        ),
        body="""\
# `case when <cond> then <val> ... else <val> end` builds a derived value.
# Assign it to an `auto` concept, then group/filter by it like any other.
# Use `else null` when unmatched rows should drop out of downstream grouping.
import iris as iris;

auto petal_size <- case
    when iris.petal_length < 2.0 then 'small'
    when iris.petal_length < 5.0 then 'medium'
    else 'large'
end;

select
    petal_size,
    count(iris.petal_length) as flower_count,
    avg(iris.petal_width) as avg_petal_width,
order by flower_count desc;
""",
    ),
    SyntaxExample(
        name="window-period-over-period",
        title="Period-over-period ratio with a window function",
        summary=(
            "`lead`/`lag` over (order by ...) to compare a row to an offset period; "
            "build a scoped set with `<col> ? <cond>` and filter with `in`"
        ),
        body="""\
# `<col> ? <cond>` builds a filtered set of values you can match with `in`.
# lead()/lag() over (order by ...) compares each row to an offset row — here
# each year's enrollment count divided by the PRIOR year's.
import enrollments as enroll;

auto recent_years <- enroll.year ? enroll.year >= 2015;

def vs_prior_year(amt) -> round(amt / (lag(amt, 1) over (order by enroll.year asc)), 2);

where enroll.year in recent_years
select
    enroll.year,
    @vs_prior_year(count(enroll.id)) as enrollment_growth,
having
    enrollment_growth is not null
order by enroll.year asc nulls first;
""",
    ),
    SyntaxExample(
        name="aligned-multi-select",
        title="Combine two aggregations over shared dimensions (multi-select)",
        summary=(
            "`merge … align … derive` joins two `select` blocks on shared dims; "
            "`by rollup` adds subtotals; `--col` keeps a column out of the output"
        ),
        body="""\
# Two independent aggregations (here total enrollments vs completed
# enrollments) over the same model, aligned on a shared dimension, then
# combined in `derive`.
#  - prefix a projection with `--` to keep it for alignment but OUT of the
#    printed rows (the printed columns come from `align` + `derive`).
#  - `by rollup <dims>` adds subtotal + grand-total rows (the dim is NULL there).
#  - `align <name>: <colA>, <colB>` ties one column from each block together;
#    chain more with `and <name2>: ...`.
import enrollments as enroll;

where enroll.year = 2020
select
    --enroll.course as a_course,
    --count(enroll.id) by rollup enroll.course as enrolled_a,
merge
where enroll.completed = true
select
    --enroll.course as b_course,
    --count(enroll.id) by rollup enroll.course as completed_b,
align
    course: a_course, b_course
derive
    coalesce(enrolled_a, 0) -> enrolled,
    coalesce(completed_b, 0) -> completed,
    round(coalesce(completed_b, 0) * 1.0 / coalesce(enrolled_a, 0), 2) -> completion_rate
order by course asc nulls first
limit 100;
""",
    ),
    SyntaxExample(
        name="existence-anti-join",
        title="Existence / anti-join across models",
        summary=(
            "keep or exclude rows by whether a key matches another model: "
            "`key in other.key` (semi-join) / `not in` (anti-join)"
        ),
        body="""\
# `in` / `not in` against another model's key is the semi-join / anti-join
# idiom — no manual JOIN. Use it instead of `count(...) is null` hacks.
import students as students;
import enrollments as enroll;

# semi-join: majors with at least one student enrolled in 'Biology 101'
auto bio_students <- enroll.student_id ? enroll.course = 'Biology 101';
where students.id in bio_students
select students.major, count(students.id) as enrolled_students;

# anti-join: students who never enrolled in ANY course
where students.id not in enroll.student_id
select students.major, count(students.id) as never_enrolled;
""",
    ),
]

SYNTAX_EXAMPLES: dict[str, SyntaxExample] = {e.name: e for e in _EXAMPLES}


def example_headers() -> str:
    """Compact `- name — summary` list embedded in the syntax reference."""
    return "\n".join(f"- `{e.name}` — {e.summary}" for e in _EXAMPLES)


def example_index() -> str:
    """Standalone listing for the CLI (headers + how to fetch a full example)."""
    return (
        "Available Trilogy syntax examples — print one with "
        "`trilogy agent-info syntax example <name>`:\n\n" + example_headers()
    )


def render_example(name: str) -> str | None:
    """Full example body for ``name``, or None if there is no such example."""
    example = SYNTAX_EXAMPLES.get(name)
    if example is None:
        return None
    return f"# {example.title}\n\n{example.body.strip()}\n"


def available_names() -> list[str]:
    return [e.name for e in _EXAMPLES]
