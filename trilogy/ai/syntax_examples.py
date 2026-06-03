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

# NULLABLE FLAG: a flag that is true only when a match exists and NULL otherwise
# (e.g. `enroll.completed` set only for graded rows) needs `is null` for the
# "never matched" case — `not flag` evaluates NULL→dropped, losing those rows.
where enroll.completed is null
select enroll.student_id;
""",
    ),
    SyntaxExample(
        name="set-intersect-difference",
        title="Set intersection / difference on a multi-column key (presence per group)",
        summary=(
            "count tuples present in / absent from other groups via per-group "
            "presence flags — the multi-column INTERSECT/EXCEPT idiom (NOT concat + `in`)"
        ),
        body="""\
# To intersect or difference rows on a MULTI-COLUMN key (the SQL INTERSECT /
# EXCEPT of several columns), group by the full key tuple and build a presence
# flag per set with `sum(case when <set> then 1 else 0 end)`. Grouping keeps
# NULL key parts as their own group (correct INTERSECT/EXCEPT semantics).
# Do NOT concat the columns into one string and use `in`/`not in` — concat drops
# NULLs and gives the wrong count.
import enrollments as enroll;

# Presence of each (student_id, course) tuple among completed vs open enrollments.
auto in_completed <- sum(case when enroll.completed = true then 1 else 0 end)
    by enroll.student_id, enroll.course;
auto in_open <- sum(case when enroll.completed = false then 1 else 0 end)
    by enroll.student_id, enroll.course;

select
    # intersection: tuples appearing in BOTH sets
    sum(case when in_completed > 0 and in_open > 0 then 1 else 0 end) as in_both,
    # difference: tuples completed but never open (completed EXCEPT open)
    sum(case when in_completed > 0 and in_open = 0 then 1 else 0 end) as completed_only,
;
""",
    ),
    SyntaxExample(
        name="dedup-before-aggregate",
        title="Deduplicate identical rows before aggregating (UNION DISTINCT)",
        summary=(
            "a `rowset <- select <cols>` collapses identical tuples to one row "
            "(SELECT-grain group by) before a downstream sum — the `distinct`/UNION substitute"
        ),
        body="""\
# `distinct` and UNION are not available. To collapse identical tuples to one
# row BEFORE aggregating (so duplicates don't double-count), materialize the
# tuple in a `rowset <- select <cols>`: a rowset select groups at its own grain,
# emitting one row per distinct tuple. Then aggregate over the rowset's columns.
import enrollments as enroll;

# Collapse repeat (student, course) enrollments to one row each (a student who
# took a course in multiple years counts once).
rowset unique_sc <- select
    enroll.student_id as sid,
    enroll.course as course,
;

# Count over the deduplicated rows — identical (student, course) pairs counted once.
select
    unique_sc.course,
    count(unique_sc.sid) as distinct_students,
order by unique_sc.course asc;
""",
    ),
    SyntaxExample(
        name="rank-over-rollup",
        title="Rank within each rollup level (one window over the rollup)",
        summary=(
            "rank rollup subtotals/leaves with a SINGLE `rank(a,b) over (partition by "
            "level, parent ...)` — not separate ranks per level"
        ),
        body="""\
# When ranking the rows of a ROLLUP within each hierarchy level, use ONE
# multi-key window over the rollup output. `grouping()` gives the level; the
# window partitions by (level, parent) so leaves rank within their parent and
# subtotals rank among themselves — all in one pass. Do NOT write separate rank
# concepts per level (they mis-rank across the wrong row set).
import enrollments as enroll;

auto total <- count(enroll.id) by rollup enroll.course, enroll.year;
auto g_course <- grouping(enroll.course) by rollup enroll.course, enroll.year;
auto g_year <- grouping(enroll.year) by rollup enroll.course, enroll.year;
auto level <- g_course + g_year;   # 0 = leaf, 1 = course subtotal, 2 = grand total
auto parent <- case when g_year = 0 then enroll.course else null end;
auto rnk <- rank(enroll.course, enroll.year)
    over (partition by level, parent order by total desc);

select
    enroll.course,
    enroll.year,
    total,
    level,
    --parent,
    rnk,
order by level desc nulls first, parent asc nulls first, rnk asc nulls first
limit 100;
""",
    ),
    SyntaxExample(
        name="staged-membership",
        title="Stage a coarse-grain membership set, then filter a fine-grain query",
        summary=(
            "compute a membership set in a `rowset` (keys meeting a count/HAVING), "
            "then filter the main query with `<key> in <rowset>.<col>`"
        ),
        body="""\
# When a query filters on a condition computed at a COARSER grain (e.g. "items
# sold in all 3 channels", "courses offered in more than one year"), compute that
# membership set in its own `rowset` and filter the fine-grain query with `in`.
# Staging keeps the coarse aggregate from entangling with the fine-grain
# aggregates (which otherwise re-aggregate / over-count).
import enrollments as enroll;

# Coarse stage: courses offered in more than one year.
rowset multi_year <- select
    enroll.course as course,
    --count_distinct(enroll.year) as year_count,
having year_count > 1
;

# Fine stage: enrollments per course, only for those courses.
where enroll.course in multi_year.course
select
    enroll.course,
    count(enroll.id) as enrollments,
order by enrollments desc
limit 100;
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
