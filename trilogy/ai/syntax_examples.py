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
            "to COUNT ROWS count the unique grain/row key, not a non-unique sub-key; "
            "`by <grain>` pins the aggregate's grain"
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

# COUNTING ROWS: `count(k ? cond)` counts DISTINCT values of k — `count` is
# distinct-by-argument. To count matching ROWS, count the model's unique grain /
# row key (often named `row_key`), NOT a non-unique sub-key. e.g. counting line
# items by `count(line_number ? cond)` collapses to the number of distinct line
# numbers (1..7) and undercounts — use `count(row_key ? cond)`.
""",
    ),
    SyntaxExample(
        name="row-level-where-vs-having",
        title=(
            "Row-level filter (WHERE) vs aggregate filter (HAVING) — don't wrap a "
            "per-row threshold in max()/min()"
        ),
        summary=(
            "a plain per-ROW condition (`x.col > N`) goes in WHERE (filters the rows "
            "feeding every aggregate); HAVING filters an aggregate RESULT. "
            "`max(col) by g > N` in HAVING filters GROUPS, not rows — different meaning"
        ),
        body="""\
# A non-aggregate, per-ROW condition belongs in WHERE — it filters the rows that
# feed every aggregate, BEFORE grouping. HAVING is ONLY for filtering on an
# aggregate RESULT. These are different operations; do not substitute one for the
# other.
import iris as iris;

# RIGHT — keep only large-sepal FLOWERS, then average their petals.
# `sepal_length > 6.0` is a per-row test on a column -> WHERE.
where iris.sepal_length > 6.0
select
    iris.species,
    avg(iris.petal_length) as avg_petal,
order by iris.species asc;

# WRONG — `having max(iris.sepal_length) by iris.species > 6.0` does NOT keep
# large-sepal rows. It keeps whole SPECIES whose single largest flower exceeds
# 6.0, then averages ALL of that species' flowers (small ones included). Wrapping
# a per-row threshold in max()/min() in HAVING silently changes the meaning, and
# AND-ing it with another filter (e.g. a rank cutoff) often yields 0 rows because
# the two now select disjoint populations.
#
# Pick by what the condition is ABOUT:
#   per-row value of a column   -> WHERE     `where x.amount > 10000`
#   an aggregate's result       -> HAVING    `having total_amount > 10000`
#                                             (select it, hide with `--`)
#   aggregate just SOME rows    -> inline `?` `sum(x.amount ? x.amount > 10000)`
""",
    ),
    SyntaxExample(
        name="nested-aggregate-group-average",
        title="Compare a per-entity total to the group average of those totals",
        summary=(
            'the "above 1.2x the group norm" ask: inner `sum(x) by entity, group`, '
            "outer `avg(inner) by group`, compared in HAVING with both metrics hidden via `--`"
        ),
        body="""\
# Compare each entity's total to the GROUP AVERAGE of those per-entity totals
# (the "above 1.2x the group norm" pattern). Give EACH grain its own `by`: the
# inner total is per (entity, group); the outer average re-aggregates those
# totals up to the group. Select both derived metrics hidden (`--`) so HAVING
# can reference them while the output stays just the entity id.
import enrollments as enroll;

auto student_dept_credits <- sum(enroll.credits) by enroll.student_id, enroll.department;
auto dept_avg_credits <- avg(student_dept_credits) by enroll.department;

select
    enroll.student_id,
    --student_dept_credits,
    --dept_avg_credits,
having student_dept_credits > 1.2 * dept_avg_credits;
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
        title="Combine / pair aggregations with a multi-select (merge … align … derive)",
        summary=(
            "`merge … align … derive` stitches independent `select` blocks on a shared key — "
            "use it to COMBINE two measures, SELF-PAIR one model across two periods/sets, or "
            "STACK channels; `by rollup` adds subtotals. Read the GOTCHAS — agents misuse this a lot"
        ),
        body="""\
# A multi-select runs several independent `select` blocks (arms) and stitches
# them on a shared key. It is the tool for THREE shapes agents reach for:
#   (1) COMBINE different measures over one population (total vs completed below);
#   (2) SELF-PAIR one model against itself across two periods/sets — give each arm
#       a different `where` over the SAME model and align on the (possibly shifted)
#       key. This is how you compare "this period vs another period" of one fact;
#   (3) STACK channels / sources — one arm per source, aligned on the common key.
#  - prefix a projection with `--` to keep it for alignment but OUT of the printed
#    rows (the printed columns come from `align` + `derive`).
#  - `by rollup <dims>` adds subtotal + grand-total rows (the dim is NULL there).
#  - `align <name>: <colA>, <colB>` ties one column from EACH arm together; chain
#    more alignments with `and <name2>: <colA2>, <colB2>`.
#  - `having <cond>` (after `derive`) filters on the COMBINED/derived outputs —
#    this is the only way to compare one arm to another (e.g. `completed < enrolled`
#    or "rows present in both periods"). There is no top-level `where` on a
#    multi-select (a pre-combination filter lives inside each arm's own `where`).
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
    coalesce(enrolled_a, 0) as enrolled,
    coalesce(completed_b, 0) as completed,
    round(coalesce(completed_b, 0) * 1.0 / coalesce(enrolled_a, 0), 2) as completion_rate
having completed < enrolled
order by course asc nulls first
limit 100;

# ---------------------------------------------------------------------------
# GOTCHAS (each of these is a real, repeated agent failure):
#  - `derive` names each output with `as` or `->` (`coalesce(a,0) as x` ==
#    `coalesce(a,0) -> x`); it takes only FUNCTIONS/CONDITIONALS (coalesce, round,
#    case, arithmetic, +/-/*//). A bare column rename (`derive amt as x`) or a
#    literal (`derive 2001 as y`) errors with "Invalid derive expression … must be
#    a function or conditional". To pass a value through unchanged, make it an
#    `align` key (aligned keys are output directly), or wrap it (`coalesce(amt,amt)`).
#  - `derive` outputs are referenceable in `having` and `order by` (e.g.
#    `having completed < enrolled`, `order by completion_rate desc`).
#  - Every arm must contribute ONE column per `align` name, and the arms' selects
#    should have matching shape. Output columns are the align keys + derive results.
#  - The `align` group name must be DISTINCT from every arm column name (use
#    `align course: a_course, b_course`, NOT `align a_course: a_course, b_course`).
#    Reusing an arm name for the group collapses them and errors at parse.
#  - Each arm has its OWN leading `where` (that is how self-pairing filters each
#    side differently). There is NO top-level `where` after `derive` — to filter
#    the combined result use `having`. A standalone `where …;` with no following
#    `select` is a parse error.
#  - To pair on a SHIFTED key (e.g. match a period to the next one), project the
#    shifted value as a hidden `--<expr> as key_b` column in one arm and align it
#    to the other arm's plain key. Do NOT rely on `lead/lag(N)` for this when the
#    ordering key is sparse/non-contiguous — an N-row offset is not an N-period
#    offset; pair on the explicit key instead.
#  - Window functions (`rank/lead/lag … over (…)`) cannot live in `derive` or in a
#    top-level `where`. Compute a window in an arm's `select` (hidden with `--`) and
#    reference it, or filter it via a hidden select column + `having`.
#  - Don't reference an alias inside its own definition (`x as foo` then reading
#    `foo` in the same expression) — that is a recursive self-reference and is
#    rejected. Name the upstream pieces as distinct `auto`s instead.
#  - Every projection needs an explicit `as` alias (`min(x) as x_min`, not `min(x)`).
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
#  - ALIAS every rowset column you reference downstream with `as`. Without `as`,
#    the column keeps its FULL source path under the rowset name — `select
#    sales.item.brand_id` is reachable as `rs.sales.item.brand_id`, NOT
#    `rs.brand_id`. `as brand_id` makes it `rs.brand_id`.
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
    SyntaxExample(
        name="correlated-exists-via-grouped-counts",
        title="Correlated EXISTS / NOT-EXISTS over rows of the SAME model",
        summary=(
            "translate `EXISTS other` / `NOT EXISTS other matching` over the same "
            "model into two `count(...) by <grain>` compared in `where` "
            "(`> 1` = another exists, `= 1` = no other matches) — don't filter on a boolean-of-aggregate"
        ),
        body="""\
# A self-referential EXISTS / NOT-EXISTS (e.g. "the ONLY enrollee who completed
# a course that had more than one enrollee") becomes TWO filtered counts pinned
# to the correlation grain, compared in `where`:
#   count(k) by <grain> > 1            -- EXISTS another row in the group
#   count(k ? cond) by <grain> = 1     -- NOT EXISTS another row matching cond
#                                         (exactly one matches: the row itself)
# Do NOT build `auto flag <- count(...) > 0` and filter on `flag` — a boolean
# derived from an aggregate can't be used in `where`/another aggregate (the
# planner emits "WHERE clause cannot contain aggregates"). Inline the comparison.
import enrollments as enroll;

auto enrollees_per_course <- count(enroll.student_id) by enroll.course;
auto completers_per_course <- count(enroll.student_id ? enroll.completed = true) by enroll.course;

where
    enroll.completed = true
    and enrollees_per_course > 1     # the course had at least one OTHER enrollee
    and completers_per_course = 1    # and no OTHER enrollee completed it
select
    enroll.student_id,
    count(enroll.course) as sole_completions
order by sole_completions desc
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
