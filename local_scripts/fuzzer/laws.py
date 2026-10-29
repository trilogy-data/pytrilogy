"""Metamorphic cases: each pairs two Trilogy programs a theorem in `formal/`
proves return the same rows, so no hand-written SQL oracle is needed.

The theorem is named in each case's description; `formal/README.md` maps the
other way."""

from __future__ import annotations

import re
from collections.abc import Iterable

from local_scripts.fuzzer.models import FuzzCase, SeedData, TableData

# AND/OR are Kleene and NOT is two-valued (UNKNOWN negates to TRUE); either
# only bites where a predicate can be UNKNOWN.
NULLABLE_P = "nullable_amount > 0"
BOOL_Q = "active"


def _law(
    seed: SeedData,
    name: str,
    theorem: str,
    description: str,
    tags: Iterable[str],
    body: str,
    twin: str,
    model_extra: str = "",
    twin_extra: str = "",
    twin_model: str | None = None,
) -> FuzzCase:
    model = seed.trilogy_model()
    normalized_tags = tuple(tags)
    if "where " in (body + twin).lower() and "where" not in normalized_tags:
        normalized_tags = (*normalized_tags, "where")
    return FuzzCase(
        case_id=f"{seed.name}__law__{name}",
        seed=seed.name,
        family="law",
        description=f"`{theorem}`: {description}",
        tags=("law", *normalized_tags),
        trilogy=model + model_extra + body.strip() + "\n",
        oracle_sql="",
        twin_trilogy=(twin_model or model)
        + (twin_extra or model_extra)
        + twin.strip()
        + "\n",
    )


def _boolean_laws(seed: SeedData) -> list[FuzzCase]:
    p, q = NULLABLE_P, BOOL_Q
    pairs = (
        (
            "and_commutes",
            "Logic.and_comm",
            f"where {p} and {q}",
            f"where {q} and {p}",
        ),
        (
            "true_is_and_identity",
            "Logic.and_true_left",
            f"where true and {p}",
            f"where {p}",
        ),
        (
            "and_binds_tighter_than_or",
            "grammar: and binds tighter than or",
            f"where {q} or {p} and {q}",
            f"where {q} or ({p} and {q})",
        ),
        (
            "de_morgan_and",
            "Logic.not_and",
            f"where not ({p} and {q})",
            f"where (not {p}) or (not {q})",
        ),
        (
            "de_morgan_or",
            "Logic.not_or",
            f"where not ({p} or {q})",
            f"where (not {p}) and (not {q})",
        ),
        (
            "double_negation",
            "Logic.not_not_holds",
            f"where not (not ({p}))",
            f"where {p}",
        ),
        (
            "excluded_middle_holds",
            "Logic.or_not_self_holds",
            f"where {p} or not {p}",
            "where event_id is not null",
        ),
        (
            "compound_not_matches_atom_not",
            "Logic.not_paren_atom",
            f"where not ({p}) and not ({q})",
            f"where not {p} and not {q}",
        ),
        (
            "contradiction_never_holds",
            "Logic.and_not_self_never_holds",
            f"where ({p} and not {p}) or {q}",
            f"where {q}",
        ),
        (
            "staged_row_stage_folds",
            "Staged.row_stage_folds",
            f"where {q} then where {p}",
            f"where {q} and {p}",
        ),
        (
            "staged_idempotent",
            "Staged.stage_idempotent",
            f"where {p} then where {p}",
            f"where {p}",
        ),
    )
    projection = "select event_id, group_id, event_amount order by event_id asc;"
    return [
        _law(
            seed,
            name,
            theorem,
            f"`{left}` and `{right}` admit the same rows.",
            ("boolean", "nullable"),
            f"{left}\n{projection}",
            f"{right}\n{projection}",
        )
        for name, theorem, left, right in pairs
    ]


def _aggregate_laws(seed: SeedData) -> list[FuzzCase]:
    p, q = NULLABLE_P, BOOL_Q
    cases = [
        _law(
            seed,
            "not_partitions_count",
            "Agg.count_split_not",
            "A count splits over `p` and `not p`: two-valued NOT sends every "
            "UNKNOWN row to the second half.",
            ("aggregate", "nullable", "filtered"),
            "select group_id, count(event_id) as c order by group_id asc;",
            f"""
select group_id,
    count(event_id ? {p}) + count(event_id ? not {p}) as c
order by group_id asc;
""",
        ),
        _law(
            seed,
            "filtered_aggregate_is_where",
            "Agg.filtered_agg_eq_where",
            "`sum(x ? p)` per group, dropping empty groups, is `where p` "
            "then `sum(x)`.",
            ("aggregate", "filtered"),
            f"""
select group_id, sum(event_amount ? {q}) as t
having t is not null
order by group_id asc;
""",
            f"where {q}\nselect group_id, sum(event_amount) as t order by group_id asc;",
        ),
        _law(
            seed,
            "count_sums_over_groups",
            "Agg.count_eq_sum_group_counts",
            "A grand count equals the sum of per-group counts.",
            ("aggregate", "rowset", "nested"),
            "select count(event_id) as c;",
            """
rowset per_group <- select group_id as gid, count(event_id) as c;
select sum(per_group.c) as c;
""",
        ),
        _law(
            seed,
            "rollup_total_is_global",
            "Agg.rollup_total_eq_global",
            "ROLLUP's grand-total row equals the ungrouped aggregate.",
            ("aggregate", "grouping", "rowset"),
            """
rowset rolled <- select group_id, grouping(group_id) as lvl,
    sum(event_amount) as t
by rollup (group_id);
where rolled.lvl = 1
select rolled.t;
""",
            "select sum(event_amount) as t;",
        ),
        _law(
            seed,
            "fd_property_projection_invariant",
            "Grain.fd_extension_preserves_aggregates",
            "Selecting a property of the grouping key neither splits nor "
            "changes the aggregates.",
            ("aggregate", "rowset", "grain"),
            """
select group_id, sum(event_amount) as t, count(event_id) as c
order by group_id asc;
""",
            """
rowset wide <- select group_id as gid, group_name as nm,
    sum(event_amount) as t, count(event_id) as c;
select wide.gid, wide.t, wide.c order by wide.gid asc;
""",
        ),
        _law(
            seed,
            "sibling_aggregate_invariant",
            "Grain.sibling_aggregate_independent",
            "A sibling aggregate over another fact (a chasm) leaves the first "
            "aggregate's values alone.",
            ("aggregate", "chasm", "fanout", "rowset"),
            """
select group_id, sum(sale_amount) as s
having s is not null
order by group_id asc;
""",
            """
rowset paired <- select group_id as gid, sum(sale_amount) as s,
    sum(return_amount) as r;
where paired.s is not null
select paired.gid, paired.s order by paired.gid asc;
""",
        ),
        _law(
            seed,
            "having_is_outer_where",
            "Where.having_eq_outer_where",
            "HAVING on an aggregate equals a WHERE over the aggregated rowset.",
            ("aggregate", "having", "rowset"),
            """
select group_id, sum(event_amount) as t
having t > 0
order by group_id asc;
""",
            """
rowset summed <- select group_id as gid, sum(event_amount) as t;
where summed.t > 0
select summed.gid, summed.t order by summed.gid asc;
""",
        ),
        _law(
            seed,
            "where_aggregate_is_membership",
            "Where.flat_agg_atom_eq_membership",
            "A flat WHERE aggregate is computed over every row, so it is "
            "membership in a rowset of qualifying keys.",
            ("aggregate", "membership", "rowset"),
            f"""
where {q} and sum(event_amount) by group_id > 0
select group_id, sum(event_amount) as t
order by group_id asc;
""",
            f"""
rowset big <- where sum(event_amount) by group_id > 0 select group_id as gid;
where {q} and group_id in big.gid
select group_id, sum(event_amount) as t
order by group_id asc;
""",
        ),
        _law(
            seed,
            "staged_aggregate_is_inline_filter",
            "Staged.staged_eq_inline_filter",
            "A later stage's aggregate sees only the earlier stage's rows, "
            "exactly like an inline filter.",
            ("aggregate", "staged", "filtered"),
            f"""
where {q}
then where sum(event_amount) by group_id > 0
select group_id, sum(event_amount) as t
order by group_id asc;
""",
            f"""
where {q} and sum(event_amount ? {q}) by group_id > 0
select group_id, sum(event_amount) as t
order by group_id asc;
""",
        ),
        _law(
            seed,
            "keyed_rowset_preserves_rows",
            "Rows.keyed_projection_preserves_multiplicity",
            "A rowset that carries the row key keeps every row, so aggregating "
            "through it equals aggregating directly.",
            ("aggregate", "rowset"),
            f"where {q}\nselect group_id, sum(event_amount) as t order by group_id asc;",
            f"""
rowset kept <- where {q} select event_id, group_id as gid, event_amount as amt;
select kept.gid, sum(kept.amt) as t order by kept.gid asc;
""",
        ),
        _law(
            seed,
            "distinct_is_group",
            "Rows.distinct_eq_group",
            "`distinct(x)` is `group(x) by x`.",
            ("distinct", "nullable"),
            "select distinct(nullable_name) as n order by n asc nulls first;",
            "select group(nullable_name) by nullable_name as n "
            "order by n asc nulls first;",
        ),
    ]
    return cases


def _band_sql(sales: TableData) -> str:
    return (
        "select id, case when amount > 10 then 'big' else 'small' end as band "
        f"from ({sales.select_sql()})"
    )


def _materialization_laws(seed: SeedData) -> list[FuzzCase]:
    derived = """
auto sale_band <- case when sale_amount > 10 then 'big' else 'small' end;
"""
    stored = derived + f"""
datasource sale_bands (id: sale_id, band: sale_band)
grain (sale_id)
query '''{_band_sql(seed.sales)}''';
"""
    queries = (
        ("grouped", "select group_id, sale_band, count(sale_id) as c;"),
        ("padded_projection", "select group_id, sale_band;"),
        ("padded_filter", "where sale_band = 'small' select group_id;"),
        ("pinned_aggregate", "select group_id, count(sale_band) as c;"),
    )
    return [
        _law(
            seed,
            f"materialized_{name}",
            "Domain.materialization_invariance",
            "A derived property answers the same whether computed or bound "
            "to a column at its own grain, including on `~` padded rows.",
            ("materialization", "partial", "pin"),
            query,
            query,
            model_extra=derived,
            twin_extra=stored,
        )
        for name, query in queries
    ]


def _pin_laws(seed: SeedData) -> list[FuzzCase]:
    named = "\nauto sale_or_zero <- coalesce(sale_amount, 0);\n"
    return [
        _law(
            seed,
            "pin_named_eq_inline",
            "Pin.named_eq_inline",
            "An inline NULL-absorbing expression pins like its named spelling.",
            ("pin", "partial"),
            "select group_id, sale_or_zero order by group_id asc;",
            "select group_id, coalesce(sale_amount, 0) as sale_or_zero "
            "order by group_id asc;",
            model_extra=named,
        ),
        _law(
            seed,
            "pin_sum_of_coalesce",
            "Pin.sum_coalesce_eq_coalesce_sum",
            "On a `~` padded group the pinned fallback sums to the fallback.",
            ("pin", "partial", "aggregate"),
            "select group_id, sum(coalesce(sale_amount, 0)) as s "
            "order by group_id asc;",
            "select group_id, coalesce(sum(sale_amount), 0) as s "
            "order by group_id asc;",
        ),
        _law(
            seed,
            "pin_projection_invariant",
            "Pin.projection_invariant",
            "A comparison beside a pinned value, at its reads' grain, leaves "
            "the value on padded rows alone.",
            ("pin", "partial", "rowset"),
            "select group_id, sale_or_zero order by group_id asc;",
            """
rowset wide <- select group_id as gid, sale_or_zero as z,
    sale_amount > 15 as big, sale_amount is null as missing;
select wide.gid, wide.z order by wide.gid asc;
""",
            model_extra=named,
        ),
        _law(
            seed,
            "pin_where_reads_projected_value",
            "Pin.one_address_one_value",
            "A WHERE reads the value the select would project.",
            ("pin", "partial", "rowset"),
            "where sale_or_zero = 0 select group_id order by group_id asc;",
            """
rowset projected <- select group_id as gid, sale_or_zero as z;
where projected.z = 0
select projected.gid order by projected.gid asc;
""",
            model_extra=named,
        ),
    ]


def _extended_laws(seed: SeedData) -> list[FuzzCase]:
    p, q = NULLABLE_P, BOOL_Q
    return [
        _law(
            seed,
            "or_inclusion_exclusion",
            "Agg.count_or_inclusion_exclusion",
            "`count(? p or q)` is inclusion-exclusion over `p` and `q`.",
            ("aggregate", "filtered", "nullable"),
            f"select group_id, count(event_id ? {p} or {q}) as c "
            "order by group_id asc;",
            f"""
select group_id,
    count(event_id ? {p}) + count(event_id ? {q})
        - count(event_id ? {p} and {q}) as c
order by group_id asc;
""",
        ),
        _law(
            seed,
            "staged_three_stage_chain",
            "Staged.chain_eq_inline_filter",
            "Each stage's aggregate sees the conjunction of all earlier stages.",
            ("aggregate", "staged", "filtered", "nullable"),
            f"""
where {q}
then where {p}
then where sum(event_amount) by group_id > 0
select group_id, sum(event_amount) as t
order by group_id asc;
""",
            f"""
where {q} and {p}
    and sum(event_amount ? {q} and {p}) by group_id > 0
select group_id, sum(event_amount) as t
order by group_id asc;
""",
        ),
        _law(
            seed,
            "staged_membership_stage",
            "Staged.staged_eq_inline_filter",
            "An existence stage rides the later stage's aggregate input.",
            ("aggregate", "staged", "membership", "rowset"),
            """
rowset named <- where nullable_name is not null select group_id as gid;
where group_id in named.gid
then where count(event_id) by group_id > 1
select group_id, count(event_id) as c
order by group_id asc;
""",
            """
rowset named <- where nullable_name is not null select group_id as gid;
where group_id in named.gid
    and count(event_id ? group_id in named.gid) by group_id > 1
select group_id, count(event_id) as c
order by group_id asc;
""",
        ),
        _law(
            seed,
            "rollup_leaves_are_group_by",
            "Agg.rollup_leaves_eq_group_by",
            "ROLLUP's leaf rows are the plain GROUP BY.",
            ("aggregate", "grouping", "rowset"),
            """
rowset rolled <- select group_id, grouping(group_id) as lvl,
    sum(event_amount) as t
by rollup (group_id);
where rolled.lvl = 0
select rolled.group_id, rolled.t order by rolled.group_id asc;
""",
            "select group_id, sum(event_amount) as t order by group_id asc;",
        ),
        _law(
            seed,
            "nullable_fk_counts_sum",
            "Agg.count_eq_sum_group_counts",
            "Per-group counts over a `?` FK, NULL group included, sum to the "
            "grand count.",
            ("aggregate", "nullable", "rowset"),
            "select count(visit_id) as c;",
            """
rowset per_group <- select group_id as gid, count(visit_id) as c;
select sum(per_group.c) as c;
""",
        ),
        _law(
            seed,
            "partial_fk_counts_sum",
            "Agg.count_eq_sum_group_counts",
            "Per-group counts over a `~` FK sum to the grand count; padded "
            "groups count zero.",
            ("aggregate", "partial", "rowset"),
            "select count(sale_id) as c;",
            """
rowset per_group <- select group_id as gid, count(sale_id) as c;
select sum(per_group.c) as c;
""",
        ),
        _law(
            seed,
            "rowset_filter_pushdown",
            "Rows.filter_commutes_with_keyed_projection",
            "Filtering a keyed rowset's output equals filtering its input.",
            ("rowset", "aggregate"),
            """
rowset base <- select event_id, group_id as gid, event_amount as amt;
where base.amt > 0
select base.gid, sum(base.amt) as t order by base.gid asc;
""",
            "where event_amount > 0\n"
            "select group_id, sum(event_amount) as t order by group_id asc;",
        ),
        _law(
            seed,
            "named_where_beside_subset_join",
            "Pin.named_eq_inline",
            "A WHERE over a named derivation reads as its inline spelling, "
            "beside a `subset join`ed rowset too.",
            ("rowset", "subset", "where"),
            """
rowset totals <- select group_id as tgid, sum(event_amount) as tt;
rowset joined <- where has_amount = true and totals.tt > 0
select group_id as gid, sum(event_amount) as t, max(totals.tt) as m
subset join totals.tgid = group_id;
select joined.gid, joined.t, joined.m order by joined.gid asc;
""",
            """
rowset totals <- select group_id as tgid, sum(event_amount) as tt;
rowset joined <- where nullable_amount is not null and totals.tt > 0
select group_id as gid, sum(event_amount) as t, max(totals.tt) as m
subset join totals.tgid = group_id;
select joined.gid, joined.t, joined.m order by joined.gid asc;
""",
            model_extra="\nauto has_amount <- nullable_amount is not null;\n",
        ),
        _law(
            seed,
            "key_count_is_source_independent",
            "Rows.count_key_eq_count_distinct",
            "`count(key)` counts distinct key values, whichever rows carry it.",
            ("aggregate", "rowset", "distinct"),
            f"where {q} select count(group_id) as c;",
            f"""
rowset keys <- where {q} select group_id;
select count(keys.group_id) as c;
""",
        ),
        _law(
            seed,
            "window_partition_eq_by",
            "Grain.window_partition_eq_by",
            "A partitioned window total equals the same aggregate `by` the "
            "partition, read back at row grain.",
            ("window", "aggregate"),
            "select event_id, sum(event_amount) by group_id as t "
            "order by event_id asc;",
            "select event_id, "
            "sum(event_amount) over (partition by group_id) as t "
            "order by event_id asc;",
        ),
        _law(
            seed,
            "max_is_negated_min",
            "Agg.max_eq_neg_min_neg",
            "`max(x)` is `-min(-x)` per group.",
            ("aggregate",),
            "select group_id, max(event_amount) as m order by group_id asc;",
            "select group_id, 0 - min(0 - event_amount) as m " "order by group_id asc;",
        ),
    ]


EVENT_COLUMNS = """eid: event_id,
    gid: group_id,
    amount: event_amount,
    nullable_amount: nullable_amount,
    active: active"""

SOURCING_QUERIES = (
    (
        "rows",
        (
            "select event_id, event_amount, nullable_amount, active "
            "order by event_id asc;"
        ),
    ),
    (
        "grouped",
        (
            "select group_id, count(event_id) as c, sum(event_amount) as t "
            "order by group_id asc nulls first;"
        ),
    ),
    (
        "joined",
        "select group_name, sum(nullable_amount) as t order by group_name asc;",
    ),
    ("total", "select count(event_id) as c, sum(event_amount) as t;"),
)


def _events_as_arms(seed: SeedData, arms: tuple[tuple[str, str], ...]) -> str:
    """The seed model with `events` replaced by `complete where` arms, each
    holding exactly the rows its claim is TRUE on."""
    blocks = "".join(
        f"""
partial datasource events_{suffix} (
    {EVENT_COLUMNS}
)
grain (event_id)
complete where {claim}
query '''select * from ({seed.events.select_sql()}) where {column_claim}''';
"""
        for suffix, claim in arms
        for column_claim in [claim.replace("event_amount", "amount")]
    )
    model, count = re.subn(
        r"datasource events \(.*?''';\n", blocks, seed.trilogy_model(), flags=re.DOTALL
    )
    assert count == 1
    return model


def _sourcing_laws(seed: SeedData) -> list[FuzzCase]:
    families = (
        (
            "split",
            "the arms `<= 10` and `> 10` partition a non-null int.",
            (("low", "event_amount <= 10"), ("high", "event_amount > 10")),
            "",
        ),
        (
            "point",
            "a point arm `= 5` between `< 5` and `> 5` partitions too.",
            (
                ("below", "event_amount < 5"),
                ("at", "event_amount = 5"),
                ("above", "event_amount > 5"),
            ),
            "",
        ),
        (
            "bool",
            "`active = true` and `active = false` partition a non-null bool.",
            (("on", "active = true"), ("off", "active = false")),
            "",
        ),
        (
            "null_arm",
            "a nullable discriminator needs its `is null` arm.",
            (
                ("neg", "nullable_amount <= 0"),
                ("pos", "nullable_amount > 0"),
                ("null", "nullable_amount is null"),
            ),
            "",
        ),
        (
            "gated",
            (
                "a gate proving the discriminator non-null removes NULL from "
                "the domain the arms must partition."
            ),
            (("neg", "nullable_amount <= 0"), ("pos", "nullable_amount > 0")),
            "where nullable_amount is not null\n",
        ),
        (
            "copy",
            (
                "a materialized copy of one arm overlaps it; the union picks "
                "one copy, not both."
            ),
            (
                ("low", "event_amount <= 10"),
                ("high", "event_amount > 10"),
                ("low_copy", "event_amount <= 10"),
            ),
            "",
        ),
    )
    cases = [
        _law(
            seed,
            f"union_{family}_{name}",
            (
                "Sourcing.gated_union_perm"
                if gate
                else (
                    "Sourcing.cover_choice_irrelevant"
                    if family == "copy"
                    else "Sourcing.union_perm_of_partition"
                )
            ),
            f"A partition family unions back to the table: {why}",
            ("sourcing", "partial", "union"),
            gate + query,
            gate + query,
            twin_model=_events_as_arms(seed, arms),
        )
        for family, why, arms, gate in families
        for name, query in SOURCING_QUERIES
    ]
    active_arm = f"""
partial datasource events_active (
    {EVENT_COLUMNS}
)
grain (event_id)
complete where active = true
query '''select * from ({seed.events.select_sql()}) where active''';
"""
    cases += [
        _law(
            seed,
            f"partial_under_implication_{name}",
            "Sourcing.partial_is_full_under_implication",
            "A `complete where` arm beside the full table answers a query "
            "whose WHERE implies its claim, without losing a row.",
            ("sourcing", "partial", "where"),
            f"where active = true\n{query}",
            f"where active = true\n{query}",
            twin_extra=active_arm,
        )
        for name, query in SOURCING_QUERIES
    ]
    summary = "\nauto group_total <- sum(event_amount) by group_id;\n"
    stored = summary + f"""
datasource group_totals (gid: group_id, total: group_total)
grain (group_id)
query '''select gid, sum(amount) as total
from ({seed.events.select_sql()}) group by gid''';
"""
    cases += [
        _law(
            seed,
            f"summary_table_{name}",
            "Domain.materialization_invariance",
            "An aggregate answers the same computed or read from a summary "
            "table at its grain.",
            ("sourcing", "aggregate", "materialization"),
            query,
            query,
            model_extra=summary,
            twin_extra=stored,
        )
        for name, query in (
            ("projection", "select group_id, group_total order by group_id asc;"),
            (
                "filter",
                "where group_total > 10 select group_id order by group_id asc;",
            ),
            (
                "dimension",
                "select group_name, group_total order by group_name asc;",
            ),
        )
    ]
    return cases


ROLLUP_MEASURES = """
auto rollup_count <- count(event_id);
auto rollup_amount <- sum(event_amount);
auto rollup_nullable <- sum(nullable_amount);
auto rollup_low <- min(nullable_amount);
auto rollup_high <- max(event_amount);
"""

ROLLUP_QUERIES = (
    ("total", "select count(event_id) as n, sum(nullable_amount) as s;"),
    (
        "finer_filter_total",
        "where group_id > 1 select count(event_id) as n, sum(event_amount) as s;",
    ),
    (
        "joined_filter_total",
        "where group_id > 1 or group_name is null select sum(nullable_amount) as s;",
    ),
    (
        "property_grain",
        "select group_name, count(event_id) as n order by group_name asc;",
    ),
    (
        "property_grain_compound_filter",
        (
            "where group_id > 1 or group_name is null "
            "select group_name, sum(event_amount) as s order by group_name asc;"
        ),
    ),
    (
        "coarser_grain_compound_filter",
        (
            "where group_id > 1 or group_name is null "
            "select nullable_name, count(event_id) as n order by nullable_name asc;"
        ),
    ),
    (
        "window_over_count",
        (
            "where group_id > 1 select group_name, "
            "rank(group_name) over (order by count(event_id) by group_name desc, "
            "group_name asc) as r order by group_name asc;"
        ),
    ),
    (
        "sum_of_counts",
        "where group_id > 1 select sum(count(event_id) by group_name) as s;",
    ),
    (
        "min_max_total",
        "select min(nullable_amount) as lo, max(event_amount) as hi;",
    ),
    (
        "min_max_compound_filter",
        (
            "where group_id > 1 or group_name is null "
            "select nullable_name, min(nullable_amount) as lo, "
            "max(event_amount) as hi order by nullable_name asc;"
        ),
    ),
    (
        "max_of_mins",
        "where group_id > 1 select max(min(nullable_amount) by group_name) as m;",
    ),
)


def _rollup_laws(seed: SeedData) -> list[FuzzCase]:
    summary = f"""
datasource group_rollup (
    gid: group_id,
    n: rollup_count,
    amount: rollup_amount,
    nullable_total: ?rollup_nullable,
    low: ?rollup_low,
    high: ?rollup_high
)
grain (group_id)
query '''select gid, count(eid) as n, sum(amount) as amount,
sum(nullable_amount) as nullable_total, min(nullable_amount) as low,
max(amount) as high
from ({seed.events.select_sql()}) group by gid''';
"""
    return [
        _law(
            seed,
            f"rollup_{name}",
            "Rollup.min_exact" if "min" in name else "Rollup.sum_exact",
            "A summary at a finer grain answers a rollable aggregate under any "
            "filter and target its grain determines.",
            ("sourcing", "aggregate", "rollup"),
            query,
            query,
            model_extra=ROLLUP_MEASURES,
            twin_extra=ROLLUP_MEASURES + summary,
        )
        for name, query in ROLLUP_QUERIES
    ]


def law_cases(seed: SeedData) -> list[FuzzCase]:
    return [
        *_boolean_laws(seed),
        *_aggregate_laws(seed),
        *_materialization_laws(seed),
        *_pin_laws(seed),
        *_extended_laws(seed),
        *_sourcing_laws(seed),
        *_rollup_laws(seed),
    ]
