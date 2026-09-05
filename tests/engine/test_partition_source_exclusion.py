"""A sibling model's partition sources must never rebuild a target whose row
gate excludes them: once the statement WHERE contradicts a `complete where`,
that source is hidden from discovery, so a `merge` origin is planned instead of
the sibling's direct binding and no union is filtered to nothing."""

import pytest

from trilogy import Dialects
from trilogy.core.enums import ComparisonOperator
from trilogy.core.models.build import BuildComparison, BuildWhereClause
from trilogy.core.processing.partial_bridging import drop_excluded_partials

COMMON = """
key city enum<string>['A', 'B'];
key tree_id string;
property tree_id.dbh float;
"""

MODEL_A_SOURCES = """
property tree_id.raw_dbh float;
auto proc_dbh <- raw_dbh * 1.0;
merge proc_dbh into dbh;

key a_source enum<string>['A1', 'A2'];

root partial datasource a_one (tree_id: tree_id, city: city, src: a_source, raw: ?raw_dbh)
grain (tree_id) complete where city = 'A' and a_source = 'A1'
query '''select 'a1' as tree_id, 'A' as city, 'A1' as src, 1.0 as raw''';

root partial datasource a_two (tree_id: tree_id, city: city, src: a_source, raw: ?raw_dbh)
grain (tree_id) complete where city = 'A' and a_source = 'A2'
query '''select 'a2' as tree_id, 'A' as city, 'A2' as src, 2.0 as raw''';
"""

MODEL_A_TARGET = """
partial datasource a_out (tree_id, city, src: a_source, ?dbh)
grain (tree_id) complete where city = 'A'
address out_a;
"""

MODEL_B = """
key b_source enum<string>['B1', 'B2'];

root partial datasource b_one (tree_id: tree_id, city: city, src: b_source, dbh: ?dbh)
grain (tree_id) complete where city = 'B' and b_source = 'B1'
query '''select 'b1' as tree_id, 'B' as city, 'B1' as src, 9.0 as dbh''';

root partial datasource b_two (tree_id: tree_id, city: city, src: b_source, dbh: ?dbh)
grain (tree_id) complete where city = 'B' and b_source = 'B2'
query '''select 'b2' as tree_id, 'B' as city, 'B2' as src, 8.0 as dbh''';
"""

MODEL_B_SINGLE = """
key b_source enum<string>['B1'];

root partial datasource b_one (tree_id: tree_id, city: city, src: b_source, dbh: ?dbh)
grain (tree_id) complete where city = 'B' and b_source = 'B1'
query '''select 'b1' as tree_id, 'B' as city, 'B1' as src, 9.0 as dbh''';
"""

A_ROWS = [("a1", "A", "A1", 1.0), ("a2", "A", "A2", 2.0)]


def _executor(*models: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text("".join(models))
    return executor


def _refresh_target(executor) -> tuple[str, list[tuple]]:
    target = executor.environment.datasources["a_out"]
    sql = executor.update_datasource(target, dry_run=True)
    assert sql is not None
    executor.update_datasource(target)
    rows = executor.execute_raw_sql(
        "select tree_id, city, src, dbh from out_a order by tree_id"
    ).fetchall()
    return sql, rows


def test_target_alone_builds_from_own_partition():
    sql, rows = _refresh_target(_executor(COMMON, MODEL_A_SOURCES, MODEL_A_TARGET))
    assert "a_one" in sql and "a_two" in sql
    assert rows == A_ROWS


@pytest.mark.parametrize(
    "sibling", [MODEL_B, MODEL_B_SINGLE], ids=["two_partitions", "one_partition"]
)
def test_sibling_partition_never_rebuilds_target(sibling):
    sql, rows = _refresh_target(
        _executor(COMMON, MODEL_A_SOURCES, MODEL_A_TARGET, sibling)
    )
    assert "b_one" not in sql and "b_two" not in sql, sql
    assert "a_one" in sql and "a_two" in sql, sql
    assert rows == A_ROWS


def test_select_through_merge_reads_own_partition_only():
    executor = _executor(COMMON, MODEL_A_SOURCES, MODEL_B)
    a_rows = executor.execute_text(
        "where city = 'A' select tree_id, dbh order by tree_id asc;"
    )[-1].fetchall()
    assert a_rows == [("a1", 1.0), ("a2", 2.0)]
    b_rows = executor.execute_text(
        "where city = 'B' select tree_id, dbh order by tree_id asc;"
    )[-1].fetchall()
    assert b_rows == [("b1", 9.0), ("b2", 8.0)]


def _city_gate(environment, value: str) -> BuildWhereClause:
    return BuildWhereClause(
        conditional=BuildComparison(
            left=environment.concepts["local.city"],
            right=value,
            operator=ComparisonOperator.EQ,
        )
    )


def test_drop_excluded_partials_hides_contradicted_sources():
    executor = _executor(COMMON, MODEL_A_SOURCES, MODEL_B)
    environment = executor.environment.materialize_for_select()
    drop_excluded_partials(environment, _city_gate(environment, "A"))
    assert set(environment.datasources) == {"a_one", "a_two"}


def test_drop_excluded_partials_keeps_everything_without_gate():
    executor = _executor(COMMON, MODEL_A_SOURCES, MODEL_B)
    environment = executor.environment.materialize_for_select()
    before = set(environment.datasources)
    drop_excluded_partials(environment, None)
    assert set(environment.datasources) == before


def test_drop_excluded_partials_stands_down_for_cross_row_stage():
    executor = _executor(
        COMMON, MODEL_A_SOURCES, MODEL_B, "auto tree_count <- count(tree_id) by city;"
    )
    environment = executor.environment.materialize_for_select()
    before = set(environment.datasources)
    gate = BuildWhereClause(
        conditional=_city_gate(environment, "A").conditional
        + BuildComparison(
            left=environment.concepts["local.tree_count"],
            right=0,
            operator=ComparisonOperator.GT,
        )
    )
    drop_excluded_partials(environment, gate)
    assert set(environment.datasources) == before


THREE_WAY = """
key channel enum<string>['WEB', 'CATALOG', 'STORE'];
key order_id int;
property <order_id, channel>.amount float;

partial datasource web (raw(''' 'WEB' '''): channel, id: order_id, amt: amount)
grain (order_id, channel) complete where channel = 'WEB'
query '''select 1 as id, 10.0 as amt''';

partial datasource catalog (raw(''' 'CATALOG' '''): channel, id: order_id, amt: amount)
grain (order_id, channel) complete where channel = 'CATALOG'
query '''select 2 as id, 20.0 as amt''';

partial datasource store (raw(''' 'STORE' '''): channel, id: order_id, amt: amount)
grain (order_id, channel) complete where channel = 'STORE'
query '''select 3 as id, 30.0 as amt''';
"""


def test_surviving_arms_still_union_over_reduced_domain():
    executor = _executor(THREE_WAY)
    query = "where channel in ('WEB', 'CATALOG') select sum(amount) as total;"
    sql = executor.generate_sql(query)[-1]
    assert "store" not in sql, sql
    assert executor.execute_text(query)[-1].fetchall() == [(30.0,)]


def test_reduced_domain_recorded_by_address_and_canonical():
    executor = _executor(THREE_WAY)
    environment = executor.environment.materialize_for_select()
    gate = BuildWhereClause(
        conditional=BuildComparison(
            left=environment.concepts["local.channel"],
            right=("WEB", "CATALOG"),
            operator=ComparisonOperator.IN,
        )
    )
    drop_excluded_partials(environment, gate)
    assert set(environment.datasources) == {"web", "catalog"}
    assert environment.excluded_enum_values["local.channel"] == frozenset({"STORE"})


CLUSTERED = """
key city enum<string>['A', 'B'];
key id string;
key source enum<string>['municipal', 'community', 'osm'];
property id.cell string;

root partial datasource municipal (id: id, city: city, source: source, cell: cell)
grain (id) complete where city = 'A' and source = 'municipal'
query '''select 'm1' as id, 'A' as city, 'municipal' as source, 'c1' as cell''';

root partial datasource community (id: id, city: city, source: source, cell: cell)
grain (id) complete where city = 'A' and source = 'community'
query '''select 'k1' as id, 'A' as city, 'community' as source, 'c2' as cell''';

root partial datasource osm (id: id, city: city, source: source, cell: cell)
grain (id) complete where city = 'A' and source = 'osm'
query '''select 'o1' as id, 'A' as city, 'osm' as source, 'c1' as cell
union all select 'o2' as id, 'A' as city, 'osm' as source, 'c3' as cell''';

auto anchor <- min(id ? source != 'osm') by cell;
auto cluster_id <- coalesce(anchor, id);
auto is_primary <- id = cluster_id;
"""

PRUNE_SPELLINGS = {
    "target_where": """
partial datasource pruned (id, city, cluster_id)
grain (id) complete where city = 'A'
address pruned_out
where id = cluster_id;
""",
    "claim_predicate": """
partial datasource pruned (id, city, cluster_id)
grain (id) complete where city = 'A' and id = cluster_id
address pruned_out;
""",
    "claim_boolean": """
partial datasource pruned (id, city, is_primary)
grain (id) complete where city = 'A' and is_primary = true
address pruned_out;
""",
}


def _refresh_pruned(executor) -> list[str]:
    target = executor.environment.datasources["pruned"]
    executor.update_datasource(target)
    return [
        r[0]
        for r in executor.execute_raw_sql(
            "select id from pruned_out order by id"
        ).fetchall()
    ]


def test_unpruned_target_unions_own_partitions():
    executor = _executor(
        CLUSTERED,
        """
partial datasource pruned (id, city, cluster_id)
grain (id) complete where city = 'A'
address pruned_out;
""",
    )
    assert _refresh_pruned(executor) == ["k1", "m1", "o1", "o2"]


@pytest.mark.parametrize("spelling", PRUNE_SPELLINGS, ids=list(PRUNE_SPELLINGS))
def test_row_gate_on_partitioned_target_plans_inside_partition_scope(spelling):
    """A row gate that depends on an aggregate is evaluated after the union
    of the arms covering the target's own partition, not against the whole
    `city` domain: the partials are exhaustive within `city = 'A'` only."""
    executor = _executor(CLUSTERED, PRUNE_SPELLINGS[spelling])
    assert _refresh_pruned(executor) == ["k1", "m1", "o2"]


def _stages(executor, claim: str, gate: str | None) -> list[str]:
    from trilogy.core.models.author import WhereClause, prepend_where_stage
    from trilogy.core.query_processor import _partition_scoped_stages

    env = executor.environment

    def clause(text: str) -> WhereClause:
        statement = env.parse(f"{text} select id;")[1][-1]
        assert statement.where_clause is not None
        return statement.where_clause

    claim_clause = clause(claim)
    stages = [clause(gate)] if gate else []
    scoped = _partition_scoped_stages(stages, claim_clause, env)
    if len(scoped) == 1:
        assert str(scoped) == str(prepend_where_stage(stages, claim_clause))
    return [str(s) for s in scoped]


def test_scalar_gate_stays_one_stage_with_the_claim():
    executor = _executor(CLUSTERED)
    assert len(_stages(executor, "where city = 'A'", None)) == 1
    assert len(_stages(executor, "where city = 'A'", "where source = 'osm'")) == 1


def test_cross_row_gate_follows_the_scalar_claim_as_its_own_stage():
    executor = _executor(CLUSTERED)
    stages = _stages(executor, "where city = 'A'", "where id = cluster_id")
    assert len(stages) == 2 and "city" in stages[0] and "cluster_id" in stages[1]
    stages = _stages(executor, "where city = 'A' and id = cluster_id", None)
    assert len(stages) == 2 and "cluster_id" not in stages[0]
    stages = _stages(
        executor, "where city = 'A' and is_primary = true", "where source = 'osm'"
    )
    assert len(stages) == 2 and "source" in stages[1] and "is_primary" in stages[1]
