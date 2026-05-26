from trilogy import Dialects, Environment
from trilogy.core.models.author import AggregateWrapper, ConceptRef
from trilogy.core.statements.author import SelectStatement
from trilogy.parser import parse


def _customer_env() -> str:
    return """
key id int;
property id.phone string;
property id.acctbal float;
datasource customer (
  id: id,
  phone: phone,
  acctbal: acctbal
) grain (id) address customer;
"""


def test_aggregate_by_paren_expression_parses() -> None:
    script = (
        _customer_env()
        + "select substring(phone, 1, 2) as cntrycode, "
        "avg(acctbal) by (substring(phone, 1, 2)) as avg_by_cc;"
    )
    env, parsed = parse(script)
    select: SelectStatement = parsed[-1]

    found = None
    for c in env.concepts.values():
        if isinstance(c.lineage, AggregateWrapper) and c.lineage.by:
            found = c.lineage
            break
    assert found is not None
    assert len(found.by) == 1
    assert isinstance(found.by[0], ConceptRef)


def test_aggregate_by_paren_expression_groups_at_expression_grain() -> None:
    script = (
        _customer_env()
        + "select substring(phone, 1, 2) as cntrycode, "
        "avg(acctbal) by (substring(phone, 1, 2)) as avg_by_cc;"
    )
    env = Environment()
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    ex.parse_text(script)
    sql = ex.generate_sql(
        "select substring(phone, 1, 2) as cntrycode, "
        "avg(acctbal) by (substring(phone, 1, 2)) as avg_by_cc;"
    )[0]
    assert "SUBSTRING" in sql.upper()
    assert "avg(" in sql or "AVG(" in sql.upper()


def test_aggregate_by_paren_multi_expression() -> None:
    script = (
        _customer_env()
        + "select avg(acctbal) by (substring(phone, 1, 2), id) as a;"
    )
    env, parsed = parse(script)
    select: SelectStatement = parsed[-1]
    found = None
    for c in env.concepts.values():
        if isinstance(c.lineage, AggregateWrapper) and len(c.lineage.by) == 2:
            found = c.lineage
            break
    assert found is not None
    assert len(found.by) == 2


def test_aggregate_by_bare_identifier_list_still_works() -> None:
    script = (
        _customer_env()
        + "select avg(acctbal) by id as a;"
    )
    env, parsed = parse(script)
    assert parsed[-1] is not None
