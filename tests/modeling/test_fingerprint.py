from trilogy import parse
from trilogy.core.fingerprint import (
    ChangeKind,
    EnvironmentFingerprint,
    build_environment_fingerprint,
    diff_fingerprints,
)

BASE = """
key order_id int;
property order_id.amount float;
key customer_id int;

datasource orders (
    order_id: order_id,
    amount: amount,
    customer_id: customer_id
)
grain (order_id)
address orders_tbl;
"""

DERIVED = BASE + """
auto total_amount <- sum(amount) by customer_id;

datasource customer_totals (
    customer_id: customer_id,
    total_amount: total_amount
)
grain (customer_id)
address customer_totals_tbl;
"""


def fingerprint(text: str) -> EnvironmentFingerprint:
    env, _ = parse(text)
    return build_environment_fingerprint(env)


def test_stable_across_fresh_parses():
    a = fingerprint(DERIVED)
    b = fingerprint(DERIVED)
    assert a.root == b.root
    assert a.model_dump() == b.model_dump()


def test_whitespace_and_comments_do_not_change_root():
    variant = DERIVED.replace(
        "key order_id int;", "# an order\n\n\nkey order_id   int;"
    )
    assert fingerprint(variant).root == fingerprint(DERIVED).root


def test_identical_environments_diff_empty():
    diff = diff_fingerprints(fingerprint(DERIVED), fingerprint(DERIVED))
    assert diff.identical
    assert diff.concepts.empty
    assert diff.datasources.empty
    assert not diff.invalidated_datasources


def test_metric_change_invalidates_exactly_its_datasource():
    changed = DERIVED.replace(
        "sum(amount) by customer_id", "sum(amount * 2) by customer_id"
    )
    diff = diff_fingerprints(fingerprint(DERIVED), fingerprint(changed))
    assert not diff.identical
    assert diff.concepts.changed == {"local.total_amount": ChangeKind.DEFINITION}
    assert not diff.concepts.added and not diff.concepts.removed
    assert diff.datasources.changed == {"customer_totals": ChangeKind.UPSTREAM}
    assert diff.invalidated_datasources == ["customer_totals"]


def test_root_type_change_propagates_downstream():
    changed = DERIVED.replace(
        "property order_id.amount float;", "property order_id.amount int;"
    )
    diff = diff_fingerprints(fingerprint(DERIVED), fingerprint(changed))
    assert diff.concepts.changed["local.amount"] == ChangeKind.DEFINITION
    assert diff.concepts.changed["local.total_amount"] == ChangeKind.UPSTREAM
    assert "orders" in diff.invalidated_datasources
    assert "customer_totals" in diff.invalidated_datasources


INLINE_DOWNSTREAM = BASE + """
auto double_total <- (sum(amount) by customer_id) * 2;

datasource customer_doubles (
    customer_id: customer_id,
    double_total: double_total
)
grain (customer_id)
address customer_doubles_tbl;
"""

NAMED_DOWNSTREAM = BASE + """
auto total_amount <- sum(amount) by customer_id;
auto double_total <- total_amount * 2;

datasource customer_doubles (
    customer_id: customer_id,
    double_total: double_total
)
grain (customer_id)
address customer_doubles_tbl;
"""


def test_naming_an_intermediate_is_identity():
    inline = fingerprint(INLINE_DOWNSTREAM)
    named = fingerprint(NAMED_DOWNSTREAM)
    assert (
        inline.concepts["local.double_total"].effective
        == named.concepts["local.double_total"].effective
    )
    assert (
        inline.datasources["customer_doubles"].effective
        == named.datasources["customer_doubles"].effective
    )
    diff = diff_fingerprints(inline, named)
    assert not diff.invalidated_datasources
    assert diff.concepts.added == ["local.total_amount"]
    assert diff.concepts.changed.get("local.double_total") in (
        None,
        ChangeKind.REFACTOR,
    )


def test_rename_of_derived_concept_is_refactor_only():
    renamed = DERIVED.replace("auto total_amount <-", "auto renamed_total <-").replace(
        "total_amount: total_amount", "total_amount: renamed_total"
    )
    diff = diff_fingerprints(fingerprint(DERIVED), fingerprint(renamed))
    assert diff.concepts.renamed == {"local.total_amount": "local.renamed_total"}
    assert not diff.concepts.added and not diff.concepts.removed
    assert not diff.invalidated_datasources
    assert diff.datasources.changed == {"customer_totals": ChangeKind.REFACTOR}


ROWSET = DERIVED + """
rowset big_customers <- select customer_id, total_amount where total_amount > 100;
auto big_customer_total <- sum(big_customers.total_amount);
"""


def test_rowset_models_fingerprint_deterministically():
    a = fingerprint(ROWSET)
    b = fingerprint(ROWSET)
    assert a.root == b.root
    assert "local.big_customer_total" in a.concepts


def test_rowset_reacts_to_upstream_change():
    changed = ROWSET.replace(
        "sum(amount) by customer_id", "sum(amount * 2) by customer_id"
    )
    diff = diff_fingerprints(fingerprint(ROWSET), fingerprint(changed))
    assert diff.concepts.changed["local.big_customer_total"] == ChangeKind.UPSTREAM


def test_datasource_address_change_is_definition_change():
    moved = DERIVED.replace("address customer_totals_tbl;", "address other_tbl;")
    diff = diff_fingerprints(fingerprint(DERIVED), fingerprint(moved))
    assert diff.datasources.changed == {"customer_totals": ChangeKind.DEFINITION}
    assert diff.invalidated_datasources == ["customer_totals"]
