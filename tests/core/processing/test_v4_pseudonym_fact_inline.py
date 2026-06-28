"""Lock: `InlineDatasource` folds a passthrough datasource scan whose join key
the consumer inherited under a *pseudonym* address (audit: q2.1/q2.2).

A bare fact-scan passthrough (tpc_ds q2.x's `web_sales`/`catalog_sales`) is a
parent of the aggregate join. v3 fuses that scan into the aggregate's FROM; v4
materializes it as a standalone `DatasourceCTE` and only inlines it back if
`InlineDatasource` accepts the fold. It used to reject: a merge join key reaches
the consumer under its canonical address (`date.id`) while the raw datasource
exposes it under the local FK pseudonym (`web_sales.date.id`), so the
literal-address subset check reported the key "missing". Pseudonyms now count as
satisfiable. The control test pins that a genuinely-absent column still blocks
the fold, so the relaxation didn't widen the gate."""

from trilogy.core.models.build import BuildGrain
from trilogy.core.models.execute import CTE, QueryDatasource
from trilogy.core.optimizations.inline_datasource import InlineDatasource


def _child(parent: CTE, source_map: dict, product_id) -> CTE:
    return CTE(
        name="child",
        source=QueryDatasource(
            input_concepts=[product_id],
            output_concepts=[product_id],
            datasources=[parent.source],
            grain=BuildGrain(),
            joins=[],
            source_map={k: {parent.source} for k in source_map},
        ),
        output_columns=[product_id],
        parent_ctes=[parent],
        grain=BuildGrain(),
        source_map=source_map,
        existence_source_map={},
    )


def test_pseudonym_join_key_inlines(test_environment):
    env = test_environment.materialize_for_select()
    product_id = env.concepts["product_id"]
    canonical = "local.canonical_join_id"
    parent = CTE.from_datasource(env.datasources["products"])
    parent.name = "parent_products"
    # the datasource exposes the key only under its local address; the merge
    # pseudonym is the canonical address the consumer actually inherited.
    for column in parent.source.base_datasource.output_concepts:
        if column.address == product_id.address:
            column.pseudonyms.add(canonical)
    child = _child(parent, {canonical: [parent.name]}, product_id)

    assert InlineDatasource().optimize(child, {"parent_products": [child]})[0] is True


def test_absent_join_key_still_blocks_inline(test_environment):
    env = test_environment.materialize_for_select()
    product_id = env.concepts["product_id"]
    parent = CTE.from_datasource(env.datasources["products"])
    parent.name = "parent_products"
    # product_id resolves off the datasource, but the consumer also inherited a
    # column genuinely absent from it (no pseudonym) -> the fold must be blocked.
    child = _child(
        parent,
        {product_id.address: [parent.name], "local.not_on_datasource": [parent.name]},
        product_id,
    )

    assert InlineDatasource().optimize(child, {"parent_products": [child]})[0] is False
