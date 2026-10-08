"""`scan_partial_addresses` is the one rule for what a scan binds partially,
read by the network candidate, the scan node's stamp and the union node's."""

from trilogy import Environment
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.processing.scan_partials import scan_partial_addresses

MODEL = """
key item_sk int;
key ticket int;
property <item_sk, ticket>.qty int;
property <item_sk, ticket>.ret_qty int?;
property <item_sk, ticket>.reason_sk int;
auto is_returned <- ret_qty is not null;
auto doubled <- qty * 2;
auto returns_per_ticket <- count(ret_qty) by ticket;

datasource sales (sk: item_sk, t: ticket, q: qty)
grain (item_sk, ticket)
query '''select 1 as sk, 1 as t, 5 as q''';

datasource returns (sk: ~item_sk, t: ~ticket, rq: ret_qty, r: reason_sk)
grain (item_sk, ticket)
query '''select 1 as sk, 1 as t, 1 as rq, 1 as r''';
"""


def _build(model: str = MODEL):
    env = Environment()
    env.parse(model)
    return env.materialize_for_select()


def _stored(datasource) -> set[str]:
    return {c.concept.address for c in datasource.columns}


def test_tilde_columns_and_their_inline_derivations_are_partial():
    benv = _build()
    returns = benv.datasources["returns"]
    emitted = [
        benv.concepts[a]
        for a in (
            "local.item_sk",
            "local.ticket",
            "local.ret_qty",
            "local.is_returned",
            "local.returns_per_ticket",
        )
    ]
    partial = scan_partial_addresses(returns, emitted, _stored(returns))
    assert "local.item_sk" in partial
    assert "local.ticket" in partial
    assert "local.is_returned" in partial
    assert "local.ret_qty" not in partial
    assert "local.returns_per_ticket" not in partial


def test_a_complete_scan_binds_its_derivations_fully():
    benv = _build()
    sales = benv.datasources["sales"]
    emitted = [benv.concepts[a] for a in ("local.item_sk", "local.doubled")]
    assert scan_partial_addresses(sales, emitted, _stored(sales)) == set()


def test_an_exempt_key_completes_what_it_keys():
    benv = _build()
    returns = benv.datasources["returns"]
    emitted = [
        benv.concepts[a] for a in ("local.item_sk", "local.ticket", "local.is_returned")
    ]
    partial = scan_partial_addresses(
        returns, emitted, _stored(returns), exempt={"local.item_sk", "local.ticket"}
    )
    assert partial == set()


def test_exemption_matches_the_canonical_spelling():
    benv = _build()
    returns = benv.datasources["returns"]
    ticket = benv.concepts["local.ticket"]
    partial = scan_partial_addresses(
        returns,
        [ticket, benv.concepts["local.item_sk"]],
        _stored(returns),
        exempt={ticket.canonical_address},
    )
    assert partial == {"local.item_sk"}


def test_a_satisfied_complete_where_keeps_only_structural_partials():
    benv = _build(MODEL + """
datasource late_returns (sk: ~item_sk, t: ~ticket, rq: ret_qty)
grain (item_sk, ticket)
complete where ticket > 10
query '''select 1 as sk, 11 as t, 1 as rq''';
""")
    late = benv.datasources["late_returns"]
    emitted = [benv.concepts[a] for a in ("local.item_sk", "local.ticket")]
    assert scan_partial_addresses(late, emitted, _stored(late)) == {
        "local.item_sk",
        "local.ticket",
    }
    # `ticket` is the pin's own discriminator, healed by the pin; `item_sk`
    # stays an extension license
    assert scan_partial_addresses(
        late, emitted, _stored(late), partial_is_full=True
    ) == {"local.item_sk"}


def test_a_union_stamps_its_unhealed_tilde_and_derivations():
    benv = _build()
    returns = benv.datasources["returns"]
    union = BuildUnionDatasource(children=[returns])
    emitted = [benv.concepts[a] for a in ("local.ticket", "local.is_returned")]
    partial = scan_partial_addresses(union, emitted, _stored(union))
    assert "local.ticket" in partial
    assert "local.is_returned" in partial
