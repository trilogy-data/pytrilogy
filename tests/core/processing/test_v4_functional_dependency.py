from trilogy import Environment
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.models.build import BuildGrain
from trilogy.core.processing.v4_helper import functional_dependency as fd
from trilogy.core.processing.v4_helper.constants import DepthLabel
from trilogy.core.processing.v4_helper.models import ConceptAttrs

# orders binds customer_id at grain(order_id) and customers binds nation_id at
# grain(customer_id), so fk-derived keys give the chain
# order_id -> customer_id -> nation_id -> nation_name.
FD_MODEL = """
key order_id int;
key customer_id int;
key nation_id int;
key store_id int;
property order_id.order_total float;
property customer_id.customer_name string;
property nation_id.nation_name string;
property <customer_id,store_id>.pair_note string;
const pi <- 3.14;

datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    order_total: order_total,
)
grain (order_id)
query '''select 1 order_id, 10 customer_id, 5.0 order_total''';

datasource customers (
    customer_id: customer_id,
    nation_id: nation_id,
    customer_name: customer_name,
)
grain (customer_id)
query '''select 10 customer_id, 100 nation_id, 'acme' customer_name''';

datasource nations (
    nation_id: nation_id,
    nation_name: nation_name,
)
grain (nation_id)
query '''select 100 nation_id, 'zed' nation_name''';
"""


def _build(model: str = FD_MODEL):
    env = Environment()
    env.parse(model)
    return env.materialize_for_select()


class TestBuildFDClosure:
    def test_grain_bound_property_folds(self):
        benv = _build()
        assert fd.build_fd_determines(benv, {"local.order_id"}, "local.order_total")
        assert not fd.build_fd_determines(benv, {"local.order_total"}, "local.order_id")

    def test_transitive_fk_chain(self):
        benv = _build()
        assert fd.build_fd_determines(benv, {"local.order_id"}, "local.nation_name")
        assert not fd.build_fd_determines(benv, {"local.customer_id"}, "local.order_id")

    def test_composite_key_requires_every_component(self):
        benv = _build()
        assert fd.build_fd_determines(
            benv, {"local.customer_id", "local.store_id"}, "local.pair_note"
        )
        assert not fd.build_fd_determines(
            benv, {"local.customer_id"}, "local.pair_note"
        )
        assert not fd.build_fd_determines(benv, {"local.store_id"}, "local.pair_note")

    def test_determinant_determines_itself(self):
        benv = _build()
        assert fd.build_fd_determines(benv, {"local.order_id"}, "local.order_id")

    def test_unknown_address_not_determined(self):
        benv = _build()
        assert not fd.build_fd_determines(benv, {"local.order_id"}, "local.missing")

    def test_include_empty_grain_flag(self):
        benv = _build()
        included = fd.build_fd_closure(
            benv, {"local.order_id"}, include_empty_grain=True
        )
        excluded = fd.build_fd_closure(
            benv, {"local.order_id"}, include_empty_grain=False
        )
        assert "local.pi" in included
        assert "local.pi" not in excluded

    def test_keys_fold_a_concept_with_no_grain(self):
        # Select-processing virtuals (q28's _virt_filter_lp_*) carry declared
        # keys but an empty grain; the keys FD must still fold them under
        # include_empty_grain=False.
        benv = _build()
        benv.concepts["local.customer_name"].grain = BuildGrain(components=set())
        fd._FACTS_CACHE.clear()

        determined = fd.build_fd_closure(
            benv, {"local.customer_id"}, include_empty_grain=False
        )
        undetermined = fd.build_fd_closure(
            benv, {"local.order_total"}, include_empty_grain=False
        )

        assert "local.customer_name" in determined
        assert "local.customer_name" not in undetermined

    def test_closure_expands_pseudonyms_of_members(self):
        benv = _build()
        benv.concepts["local.customer_name"].pseudonyms.add("local.cname_alias")
        fd._FACTS_CACHE.clear()

        closure = fd.build_fd_closure(benv, {"local.customer_id"})

        assert "local.customer_name" in closure
        assert "local.cname_alias" in closure


class TestMinimizeBuildGrain:
    def test_folds_fk_chain_to_root_key(self):
        benv = _build()
        assert fd.minimize_build_grain(
            benv, {"local.order_id", "local.customer_id", "local.nation_id"}
        ) == {"local.order_id"}

    def test_property_folds_into_its_key(self):
        benv = _build()
        assert fd.minimize_build_grain(
            benv, {"local.order_id", "local.order_total"}
        ) == {"local.order_id"}

    def test_keeps_independent_keys(self):
        benv = _build()
        assert fd.minimize_build_grain(
            benv, {"local.customer_id", "local.store_id"}
        ) == {"local.customer_id", "local.store_id"}

    def test_singleton_unchanged(self):
        benv = _build()
        assert fd.minimize_build_grain(benv, {"local.order_id"}) == {"local.order_id"}


def _attrs(
    address: str,
    grain: frozenset[str] = frozenset(),
    keys: frozenset[str] = frozenset(),
) -> ConceptAttrs:
    return ConceptAttrs(
        address=address,
        label="",
        derivation=Derivation.ROOT,
        purpose=Purpose.KEY,
        granularity=Granularity.MULTI_ROW,
        depth_label=DepthLabel.STAR,
        grain_components=grain,
        keys=keys,
    )


class TestConceptAttrFDClosure:
    def test_grain_subset_rule(self):
        attrs = {
            "k": _attrs("k", grain=frozenset({"k"})),
            "v": _attrs("v", grain=frozenset({"k"})),
        }
        assert fd.concept_attr_fd_determines(attrs, {"k"}, "v")
        assert not fd.concept_attr_fd_determines(attrs, {"v"}, "k")

    def test_keys_rule_and_transitivity(self):
        attrs = {
            "a": _attrs("a", grain=frozenset({"a"})),
            "b": _attrs("b", grain=frozenset({"b"}), keys=frozenset({"a"})),
            "c": _attrs("c", grain=frozenset({"b"})),
        }
        assert fd.concept_attr_fd_determines(attrs, {"a"}, "c")
        assert not fd.concept_attr_fd_determines(attrs, {"c"}, "a")

    def test_include_empty_grain_flag(self):
        attrs = {"free": _attrs("free")}
        assert fd.concept_attr_fd_determines(attrs, {"k"}, "free")
        assert "free" not in fd.concept_attr_fd_closure(
            attrs, {"k"}, include_empty_grain=False
        )

    def test_unknown_address_not_determined(self):
        attrs = {"k": _attrs("k", grain=frozenset({"k"}))}
        assert not fd.concept_attr_fd_determines(attrs, {"k"}, "missing")

    def test_keys_fold_without_grain_matches_build_closure(self):
        attrs = {
            "k": _attrs("k", grain=frozenset({"k"})),
            "v": _attrs("v", keys=frozenset({"k"})),
        }
        closure = fd.concept_attr_fd_closure(attrs, {"k"}, include_empty_grain=False)
        assert "v" in closure
