import pytest

from trilogy import parse
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.author import (
    Concept,
    ConceptRef,
    DataType,
    SubselectItem,
)
from trilogy.core.processing.node_generators.subselect_node import (
    resolve_subselect_parent_concepts,
)


def test_subselect_item_repr():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref)
    assert "subselect(" in repr(item)
    assert "subselect(" in str(item)


def test_subselect_item_repr_with_limit():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref, limit=5)
    r = repr(item)
    assert "subselect(" in r
    assert "limit" in r


def test_subselect_item_concept_arguments_no_outer():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref)
    args = item.concept_arguments
    assert len(args) == 1
    assert args[0].address == "local.val"


def test_subselect_item_concept_arguments_with_outer():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    outer = ConceptRef(address="local.lat", datatype=DataType.FLOAT)
    item = SubselectItem(content=ref, outer_arguments=[outer])
    args = item.concept_arguments
    assert len(args) == 1
    assert args[0].address == "local.lat"


def test_subselect_item_output_datatype():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref)
    dt = item.output_datatype
    assert dt.type == DataType.INTEGER


def test_subselect_item_with_namespace():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref, limit=3)
    ns_item = item.with_namespace("ns")
    assert ns_item.limit == 3


def test_subselect_item_with_merge():
    env, _ = parse("""
key id int;
property id.val int;
    """)
    source = env.concepts["val"]
    target = env.concepts["val"]
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref, limit=2)
    merged = item.with_merge(source, target, [])
    assert merged.limit == 2


def test_subselect_item_with_reference_replacement():
    ref = ConceptRef(address="local.val", datatype=DataType.INTEGER)
    replacement = ConceptRef(address="local.new_val", datatype=DataType.INTEGER)
    item = SubselectItem(content=ref, limit=2)
    replaced = item.with_reference_replacement(ref, replacement)
    assert replaced.limit == 2


def test_resolve_subselect_parent_concepts_wrong_lineage():
    env, _ = parse("""
key id int;
property id.val int;
    """)
    env = env.materialize_for_select()
    concept = env.concepts["val"]
    with pytest.raises(ValueError, match="Expected subselect lineage"):
        resolve_subselect_parent_concepts(concept, env, 0)


def test_parse_def_table_basic():
    env, _ = parse("""
key id int;
property id.val int;
datasource nums(id: id, val: val)
grain (id)
address test_addr;

def table top_vals() -> select val order by val desc limit 3;
    """)
    assert "top_vals" in env.functions


def test_parse_def_table_with_args():
    env, _ = parse("""
key id int;
property id.val int;
property id.lat float;
datasource nums(id: id, val: val, lat: lat)
grain (id)
address test_addr;

def table nearby(x) -> select val order by val desc limit 2;
    """)
    assert "nearby" in env.functions


def test_parse_def_table_with_where():
    env, _ = parse("""
key id int;
property id.val int;
property id.category string;
datasource nums(id: id, val: val, category: category)
grain (id)
address test_addr;

def table top_by_cat() -> select val where category order by val desc limit 2;
    """)
    assert "top_by_cat" in env.functions


def test_parse_def_table_with_filter_where():
    env, _ = parse("""
key id int;
property id.val int;
datasource nums(id: id, val: val)
grain (id)
address test_addr;

def table filtered() -> select val where val > 20 order by val asc limit 2;
    """)
    assert "filtered" in env.functions


def test_subselect_concept_derivation():
    env, queries = parse("""
key id int;
property id.val int;
datasource nums(id: id, val: val)
grain (id)
address test_addr;

def table top_vals() -> select val order by val desc limit 3;
select @top_vals() as top;
    """)
    top = [c for c in env.concepts.values() if c.derivation == Derivation.SUBSELECT]
    assert len(top) > 0
    assert top[0].purpose == Purpose.PROPERTY


def test_subselect_nested_in_function():
    """Verify subselect works as argument to built-in function."""
    env, queries = parse("""
key id int;
property id.val int;
datasource nums(id: id, val: val)
grain (id)
address test_addr;

def table top_vals() -> select val order by val desc limit 3;
auto result <- unnest(@top_vals());
select result;
    """)
    assert len(queries) > 0


def test_subselect_item_enforce_concept_ref():
    """Validator should convert Concept to ConceptRef."""
    env, _ = parse("""
key id int;
property id.val int;
    """)
    concept = env.concepts["val"]
    assert isinstance(concept, Concept)
    item = SubselectItem(content=concept)
    assert isinstance(item.content, ConceptRef)


def test_inline_subselect_with_where():
    """Parse inline SUBSELECT(...) with WHERE clause."""
    env, queries = parse("""
key id int;
property id.val int;
property id.category string;
datasource nums(id: id, val: val, category: category)
grain (id)
address test_addr;

auto top_items <- SUBSELECT(val WHERE category = 'a' ORDER BY val DESC LIMIT 5);
select top_items;
    """)
    assert len(queries) > 0
    top = [c for c in env.concepts.values() if c.derivation == Derivation.SUBSELECT]
    assert len(top) > 0


def test_inline_subselect_basic():
    """Parse inline SUBSELECT(...) without where."""
    env, queries = parse("""
key id int;
property id.val int;
datasource nums(id: id, val: val)
grain (id)
address test_addr;

auto top_items <- SUBSELECT(val ORDER BY val DESC LIMIT 3);
select top_items;
    """)
    assert len(queries) > 0
