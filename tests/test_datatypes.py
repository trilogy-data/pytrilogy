from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructComponent,
    StructType,
    TraitDataType,
    merge_datatypes,
)
from trilogy.parsing.parse_engine import (
    parse_text,
)

from trilogy import Dialects


def test_numeric():
    env, _ = parse_text(
        "const order_id numeric(12,2); auto rounded <- cast(order_id as numeric(15,2));"
    )
    assert env.concepts["order_id"].datatype == NumericType(precision=12, scale=2)


def test_cast_error():
    found = False
    try:
        env, _ = parse_text(
            """
    const x <- 1;
    const y <- 'fun';

    select 
        x
    where 
        x = y;

    """
        )
    except InvalidSyntaxException as e:
        assert "Cannot compare INTEGER (ref:local.x) and STRING (ref:local.y)" in str(e)
        found = True
    if not found:
        assert False, "Expected InvalidSyntaxException not raised"


def test_is_error():
    found = False
    try:
        env, _ = parse_text(
            """
    const x <- 1;
    const y <- 'fun';

    select
        x
    where
        x is [1,2];

    """
        )
    except InvalidSyntaxException as e:
        assert "Cannot use is with non-null or boolean value [1, 2]" in str(e)
        found = True
    if not found:
        assert False, "Expected InvalidSyntaxException not raised"


def test_merge_datatypes_single_input():
    assert merge_datatypes([DataType.INTEGER]) == DataType.INTEGER
    assert merge_datatypes([DataType.STRING]) == DataType.STRING


def test_merge_datatypes_integer_float():
    assert merge_datatypes([DataType.INTEGER, DataType.FLOAT]) == DataType.FLOAT
    assert merge_datatypes([DataType.FLOAT, DataType.INTEGER]) == DataType.FLOAT


def test_merge_datatypes_integer_numeric():
    assert merge_datatypes([DataType.INTEGER, DataType.NUMERIC]) == DataType.NUMERIC
    assert merge_datatypes([DataType.NUMERIC, DataType.INTEGER]) == DataType.NUMERIC


def test_merge_datatypes_with_numeric_type():
    numeric = NumericType(precision=10, scale=2)
    assert merge_datatypes([numeric, DataType.INTEGER]) == numeric
    assert merge_datatypes([DataType.FLOAT, numeric]) == numeric
    assert merge_datatypes([numeric, DataType.NUMERIC]) == numeric


def test_merge_datatypes_with_array_type():
    arr = ArrayType(type=DataType.INTEGER)
    assert merge_datatypes([arr]) == arr
    assert merge_datatypes([arr, arr]) == arr


def test_merge_datatypes_with_struct_type():
    struct = StructType(
        fields=[StructComponent(name="x", type=DataType.INTEGER)],
        fields_map={"x": StructComponent(name="x", type=DataType.INTEGER)},
    )
    assert merge_datatypes([struct]) == struct
    assert merge_datatypes([struct, struct]) == struct


def test_merge_datatypes_with_map_type():
    map_type = MapType(key_type=DataType.STRING, value_type=DataType.INTEGER)
    assert merge_datatypes([map_type]) == map_type
    assert merge_datatypes([map_type, map_type]) == map_type


def test_merge_datatypes_with_trait_datatype():
    trait = TraitDataType(type=DataType.INTEGER, traits=["sortable"])
    assert merge_datatypes([trait]) == trait
    assert merge_datatypes([trait, trait]) == trait


def test_trait_propagation():
    engine = Dialects.DUCK_DB.default_executor()

    engine.parse_text(
        """
                      
type money float;
key order_id int;
property order_id.price float::money;
                      
datasource orders(
    order_id: order_id,
    price: price
)
                      
grain (order_id)
address orders;
                      
select 
    count(order_id) as order_count,
    sum(price) as total_revenue,
    total_revenue/order_count as aov;

"""
    )

    assert engine.environment.concepts["aov"].datatype == TraitDataType(
        type=DataType.FLOAT, traits=["money"]
    )
