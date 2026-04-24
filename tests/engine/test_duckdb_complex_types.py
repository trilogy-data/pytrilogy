from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment


def test_complex_map_struct_access():
    """Test map<string, struct> access."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.data map<string, struct<a:int, b:int, c:int>>;

datasource test_data (
    id,
    data
)
grain (id)
query \'\'\'select 1 as id, MAP {'c': {'a': 1, 'b': 2, 'c': 3}, 'd': {'a': 4, 'b': 5, 'c': 6}} as data\'\'\';

select id, data['c'].a as a_value, map_keys(data) as map_keys;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].a_value == 1
    assert results[0].map_keys == ["c", "d"]


def test_array_struct_access():
    """Test array<struct> access."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.items array<struct<name:string, value:int>>;

datasource test_data (
    id,
    items
)
grain (id)
query \'\'\'select 1 as id, [{'name': 'first', 'value': 10}, {'name': 'second', 'value': 20}] as items\'\'\';

select id, items[1].name as first_name, items[2].value as second_value;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].first_name == "first"
    assert results[0].second_value == 20


def test_array_map_access():
    """Test array<map> access."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.maps array<map<string, int>>;

datasource test_data (
    id,
    maps
)
grain (id)
query \'\'\'select 1 as id, [MAP {'a': 1, 'b': 2}, MAP {'x': 10, 'y': 20}] as maps\'\'\';

select id, maps[1]['a'] as first_a, maps[2]['y'] as second_y;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].first_a == 1
    assert results[0].second_y == 20


def test_map_array_access():
    """Test map<string, array> access."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.data map<string, array<int>>;

datasource test_data (
    id,
    data
)
grain (id)
query \'\'\'select 1 as id, MAP {'nums': [1, 2, 3], 'more': [10, 20, 30]} as data\'\'\';

select id, data['nums'][1] as first_num, data['more'][3] as last_more;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].first_num == 1
    assert results[0].last_more == 30


def test_struct_array_access():
    """Test struct with array field access."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.container struct<name:string, values:array<int>>;

datasource test_data (
    id,
    container
)
grain (id)
query \'\'\'select 1 as id, {'name': 'test', 'values': [100, 200, 300]} as container\'\'\';

select id, container.name as container_name, container.values[2] as second_value;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].container_name == "test"
    assert results[0].second_value == 200


def test_map_array_struct_triple_chain():
    """Test triple chained access: map -> array -> struct."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.data map<string, array<struct<x:int, y:int>>>;

datasource test_data (
    id,
    data
)
grain (id)
query \'\'\'select 1 as id, MAP {'points': [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}], 'coords': [{'x': 10, 'y': 20}]} as data\'\'\';

select id, data['points'][1].x as first_x, data['points'][2].y as second_y, data['coords'][1].x as coord_x;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].first_x == 1
    assert results[0].second_y == 4
    assert results[0].coord_x == 10


def test_struct_map_array_triple_chain():
    """Test triple chained access: struct -> map -> array."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.container struct<label:string, lookup:map<string, array<int>>>;

datasource test_data (
    id,
    container
)
grain (id)
query \'\'\'select 1 as id, {'label': 'mydata', 'lookup': MAP {'a': [1, 2, 3], 'b': [4, 5, 6]}} as container\'\'\';

select id, container.label as label, container.lookup['a'][2] as a_second, container.lookup['b'][1] as b_first;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].label == "mydata"
    assert results[0].a_second == 2
    assert results[0].b_first == 4


def test_array_struct_map_triple_chain():
    """Test triple chained access: array -> struct -> map."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
key id int;
property id.items array<struct<name:string, attrs:map<string, int>>>;

datasource test_data (
    id,
    items
)
grain (id)
query \'\'\'select 1 as id, [{'name': 'item1', 'attrs': MAP {'weight': 10, 'height': 5}}, {'name': 'item2', 'attrs': MAP {'weight': 20, 'height': 15}}] as items\'\'\';

select id, items[1].name as first_name, items[1].attrs['weight'] as first_weight, items[2].attrs['height'] as second_height;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].first_name == "item1"
    assert results[0].first_weight == 10
    assert results[0].second_height == 15


def test_inline_map_struct_access():
    """Test chained access with inline map/struct literals."""
    environment = Environment()

    executor = Dialects.DUCK_DB.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    rows = """
auto map_struct <- { 'c': struct(1->a,2->b,3->c), 'd': struct(4->a,5->b,6->c) };

select map_struct['c'].a as a_value, map_keys(map_struct) as map_keys;
"""

    results = executor.execute_text(rows)[-1].fetchall()
    assert len(results) == 1
    assert results[0].a_value == 1
    assert results[0].map_keys == ["c", "d"]
