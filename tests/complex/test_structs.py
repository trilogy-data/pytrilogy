from trilogy import Dialects
from trilogy.core.models.core import DataType, StructType


def test_anon_struct():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text("""

key wrapper struct<a:int,b:int>;

datasource struct_array (
    wrapper
)
grain (wrapper)
query '''                    
select {a: 1, b: 2} as wrapper union all select {a: 3, b: 4}
'''
;
""")
    results = executor.execute_text("""
select 
    sum(wrapper.a) as a;
""")[0].fetchall()

    assert len(results) == 1
    assert results[0].a == 4, results[0].a


def test_lambda_access():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text("""

key wrapper struct<a:int,b:int>;

datasource struct_array (
    wrapper
)
grain (wrapper)
query '''                    
select {a: 1, b: 2} as wrapper union all select {a: 3, b: 4}
'''
;
""")
    results = executor.execute_text("""
def get_a(x)-> x.a; 
select 
    sum(@get_a(wrapper)) as a;
""")[0].fetchall()

    assert len(results) == 1
    assert results[0].a == 4, results[0].a


def test_array_struct_lambda():
    executor = Dialects.DUCK_DB.default_executor()
    rows = executor.execute_text("""


key array_struct list<struct<a:int,b:int>>;

auto unnest_array<-unnest(array_struct);
                                    
datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''                    
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;

def get_a(xz)-> getattr(xz,'a');          
def get_a2(x2)-> x2.a;       
SELECT
  array_sum(array_transform(array_struct,@get_a)) as total_a,
   @get_a(struct(1->a, 2->b)) as a2,
   @get_a2(struct(1->a, 2->b)) as a22
;
    
  """)[-1].fetchall()
    assert len(rows) == 1
    assert executor.environment.concepts["a2"].datatype == DataType.INTEGER
    assert executor.environment.concepts["a22"].datatype == DataType.INTEGER
    assert rows[0].total_a == 4, rows[0].total_a


def test_struct_in_array_parsing():
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.parse_text("""

key a int;
key b int;
key array_struct list<struct<a,b>>;

auto unnest_array<-unnest(array_struct);
                                    
datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''                    
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;
                          
SELECT
    unnest_array.a,
    unnest_array.b,
;
    
                                    """)
    assert isinstance(
        executor.environment.concepts["unnest_array"].datatype, StructType
    )

    for val in ["a", "b"]:
        comp = executor.environment.concepts[val]
        assert (
            f"unnest_array.{val}" in executor.environment.concepts[val].pseudonyms
        ), comp
        assert (
            f"unnest_array.{val}"
            in executor.environment.concepts[f"local.{val}"].pseudonyms
            is not None
        ), comp
        assert (
            f"local.{val}"
            in executor.environment.alias_origin_lookup[
                f"unnest_array.{val}"
            ].pseudonyms
        )

    # for x in results[-1].output_columns:
    #     assert len(list(x.pseudonyms)) == 1, x.pseudonyms
    results = executor.execute_text("""

key a int;
key b int;
key array_struct list<struct<a,b>>;

auto unnest_array<-unnest(array_struct);

datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''                    
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;
                          

SELECT
    unnest_array.a,
    unnest_array.b,
order by
    unnest_array.a asc
;
                          """)
    rows = results[-1].fetchall()
    assert len(rows) == 2, rows
    assert rows[0].b == 2


def test_struct_in_array_concept_parsing():
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.parse_text("""

key a int;
key b int;
key wrapper struct<a,b>;
key array_struct list<wrapper>;

auto unnest_array<-unnest(array_struct);
                                    
datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''                    
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;
                          
SELECT
    unnest_array
;
    
                                    """)
    assert isinstance(
        executor.environment.concepts["unnest_array"].datatype, StructType
    )

    for val in ["a", "b"]:
        comp = executor.environment.concepts[val]
        assert (
            f"unnest_array.{val}" in executor.environment.concepts[val].pseudonyms
        ), comp
        assert (
            f"unnest_array.{val}"
            in executor.environment.concepts[f"local.{val}"].pseudonyms
            is not None
        ), comp
        assert (
            f"local.{val}"
            in executor.environment.alias_origin_lookup[
                f"unnest_array.{val}"
            ].pseudonyms
        )

    results = executor.execute_text("""
key a int;
key b int;
key wrapper struct<a,b>;
key array_struct list<wrapper>;

auto unnest_array<-unnest(array_struct);
                                    
datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''                    
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;
                          
SELECT
    unnest_array
;
                          """)
    rows = results[-1].fetchall()
    assert len(rows) == 2, rows
    assert rows[0].unnest_array["b"] == 2


def test_struct_in_array_item_access():
    executor = Dialects.DUCK_DB.default_executor()

    results = executor.execute_text("""
key a int;
key b int;
key wrapper struct<a,b>;
key array_struct list<wrapper>;

auto unnest_array<-unnest(array_struct);
merge unnest_array into wrapper;
                                    
datasource struct_array (
    array_struct: array_struct
)
grain (array_struct)
query '''                    
select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
'''
;
                          
SELECT
    wrapper.a,
    b
order by
    wrapper.a asc
    ;
                          """)
    rows = results[-1].fetchall()
    assert len(rows) == 2, rows
    assert rows[0].a == 1
