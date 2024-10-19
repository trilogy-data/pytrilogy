from trilogy import Dialects
from trilogy.core.models import StructType


def test_struct_in_array_parsing():
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.parse_text(
        """

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
    
                                    """
    )
    assert isinstance(
        executor.environment.concepts["unnest_array"].datatype, StructType
    )
    b_side = executor.environment.concepts["b"]
    assert (
        "unnest_array.b" in executor.environment.concepts["b"].pseudonyms
    ), b_side.pseudonyms
    assert (
        "unnest_array.b"
        in executor.environment.concepts["local.b"].pseudonyms
        is not None
    ), b_side.pseudonyms
    for x in results[-1].output_columns:
        assert len(list(x.pseudonyms)) == 1, x.pseudonyms
    results = executor.execute_text(
        """

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
                          """
    )
    assert results[-1].fetchall()[0].b == 2


#     results = executor.execute_text("""

# key a int;
# key b int;
# key array_struct list<struct<a,b>>;

# auto unnest_array<-unnest(array_struct);

# datasource struct_array (
#     array_struct: array_struct
# )
# grain (array_struct)
# query '''

# select [{a: 1, b: 2}, {a: 3, b: 4}] as array_struct
# '''
# ;


# SELECT
#     a,
#     b,
# order by
#     a asc
#                                     ;
#                           """
#                          )
#     assert results[-1].fetchall()[0].b == 2
