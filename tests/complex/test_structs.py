from trilogy import Dialects
from trilogy.core.models_core import StructType


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
