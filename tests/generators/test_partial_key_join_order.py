# """
# Test join order resolution with partial keys.

# When two fact tables each have partial keys that merge into shared dimension keys,
# the join generation should:
# 1. FULL OUTER JOIN all partial-keyed fact tables together first
# 2. Then join dimensions using COALESCE on the combined partial keys

# This ensures we get the complete set from both fact tables before joining to dimensions.
# """

# from trilogy import Dialects
# from trilogy.core.models.environment import Environment


# def test_double_partial_key_join_order():
#     """
#     Test case: two fact tables (fact1, fact2) with two shared dimension keys
#     (customer_id, item_id) joined as partial keys, plus two dimensions (customer, item).

#     The expected join pattern should be:
#     - fact1 FULL OUTER JOIN fact2 ON customer_id AND item_id
#     - Then join customer dimension on COALESCE(fact1.customer_id, fact2.customer_id)
#     - Then join item dimension on COALESCE(fact1.item_id, fact2.item_id)

#     The current (broken) behavior may be:
#     - Starting from one fact table and joining dimensions before properly
#       combining the fact tables
#     """
#     env = Environment()

#     env.parse(
#         """
# # Dimensions
# key customer_id int;
# property customer_id.customer_name string;

# key item_id int;
# property item_id.item_name string;

# datasource customers (
#     id:customer_id,
#     name:customer_name
# )
# grain (customer_id)
# query '''
# SELECT 1 as id, 'Alice' as name
# UNION ALL SELECT 2 as id, 'Bob' as name
# UNION ALL SELECT 3 as id, 'Charlie' as name
# ''';

# datasource items (
#     id:item_id,
#     name:item_name
# )
# grain (item_id)
# query '''
# SELECT 100 as id, 'Widget' as name
# UNION ALL SELECT 200 as id, 'Gadget' as name
# UNION ALL SELECT 300 as id, 'Gizmo' as name
# ''';

# # Fact tables with partial keys to dimensions
# key fact1_id int;
# property fact1_id.fact1_customer_id int;
# property fact1_id.fact1_item_id int;
# property fact1_id.fact1_value int;

# key fact2_id int;
# property fact2_id.fact2_customer_id int;
# property fact2_id.fact2_item_id int;
# property fact2_id.fact2_value int;

# datasource fact1 (
#     id:fact1_id,
#     customer_id:fact1_customer_id,
#     item_id:fact1_item_id,
#     value:fact1_value
# )
# grain (fact1_id)
# query '''
# SELECT 1 as id, 1 as customer_id, 100 as item_id, 10 as value
# UNION ALL SELECT 2 as id, 1 as customer_id, 200 as item_id, 20 as value
# UNION ALL SELECT 3 as id, 2 as customer_id, 100 as item_id, 30 as value
# ''';

# datasource fact2 (
#     id:fact2_id,
#     customer_id:fact2_customer_id,
#     item_id:fact2_item_id,
#     value:fact2_value
# )
# grain (fact2_id)
# query '''
# SELECT 1 as id, 2 as customer_id, 200 as item_id, 15 as value
# UNION ALL SELECT 2 as id, 3 as customer_id, 100 as item_id, 25 as value
# UNION ALL SELECT 3 as id, 3 as customer_id, 300 as item_id, 35 as value
# ''';

# # Merge the partial keys into shared dimension keys
# merge fact1_customer_id into customer_id;
# merge fact2_customer_id into customer_id;
# merge fact1_item_id into item_id;
# merge fact2_item_id into item_id;
# """
#     )

#     exec = Dialects.DUCK_DB.default_executor(environment=env)

#     # Query that requires both facts and both dimensions
#     # This should require proper partial key handling
#     test_select = """
# SELECT
#     customer_name,
#     item_name,
#     sum(fact1_value) -> total_fact1_value,
#     sum(fact2_value) -> total_fact2_value
# ;
# """

#     sql = exec.generate_sql(test_select)
#     compiled_sql = sql[-1]

#     # Check the generated SQL structure
#     # The key assertions are about the join pattern

#     # We should see:
#     # 1. Both fact tables being FULL OUTER JOINed first
#     # 2. Dimensions joined with COALESCE-based keys

#     # Check for FULL JOIN between facts (they should be joined together)
#     has_full_join = "FULL" in compiled_sql.upper()

#     # Check for COALESCE usage on dimension joins
#     # This is the key indicator that partial keys are properly handled
#     has_coalesce = "COALESCE" in compiled_sql.upper()

#     print("Generated SQL:")
#     print(compiled_sql)
#     print()
#     print(f"Has FULL JOIN: {has_full_join}")
#     print(f"Has COALESCE: {has_coalesce}")

#     # The test SHOULD FAIL with current implementation because:
#     # - Dimensions are joined without COALESCE
#     # - We don't properly combine the partial keys from both fact tables

#     # This assertion checks that dimensions are joined using COALESCE
#     # which ensures we get the full set from both fact tables
#     assert has_coalesce, (
#         "Expected COALESCE in dimension joins to handle partial keys from multiple "
#         f"fact tables. Generated SQL:\n{compiled_sql}"
#     )


# def test_double_partial_key_results():
#     """
#     Test the actual results of a partial key join scenario.

#     This tests that we get all combinations of customer/item that appear
#     in EITHER fact table, not just the ones that appear in one or the other.
#     """
#     env = Environment()

#     env.parse(
#         """
# # Dimensions
# key customer_id int;
# property customer_id.customer_name string;

# key item_id int;
# property item_id.item_name string;

# datasource customers (
#     id:customer_id,
#     name:customer_name
# )
# grain (customer_id)
# query '''
# SELECT 1 as id, 'Alice' as name
# UNION ALL SELECT 2 as id, 'Bob' as name
# UNION ALL SELECT 3 as id, 'Charlie' as name
# ''';

# datasource items (
#     id:item_id,
#     name:item_name
# )
# grain (item_id)
# query '''
# SELECT 100 as id, 'Widget' as name
# UNION ALL SELECT 200 as id, 'Gadget' as name
# UNION ALL SELECT 300 as id, 'Gizmo' as name
# ''';

# # Fact table 1: has customer 1 with items 100, 200
# key fact1_id int;
# property fact1_id.fact1_customer_id int;
# property fact1_id.fact1_item_id int;

# datasource fact1 (
#     id:fact1_id,
#     customer_id:fact1_customer_id,
#     item_id:fact1_item_id
# )
# grain (fact1_id)
# query '''
# SELECT 1 as id, 1 as customer_id, 100 as item_id
# UNION ALL SELECT 2 as id, 1 as customer_id, 200 as item_id
# ''';

# # Fact table 2: has customer 3 with item 300
# key fact2_id int;
# property fact2_id.fact2_customer_id int;
# property fact2_id.fact2_item_id int;

# datasource fact2 (
#     id:fact2_id,
#     customer_id:fact2_customer_id,
#     item_id:fact2_item_id
# )
# grain (fact2_id)
# query '''
# SELECT 1 as id, 3 as customer_id, 300 as item_id
# ''';

# # Merge the partial keys into shared dimension keys
# merge fact1_customer_id into customer_id;
# merge fact2_customer_id into customer_id;
# merge fact1_item_id into item_id;
# merge fact2_item_id into item_id;
# """
#     )

#     exec = Dialects.DUCK_DB.default_executor(environment=env)

#     # Query customer names that appear in either fact table
#     test_select = """
# SELECT
#     customer_name,
#     count(fact1_id) -> fact1_count,
#     count(fact2_id) -> fact2_count
# ;
# """

#     sql = exec.generate_sql(test_select)
#     compiled_sql = sql[-1]

#     print("Generated SQL:")
#     print(compiled_sql)

#     results = list(exec.execute_text(test_select)[0].fetchall())

#     # We should get customer names for all customers that appear in either fact table
#     # fact1 has customer 1 (Alice)
#     # fact2 has customer 3 (Charlie)
#     # So we should get both Alice and Charlie

#     customer_names = set(r.customer_name for r in results)
#     print(f"Results: {results}")
#     print(f"Customer names: {customer_names}")

#     # This assertion should FAIL if partial keys aren't properly handled
#     # because one of the fact tables' customers might be excluded
#     assert customer_names == {"Alice", "Charlie"}, (
#         f"Expected customers from both fact tables. Got: {customer_names}. "
#         f"SQL:\n{compiled_sql}"
#     )
