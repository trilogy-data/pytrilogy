# from preql.compiler import compile
from preql.core.models import Select, Grain
from preql.core.query_processor import process_query
from preql.parser import parse

TEST_SETUP = r"""


key order_key int;
property order_key.order_amount float;
metric total_order_amount <- sum(order_amount);

key key int;
property key.weight float;
property key.name string;


key sub_category_key int;
key category_key int;

property category_key.category_name string;


datasource product_info (
productKey:key,
productSubCategoryKey:sub_category_key,
englishProductName:name
)
grain (key)
address AdventureWorksDW2019.dbo.DimProduct
;

datasource orders (
order:order_key,
product_key:key,
order_amount:order_amount
)
grain(order_key)
address AdventureWorksDW2019.dbo.DimOrder;


datasource product_sub_category (
productSubCategoryKey:sub_category_key,
productCategoryKey:category_key,
)
grain (sub_category_key)
address AdventureWorksDW2019.dbo.DimProductSubCategory
;

datasource product_category (
productCategoryKey:category_key,
englishProductCategoryName:category_name
)
address AdventureWorksDW2019.dbo.DimProductCategory
;

select 
    category_name,
    total_order_amount;
"""


def test_select():
    env, parsed = parse(TEST_SETUP)
    select: Select = parsed[-1]

    assert select.grain == Grain(components=[env.concepts["category_name"]])

    process_query(statement=select, environment=env)


def test_joins_to_ctes():
    from preql.core.processing.utility import parse_path_to_matches

    TEST = [
        "ds~internet_sales.fact_internet_sales",
        "c~product.key@Grain<order_line_number>",
        "c~product.key@Grain<key>",
        "ds~product.product_info",
        "c~product.sub_category_key@Grain<key>",
        "c~product.sub_category_key@Grain<sub_category_key>",
        "ds~product.product_sub_category",
        "c~product.category_key@Grain<sub_category_key>",
        "c~product.category_key@Grain<category_key>",
    ]

    assert len(parse_path_to_matches(TEST)) == 2
