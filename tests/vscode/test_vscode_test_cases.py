from trilogy import Dialects


def test_vs_code_test_cases():
    raw = """

key uuid string;

key value int;

key date string;

key other_thing string;

datasource db (
    uuid:uuid,
    value:value,
    date:date,
    other_string:other_string,
)
address test;


auto max_val <- max(value) by uuid;
auto min_val <- min(value) by uuid;

select
    other_thing,
    date,
    avg(max_val) -> avg_max_val
;

PERSIST db INTO db FROM  SELECT
a,b,c;



SELECT 
    other_thing,
    date,
    avg(max_val) -> avg_max_val
;


RAW_SQL('''
select 1''');
"""

    parsed = Dialects.DUCK_DB.default_executor().parse_text(raw)