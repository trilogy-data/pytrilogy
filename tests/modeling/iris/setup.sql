

INSTALL httpfs;

LOAD httpfs;

CREATE OR REPLACE TABLE  genus_info AS 
SELECT *,
row_number() over () as sample_id 
from read_csv_auto('tests/modeling/iris/distribution.csv',
sample_size=-1);
