COPY customer FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/customer.parquet' (FORMAT 'parquet');
COPY lineitem FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/lineitem.parquet' (FORMAT 'parquet');
COPY nation FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/nation.parquet' (FORMAT 'parquet');
COPY orders FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/orders.parquet' (FORMAT 'parquet');
COPY part FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/part.parquet' (FORMAT 'parquet');
COPY partsupp FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/partsupp.parquet' (FORMAT 'parquet');
COPY region FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/region.parquet' (FORMAT 'parquet');
COPY supplier FROM 'C:/Users/ethan/coding_projects/pytrilogy/tests/modeling/tpc_h/memory/supplier.parquet' (FORMAT 'parquet');
