import sys
from pathlib import Path
from trilogy.core.query_processor import process_query
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.parse_engine_v2 import parse_text
ROOT = Path("tests/modeling/tpc_ds_duckdb")
text = Path(sys.argv[1]).read_text()
env, parsed = parse_text(text, root=ROOT)
try:
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    print("OK", "INVALID" if "INVALID_REFERENCE_BUG" in sql else "clean")
except Exception as e:
    print(type(e).__name__, "::", str(e)[:160])
