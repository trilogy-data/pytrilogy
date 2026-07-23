"""Capture v4 BuildInfo for the window-key union join via the real pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from discovery_v4 import write_diagnostics  # noqa: E402
from repro_window_join import CUSTOMERS, ORDERS, QUERY

import trilogy.core.query_processor as qp
from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

captured = []
_orig = qp.search_concepts_v4


def capture(*args, **kwargs):
    info = _orig(*args, **kwargs)
    captured.append(info)
    return info


qp.search_concepts_v4 = capture


def main() -> None:
    root = Path(sys.argv[1]).resolve()
    out_dir = Path(sys.argv[2]).resolve()
    (root / "orders.preql").write_text(ORDERS)
    (root / "customers.preql").write_text(CUSTOMERS)
    CONFIG.use_v4_discovery = True
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=root))
    try:
        stmts = eng.parse_text(QUERY)
        print(eng.generator.compile_statement(stmts[-1]))
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {str(e)[:500]}")
    if captured:
        write_diagnostics(captured[0], "wjoin", out_dir)
    else:
        print("no BuildInfo captured")


if __name__ == "__main__":
    main()
