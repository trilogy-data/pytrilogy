"""Record one statement's discovery as a plan trace and open it in the viewer.

    python local_scripts/plan_debugger/trace_query.py examples/customers_orders.preql
    python local_scripts/plan_debugger/trace_query.py q.preql --index 0 --rows --open
    python local_scripts/plan_debugger/trace_query.py --text "select name, count(order_id) as n;" --model m.preql

Writes `<stem>.trace.json` beside the source (or `--out`) and, unless
`--no-html`, a self-contained `<stem>.trace.html` (the viewer with the trace
embedded) that opens with no server. `viewer.html` alone also loads any
trace JSON through its file picker or by drag and drop.

The trace covers every phase of `process_query` for the chosen statement
(the last SELECT by default): requested concepts, concept graph, keyspace,
grouping passes, group graph passes, every source search, every group's
node, the FINAL assembly, the resolved datasource tree, the CTEs before and
after optimization, and the rendered SQL. `--rows` executes the SQL on
DuckDB and stores the result too.
"""

from __future__ import annotations

import argparse
import json
import sys
import webbrowser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trilogy import Environment
from trilogy.core.processing import plan_trace
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import (
    MultiSelectStatement,
    SelectStatement,
)
from trilogy.dialect.enums import Dialects

VIEWER = Path(__file__).with_name("viewer.html")
EMBED_MARKER = "<!--TRACE-->"


def _selects(statements: list) -> list[SelectStatement | MultiSelectStatement]:
    return [
        s for s in statements if isinstance(s, (SelectStatement, MultiSelectStatement))
    ]


def run(
    text: str,
    working_path: Path,
    index: int | None,
    dialect: Dialects,
    rows: bool,
) -> dict:
    env = Environment(working_path=working_path)
    env, statements = env.parse(text)
    selects = _selects(statements)
    if not selects:
        raise SystemExit("no SELECT statement to trace")
    statement = selects[index if index is not None else -1]
    renderer = dialect.default_renderer()
    trace = plan_trace.start(text)
    try:
        processed = process_query(
            env,
            statement,
            having_alias=renderer.SUPPORTS_ALIAS_IN_HAVING,
            supports_full_join=renderer.SUPPORTS_FULL_JOIN,
        )
        sql = renderer.compile_statement(processed)
        result_rows: list | None = None
        columns: list[str] | None = None
        if rows:
            executor = dialect.default_executor(environment=env)
            cursor = executor.execute_raw_sql(sql)
            columns = list(cursor.keys())
            result_rows = [list(r) for r in cursor.fetchall()]
        plan_trace.record(
            "sql",
            f"{dialect.value} SQL",
            dialect=dialect.value,
            statement_index=selects.index(statement),
            outputs=[c.address for c in statement.output_components],
            ctes={c.name: renderer.render_cte(c).statement for c in processed.ctes},
            sql=sql,
            columns=columns,
            rows=result_rows,
        )
    finally:
        plan_trace.stop()
    return trace.to_dict()


def embed(trace: dict, out: Path) -> Path:
    payload = json.dumps(trace, ensure_ascii=False).replace("</", "<\\/")
    html = VIEWER.read_text(encoding="utf-8").replace(
        EMBED_MARKER,
        f'<script id="trace-data" type="application/json">{payload}</script>',
    )
    out.write_text(html, encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "source", nargs="?", help="a .preql file; its last SELECT is traced"
    )
    parser.add_argument("--text", help="statement text to trace instead of a file")
    parser.add_argument("--model", help="a .preql file parsed before --text")
    parser.add_argument("--index", type=int, help="which SELECT of the file (0-based)")
    parser.add_argument(
        "--dialect", default="duck_db", choices=[d.value for d in Dialects]
    )
    parser.add_argument("--out", help="trace JSON path")
    parser.add_argument(
        "--rows", action="store_true", help="execute on the dialect and keep the rows"
    )
    parser.add_argument(
        "--no-html", action="store_true", help="skip the self-contained viewer"
    )
    parser.add_argument(
        "--open", action="store_true", help="open the viewer in a browser"
    )
    args = parser.parse_args()

    if args.source:
        source = Path(args.source)
        text = source.read_text(encoding="utf-8")
        working_path = source.parent
        stem = source.with_suffix("")
    elif args.text:
        model = Path(args.model).read_text(encoding="utf-8") if args.model else ""
        text = f"{model}\n{args.text}"
        working_path = Path(args.model).parent if args.model else Path.cwd()
        stem = Path.cwd() / "adhoc"
    else:
        parser.error("give a source file or --text")

    trace = run(text, working_path, args.index, Dialects(args.dialect), args.rows)
    out = Path(args.out) if args.out else stem.with_suffix(".trace.json")
    out.write_text(json.dumps(trace, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"trace: {out} ({len(trace['steps'])} steps, {len(trace['plans'])} plans)")
    if not args.no_html:
        html = embed(trace, out.with_suffix(".html"))
        print(f"viewer: {html}")
        if args.open:
            webbrowser.open(html.resolve().as_uri())


if __name__ == "__main__":
    main()
