from click import Path, argument, option, group, pass_context
from preql import Executor, Environment, parse
from preql.dialect.enums import Dialects
from datetime import datetime
from pathlib import Path as PathlibPath
from preql.hooks.query_debugger import DebuggingHook
from preql.parsing.render import Renderer


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


@group()
@option("--debug", default=False)
@pass_context
def cli(ctx, debug: bool):
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug


@cli.command("fmt")
@argument("input", type=Path(exists=True))
@pass_context
def fmt(ctx, input):
    start = datetime.now()
    with open(input, "r") as f:
        script = f.read()

    _, queries = parse(script)
    r = Renderer()
    with open(input, "w") as f:
        f.write("\n".join([r.to_string(x) for x in queries]))
    print(f"Completed all in {(datetime.now()-start)}")


@cli.command("run")
@argument("input", type=Path())
@argument("dialect", type=str)
@pass_context
def run(ctx, input, dialect: str):
    if PathlibPath(input).exists():
        with open(input, "r") as f:
            script = f.read()
    else:
        script = input
    edialect = Dialects(dialect)
    inputp = PathlibPath(input)
    directory = inputp.parent
    debug = ctx.obj["DEBUG"]
    exec = Executor(
        dialect=edialect,
        engine=edialect.default_engine(),
        environment=Environment(working_path=str(directory), namespace=inputp.stem),
        hooks=[DebuggingHook()] if debug else [],
    )

    queries = exec.parse_text(script)
    start = datetime.now()
    print(f"Executing {len(queries)} statements...")
    for idx, query in enumerate(queries):
        lstart = datetime.now()
        results = exec.execute_statement(query)
        end = datetime.now()
        print(f"Statement {idx+1} of {len(queries)} done, duration: {end-lstart}.")
        if not results:
            continue
        try:
            import tabulate

            print_tabulate(results, tabulate.tabulate)
        except ImportError:
            print(", ".join(results.keys()))
            for row in results:
                print(row)
            print("---")
    print(f"Completed all in {(datetime.now()-start)}")


if __name__ == "__main__":
    cli()
