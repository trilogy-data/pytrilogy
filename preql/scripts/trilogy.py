from click import command, Path, argument, option
import os
from sys import path

nb_path = os.path.abspath("")
path.insert(0, nb_path)

from preql import Executor, Environment  # noqa
from preql.dialect.enums import Dialects  # noqa
from datetime import datetime  # noqa
from pathlib import Path as PathlibPath  # noqa
from preql.hooks.query_debugger import DebuggingHook  # noqa


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


@command()
@argument("input", type=Path(exists=True))
@argument("dialect", type=str)
@option("--debug", type=bool, default=False)
def main(input, dialect: str, debug: bool):
    with open(input, "r") as f:
        script = f.read()
    edialect = Dialects(dialect)
    inputp = PathlibPath(input)
    directory = inputp.parent
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
        print(exec.generator.compile_statement(query))
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
    main()
