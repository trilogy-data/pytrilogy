from datetime import datetime
from pathlib import Path as PathlibPath
from typing import Iterable

from click import UNPROCESSED, Path, argument, group, option, pass_context

from trilogy import Executor, parse
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.models.environment import Environment
from trilogy.dialect.enums import Dialects
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.render import Renderer


def smart_convert(value: str):
    """Convert string to appropriate Python type."""
    if not value:
        return value

    # Handle booleans
    if value.lower() in ("true", "false"):
        return value.lower() == "true"

    # Try numeric conversion
    try:
        if "." not in value and "e" not in value.lower():
            return int(value)
        return float(value)
    except ValueError:
        return value


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


def pairwise(t):
    it = iter(t)
    return zip(it, it)


def extra_to_kwargs(arg_list: Iterable[str]) -> dict[str, str | int]:
    pairs = pairwise(arg_list)
    final = {}
    for k, v in pairs:
        k = k.lstrip("--")
        final[k] = v
    return final


def parse_env_params(env_param_list: tuple[str]) -> dict[str, str]:
    """Parse environment parameters from key=value format."""
    env_params = {}
    for param in env_param_list:
        if "=" not in param:
            raise ValueError(
                f"Environment parameter must be in key=value format: {param}"
            )
        key, value = param.split("=", 1)  # Split on first = only
        env_params[key] = smart_convert(value)
    return env_params


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


@cli.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@argument("dialect", type=str)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(ctx, input, dialect: str, param, conn_args):
    if PathlibPath(input).exists():
        inputp = PathlibPath(input)
        with open(input, "r") as f:
            script = f.read()
        namespace = DEFAULT_NAMESPACE
        directory = inputp.parent
    else:
        script = input
        namespace = DEFAULT_NAMESPACE
        directory = PathlibPath.cwd()

    edialect = Dialects(dialect)
    debug = ctx.obj["DEBUG"]

    # Parse environment parameters from dedicated flag
    env_params = parse_env_params(param)

    # Parse connection arguments from remaining args
    conn_dict = extra_to_kwargs(conn_args)

    if edialect == Dialects.DUCK_DB:
        from trilogy.dialect.config import DuckDBConfig

        conf = DuckDBConfig(**conn_dict)  # type: ignore
    elif edialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.config import SnowflakeConfig

        conf = SnowflakeConfig(**conn_dict)  # type: ignore
    elif edialect == Dialects.SQL_SERVER:
        from trilogy.dialect.config import SQLServerConfig

        conf = SQLServerConfig(**conn_dict)  # type: ignore
    elif edialect == Dialects.POSTGRES:
        from trilogy.dialect.config import PostgresConfig

        conf = PostgresConfig(**conn_dict)  # type: ignore
    else:
        conf = None

    # Create environment and set additional parameters if any exist
    environment = Environment(working_path=str(directory), namespace=namespace)
    if env_params:
        environment.set_parameters(**env_params)

    exec = Executor(
        dialect=edialect,
        engine=edialect.default_engine(conf=conf),
        environment=environment,
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
            print("Install tabulate (pip install tabulate) for a prettier output")
            print(", ".join(results.keys()))
            for row in results:
                print(row)
            print("---")
    print(f"Completed all in {(datetime.now()-start)}")


if __name__ == "__main__":
    cli()
