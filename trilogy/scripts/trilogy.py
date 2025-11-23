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
        env_params[key] = value
    return env_params


def separate_conn_and_env_args(
    args: tuple[str], dialect: Dialects
) -> tuple[dict[str, str | int], dict[str, str | int]]:
    """
    Separates connection arguments from environment parameters.
    Connection args are dialect-specific, environment args are everything else.
    """
    # Define known connection argument keys for each dialect
    conn_arg_keys = {
        Dialects.DUCK_DB: {"database", "path", "config", "read_only"},
        Dialects.SNOWFLAKE: {
            "account",
            "user",
            "password",
            "database",
            "schema",
            "warehouse",
            "role",
        },
        Dialects.SQL_SERVER: {
            "server",
            "database",
            "username",
            "password",
            "driver",
            "trusted_connection",
        },
        Dialects.POSTGRES: {"host", "port", "database", "user", "password", "sslmode"},
    }

    all_args = extra_to_kwargs(args)
    dialect_conn_keys = conn_arg_keys.get(dialect, set())

    conn_args = {}
    env_args = {}

    for key, value in all_args.items():
        if key in dialect_conn_keys:
            conn_args[key] = value
        else:
            env_args[key] = value

    return conn_args, env_args


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
@option("--env", multiple=True, help="Environment parameters as key=value pairs")
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(ctx, input, dialect: str, env, conn_args):
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
    env_params = parse_env_params(env)

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
