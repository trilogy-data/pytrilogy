"""Progressive-disclosure documentation router for AI agents."""

import click

from trilogy.ai.syntax_examples import example_index, render_example
from trilogy.scripts.agent_info_docs.cli import CLI_DOC
from trilogy.scripts.agent_info_docs.content import (
    CONFIG_DOC,
    DATASOURCES_DOC,
    INGEST_DOC,
    REPORT_FORMAT_DOC,
    SERVE_DOC,
    STATE_DOC,
    get_authoring_output,
    get_query_authoring_output,
)
from trilogy.scripts.agent_info_docs.directory import AGENT_INFO_DIRECTORY


def get_agent_info_output() -> str:
    return AGENT_INFO_DIRECTORY


@click.group(invoke_without_command=True)
@click.pass_context
def agent_info(ctx: click.Context) -> None:
    """Route AI agents to focused Trilogy documentation."""
    if ctx.invoked_subcommand is None:
        print(AGENT_INFO_DIRECTORY)


@agent_info.command("query")
def agent_info_query() -> None:
    print(get_query_authoring_output())


@agent_info.command("authoring")
def agent_info_authoring() -> None:
    print(get_authoring_output())


@agent_info.command("cli")
def agent_info_cli() -> None:
    print(CLI_DOC)


@agent_info.command("report")
def agent_info_report() -> None:
    print(REPORT_FORMAT_DOC)


@agent_info.command("datasources")
def agent_info_datasources() -> None:
    print(DATASOURCES_DOC)


@agent_info.command("ingest")
def agent_info_ingest() -> None:
    print(INGEST_DOC)


@agent_info.command("config")
def agent_info_config() -> None:
    print(CONFIG_DOC)


@agent_info.command("serve")
def agent_info_serve() -> None:
    print(SERVE_DOC)


@agent_info.command("state")
def agent_info_state() -> None:
    print(STATE_DOC)


@agent_info.group("syntax", invoke_without_command=True)
@click.pass_context
def agent_info_syntax(ctx: click.Context) -> None:
    if ctx.invoked_subcommand is None:
        print(example_index())


@agent_info_syntax.command("example")
@click.argument("name", required=False)
def agent_info_syntax_example(name: str | None) -> None:
    if name is None:
        print(example_index())
        return
    body = render_example(name)
    if body is None:
        print(f"Unknown syntax example: {name!r}\n")
        print(example_index())
        raise SystemExit(2)
    print(body)
