"""The click options must collect exactly what the argparse fast path does.

They decode through the same `request_from_strings`, so what these guard is the
flag set: a field the argparse parser accepts and click does not is a source
that cannot be asked for it.
"""

import click
import pytest
from click.testing import CliRunner

from trilogy.io.click_support import click_options, request_from_kwargs
from trilogy.io.contract import Filter, Sort, SourceRequest
from trilogy.io.errors import ContractError
from trilogy.io.runner import build_parser


@pytest.fixture
def collected() -> list[SourceRequest]:
    return []


@pytest.fixture
def command(collected: list[SourceRequest]):
    @click.command()
    @click_options
    def source(**kwargs):
        collected.append(request_from_kwargs(**kwargs))
        click.echo(kwargs["fmt"])
        click.echo(kwargs["output"] or "-")

    return source


def test_click_offers_every_flag_the_argparse_parser_does():
    argparse_flags = {
        option
        for action in build_parser()._actions
        for option in action.option_strings
        if option.startswith("--")
    } - {"--help", "--describe"}
    click_flags = {
        option
        for parameter in click_options(lambda **kwargs: None).__click_params__
        for option in parameter.opts
    }
    assert argparse_flags <= click_flags


def test_options_collect_a_whole_request(command, collected: list[SourceRequest]):
    result = CliRunner().invoke(
        command,
        [
            "--limit",
            "5",
            "--columns",
            "i, state",
            "--filter",
            "state = CA",
            "--filter",
            "i >= 2",
            "--order-by",
            "state:desc,i",
            "--since",
            "2026-01-01",
            "--partition",
            "day=2026-01-01",
            "--format",
            "csv",
            "--output",
            "out.csv",
        ],
    )
    assert result.exit_code == 0, result.output
    assert collected == [
        SourceRequest(
            limit=5,
            columns=("i", "state"),
            filters=(Filter("state", "=", "CA"), Filter("i", ">=", 2)),
            order_by=(Sort("state", True), Sort("i", False)),
            since="2026-01-01",
            partition={"day": "2026-01-01"},
        )
    ]
    assert result.output.splitlines() == ["csv", "out.csv"]


def test_no_flags_is_an_empty_request(command, collected: list[SourceRequest]):
    result = CliRunner().invoke(command, [])
    assert result.exit_code == 0, result.output
    assert collected == [SourceRequest()]
    assert result.output.splitlines() == ["arrow", "-"]


def test_format_is_constrained_to_the_supported_sinks(command):
    assert CliRunner().invoke(command, ["--format", "avro"]).exit_code != 0


def test_a_malformed_partition_is_a_contract_error():
    with pytest.raises(ContractError, match="--partition expects KEY=VALUE"):
        request_from_kwargs(partition=("day",))


def test_request_from_kwargs_tolerates_absent_keys():
    assert request_from_kwargs() == SourceRequest()
