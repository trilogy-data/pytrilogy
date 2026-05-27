"""Fixture module used by ``test_click_utils`` to exercise LazyGroup's
plain-callable wrapping path. Kept in its own module so the lazy importer
has a real attribute to look up."""

import click


def _say_hi():
    click.echo("hi")


@click.command()
def _premade_cmd():
    click.echo("premade")
