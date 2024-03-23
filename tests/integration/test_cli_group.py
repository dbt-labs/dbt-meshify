from click.testing import CliRunner

from dbt_meshify.cli import get_version
from dbt_meshify.main import cli


def test_version_option():
    """When the --version option is passed, meshify returns the version and exits."""

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--version",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout.rstrip() == get_version()


def test_no_command_prints_help():
    """When an invoked subcommand is not provided, print the help and exit."""

    runner = CliRunner()
    result = runner.invoke(cli)

    assert result.exit_code == 0

    help_result = result = runner.invoke(cli, ["--help"])
    assert result.stdout == help_result.stdout
