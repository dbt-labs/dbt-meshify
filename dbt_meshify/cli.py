import functools

import click
from click import Context, HelpFormatter
from dbt.cli.options import MultiOption

# define common parameters
project_path = click.option(
    "--project-path",
    type=click.Path(exists=True),
    default=".",
    help="The path to the dbt project to operate on. Defaults to the current directory.",
)

project_paths = click.option(
    "--project-paths",
    cls=MultiOption,
    multiple=True,
    type=tuple,
    default=None,
    help="The paths to the set of dbt projects to connect. Must supply 2+ paths.",
)

projects_dir = click.option(
    "--projects-dir",
    type=click.Path(exists=True),
    default=None,
    help="The path to a directory containing multiple dbt projects. Directory must contain 2+ projects.",
)

exclude_projects = click.option(
    "--exclude-projects",
    "-e",
    cls=MultiOption,
    multiple=True,
    type=tuple,
    default=None,
    help="The set of dbt projects to exclude from the operation when using the --projects-dir option.",
)

create_path = click.option(
    "--create-path",
    type=click.Path(exists=False),
    default=None,
    help="The path to create the new dbt project. Defaults to the name argument supplied.",
)

exclude = click.option(
    "--exclude",
    "-e",
    cls=MultiOption,
    multiple=True,
    type=tuple,
    default=None,
    help="The dbt selection syntax specifying the resources to exclude in the operation",
)

group_yml_path = click.option(
    "--group-yml-path",
    type=click.Path(exists=False),
    help="An optional path to store the new group YAML definition.",
)

select = click.option(
    "--select",
    "-s",
    cls=MultiOption,
    multiple=True,
    type=tuple,
    default=None,
    help="The dbt selection syntax specifying the resources to include in the operation",
)

selector = click.option(
    "--selector",
    cls=MultiOption,
    multiple=True,
    type=tuple,
    default=None,
    help="The name(s) of the YML selector specifying the resources to include in the operation",
)

owner_name = click.option(
    "--owner-name",
    help="The group Owner's name.",
)

owner_email = click.option(
    "--owner-email",
    help="The group Owner's email address.",
)

owner_properties = click.option(
    "--owner-properties",
    help="Additional properties to assign to a group Owner.",
)

read_catalog = click.option(
    "--read-catalog",
    "-r",
    is_flag=True,
    envvar="DBT_MESHIFY_READ_CATALOG",
    help="Skips the dbt docs generate step and reads the local catalog.json file.",
)

latest = click.option(
    "--latest",
    "-l",
    is_flag=True,
    help="Makes the newest version the latest version when incrementing model versions",
)


def owner(func):
    """Add click options and argument validation for creating Owner objects."""

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        if kwargs.get('owner_name') is None and kwargs.get('owner_email') is None:
            raise click.UsageError(
                "Groups require an Owner to be defined using --owner-name and/or --owner-email."
            )
        return func(*args, **kwargs)

    return wrapper_decorator


class TupleCompatibleCommand(click.Command):
    """
    A Click Command with a custom formatter that adds metavar options after all arguments.
    This is valuable for commands that use tuple-type options, since type types will eat
    arguments.
    """

    def format_usage(self, ctx: Context, formatter: HelpFormatter) -> None:
        pieces = self.collect_usage_pieces(ctx)
        pieces = pieces[1:] + [pieces[0]]
        formatter.write_usage(ctx.command_path, " ".join(pieces))
