import functools

import click
from dbt.cli.options import MultiOption

# define common parameters
project_path = click.option(
    "--project-path",
    type=click.Path(exists=True),
    default=".",
    help="The path to the dbt project to operate on. Defaults to the current directory.",
)

create_path = click.option(
    "--create-path",
    type=click.Path(exists=True),
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
