import click
from pathlib import Path

from .dbt_meshify import Meshify
from .dbt_projects import DbtProjectHolder, DbtProject, DbtSubProject


@click.group()
def cli():
    pass


@cli.command(name="connect")
def connect():
    holder = DbtProjectHolder()
    while True:
        path = input("Enter the relative path to a dbt project (enter 'done' to finish): ")
        if path == "done":
            break
        holder.add_relative_project_path(path)

    print(holder.project_map())


@cli.command(name="split")
def split():
    path_string = input("Enter the relative path to a dbt project you'd like to split: ")

    path = Path(path_string).expanduser().resolve()
    project = DbtProject.from_directory(path)

    meshify = Meshify()

    while True:
        subproject_name = input("Enter the name for your subproject ('done' to finish): ")
        if subproject_name == "done":
            break
        subproject_selector = input(
            f"Enter the selector that represents the subproject {subproject_name}: "
        )
        subproject: DbtSubProject = meshify.create_subproject(
            dbt_project=project, project_name=subproject_name, select=subproject_selector
        )

    print(meshify.relationships)
