from pathlib import Path

import click

from .dbt_projects import DbtProject, DbtSubProject, DbtProjectHolder


@click.group()
def cli():
    pass


@cli.command(name="connect")
def connect():
    holder = DbtProjectHolder()

    while True:
        path_string = input("Enter the relative path to a dbt project (enter 'done' to finish): ")
        if path_string == "done":
            break

        path = Path(path_string).expanduser().resolve()
        project = DbtProject.from_directory(path)
        holder.register_project(project)

    print(holder.project_map())


@cli.command(name="split")
def split():
    path_string = input("Enter the relative path to a dbt project you'd like to split: ")

    holder = DbtProjectHolder()

    path = Path(path_string).expanduser().resolve()
    project = DbtProject.from_directory(path)
    holder.register_project(project)

    while True:
        subproject_name = input("Enter the name for your subproject ('done' to finish): ")
        if subproject_name == "done":
            break
        subproject_selector = input(
            f"Enter the selector that represents the subproject {subproject_name}: "
        )

        subproject: DbtSubProject = project.split(
            project_name=subproject_name, select=subproject_selector
        )
        holder.register_project(subproject)

    print(holder.project_map())
