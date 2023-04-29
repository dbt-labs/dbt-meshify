from typing import Dict
import json
import click
from pathlib import Path

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


@cli.command(name="contract")
@click.option(
    "--select",
    "-s"
    )
@click.option(
    "--exclude",
    "-e"
    )
@click.option(
    "--project-path",
    default="."
)
def contract(select, exclude, project_path):
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(project.select_resources(select=select, exclude=exclude, output_key="unique_id"))
    models = filter(lambda x: x.startswith('model'), resources)
    for model_unique_id in models:
        project.add_model_contract(model_unique_id)