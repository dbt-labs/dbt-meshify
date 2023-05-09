import os
from pathlib import Path
from typing import Optional, List, Tuple

import click
from dbt.contracts.graph.unparsed import Owner

from .dbt_projects import DbtProject, DbtSubProject, DbtProjectHolder
from .storage.yaml_generator import  ProjectYamlStorage


@click.group()
def cli():
    pass


@cli.command()
@click.argument("name")
@click.option("--select", "-s", required=True)
@click.option("--exclude", "-e")
@click.option("--project-path", default=os.getcwd(), type=click.Path(exists=True))
@click.option("--owner", nargs=2, multiple=True, type=click.Tuple([str, str]))
def group(
    name,
    project_path: os.PathLike,
    owner: List[Tuple[str, str]],
    select: str,
    exclude: Optional[str] = None,
):
    """Add a dbt group to your project, and assign models to the new group using Node Selection syntax."""

    from dbt_meshify.utilities.grouper import ResourceGrouper

    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)

    owner: Owner = Owner(**{key: value for key, value in owner})

    grouper = ResourceGrouper(project)
    output_project = grouper.add_group(name=name, owner=owner, select=select, exclude=exclude)

    storage = ProjectYamlStorage(project_path=path)
    storage.store(output_project)


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
@click.option("--select", "-s")
@click.option("--exclude", "-e")
@click.option("--project-path", default=".")
def contract(select, exclude, project_path):
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(
        project.select_resources(select=select, exclude=exclude, output_key="unique_id")
    )
    models = filter(lambda x: x.startswith("model"), resources)
    for model_unique_id in models:
        project.add_model_contract(model_unique_id)
