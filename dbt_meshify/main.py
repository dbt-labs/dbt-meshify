import os
from pathlib import Path
from typing import Optional, List, Tuple

import click
from dbt.contracts.graph.unparsed import Owner

from .dbt_projects import DbtProject, DbtSubProject, DbtProjectHolder
from .storage.yaml_generator import ProjectYamlStorage

# define common parameters
project_path = click.option(
    "--project-path",
    type=click.Path(exists=True),
    default="."
)

exclude = click.option(
    "--exclude",
    "-e",
    default=None,
    help="The dbt selection syntax specifying the resources to exclude in the operation"
)

select = click.option(
    "--select",
    "-s",
    default=None,
    help="The dbt selection syntax specifying the resources to include in the operation"
)

selector = click.option(
    "--selector",
    default=None,
    help="The name of the YML selector specifying the resources to include in the operation"
)


# define cli group
@click.group()
def cli():
    pass


@cli.command(name="connect")
@click.argument("projects-dir", type=click.Path(exists=True), default=".")
def connect(projects_dir):
    """
    Connects multiple dbt projects together by adding all necessary dbt Mesh constructs

    PROJECTS_DIR: The directory containing the dbt projects to connect. Defaults to the current directory.
    """
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
@exclude
@project_path
@select
@selector
def split():
    """
    Splits dbt projects apart by adding all necessary dbt Mesh constructs based on the selection syntax. 

    Order of operations:
    1. Regsiter the selected resources as a subproject of the main project
    2. Add the resources to a group
    2. Identifies the edges of the subproject with the remainder of the project
    3. Adds contracts to all edges
    """
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


@cli.command(name="add-contract")
@exclude
@project_path
@select
@selector
def add_contract(select, exclude, project_path, selector):
    """
    Adds a contract to all selected models.
    """
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(
        project.select_resources(select=select, exclude=exclude, output_key="unique_id")
    )
    models = filter(lambda x: x.startswith("model"), resources)
    for model_unique_id in models:
        project.add_model_contract(model_unique_id)


@cli.command(name="add-version")
@exclude
@project_path
@select
@selector
def add_version(select, exclude, project_path, selector):
    """
    Increments a model version on all selected models. Increments the version of the model if a version exists. 
    """
    pass


@cli.command(name="create-group")
@exclude
@project_path
@select
@selector
@click.argument("name")
@click.option("--owner", nargs=2, multiple=True, type=click.Tuple([str, str]))
def create_group(
        name,
        project_path: os.PathLike,
        owner: List[Tuple[str, str]],
        select: str,
        exclude: Optional[str] = None,
        selector: Optional[str] = None
):
    """
    Create a group and add selected resources to the group.
    """

    from dbt_meshify.utilities.grouper import ResourceGrouper

    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)

    owner: Owner = Owner(**{key: value for key, value in owner})

    grouper = ResourceGrouper(project)
    output_project = grouper.add_group(name=name, owner=owner, select=select, exclude=exclude)

    storage = ProjectYamlStorage(project_path=path)
    storage.store(output_project)
