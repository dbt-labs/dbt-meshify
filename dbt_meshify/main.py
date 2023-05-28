import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
from dbt.contracts.graph.unparsed import Owner

from .dbt_projects import DbtProject, DbtProjectHolder, DbtSubProject
from .storage.yaml_editors import DbtMeshModelConstructor

# define common parameters
project_path = click.option("--project-path", type=click.Path(exists=True), default=".")

exclude = click.option(
    "--exclude",
    "-e",
    default=None,
    help="The dbt selection syntax specifying the resources to exclude in the operation",
)

select = click.option(
    "--select",
    "-s",
    default=None,
    help="The dbt selection syntax specifying the resources to include in the operation",
)

selector = click.option(
    "--selector",
    default=None,
    help="The name of the YML selector specifying the resources to include in the operation",
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
        project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )
    )
    models = filter(lambda x: x.startswith("model"), resources)
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        model_catalog = project.get_catalog_entry(model_unique_id)
        meshify_constructor = DbtMeshModelConstructor(
            project_path=project_path, model_node=model_node, model_catalog=model_catalog
        )
        meshify_constructor.add_model_contract()


@cli.command(name="add-version")
@exclude
@project_path
@select
@selector
@click.option("--prerelease", "--pre", default=False, is_flag=True)
@click.option("--defined-in", default=None)
def add_version(select, exclude, project_path, selector, prerelease, defined_in):
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)
    resources = list(
        project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )
    )
    models = filter(lambda x: x.startswith("model"), resources)
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        if model_node.version == model_node.latest_version:
            meshify_constructor = DbtMeshModelConstructor(
                project_path=project_path, model_node=model_node
            )
            meshify_constructor.add_model_version(prerelease=prerelease, defined_in=defined_in)


@cli.command(name="create-group")
@exclude
@project_path
@select
@selector
@click.argument("name")
@click.option(
    "--owner",
    nargs=2,
    multiple=True,
    type=click.Tuple([str, str]),
    help="A tuple of Owner information for the group. For example " "`--owner name example`",
)
@click.option(
    "--group-yml-path",
    type=click.Path(exists=False),
    help="An optional path to store the new group YAML definition.",
)
def create_group(
    name,
    project_path: os.PathLike,
    owner: List[Tuple[str, str]],
    group_yml_path: os.PathLike,
    select: str,
    exclude: Optional[str] = None,
    selector: Optional[str] = None,
):
    """
    Create a group and add selected resources to the group.
    """

    from dbt_meshify.utilities.grouper import ResourceGrouper

    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path)

    if group_yml_path is None:
        group_yml_path = (path / Path("models/_groups.yml")).resolve()
    else:
        group_yml_path = Path(group_yml_path).resolve()

    if not str(os.path.commonpath([group_yml_path, path])) == str(path):
        raise Exception(
            "The provided group-yml-path is not contained within the provided dbt project."
        )

    owner_dict: Dict[str, Any] = {key: value for key, value in owner}
    owner_object: Owner = Owner(**owner_dict)

    grouper = ResourceGrouper(project)
    grouper.add_group(
        name=name, owner=owner_object, select=select, exclude=exclude, path=group_yml_path
    )
