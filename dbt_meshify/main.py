import os
from pathlib import Path
from typing import Optional

import click
import yaml
from dbt.contracts.graph.unparsed import Owner

from .cli import (
    exclude,
    group_yml_path,
    owner,
    owner_email,
    owner_name,
    owner_properties,
    project_path,
    select,
    selector,
)
from .dbt_projects import DbtProject, DbtProjectHolder, DbtSubProject
from .storage.yaml_editors import DbtMeshModelConstructor


# define cli group
@click.group()
def cli():
    pass


@cli.group()
def operation():
    """
    Set of subcommands for performing mesh operations on dbt projects
    """
    pass


@cli.command(name="connect")
@click.argument("projects-dir", type=click.Path(exists=True), default=".")
def connect(projects_dir):
    """
    !!! info
        This command is not yet implemented

    Connects multiple dbt projects together by adding all necessary dbt Mesh constructs
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
    !!! info
        This command is not yet implemented

    Splits dbt projects apart by adding all necessary dbt Mesh constructs based on the selection syntax.

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


@operation.command(name="add-contract")
@exclude
@project_path
@select
@selector
def add_contract(select, exclude, project_path, selector, public_only=False):
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
    if public_only:
        models = filter(lambda x: project.get_manifest_node(x).access == "public", models)
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        model_catalog = project.get_catalog_entry(model_unique_id)
        meshify_constructor = DbtMeshModelConstructor(
            project_path=project_path, model_node=model_node, model_catalog=model_catalog
        )
        meshify_constructor.add_model_contract()


@operation.command(name="add-version")
@exclude
@project_path
@select
@selector
@click.option("--prerelease", "--pre", default=False, is_flag=True)
@click.option("--defined-in", default=None)
def add_version(select, exclude, project_path, selector, prerelease, defined_in):
    """
    Adds/increments model versions for all selected models.
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
        if model_node.version == model_node.latest_version:
            meshify_constructor = DbtMeshModelConstructor(
                project_path=project_path, model_node=model_node
            )
            meshify_constructor.add_model_version(prerelease=prerelease, defined_in=defined_in)


@operation.command(name="create-group")
@exclude
@project_path
@select
@selector
@click.argument("name")
@owner_name
@owner_email
@owner_properties
@owner
@group_yml_path
def create_group(
    name,
    project_path: os.PathLike,
    group_yml_path: os.PathLike,
    select: str,
    owner_name: Optional[str] = None,
    owner_email: Optional[str] = None,
    owner_properties: Optional[str] = None,
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

    group_owner: Owner = Owner(
        name=owner_name, email=owner_email, _extra=yaml.safe_load(owner_properties or '{}')
    )

    grouper = ResourceGrouper(project)
    grouper.add_group(
        name=name,
        owner=group_owner,
        select=select,
        exclude=exclude,
        selector=selector,
        path=group_yml_path,
    )


@cli.command(name="group")
@exclude
@project_path
@select
@selector
@click.argument("name")
@owner_name
@owner_email
@owner_properties
@owner
@group_yml_path
@click.pass_context
def group(
    ctx,
    name,
    project_path: os.PathLike,
    group_yml_path: os.PathLike,
    select: str,
    owner_name: Optional[str] = None,
    owner_email: Optional[str] = None,
    owner_properties: Optional[str] = None,
    exclude: Optional[str] = None,
    selector: Optional[str] = None,
):
    """
    Creates a new dbt group based on the selection syntax
    Detects the edges of the group, makes their access public, and adds contracts to them
    """
    ctx.forward(create_group)
    ctx.invoke(add_contract, select=f'group:{name}', project_path=project_path, public_only=True)
