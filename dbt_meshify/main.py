import os
import sys
from itertools import combinations
from pathlib import Path
from typing import List, Optional

import click
import yaml
from dbt.contracts.graph.unparsed import Owner
from loguru import logger

from dbt_meshify.storage.dbt_project_editors import DbtSubprojectCreator

from .cli import (
    TupleCompatibleCommand,
    create_path,
    exclude,
    exclude_projects,
    group_yml_path,
    owner,
    owner_email,
    owner_name,
    owner_properties,
    project_path,
    project_paths,
    projects_dir,
    read_catalog,
    select,
    selector,
)
from .dbt_projects import DbtProject, DbtProjectHolder
from .exceptions import FatalMeshifyException
from .linker import Linker
from .storage.file_content_editors import DbtMeshConstructor

log_format = "<white>{time:HH:mm:ss}</white> | <level>{level}</level> | <level>{message}</level>"
logger.remove()  # Remove the default sink added by Loguru
logger.add(sys.stdout, format=log_format)


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
@project_paths
@projects_dir
@exclude_projects
@read_catalog
def connect(
    project_paths: tuple, projects_dir: Path, exclude_projects: List[str], read_catalog: bool
):
    """
    Connects multiple dbt projects together by adding all necessary dbt Mesh constructs
    """
    if project_paths and projects_dir:
        raise click.BadOptionUsage(
            option_name="project_paths",
            message="Cannot specify both project_paths and projects_dir",
        )
    # 1. initialize all the projects supplied to the command
    # 2. compute the dependency graph between each combination of 2 projects in that set
    # 3. for each dependency, add the necessary dbt Mesh constructs to each project.
    #    This includes:
    #    - adding the dependency to the dependencies.yml file of the downstream project
    #    - adding contracts and public access to the upstream models
    #    - deleting the source definition of the upstream models in the downstream project
    #    - updating the `{{ source }}` macro in the downstream project to a {{ ref }} to the upstream project

    linker = Linker()
    if project_paths:
        dbt_projects = [
            DbtProject.from_directory(project_path, read_catalog) for project_path in project_paths
        ]

    if projects_dir:
        dbt_project_paths = [path.parent for path in Path(projects_dir).glob("**/dbt_project.yml")]
        all_dbt_projects = [
            DbtProject.from_directory(project_path, read_catalog)
            for project_path in dbt_project_paths
        ]
        dbt_projects = [
            project for project in all_dbt_projects if project.name not in exclude_projects
        ]

    project_map = {project.name: project for project in dbt_projects}
    dbt_project_combinations = [combo for combo in combinations(dbt_projects, 2)]
    all_dependencies = set()
    for dbt_project_combo in dbt_project_combinations:
        dependencies = linker.dependencies(dbt_project_combo[0], dbt_project_combo[1])
        if len(dependencies) == 0:
            logger.info(
                f"No dependencies found between {dbt_project_combo[0].name} and {dbt_project_combo[1].name}"
            )
            continue

        noun = "dependency" if len(dependencies) == 1 else "dependencies"
        logger.info(
            f"Found {len(dependencies)} {noun} between {dbt_project_combo[0].name} and {dbt_project_combo[1].name}"
        )
        all_dependencies.update(dependencies)
    if len(all_dependencies) == 0:
        logger.info("No dependencies found between any of the projects")
        return

    noun = "dependency" if len(all_dependencies) == 1 else "dependencies"
    logger.info(f"Found {len(all_dependencies)} unique {noun} between all projects.")
    for dependency in all_dependencies:
        logger.info(
            f"Resolving dependency between {dependency.upstream_resource} and {dependency.downstream_resource}"
        )
        try:
            linker.resolve_dependency(
                dependency,
                project_map[dependency.upstream_project_name],
                project_map[dependency.downstream_project_name],
            )
        except Exception as e:
            raise FatalMeshifyException(f"Error resolving dependency : {dependency} {e}")


@cli.command(
    cls=TupleCompatibleCommand,
    name="split",
)
@create_path
@click.argument("project_name")
@exclude
@project_path
@read_catalog
@select
@selector
@click.pass_context
def split(ctx, project_name, select, exclude, project_path, selector, create_path, read_catalog):
    """
    Splits out a new subproject from a dbt project by adding all necessary dbt Mesh constructs to the resources based on the selected resources.

    """
    path = Path(project_path).expanduser().resolve()
    project = DbtProject.from_directory(path, read_catalog)

    subproject = project.split(
        project_name=project_name, select=select, exclude=exclude, selector=selector
    )
    logger.info(f"Selected {len(subproject.resources)} resources: {subproject.resources}")
    if subproject.is_project_cycle:
        raise FatalMeshifyException(
            f"Cannot create subproject {project_name} from {project.name} because it would create a project dependency cycle. Try adding a `+` to your selection syntax to ensure all upstream resources are properly selected"
        )
    if create_path:
        create_path = Path(create_path).expanduser().resolve()
        create_path.parent.mkdir(parents=True, exist_ok=True)

    subproject_creator = DbtSubprojectCreator(project=subproject, target_directory=create_path)
    logger.info(f"Creating subproject {subproject.name}...")
    try:
        subproject_creator.initialize()
        logger.success(f"Successfully created subproject {subproject.name}")
    except Exception as e:
        raise FatalMeshifyException(f"Error creating subproject {subproject.name}: error {e}")


@operation.command(name="add-contract")
@exclude
@project_path
@read_catalog
@select
@selector
def add_contract(select, exclude, project_path, selector, read_catalog, public_only=False):
    """
    Adds a contract to all selected models.
    """
    path = Path(project_path).expanduser().resolve()
    logger.info(f"Reading dbt project at {path}")
    project = DbtProject.from_directory(path, read_catalog)
    resources = list(
        project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )
    )

    logger.info(f"Selected {len(resources)} resources: {resources}")
    models = filter(lambda x: x.startswith("model"), resources)
    if public_only:
        models = filter(lambda x: project.get_manifest_node(x).access == "public", models)
    logger.info(f"Adding contracts to models in selected resources...")
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        model_catalog = project.get_catalog_entry(model_unique_id)
        meshify_constructor = DbtMeshConstructor(
            project_path=project_path, node=model_node, catalog=model_catalog
        )
        logger.info(f"Adding contract to model: {model_unique_id}")
        try:
            meshify_constructor.add_model_contract()
            logger.success(f"Successfully added contract to model: {model_unique_id}")
        except Exception as e:
            raise FatalMeshifyException(f"Error adding contract to model: {model_unique_id}")


@operation.command(name="add-version")
@exclude
@project_path
@read_catalog
@select
@selector
@click.option("--prerelease", "--pre", default=False, is_flag=True)
@click.option("--defined-in", default=None)
def add_version(select, exclude, project_path, selector, prerelease, defined_in, read_catalog):
    """
    Adds/increments model versions for all selected models.
    """
    path = Path(project_path).expanduser().resolve()

    logger.info(f"Reading dbt project at {path}")
    project = DbtProject.from_directory(path, read_catalog)
    resources = list(
        project.select_resources(
            select=select, exclude=exclude, selector=selector, output_key="unique_id"
        )
    )
    models = filter(lambda x: x.startswith("model"), resources)
    logger.info(f"Selected {len(resources)} resources: {resources}")
    logger.info(f"Adding version to models in selected resources...")
    for model_unique_id in models:
        model_node = project.get_manifest_node(model_unique_id)
        if model_node.version == model_node.latest_version:
            meshify_constructor = DbtMeshConstructor(project_path=project_path, node=model_node)
            try:
                meshify_constructor.add_model_version(prerelease=prerelease, defined_in=defined_in)
                logger.success(f"Successfully added version to model: {model_unique_id}")
            except Exception as e:
                raise FatalMeshifyException(
                    f"Error adding version to model: {model_unique_id}"
                ) from e


@operation.command(
    name="create-group",
    cls=TupleCompatibleCommand,
)
@click.argument("name")
@exclude
@group_yml_path
@owner
@owner_email
@owner_name
@owner_properties
@project_path
@read_catalog
@select
@selector
def create_group(
    name,
    project_path: os.PathLike,
    group_yml_path: os.PathLike,
    select: str,
    read_catalog: bool,
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
    logger.info(f"Reading dbt project at {path}")
    project = DbtProject.from_directory(path, read_catalog)

    if group_yml_path is None:
        group_yml_path = (path / Path("models/_groups.yml")).resolve()
    else:
        group_yml_path = Path(group_yml_path).resolve()
    logger.info(f"Creating new model group in file {group_yml_path.name}")

    if not str(os.path.commonpath([group_yml_path, path])) == str(path):
        raise FatalMeshifyException(
            "The provided group-yml-path is not contained within the provided dbt project."
        )

    group_owner: Owner = Owner(
        name=owner_name, email=owner_email, _extra=yaml.safe_load(owner_properties or "{}")
    )

    grouper = ResourceGrouper(project)
    try:
        grouper.add_group(
            name=name,
            owner=group_owner,
            select=select,
            exclude=exclude,
            selector=selector,
            path=group_yml_path,
        )
        logger.success(f"Successfully created group: {name}")
    except Exception as e:
        raise FatalMeshifyException(f"Error creating group: {name}")


@cli.command(name="group", cls=TupleCompatibleCommand)
@click.argument("name")
@exclude
@group_yml_path
@owner
@owner_email
@owner_name
@owner_properties
@project_path
@read_catalog
@select
@selector
@click.pass_context
def group(
    ctx,
    name,
    project_path: os.PathLike,
    group_yml_path: os.PathLike,
    select: str,
    read_catalog: bool,
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
    ctx.invoke(add_contract, select=f"group:{name}", project_path=project_path, public_only=True)
